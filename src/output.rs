use anyhow::Result;
use std::io::{BufWriter, Write};
use tabwriter::TabWriter;

#[derive(Clone, Copy, Debug)]
pub enum OutputFormat {
    Tsv,
    Csv,
    Box,
    Compact,
}


#[derive(Clone, Copy, Debug)]
pub struct OutputConfig {
    pub format: OutputFormat,
    #[allow(dead_code)]
    pub sig_digits: usize,
    pub expand_tabs: bool,
}

pub trait TableSink {
    fn write_header(&mut self, headers: &[String]) -> Result<()>;
    fn write_row(&mut self, row: &[String]) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

// Adaptive float formatter: prefers fixed; falls back to scientific only when needed.
pub fn fmt_float(x: f64, sig_digits: usize) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.is_infinite() {
        return if x.is_sign_negative() { "-inf" } else { "inf" }.into();
    }
    if x == 0.0 {
        return "0".into();
    }

    let sig = sig_digits.max(1);
    let abs = x.abs();
    let exp10 = abs.log10().floor() as i32;

    // Generate scientific notation (always correct for sig digits)
    let p = sig.saturating_sub(1);
    let raw = format!("{:.*e}", p, x);
    let scientific_repr = if let Some(idx) = raw.find('e') {
        let (mant, exp_part) = raw.split_at(idx + 1); // keep the 'e'
        if let Ok(exp_val) = exp_part.parse::<i32>() {
            format!("{mant}{:+03}", exp_val)
        } else {
            raw
        }
    } else {
        raw
    };

    // Generate fixed notation (for a wide range, then compare lengths)
    let fixed_repr = if (-15..=15).contains(&exp10) {
        // Calculate the power needed to shift for significant digits
        let shift = sig as i32 - 1 - exp10;
        
        // Shift, round, and shift back
        let power = 10_f64.powi(shift);
        let rounded = (x * power).round() / power;
        
        // Calculate decimal places to show
        let dec_places = if shift > 0 { shift as usize } else { 0 };
        
        // Format and clean up
        let mut s = format!("{:.*}", dec_places, rounded);
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        
        // Handle edge case of -0
        if s == "-0" {
            s = "0".to_string();
        }
        
        s
    } else {
        String::new()
    };

    // Choose the shorter representation (prefer fixed when equal)
    if !fixed_repr.is_empty() && fixed_repr.len() <= scientific_repr.len() {
        fixed_repr
    } else {
        scientific_repr
    }
}

pub fn make_sink<'a>(
    cfg: OutputConfig,
    writer: &'a mut dyn Write,
) -> Result<Box<dyn TableSink + 'a>> {
    match cfg.format {
        OutputFormat::Tsv => {
            if cfg.expand_tabs {
                Ok(Box::new(TsvSink::Tabs(TabWriter::new(writer))))
            } else {
                Ok(Box::new(TsvSink::Plain(writer)))
            }
        }
        OutputFormat::Csv => {
            let wtr = csv::WriterBuilder::new().from_writer(writer);
            Ok(Box::new(CsvSink { writer: wtr }))
        }
        OutputFormat::Box => Ok(Box::new(BoxSink::new(writer))),
        OutputFormat::Compact => Ok(Box::new(CompactSink::new(writer))),
    }
}

// --- TSV Sink ---
enum TsvSink<'a> {
    Plain(&'a mut dyn Write),
    Tabs(TabWriter<&'a mut dyn Write>),
}

impl<'a> TableSink for TsvSink<'a> {
    fn write_header(&mut self, headers: &[String]) -> Result<()> {
        self.write_row(headers)
    }
    fn write_row(&mut self, row: &[String]) -> Result<()> {
        match self {
            TsvSink::Plain(w) => {
                writeln!(w, "{}", row.join("\t"))?;
            }
            TsvSink::Tabs(w) => {
                writeln!(w, "{}", row.join("\t"))?;
            }
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        match self {
            TsvSink::Plain(w) => w.flush()?,
            TsvSink::Tabs(w) => w.flush()?,
        }
        Ok(())
    }
}

// --- CSV Sink ---
struct CsvSink<'a> {
    writer: csv::Writer<&'a mut dyn Write>,
}

impl<'a> TableSink for CsvSink<'a> {
    fn write_header(&mut self, headers: &[String]) -> Result<()> {
        self.writer.write_record(headers)?;
        Ok(())
    }
    fn write_row(&mut self, row: &[String]) -> Result<()> {
        self.writer.write_record(row)?;
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

// --- Box Sink (using tabled) ---
struct BoxSink<'a> {
    writer: BufWriter<&'a mut dyn Write>,
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl<'a> BoxSink<'a> {
    fn new(writer: &'a mut dyn Write) -> Self {
        Self {
            writer: BufWriter::new(writer),
            headers: Vec::new(),
            rows: Vec::new(),
        }
    }
}

impl<'a> TableSink for BoxSink<'a> {
    fn write_header(&mut self, headers: &[String]) -> Result<()> {
        self.headers = headers.to_vec();
        Ok(())
    }
    
    fn write_row(&mut self, row: &[String]) -> Result<()> {
        self.rows.push(row.to_vec());
        Ok(())
    }
    
    fn finish(&mut self) -> Result<()> {
        if !self.headers.is_empty() || !self.rows.is_empty() {
            // Determine the maximum number of columns
            let max_cols = self.headers.len().max(
                self.rows.iter().map(|r| r.len()).max().unwrap_or(0)
            );
            
            // Calculate maximum width for each column
            let mut col_widths: Vec<usize> = vec![0; max_cols];
            
            // Check headers
            for (i, header) in self.headers.iter().enumerate() {
                if i < max_cols {
                    col_widths[i] = col_widths[i].max(header.len());
                }
            }
            
            // Check all rows
            for row in &self.rows {
                for (i, cell) in row.iter().enumerate() {
                    if i < max_cols {
                        col_widths[i] = col_widths[i].max(cell.len());
                    }
                }
            }
            
            // Ensure minimum width of 3 for each column
            for width in &mut col_widths {
                *width = (*width).max(3);
            }
            
            // Build table string
            let mut table_str = String::new();
            
            // Create top border
            table_str.push_str("┌");
            for (i, &width) in col_widths.iter().enumerate() {
                if i > 0 { table_str.push_str("┬"); }
                table_str.push_str(&"─".repeat(width + 2));
            }
            table_str.push_str("┐\n");
            
            // Add header row if exists
            if !self.headers.is_empty() {
                table_str.push_str("│");
                for (i, header) in self.headers.iter().enumerate() {
                    let width = col_widths.get(i).unwrap_or(&3);
                    table_str.push_str(&format!(" {:^width$} ", header, width = width));
                    if i < max_cols - 1 {
                        table_str.push_str("│");
                    }
                }
                table_str.push_str("│\n");
                
                // Add separator
                table_str.push_str("├");
                for (i, &width) in col_widths.iter().enumerate() {
                    if i > 0 { table_str.push_str("┼"); }
                    table_str.push_str(&"─".repeat(width + 2));
                }
                table_str.push_str("┤\n");
            }
            
            // Add data rows
            for row in &self.rows {
                table_str.push_str("│");
                for (i, cell) in row.iter().enumerate() {
                    let width = col_widths.get(i).unwrap_or(&3);
                    table_str.push_str(&format!(" {:<width$} ", cell, width = width));
                    if i < max_cols - 1 {
                        table_str.push_str("│");
                    }
                }
                table_str.push_str("│\n");
            }
            
            // Add bottom border
            table_str.push_str("└");
            for (i, &width) in col_widths.iter().enumerate() {
                if i > 0 { table_str.push_str("┴"); }
                table_str.push_str(&"─".repeat(width + 2));
            }
            table_str.push_str("┘");
            
            write!(self.writer, "{}", table_str)?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

// --- Compact Sink (using tabled without borders) ---
struct CompactSink<'a> {
    writer: BufWriter<&'a mut dyn Write>,
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl<'a> CompactSink<'a> {
    fn new(writer: &'a mut dyn Write) -> Self {
        Self {
            writer: BufWriter::new(writer),
            headers: Vec::new(),
            rows: Vec::new(),
        }
    }
}

impl<'a> TableSink for CompactSink<'a> {
    fn write_header(&mut self, headers: &[String]) -> Result<()> {
        self.headers = headers.to_vec();
        Ok(())
    }
    
    fn write_row(&mut self, row: &[String]) -> Result<()> {
        self.rows.push(row.to_vec());
        Ok(())
    }
    
    fn finish(&mut self) -> Result<()> {
        if !self.headers.is_empty() || !self.rows.is_empty() {
            // Determine the maximum number of columns
            let max_cols = self.headers.len().max(
                self.rows.iter().map(|r| r.len()).max().unwrap_or(0)
            );
            
            // Calculate maximum width for each column
            let mut col_widths: Vec<usize> = vec![0; max_cols];
            
            // Check headers
            for (i, header) in self.headers.iter().enumerate() {
                if i < max_cols {
                    col_widths[i] = col_widths[i].max(header.len());
                }
            }
            
            // Check all rows
            for row in &self.rows {
                for (i, cell) in row.iter().enumerate() {
                    if i < max_cols {
                        col_widths[i] = col_widths[i].max(cell.len());
                    }
                }
            }
            
            // Build all rows
            let mut all_rows = Vec::new();
            
            // Add header row if exists
            if !self.headers.is_empty() {
                let mut header_cells = self.headers.clone();
                while header_cells.len() < max_cols {
                    header_cells.push("".to_string());
                }
                all_rows.push(header_cells);
            }
            
            // Add data rows
            for row in &self.rows {
                let mut row_cells = row.clone();
                while row_cells.len() < max_cols {
                    row_cells.push("".to_string());
                }
                all_rows.push(row_cells);
            }
            
            // Output with proper alignment (space-separated, not tabs)
            for row in &all_rows {
                for (i, cell) in row.iter().enumerate() {
                    if i > 0 { write!(self.writer, "  ")?; }
                    let width = col_widths.get(i).unwrap_or(&0);
                    write!(self.writer, "{:<width$}", cell, width = width)?;
                }
                writeln!(self.writer)?;
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_float_basic_values() {
        assert_eq!(fmt_float(0.0, 3), "0");
        assert_eq!(fmt_float(0.0, 1), "0");
        assert_eq!(fmt_float(1.0, 3), "1");
        assert_eq!(fmt_float(-1.0, 3), "-1");
    }

    #[test]
    fn test_fmt_float_simple_positive() {
        assert_eq!(fmt_float(123.456, 3), "123");
        assert_eq!(fmt_float(123.456, 4), "123.5");
        assert_eq!(fmt_float(123.456, 5), "123.46");
        assert_eq!(fmt_float(123.456, 6), "123.456");
        assert_eq!(fmt_float(123.456, 7), "123.456");
    }

    #[test]
    fn test_fmt_float_simple_negative() {
        assert_eq!(fmt_float(-123.456, 3), "-123");
        assert_eq!(fmt_float(-123.456, 4), "-123.5");
        assert_eq!(fmt_float(-123.456, 5), "-123.46");
        assert_eq!(fmt_float(-123.456, 6), "-123.456");
    }

    #[test]
    fn test_fmt_float_large_numbers_scientific_when_shorter() {
        // Scientific is used when it's shorter
        assert_eq!(fmt_float(98765.0, 2), "99000");
        assert_eq!(fmt_float(98765.0, 3), "98800");
        assert_eq!(fmt_float(98765.0, 4), "98770");
        // Fixed is used when it's shorter
        assert_eq!(fmt_float(1222.0, 2), "1200");
        assert_eq!(fmt_float(1222.0, 3), "1220");
        assert_eq!(fmt_float(999.9, 2), "1000");
        assert_eq!(fmt_float(999.9, 3), "1000");
        assert_eq!(fmt_float(999.9, 4), "999.9");
    }

    #[test]
    fn test_fmt_float_rounding_edge_cases() {
        // Fixed is shorter for these
        assert_eq!(fmt_float(1999.0, 2), "2000");
        assert_eq!(fmt_float(1999.0, 3), "2000");
        assert_eq!(fmt_float(999.4, 2), "1000");
        assert_eq!(fmt_float(999.5, 2), "1000");
        assert_eq!(fmt_float(999.5, 3), "1000");
    }

    #[test]
    fn test_fmt_float_single_sig_digit() {
        // Fixed is shorter for these
        assert_eq!(fmt_float(123.456, 1), "100");
        assert_eq!(fmt_float(987.654, 1), "1000");
        assert_eq!(fmt_float(9.876, 1), "10");
        assert_eq!(fmt_float(0.9876, 1), "1");
        assert_eq!(fmt_float(0.009876, 1), "0.01");
    }

    #[test]
    fn test_fmt_float_small_numbers_scientific_when_shorter() {
        assert_eq!(fmt_float(0.123456, 3), "0.123");
        assert_eq!(fmt_float(0.123456, 4), "0.1235");
        assert_eq!(fmt_float(0.123456, 5), "0.12346");
        assert_eq!(fmt_float(0.00123456, 2), "0.0012");
        assert_eq!(fmt_float(0.00123456, 3), "0.00123");
        assert_eq!(fmt_float(0.00123456, 4), "0.001235");
        assert_eq!(fmt_float(0.000123456, 2), "0.00012");
        assert_eq!(fmt_float(0.000123456, 3), "0.000123");
        assert_eq!(fmt_float(0.0000123456, 3), "1.23e-05");
        assert_eq!(fmt_float(0.00000123456, 3), "1.23e-06");
    }

    #[test]
    fn test_fmt_float_very_small_numbers() {
        assert_eq!(fmt_float(1e-6, 3), "0.000001");
        assert_eq!(fmt_float(1e-7, 3), "1.00e-07");
        assert_eq!(fmt_float(1e-8, 3), "1.00e-08");
        assert_eq!(fmt_float(1e-9, 3), "1.00e-09");
        assert_eq!(fmt_float(1.234e-7, 3), "1.23e-07");
        assert_eq!(fmt_float(1.234e-7, 4), "1.234e-07");
    }

    #[test]
    fn test_fmt_float_large_numbers_fixed_or_scientific() {
        assert_eq!(fmt_float(999999.0, 3), "1000000");
        assert_eq!(fmt_float(1e6, 3), "1000000");
        assert_eq!(fmt_float(1e7, 3), "10000000");
        assert_eq!(fmt_float(1234567.0, 4), "1235000");
        assert_eq!(fmt_float(1234567.0, 5), "1234600");
    }

    #[test]
    fn test_fmt_float_scientific_various_precision() {
        assert_eq!(fmt_float(1.23456789e10, 3), "1.23e+10");
        assert_eq!(fmt_float(1.23456789e10, 4), "1.235e+10");
        assert_eq!(fmt_float(1.23456789e10, 5), "1.2346e+10");
        // 6 sig digits: fixed "12345700000" (11) vs scientific "1.23457e+10" (11) - tie, fixed wins
        assert_eq!(fmt_float(1.23456789e10, 6), "12345700000");
        assert_eq!(fmt_float(9.87654321e10, 3), "9.88e+10");
        assert_eq!(fmt_float(9.87654321e10, 4), "9.877e+10");
    }

    #[test]
    fn test_fmt_float_special_values() {
        assert_eq!(fmt_float(f64::NAN, 3), "NaN");
        assert_eq!(fmt_float(f64::INFINITY, 3), "inf");
        assert_eq!(fmt_float(f64::NEG_INFINITY, 3), "-inf");
    }

    #[test]
    fn test_fmt_float_zero_edge_cases() {
        assert_eq!(fmt_float(0.000, 3), "0");
        assert_eq!(fmt_float(-0.000, 3), "0");
        assert_eq!(fmt_float(0.000001, 1), "1e-06");
        assert_eq!(fmt_float(0.000001, 2), "1.0e-06");
    }

    #[test]
    fn test_fmt_float_trailing_zero_removal() {
        assert_eq!(fmt_float(123.4500, 4), "123.5");
        assert_eq!(fmt_float(123.4500, 5), "123.45");
        assert_eq!(fmt_float(123.4500, 6), "123.45");
        assert_eq!(fmt_float(123.0, 3), "123");
        assert_eq!(fmt_float(123.0, 5), "123");
    }

    #[test]
    fn test_fmt_float_negative_small_numbers() {
        assert_eq!(fmt_float(-0.00123, 3), "-0.00123");
        assert_eq!(fmt_float(-0.00123, 2), "-0.0012");
        assert_eq!(fmt_float(-0.00123, 1), "-0.001");
    }

    #[test]
    fn test_fmt_float_large_negative_numbers() {
        assert_eq!(fmt_float(-1234567.0, 3), "-1230000");
        assert_eq!(fmt_float(-1234567.0, 4), "-1235000");
        // Fixed is shorter for these
        assert_eq!(fmt_float(-98765.0, 2), "-99000");
        assert_eq!(fmt_float(-98765.0, 3), "-98800");
    }

    #[test]
    fn test_fmt_float_exponent_formatting() {
        assert_eq!(fmt_float(1e8, 3), "1.00e+08");
        assert_eq!(fmt_float(1e-8, 3), "1.00e-08");
        assert_eq!(fmt_float(1e20, 3), "1.00e+20");
        assert_eq!(fmt_float(1e-20, 3), "1.00e-20");
        // 1e8 with 4 sig digits: fixed "100000000" (9) vs scientific "1.000e+08" (9) - tie, fixed wins
        assert_eq!(fmt_float(1e8, 4), "100000000");
        assert_eq!(fmt_float(1e-8, 4), "1.000e-08");
    }

    #[test]
    fn test_fmt_float_boundary_values() {
        // Fixed is shorter for these
        assert_eq!(fmt_float(9.99999, 3), "10");
        assert_eq!(fmt_float(9.99999, 4), "10");
        assert_eq!(fmt_float(9.99999, 5), "10");
        assert_eq!(fmt_float(99.9999, 3), "100");
        assert_eq!(fmt_float(99.9999, 4), "100");
        assert_eq!(fmt_float(99.9999, 5), "100");
    }

    #[test]
    fn test_fmt_float_very_large_scientific() {
        assert_eq!(fmt_float(1.23456789e15, 4), "1.235e+15");
        assert_eq!(fmt_float(1.23456789e15, 6), "1.23457e+15");
        assert_eq!(fmt_float(9.99999999e15, 3), "1.00e+16");
    }

    #[test]
    fn test_fmt_float_precision_edge_cases() {
        assert_eq!(fmt_float(0.999999, 3), "1");
        assert_eq!(fmt_float(0.999999, 4), "1");
        assert_eq!(fmt_float(0.999999, 6), "0.999999");
        assert_eq!(fmt_float(-0.999999, 3), "-1");
        assert_eq!(fmt_float(-0.999999, 6), "-0.999999");
    }

    #[test]
    fn test_fmt_float_shortest_representation() {
        // Cases where scientific is shorter
        assert_eq!(fmt_float(1e20, 3), "1.00e+20");
        assert_eq!(fmt_float(1e8, 3), "1.00e+08");
        
        // Cases where fixed is shorter
        assert_eq!(fmt_float(0.00001, 3), "0.00001");
        assert_eq!(fmt_float(0.000001, 3), "0.000001");
        assert_eq!(fmt_float(100000.0, 3), "100000");
        assert_eq!(fmt_float(0.0001, 3), "0.0001");
        assert_eq!(fmt_float(1000.0, 3), "1000");
        assert_eq!(fmt_float(10000.0, 3), "10000");
        assert_eq!(fmt_float(999.0, 3), "999");
        assert_eq!(fmt_float(123.45, 3), "123");
        assert_eq!(fmt_float(0.123, 3), "0.123");
    }
}
