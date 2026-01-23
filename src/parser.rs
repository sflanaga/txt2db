use crossbeam_channel::{Receiver, Sender};
use regex::Regex;
use pcre2::bytes::Regex as PcreRegex;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::aggregation::{AggAccumulator, AggRole, AggValue, FieldSource, MapFieldSpec};
use crate::config::DisableConfig;
use crate::database::{ColumnDef, DbRecord, FieldSource as DbFieldSource};
use crate::io_splicer::SplicedChunk;
use crate::stats::DbStats;

#[derive(Clone)]
pub enum AnyRegex {
    Std(Regex),
    Pcre(PcreRegex),
}

impl AnyRegex {
    pub fn captures_len(&self) -> usize {
        match self {
            AnyRegex::Std(r) => r.captures_len(),
            // PCRE2 captures_len() returns the number of capturing groups (excluding group 0).
            // Rust regex captures_len() includes group 0.
            // We add 1 to PCRE2 to match the behavior.
            AnyRegex::Pcre(r) => r.captures_len() + 1, 
        }
    }
}

// Cheap way to measure relative cost on x86_64
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn read_cycle_counter() -> u64 {
    unsafe { std::arch::x86_64::_rdtsc() }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn read_cycle_counter() -> u64 {
    // Fallback for non-x86 (e.g., ARM/M1) - roughly nanoseconds
    std::time::Instant::now().elapsed().as_nanos() as u64
}

pub fn run_mapper_worker(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    specs: Vec<MapFieldSpec>,
    line_re: AnyRegex,
    path_re: Option<Regex>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
    enable_profiling: bool,
    disable_flags: Arc<DisableConfig>,
) -> BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> {
    match line_re {
        AnyRegex::Std(re) => run_mapper_worker_std(rx, recycle_tx, specs, re, path_re, stats, show_errors, stop_on_error, enable_profiling, disable_flags),
        AnyRegex::Pcre(re) => run_mapper_worker_pcre(rx, recycle_tx, specs, re, path_re, stats, show_errors, stop_on_error, enable_profiling, disable_flags),
    }
}

fn fetch_path_caps<'a>(
    path_cache: &'a mut HashMap<Arc<std::path::PathBuf>, Option<Vec<Option<String>>>>,
    path_re: &Regex,
    path_arc: &Arc<std::path::PathBuf>,
) -> Option<&'a Option<Vec<Option<String>>>> {
    if !path_cache.contains_key(path_arc) {
        let p_str = path_arc.to_string_lossy();
        let entry = if let Some(caps) = path_re.captures(&p_str) {
            let mut v = Vec::with_capacity(path_re.captures_len());
            for i in 0..path_re.captures_len() {
                v.push(caps.get(i).map(|m| m.as_str().to_string()));
            }
            Some(v)
        } else {
            None
        };
        path_cache.insert(path_arc.clone(), entry);
    }
    path_cache.get(path_arc)
}

fn run_mapper_worker_std(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    specs: Vec<MapFieldSpec>,
    line_re: Regex,
    path_re: Option<Regex>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
    enable_profiling: bool,
    disable_flags: Arc<DisableConfig>,
) -> BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> {
    
    let mut map: BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> = BTreeMap::new();
    let mut path_cache: HashMap<Arc<std::path::PathBuf>, Option<Vec<Option<String>>>> = HashMap::new();
    
    // Performance counters (Cycles)
    let mut c_regex: u64 = 0;
    let mut c_parse: u64 = 0;
    let mut c_map: u64 = 0;

    // Pre-calculate indices
    let mut key_indices = Vec::new();
    let mut val_indices = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        if spec.role == AggRole::Key { key_indices.push(i); } 
        else { val_indices.push(i); }
    }

    while let Ok(chunk) = rx.recv() {
        let mut data = chunk.data;
        stats.bytes_processed.fetch_add(data.len(), Ordering::Relaxed);
        
        if disable_flags.regex {
             data.clear();
             let _ = recycle_tx.send(data);
             continue;
        }

        // Path captures (once per file path, cached)
        let path_caps = if let (Some(pr), Some(ref path_arc)) = (&path_re, chunk.file_path.as_ref()) {
            match fetch_path_caps(&mut path_cache, pr, path_arc) {
                Some(Some(v)) => Some(v),
                Some(None) => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                },
                None => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                }
            }
        } else {
            None
        };

        let path_display = chunk.file_path.as_ref().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|| "stdin".to_string());
        let chunk_offset = chunk.offset;
        
        // STD Regex works on UTF-8 strings
        let s = String::from_utf8_lossy(&data);
        
        if enable_profiling {
            // We use an explicit manual iterator loop to measure regex time strictly
            // Note: captures_iter is lazy, so "next()" does the heavy lifting.
            let mut loc = 0;
            let mut t0 = read_cycle_counter();
            
            // We loop manually to capture timing around the regex engine steps
            while let Some(capture) = line_re.captures_at(&s, loc) {
                let t1 = read_cycle_counter();
                c_regex += t1.wrapping_sub(t0);

                // Advance location for next search
                if let Some(m) = capture.get(0) {
                    loc = m.end();
                    if m.start() == m.end() { loc += 1; } // Prevent infinite loop on empty match
                } else {
                    loc += 1;
                }

                stats.matched_lines.fetch_add(1, Ordering::Relaxed);

                if disable_flags.map_target {
                    t0 = read_cycle_counter(); // Reset start for next regex search
                    continue;
                }
                
                let mut agg_row = Vec::with_capacity(specs.len());
                let mut row_has_error = false;
                
                let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                let abs_offset = chunk_offset + match_start as u64;

                for spec in &specs {
                    let raw = match spec.source {
                        FieldSource::Line => capture.get(spec.capture_index).map(|m| m.as_str()).unwrap_or(""),
                        FieldSource::Path => path_caps
                            .and_then(|caps| caps.get(spec.capture_index).and_then(|o| o.as_deref()))
                            .unwrap_or(""),
                    };
                    match AggValue::from_str(raw, spec.dtype) {
                        Some(val) => agg_row.push(val),
                        None => {
                            row_has_error = true;
                            stats.total_errors.fetch_add(1, Ordering::Relaxed);
                            *stats.error_counts.entry(spec.capture_index).or_default() += 1;

                            if show_errors || stop_on_error {
                                eprintln!("Parse Error at {}:{} (Capture Group {}): Failed to parse '{}' as {:?}", 
                                    path_display, abs_offset, spec.capture_index, raw, spec.dtype);
                            }
                            if stop_on_error {
                                eprintln!("Stopping due to error (-E flag).");
                                std::process::exit(1); 
                            }
                        }
                    }
                }
                
                let t2 = read_cycle_counter();
                c_parse += t2.wrapping_sub(t1);

                if row_has_error { 
                    t0 = read_cycle_counter(); // Reset start
                    continue; 
                }

                if disable_flags.map_write {
                    t0 = read_cycle_counter(); // Reset start
                    continue;
                }

                let mut key = Vec::with_capacity(key_indices.len());
                let mut key_valid = true;
                for &idx in &key_indices {
                    let v = &agg_row[idx];
                    if v.is_null() { key_valid = false; break; }
                    key.push(v.clone());
                }

                if key_valid {
                    stats.mapped_records.fetch_add(1, Ordering::Relaxed);
                    let entry = map.entry(key).or_insert_with(|| {
                        val_indices.iter().map(|&i| {
                            AggAccumulator::new(specs[i].role, specs[i].dtype)
                        }).collect()
                    });
                    for (acc_idx, &row_idx) in val_indices.iter().enumerate() {
                        entry[acc_idx].update(&agg_row[row_idx]);
                    }
                }
                
                let t3 = read_cycle_counter();
                c_map += t3.wrapping_sub(t2);
                
                // Reset regex start timer for the next loop iteration
                t0 = read_cycle_counter();
            }
        } else {
            // Normal Loop (Standard optimized path)
            for capture in line_re.captures_iter(&s) {
                stats.matched_lines.fetch_add(1, Ordering::Relaxed);
                
                if disable_flags.map_target { continue; }

                let mut agg_row = Vec::with_capacity(specs.len());
                let mut row_has_error = false;
                
                let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                let abs_offset = chunk_offset + match_start as u64;

                for spec in &specs {
                    let raw = match spec.source {
                        FieldSource::Line => capture.get(spec.capture_index).map(|m| m.as_str()).unwrap_or(""),
                        FieldSource::Path => path_caps
                            .and_then(|caps| caps.get(spec.capture_index).and_then(|o| o.as_deref()))
                            .unwrap_or(""),
                    };
                    match AggValue::from_str(raw, spec.dtype) {
                        Some(val) => agg_row.push(val),
                        None => {
                            row_has_error = true;
                            stats.total_errors.fetch_add(1, Ordering::Relaxed);
                            *stats.error_counts.entry(spec.capture_index).or_default() += 1;

                            if show_errors || stop_on_error {
                                eprintln!("Parse Error at {}:{} (Capture Group {}): Failed to parse '{}' as {:?}", 
                                    path_display, abs_offset, spec.capture_index, raw, spec.dtype);
                            }
                            if stop_on_error {
                                eprintln!("Stopping due to error (-E flag).");
                                std::process::exit(1); 
                            }
                        }
                    }
                }

                if row_has_error { continue; }

                if disable_flags.map_write { continue; }

                let mut key = Vec::with_capacity(key_indices.len());
                let mut key_valid = true;
                for &idx in &key_indices {
                    let v = &agg_row[idx];
                    if v.is_null() { key_valid = false; break; }
                    key.push(v.clone());
                }

                if key_valid {
                    stats.mapped_records.fetch_add(1, Ordering::Relaxed);
                    let entry = map.entry(key).or_insert_with(|| {
                        val_indices.iter().map(|&i| {
                            AggAccumulator::new(specs[i].role, specs[i].dtype)
                        }).collect()
                    });
                    for (acc_idx, &row_idx) in val_indices.iter().enumerate() {
                        entry[acc_idx].update(&agg_row[row_idx]);
                    }
                }
            }
        }

        data.clear();
        let _ = recycle_tx.send(data);
    }

    if enable_profiling {
        // Convert to percentage
        let total_cycles = c_regex + c_parse + c_map;
        let p_regex = if total_cycles > 0 { (c_regex as f64 / total_cycles as f64) * 100.0 } else { 0.0 };
        let p_parse = if total_cycles > 0 { (c_parse as f64 / total_cycles as f64) * 100.0 } else { 0.0 };
        let p_map = if total_cycles > 0 { (c_map as f64 / total_cycles as f64) * 100.0 } else { 0.0 };
        
        println!("Thread Profile (std): Regex={:.1}% Parse={:.1}% Map={:.1}% (Total Cycles: {})", 
            p_regex, p_parse, p_map, total_cycles);
    }

    map
}

fn run_mapper_worker_pcre(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    specs: Vec<MapFieldSpec>,
    line_re: PcreRegex,
    path_re: Option<Regex>,
    stats: Arc<DbStats>,
    show_errors: bool,
    stop_on_error: bool,
    enable_profiling: bool,
    disable_flags: Arc<DisableConfig>,
) -> BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> {
    
    let mut map: BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> = BTreeMap::new();
    let mut path_cache: HashMap<Arc<std::path::PathBuf>, Option<Vec<Option<String>>>> = HashMap::new();
    
    // Performance counters (Cycles)
    let mut c_regex: u64 = 0;
    let mut c_parse: u64 = 0;
    let mut c_map: u64 = 0;

    let mut key_indices = Vec::new();
    let mut val_indices = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        if spec.role == AggRole::Key { key_indices.push(i); } 
        else { val_indices.push(i); }
    }

    while let Ok(chunk) = rx.recv() {
        let mut data = chunk.data;
        stats.bytes_processed.fetch_add(data.len(), Ordering::Relaxed);
        
        if disable_flags.regex {
             data.clear();
             let _ = recycle_tx.send(data);
             continue;
        }

        let path_caps = if let (Some(pr), Some(ref path_arc)) = (&path_re, chunk.file_path.as_ref()) {
            match fetch_path_caps(&mut path_cache, pr, path_arc) {
                Some(Some(v)) => Some(v),
                Some(None) => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                },
                None => {
                    data.clear();
                    let _ = recycle_tx.send(data);
                    continue;
                }
            }
        } else {
            None
        };

        let path_display = chunk.file_path.as_ref().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|| "stdin".to_string());
        let chunk_offset = chunk.offset;
        
        // PCRE2 works on bytes directly
        
        if enable_profiling {
             let mut t0 = read_cycle_counter();
             
             for result in line_re.captures_iter(&data) {
                 if let Ok(capture) = result {
                    let t1 = read_cycle_counter();
                    c_regex += t1.wrapping_sub(t0);

                    stats.matched_lines.fetch_add(1, Ordering::Relaxed);

                    if disable_flags.map_target {
                        t0 = read_cycle_counter(); 
                        continue;
                    }

                    let mut agg_row = Vec::with_capacity(specs.len());
                    let mut row_has_error = false;
                    
                    let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                    let abs_offset = chunk_offset + match_start as u64;

                    for spec in &specs {
                        // Extract bytes and convert to string for parsing
                        // This is potentially lossy, matching the behavior of from_utf8_lossy in std mode
                        let raw = match spec.source {
                            FieldSource::Line => {
                                let raw_bytes = capture.get(spec.capture_index).map(|m| m.as_bytes()).unwrap_or(&[]);
                                String::from_utf8_lossy(raw_bytes)
                            },
                            FieldSource::Path => {
                                path_caps
                                    .and_then(|caps| caps.get(spec.capture_index).and_then(|o| o.as_deref()))
                                    .map(|s| std::borrow::Cow::Borrowed(s))
                                    .unwrap_or_else(|| std::borrow::Cow::Borrowed(""))
                            }
                        };

                        match AggValue::from_str(&raw, spec.dtype) {
                            Some(val) => agg_row.push(val),
                            None => {
                                row_has_error = true;
                                stats.total_errors.fetch_add(1, Ordering::Relaxed);
                                *stats.error_counts.entry(spec.capture_index).or_default() += 1;

                                if show_errors || stop_on_error {
                                    eprintln!("Parse Error at {}:{} (Capture Group {}): Failed to parse '{}' as {:?}", 
                                        path_display, abs_offset, spec.capture_index, raw, spec.dtype);
                                }
                                if stop_on_error {
                                    eprintln!("Stopping due to error (-E flag).");
                                    std::process::exit(1); 
                                }
                            }
                        }
                    }
                    
                    let t2 = read_cycle_counter();
                    c_parse += t2.wrapping_sub(t1);

                    if row_has_error { 
                        t0 = read_cycle_counter(); 
                        continue; 
                    }

                    if disable_flags.map_write {
                        t0 = read_cycle_counter();
                        continue;
                    }

                    let mut key = Vec::with_capacity(key_indices.len());
                    let mut key_valid = true;
                    for &idx in &key_indices {
                        let v = &agg_row[idx];
                        if v.is_null() { key_valid = false; break; }
                        key.push(v.clone());
                    }

                    if key_valid {
                        stats.mapped_records.fetch_add(1, Ordering::Relaxed);
                        let entry = map.entry(key).or_insert_with(|| {
                            val_indices.iter().map(|&i| {
                                AggAccumulator::new(specs[i].role, specs[i].dtype)
                            }).collect()
                        });
                        for (acc_idx, &row_idx) in val_indices.iter().enumerate() {
                            entry[acc_idx].update(&agg_row[row_idx]);
                        }
                    }
                    
                    let t3 = read_cycle_counter();
                    c_map += t3.wrapping_sub(t2);
                    t0 = read_cycle_counter();
                 }
             }

        } else {
            // Normal Loop PCRE
            for result in line_re.captures_iter(&data) {
                if let Ok(capture) = result {
                    stats.matched_lines.fetch_add(1, Ordering::Relaxed);
                    
                    if disable_flags.map_target { continue; }

                    let mut agg_row = Vec::with_capacity(specs.len());
                    let mut row_has_error = false;
                    
                    let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                    let abs_offset = chunk_offset + match_start as u64;

                    for spec in &specs {
                        let raw = match spec.source {
                            FieldSource::Line => {
                                let raw_bytes = capture.get(spec.capture_index).map(|m| m.as_bytes()).unwrap_or(&[]);
                                String::from_utf8_lossy(raw_bytes)
                            },
                            FieldSource::Path => {
                                path_caps
                                    .and_then(|caps| caps.get(spec.capture_index).and_then(|o| o.as_deref()))
                                    .map(|s| std::borrow::Cow::Borrowed(s))
                                    .unwrap_or_else(|| std::borrow::Cow::Borrowed(""))
                            }
                        };

                        match AggValue::from_str(&raw, spec.dtype) {
                            Some(val) => agg_row.push(val),
                            None => {
                                row_has_error = true;
                                stats.total_errors.fetch_add(1, Ordering::Relaxed);
                                *stats.error_counts.entry(spec.capture_index).or_default() += 1;

                                if show_errors || stop_on_error {
                                    eprintln!("Parse Error at {}:{} (Capture Group {}): Failed to parse '{}' as {:?}", 
                                        path_display, abs_offset, spec.capture_index, raw, spec.dtype);
                                }
                                if stop_on_error {
                                    eprintln!("Stopping due to error (-E flag).");
                                    std::process::exit(1); 
                                }
                            }
                        }
                    }

                    if row_has_error { continue; }
                    if disable_flags.map_write { continue; }

                    let mut key = Vec::with_capacity(key_indices.len());
                    let mut key_valid = true;
                    for &idx in &key_indices {
                        let v = &agg_row[idx];
                        if v.is_null() { key_valid = false; break; }
                        key.push(v.clone());
                    }

                    if key_valid {
                        stats.mapped_records.fetch_add(1, Ordering::Relaxed);
                        let entry = map.entry(key).or_insert_with(|| {
                            val_indices.iter().map(|&i| {
                                AggAccumulator::new(specs[i].role, specs[i].dtype)
                            }).collect()
                        });
                        for (acc_idx, &row_idx) in val_indices.iter().enumerate() {
                            entry[acc_idx].update(&agg_row[row_idx]);
                        }
                    }
                }
            }
        }

        data.clear();
        let _ = recycle_tx.send(data);
    }

    if enable_profiling {
        let total_cycles = c_regex + c_parse + c_map;
        let p_regex = if total_cycles > 0 { (c_regex as f64 / total_cycles as f64) * 100.0 } else { 0.0 };
        let p_parse = if total_cycles > 0 { (c_parse as f64 / total_cycles as f64) * 100.0 } else { 0.0 };
        let p_map = if total_cycles > 0 { (c_map as f64 / total_cycles as f64) * 100.0 } else { 0.0 };
        
        println!("Thread Profile (pcre): Regex={:.1}% Parse={:.1}% Map={:.1}% (Total Cycles: {})", 
            p_regex, p_parse, p_map, total_cycles);
    }

    map
}

pub fn run_db_parser(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    db_tx: Sender<DbRecord>,
    line_re: AnyRegex,
    path_re: Option<Regex>,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
) {
    match line_re {
        AnyRegex::Std(re) => run_db_parser_std(rx, recycle_tx, db_tx, re, path_re, columns, stats),
        AnyRegex::Pcre(re) => run_db_parser_pcre(rx, recycle_tx, db_tx, re, path_re, columns, stats),
    }
}

fn run_db_parser_std(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    db_tx: Sender<DbRecord>,
    line_re: Regex,
    path_re: Option<Regex>,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
) {
    while let Ok(chunk) = rx.recv() {
        let mut data = chunk.data;
        stats.bytes_processed.fetch_add(data.len(), Ordering::Relaxed);

        let path_arc = chunk.file_path;
        let chunk_offset = chunk.offset;
        
        let mut should_process = true;
        let mut path_fields = Vec::with_capacity(columns.len());

        if let Some(pre) = &path_re {
            if let Some(p) = &path_arc {
                let p_str = p.to_string_lossy();
                if let Some(caps) = pre.captures(&p_str) {
                        for col in &columns {
                            if let DbFieldSource::Path(idx) = col.source {
                                let val = caps.get(idx).map(|m| m.as_str().to_string()).unwrap_or_default();
                                path_fields.push((idx, val));
                            }
                        }
                } else {
                    should_process = false;
                }
            } else {
                    should_process = false;
            }
        }

        if should_process {
            let s = String::from_utf8_lossy(&data);
            
            for capture in line_re.captures_iter(&s) {
                stats.matched_lines.fetch_add(1, Ordering::Relaxed);
                
                // Extract Full Line Logic
                let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                let match_end = capture.get(0).map(|m| m.end()).unwrap_or(0);
                let bytes = s.as_bytes();

                let start_idx = bytes[..match_start]
                    .iter()
                    .rposition(|&b| b == b'\n')
                    .map(|i| i + 1)
                    .unwrap_or(0);

                let end_idx = bytes[match_end..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|i| match_end + i)
                    .unwrap_or(bytes.len());

                let full_line = s[start_idx..end_idx].trim_end().to_string();
                let match_offset = chunk_offset + start_idx as u64;

                let mut fields = Vec::with_capacity(columns.len());
                for col in &columns {
                    match col.source {
                        DbFieldSource::Line(idx) => {
                            fields.push(capture.get(idx).map(|m| m.as_str().to_string()));
                        },
                        DbFieldSource::Path(idx) => {
                            let val = path_fields.iter().find(|(k, _)| *k == idx).map(|(_, v)| v.clone());
                            fields.push(val);
                        }
                    }
                }
                let record = DbRecord::Data {
                    file_path: path_arc.clone(),
                    offset: match_offset,
                    line_content: full_line,
                    fields,
                };
                if db_tx.send(record).is_err() { break; }
            }
        }
        data.clear();
        let _ = recycle_tx.send(data);
    }
}

fn run_db_parser_pcre(
    rx: Receiver<SplicedChunk>,
    recycle_tx: Sender<Vec<u8>>,
    db_tx: Sender<DbRecord>,
    line_re: PcreRegex,
    path_re: Option<Regex>,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
) {
    while let Ok(chunk) = rx.recv() {
        let mut data = chunk.data;
        stats.bytes_processed.fetch_add(data.len(), Ordering::Relaxed);

        let path_arc = chunk.file_path;
        let chunk_offset = chunk.offset;
        
        let mut should_process = true;
        let mut path_fields = Vec::with_capacity(columns.len());

        if let Some(pre) = &path_re {
            if let Some(p) = &path_arc {
                let p_str = p.to_string_lossy();
                if let Some(caps) = pre.captures(&p_str) {
                        for col in &columns {
                            if let DbFieldSource::Path(idx) = col.source {
                                let val = caps.get(idx).map(|m| m.as_str().to_string()).unwrap_or_default();
                                path_fields.push((idx, val));
                            }
                        }
                } else {
                    should_process = false;
                }
            } else {
                    should_process = false;
            }
        }

        if should_process {
            // PCRE2 Iteration
            for result in line_re.captures_iter(&data) {
                if let Ok(capture) = result {
                    stats.matched_lines.fetch_add(1, Ordering::Relaxed);
                    
                    let match_start = capture.get(0).map(|m| m.start()).unwrap_or(0);
                    let match_end = capture.get(0).map(|m| m.end()).unwrap_or(0);
                    let bytes = &data;

                    let start_idx = bytes[..match_start]
                        .iter()
                        .rposition(|&b| b == b'\n')
                        .map(|i| i + 1)
                        .unwrap_or(0);

                    let end_idx = bytes[match_end..]
                        .iter()
                        .position(|&b| b == b'\n')
                        .map(|i| match_end + i)
                        .unwrap_or(bytes.len());

                    let full_line_bytes = &bytes[start_idx..end_idx];
                    let full_line = String::from_utf8_lossy(full_line_bytes).trim_end().to_string();
                    let match_offset = chunk_offset + start_idx as u64;

                    let mut fields = Vec::with_capacity(columns.len());
                    for col in &columns {
                        match col.source {
                            DbFieldSource::Line(idx) => {
                                let val = capture.get(idx).map(|m| String::from_utf8_lossy(m.as_bytes()).to_string());
                                fields.push(val);
                            },
                            DbFieldSource::Path(idx) => {
                                let val = path_fields.iter().find(|(k, _)| *k == idx).map(|(_, v)| v.clone());
                                fields.push(val);
                            }
                        }
                    }
                    let record = DbRecord::Data {
                        file_path: path_arc.clone(),
                        offset: match_offset,
                        line_content: full_line,
                        fields,
                    };
                    if db_tx.send(record).is_err() { break; }
                }
            }
        }
        data.clear();
        let _ = recycle_tx.send(data);
    }
}
