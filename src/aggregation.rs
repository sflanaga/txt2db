use anyhow::Result;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Copy)]
pub enum AggRole { Key, Sum, Count, Max, Min, Avg }

#[derive(Clone, Debug, PartialEq, Copy)]
pub enum AggType { I64, U64, F64, Str }

#[derive(Clone, Debug, PartialEq, Copy)]
pub enum FieldSource { Line, Path }

#[derive(Clone, Debug)]
pub struct MapFieldSpec {
    pub capture_index: usize,
    pub role: AggRole,
    pub dtype: AggType,
    pub source: FieldSource,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct OrderedFloat(pub f64);
impl Eq for OrderedFloat {}
impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}
impl std::fmt::Display for OrderedFloat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum AggValue {
    Null,
    I64(i64),
    U64(u64),
    F64(OrderedFloat),
    Str(String),
}

impl AggValue {
    pub fn from_str(s: &str, t: AggType) -> Option<Self> {
        if s.is_empty() { return Some(AggValue::Null); }
        match t {
            AggType::Str => Some(AggValue::Str(s.to_string())),
            AggType::I64 => s.parse().ok().map(AggValue::I64),
            AggType::U64 => s.parse().ok().map(AggValue::U64),
            AggType::F64 => s.parse().ok().map(|f| AggValue::F64(OrderedFloat(f))),
        }
    }
    
    pub fn is_null(&self) -> bool {
        matches!(self, AggValue::Null)
    }
}

#[derive(Clone, Debug)]
pub enum AggAccumulator {
    SumI(i64),
    SumU(u64),
    SumF(f64),
    Count(u64),
    MaxI(i64),
    MaxU(u64),
    MaxF(f64),
    MaxStr(String),
    MinI(i64),
    MinU(u64),
    MinF(f64),
    MinStr(String),
    AvgI { sum: i64, count: u64 },
    AvgU { sum: u64, count: u64 },
    AvgF { sum: f64, count: u64 },
    None,
}

impl AggAccumulator {
    pub fn new(role: AggRole, dtype: AggType) -> Self {
        match role {
            AggRole::Sum => match dtype {
                AggType::I64 => AggAccumulator::SumI(0),
                AggType::U64 => AggAccumulator::SumU(0),
                AggType::F64 => AggAccumulator::SumF(0.0),
                _ => AggAccumulator::None,
            },
            AggRole::Count => AggAccumulator::Count(0),
            AggRole::Max => match dtype {
                AggType::I64 => AggAccumulator::MaxI(i64::MIN),
                AggType::U64 => AggAccumulator::MaxU(u64::MIN),
                AggType::F64 => AggAccumulator::MaxF(f64::MIN),
                AggType::Str => AggAccumulator::MaxStr(String::new()),
            },
            AggRole::Min => match dtype {
                AggType::I64 => AggAccumulator::MinI(i64::MAX),
                AggType::U64 => AggAccumulator::MinU(u64::MAX),
                AggType::F64 => AggAccumulator::MinF(f64::MAX),
                AggType::Str => AggAccumulator::MinStr(String::new()), 
            },
            AggRole::Avg => match dtype {
                AggType::I64 => AggAccumulator::AvgI { sum: 0, count: 0 },
                AggType::U64 => AggAccumulator::AvgU { sum: 0, count: 0 },
                AggType::F64 => AggAccumulator::AvgF { sum: 0.0, count: 0 },
                _ => AggAccumulator::None,
            },
            _ => AggAccumulator::None,
        }
    }

    pub fn update(&mut self, val: &AggValue) {
        if val.is_null() { return; }
        match (self, val) {
            (AggAccumulator::SumI(acc), AggValue::I64(v)) => *acc += v,
            (AggAccumulator::SumU(acc), AggValue::U64(v)) => *acc += v,
            (AggAccumulator::SumF(acc), AggValue::F64(v)) => *acc += v.0,
            (AggAccumulator::Count(acc), _) => *acc += 1,
            
            (AggAccumulator::MaxI(acc), AggValue::I64(v)) => if *v > *acc { *acc = *v },
            (AggAccumulator::MaxU(acc), AggValue::U64(v)) => if *v > *acc { *acc = *v },
            (AggAccumulator::MaxF(acc), AggValue::F64(v)) => if v.0 > *acc { *acc = v.0 },
            (AggAccumulator::MaxStr(acc), AggValue::Str(v)) => if v > acc { *acc = v.clone() },

            (AggAccumulator::MinI(acc), AggValue::I64(v)) => if *v < *acc { *acc = *v },
            (AggAccumulator::MinU(acc), AggValue::U64(v)) => if *v < *acc { *acc = *v },
            (AggAccumulator::MinF(acc), AggValue::F64(v)) => if v.0 < *acc { *acc = v.0 },
            (AggAccumulator::MinStr(acc), AggValue::Str(v)) => {
                if acc.is_empty() || v < acc { *acc = v.clone() }
            },

            (AggAccumulator::AvgI { sum, count }, AggValue::I64(v)) => { *sum += v; *count += 1; },
            (AggAccumulator::AvgU { sum, count }, AggValue::U64(v)) => { *sum += v; *count += 1; },
            (AggAccumulator::AvgF { sum, count }, AggValue::F64(v)) => { *sum += v.0; *count += 1; },
            _ => {}
        }
    }

    // Merge two accumulators from different threads
    pub fn merge(&mut self, other: AggAccumulator) {
        match (self, other) {
            (AggAccumulator::SumI(a), AggAccumulator::SumI(b)) => *a += b,
            (AggAccumulator::SumU(a), AggAccumulator::SumU(b)) => *a += b,
            (AggAccumulator::SumF(a), AggAccumulator::SumF(b)) => *a += b,
            (AggAccumulator::Count(a), AggAccumulator::Count(b)) => *a += b,
            
            (AggAccumulator::MaxI(a), AggAccumulator::MaxI(b)) => *a = (*a).max(b),
            (AggAccumulator::MaxU(a), AggAccumulator::MaxU(b)) => *a = (*a).max(b),
            (AggAccumulator::MaxF(a), AggAccumulator::MaxF(b)) => *a = if *a > b { *a } else { b },
            (AggAccumulator::MaxStr(a), AggAccumulator::MaxStr(b)) => if b > *a { *a = b },

            (AggAccumulator::MinI(a), AggAccumulator::MinI(b)) => *a = (*a).min(b),
            (AggAccumulator::MinU(a), AggAccumulator::MinU(b)) => *a = (*a).min(b),
            (AggAccumulator::MinF(a), AggAccumulator::MinF(b)) => *a = if *a < b { *a } else { b },
            (AggAccumulator::MinStr(a), AggAccumulator::MinStr(b)) => {
                if a.is_empty() || (!b.is_empty() && b < *a) { *a = b }
            },

            (AggAccumulator::AvgI { sum: sa, count: ca }, AggAccumulator::AvgI { sum: sb, count: cb }) => {
                *sa += sb; *ca += cb;
            },
            (AggAccumulator::AvgU { sum: sa, count: ca }, AggAccumulator::AvgU { sum: sb, count: cb }) => {
                *sa += sb; *ca += cb;
            },
            (AggAccumulator::AvgF { sum: sa, count: ca }, AggAccumulator::AvgF { sum: sb, count: cb }) => {
                *sa += sb; *ca += cb;
            },
            _ => {}
        }
    }
}

pub fn parse_map_def(def: &str) -> Result<Vec<MapFieldSpec>> {
    let mut specs = Vec::new();
    for part in def.split(';') {
        let part = part.trim();
        let tokens: Vec<&str> = part.split('_').map(|t| t.trim()).collect();
        if tokens.len() != 3 { anyhow::bail!("Invalid map spec '{}': expected 3 tokens (index_role_type)", part); }

        let mut source = FieldSource::Line;
        let mut idx_str = tokens[0];
        if let Some(rest) = idx_str.strip_prefix('p') {
            source = FieldSource::Path;
            idx_str = rest;
        } else if let Some(rest) = idx_str.strip_prefix('P') {
            source = FieldSource::Path;
            idx_str = rest;
        } else if let Some(rest) = idx_str.strip_prefix('l') {
            source = FieldSource::Line;
            idx_str = rest;
        } else if let Some(rest) = idx_str.strip_prefix('L') {
            source = FieldSource::Line;
            idx_str = rest;
        }
        let idx: usize = idx_str.trim().parse().map_err(|e| anyhow::anyhow!("Invalid capture index '{}' in map spec '{}': {}", idx_str, part, e))?;

        let role = match tokens[1] {
            "k" => AggRole::Key,
            "s" => AggRole::Sum,
            "c" => AggRole::Count,
            "x" => AggRole::Max,
            "n" => AggRole::Min,
            "a" => AggRole::Avg,
            other => anyhow::bail!("Unknown role '{}' in map spec '{}'", other, part),
        };
        let dtype = match tokens[2] {
            "i" => AggType::I64,
            "u" => AggType::U64,
            "f" => AggType::F64,
            "s" => AggType::Str,
            other => anyhow::bail!("Unknown type '{}' in map spec '{}'", other, part),
        };
        specs.push(MapFieldSpec { capture_index: idx, role, dtype, source });
    }
    Ok(specs)
}

pub fn print_map_results(map: BTreeMap<Vec<AggValue>, Vec<AggAccumulator>>, specs: Vec<MapFieldSpec>) {
    println!("\n--- Aggregation Map Results ---");
    
    let mut key_indices = Vec::new();
    let mut val_indices = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        if spec.role == AggRole::Key { key_indices.push(i); } 
        else { val_indices.push(i); }
    }

    // Header
    let mut headers = Vec::new();
    for &i in &key_indices { 
        let prefix = match specs[i].source {
            FieldSource::Line => format!("Key_{}", specs[i].capture_index),
            FieldSource::Path => format!("Key_p{}", specs[i].capture_index),
        };
        headers.push(prefix);
    }
    for &i in &val_indices { 
        let prefix = match specs[i].source {
            FieldSource::Line => format!("{:?}_{}", specs[i].role, specs[i].capture_index),
            FieldSource::Path => format!("{:?}_p{}", specs[i].role, specs[i].capture_index),
        };
        headers.push(prefix);
    }
    println!("{}", headers.join("\t"));

    for (key, values) in map {
        let mut parts = Vec::new();
        for k in key {
            match k {
                AggValue::I64(v) => parts.push(v.to_string()),
                AggValue::U64(v) => parts.push(v.to_string()),
                AggValue::F64(v) => parts.push(v.0.to_string()),
                AggValue::Str(v) => parts.push(v),
                AggValue::Null => parts.push("".to_string()),
            }
        }
        for v in values {
            match v {
                AggAccumulator::SumI(x) => parts.push(x.to_string()),
                AggAccumulator::SumU(x) => parts.push(x.to_string()),
                AggAccumulator::SumF(x) => parts.push(x.to_string()),
                AggAccumulator::Count(x) => parts.push(x.to_string()),
                AggAccumulator::MaxI(x) => parts.push(x.to_string()),
                AggAccumulator::MaxU(x) => parts.push(x.to_string()),
                AggAccumulator::MaxF(x) => parts.push(x.to_string()),
                AggAccumulator::MaxStr(x) => parts.push(x),
                AggAccumulator::MinI(x) => parts.push(x.to_string()),
                AggAccumulator::MinU(x) => parts.push(x.to_string()),
                AggAccumulator::MinF(x) => parts.push(x.to_string()),
                AggAccumulator::MinStr(x) => parts.push(x),
                AggAccumulator::AvgI { sum, count } => {
                    if count > 0 { parts.push((sum as f64 / count as f64).to_string()) } else { parts.push(String::new()) }
                },
                AggAccumulator::AvgU { sum, count } => {
                    if count > 0 { parts.push((sum as f64 / count as f64).to_string()) } else { parts.push(String::new()) }
                },
                AggAccumulator::AvgF { sum, count } => {
                    if count > 0 { parts.push((sum / count as f64).to_string()) } else { parts.push(String::new()) }
                },
                AggAccumulator::None => parts.push("".to_string()),
            }
        }
        println!("{}", parts.join("\t"));
    }
    println!("-------------------------------");
}
