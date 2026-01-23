use dashmap::DashMap;
use std::fs;
use std::sync::atomic::AtomicUsize;
use std::time::SystemTime;

#[derive(Default)]
pub struct DbStats {
    pub matched_lines: AtomicUsize,
    pub committed_records: AtomicUsize,
    pub mapped_records: AtomicUsize,
    pub bytes_processed: AtomicUsize,
    // Error tracking
    pub total_errors: AtomicUsize,
    // Map of Capture Index -> Count
    pub error_counts: DashMap<usize, usize>,
}

pub struct RunMetadata {
    pub regex: String,
    pub command_args: String,
    pub created_at: String,
    pub cache_mb: i64,
    pub pre_sql: Vec<String>,
    pub post_sql: Vec<String>,
}

pub fn get_iso_time() -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%d %H:%M:%S.%3N"])
        .output()
        .ok();

    if let Some(o) = output {
        let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
        if !s.is_empty() {
            return s;
        }
    }
    format!("{:?}", SystemTime::now())
}

// Reads /proc/self/stat to get CPU usage (Linux only)
pub fn get_cpu_time_seconds() -> f64 {
    if let Ok(contents) = fs::read_to_string("/proc/self/stat") {
        let parts: Vec<&str> = contents.split_whitespace().collect();
        // Fields 13 (utime) and 14 (stime) are 0-indexed as 13 and 14
        if parts.len() > 14 {
            let utime: f64 = parts[13].parse().unwrap_or(0.0);
            let stime: f64 = parts[14].parse().unwrap_or(0.0);
            // Default Linux ticks per second is usually 100
            let ticks_per_sec = 100.0;
            return (utime + stime) / ticks_per_sec;
        }
    }
    0.0
}
