use anyhow::{Context, Result};
use clap::Parser;
use crossbeam_channel::bounded;
use regex::Regex;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

// --- Modules ---
mod config;
mod io_splicer;
mod stats;
mod aggregation;
mod database;
mod parser;

use crate::config::{Cli, DisableConfig};
use crate::io_splicer::{IoSplicer, SplicedChunk, SplicerConfig, SplicerStats};
use crate::stats::{DbStats, RunMetadata, get_cpu_time_seconds, get_iso_time};
use crate::aggregation::{
    AggAccumulator, AggValue, 
    parse_map_def, print_map_results
};
use crate::database::{
    DbRecord, ColumnDef, FieldSource, run_db_worker
};
use crate::parser::{run_mapper_worker, run_db_parser, AnyRegex};

fn main() -> Result<()> {
    let raw_args: Vec<String> = std::env::args().collect();
    let command_line = raw_args.join(" ");
    let cli = Cli::parse();
    
    // Parsing disable options
    let disable_ops = DisableConfig::from_str(cli.disable_operations.as_deref());
    if cli.disable_operations.is_some() {
        println!("Performance Mode: Disabling operations: {:?}", disable_ops);
    }

    // Collect Pre/Post SQL
    let mut pre_sql_scripts = Vec::new();
    if let Some(s) = &cli.pre_sql { pre_sql_scripts.push(s.clone()); }
    if let Some(p) = &cli.pre_sql_file { pre_sql_scripts.push(fs::read_to_string(p)?); }

    let mut post_sql_scripts = Vec::new();
    if let Some(s) = &cli.post_sql { post_sql_scripts.push(s.clone()); }
    if let Some(p) = &cli.post_sql_file { post_sql_scripts.push(fs::read_to_string(p)?); }


    // Setup Regexes
    let line_re = if cli.use_pcre2 {
        let re = pcre2::bytes::RegexBuilder::new()
            .jit_if_available(true)
            .build(&cli.regex)
            .map_err(|e| anyhow::anyhow!("Invalid PCRE2 Regex (--regex): {}", e))?;
        AnyRegex::Pcre(re)
    } else {
        let re = regex::RegexBuilder::new(&cli.regex)
            .multi_line(true)
            .build()
            .context("Invalid Line Regex (--regex)")?;
        AnyRegex::Std(re)
    };

    let path_re = if let Some(pr) = &cli.path_regex {
        Some(Regex::new(pr).context("Invalid Path Regex (--path-regex)")?)
    } else {
        None
    };

    // Setup Map Specs
    let map_specs = if let Some(def) = &cli.map_def {
        Some(parse_map_def(def).context(format!("Invalid map definition (--map): '{}'", def))?)
    } else {
        None
    };

    // Setup Columns
    let mut columns = Vec::new();
    if let Some(map_str) = &cli.field_map {
        for part in map_str.split(';') {
            let kv: Vec<&str> = part.split(':').collect();
            if kv.len() == 2 {
                let key = kv[0].trim(); 
                let name = kv[1].to_string();
                if let Some(idx_str) = key.strip_prefix('p') {
                    let idx: usize = idx_str.trim().parse().map_err(|e| anyhow::anyhow!("Invalid path index '{}' in --fields part '{}': {}", idx_str, part, e))?;
                    columns.push(ColumnDef { name, source: FieldSource::Path(idx) });
                } else if let Some(idx_str) = key.strip_prefix('l') {
                    let idx: usize = idx_str.trim().parse().map_err(|e| anyhow::anyhow!("Invalid line index '{}' in --fields part '{}': {}", idx_str, part, e))?;
                    columns.push(ColumnDef { name, source: FieldSource::Line(idx) });
                } else {
                    let idx: usize = key.trim().parse().map_err(|e| anyhow::anyhow!("Invalid index '{}' in --fields part '{}': {}", key, part, e))?;
                    columns.push(ColumnDef { name, source: FieldSource::Line(idx) });
                }
            }
        }
    } else {
        if let Some(pre) = &path_re {
            for i in 1..pre.captures_len() {
                columns.push(ColumnDef { name: format!("pf_{}", i), source: FieldSource::Path(i) });
            }
            // Use line_re.captures_len() which is now abstract
            for i in 1..line_re.captures_len() {
                columns.push(ColumnDef { name: format!("lf_{}", i), source: FieldSource::Line(i) });
            }
             if line_re.captures_len() == 1 && columns.iter().all(|c| matches!(c.source, FieldSource::Path(_))) {
                 columns.push(ColumnDef { name: "lf_0".to_string(), source: FieldSource::Line(0) });
             }
        } else {
            let cap_len = line_re.captures_len();
            for i in 1..cap_len {
                columns.push(ColumnDef { name: format!("f_{}", i), source: FieldSource::Line(i) });
            }
            if columns.is_empty() {
                 columns.push(ColumnDef { name: "f_0".to_string(), source: FieldSource::Line(0) });
            }
        }
    }

    let total_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let splicer_count = cli.splicer_threads.unwrap_or_else(|| std::cmp::max(1, total_cores / 2));
    
    let path_filter = if let Some(pattern) = cli.filter_pattern {
        Some(Regex::new(&pattern).context("Invalid filter regex (--filter)")?)
    } else {
        None
    };

    let config = SplicerConfig {
        chunk_size: 256 * 1024,
        max_buffer_size: 1024 * 1024,
        path_filter, 
        thread_count: splicer_count,
    };

    let splicer_stats = Arc::new(SplicerStats::default());
    let db_stats = Arc::new(DbStats::default());

    let (splicer_tx, splicer_rx) = bounded::<SplicedChunk>(256);
    let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(512);
    
    // DB Channel (only used in DB mode)
    let (db_tx, db_rx) = bounded::<DbRecord>(4096);
    
    let start_time = Instant::now();

    // Ticker
    let mon_splicer = splicer_stats.clone();
    let mon_db = db_stats.clone();
    let tick_duration = Duration::from_millis(cli.ticker_interval);

    thread::spawn(move || {
        let mut last_files = 0;
        let mut last_recs = 0;
        let mut last_bytes = 0;

        loop {
            thread::sleep(tick_duration);
            let files = mon_splicer.file_count.load(Ordering::Relaxed);
            let skipped = mon_splicer.skipped_count.load(Ordering::Relaxed);
            
            // Show DB or Map count
            let commits = mon_db.committed_records.load(Ordering::Relaxed);
            let mapped = mon_db.mapped_records.load(Ordering::Relaxed);
            let total_items = commits + mapped;
            
            let bytes = mon_db.bytes_processed.load(Ordering::Relaxed);
            
            let files_d = files - last_files;
            let recs_d = total_items - last_recs;
            let bytes_d = bytes - last_bytes;
            
            last_files = files;
            last_recs = total_items;
            last_bytes = bytes;

            let mb_total = bytes as f64 / 1024.0 / 1024.0;
            let mb_rate = bytes_d as f64 / 1024.0 / 1024.0;
            let rate_factor = 1000.0 / tick_duration.as_millis() as f64;
            let display_mb_rate = mb_rate * rate_factor;
            let display_files_rate = files_d as f64 * rate_factor;
            let display_recs_rate = recs_d as f64 * rate_factor;

            println!("Stats: [Files: {}/{} ({:.0}/s)] [Data: {:.1}MB ({:.1}MB/s)] [Processed: {} ({:.0}/s)]", 
                files, skipped, display_files_rate, mb_total, display_mb_rate, total_items, display_recs_rate);
        }
    });

    // Run Metadata
    let run_meta = RunMetadata {
        regex: cli.regex.clone(),
        command_args: command_line,
        created_at: get_iso_time(),
        cache_mb: cli.cache_mb,
        pre_sql: pre_sql_scripts,
        post_sql: post_sql_scripts,
    };

    // --- Mode Selection ---
    let mut db_handle = None;
    
    // Explicit types for handles to solve E0282
    let mut parser_handles: Vec<JoinHandle<()>> = Vec::new();
    let mut map_handles: Vec<JoinHandle<BTreeMap<Vec<AggValue>, Vec<AggAccumulator>>>> = Vec::new();

    if let Some(agg_specs) = map_specs.clone() {
        // --- MAP MODE ---
        // Spawn multiple MAP threads that consume chunks and return BTreeMaps
        let map_thread_count = cli.map_threads.unwrap_or_else(|| std::cmp::max(1, total_cores / 2));
        println!("Starting Aggregation Scan with {} mapper threads...", map_thread_count);

        let show_errors = cli.show_errors;
        let stop_on_error = cli.stop_on_error;
        let enable_profiling = cli.profile;
        let flags = Arc::new(disable_ops);

        for _ in 0..map_thread_count {
            let rx = splicer_rx.clone();
            let r_tx = recycle_tx.clone();
            let specs = agg_specs.clone();
            let stats = db_stats.clone();
            let l_re = line_re.clone();
            let t_flags = flags.clone();
            let thread_path_re = path_re.clone();
            
            map_handles.push(thread::spawn(move || {
                run_mapper_worker(rx, r_tx, specs, l_re, thread_path_re, stats, show_errors, stop_on_error, enable_profiling, t_flags)
            }));
        }

    } else {
        // --- DB MODE ---
        let db_filename = cli.db_path.clone().unwrap_or_else(|| {
            let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let seconds_in_day = secs % 86400;
            let hour = seconds_in_day / 3600;
            let minute = (seconds_in_day % 3600) / 60;
            let second = seconds_in_day % 60;
            format!("scan_{:02}{:02}{:02}.db", hour, minute, second)
        });
        println!("Database: {}", db_filename);

        // Spawn DB Worker
        let batch_size = cli.batch_size;
        let track_matches = cli.track_matches;
        let col_defs_for_db = columns.iter().map(|c| c.name.clone()).collect();
        let db_worker_stats = db_stats.clone();
        let db_splicer_stats = splicer_stats.clone();
        
        db_handle = Some(thread::spawn(move || {
            run_db_worker(
                db_filename, 
                db_rx, 
                batch_size, 
                track_matches, 
                col_defs_for_db, 
                db_worker_stats,
                db_splicer_stats,
                run_meta
            )
        }));

        // Spawn DB Parsers
        let parser_count = cli.parser_threads.unwrap_or_else(|| std::cmp::max(1, total_cores.saturating_sub(splicer_count)));
        for _ in 0..parser_count {
            let rx = splicer_rx.clone();
            let r_tx = recycle_tx.clone();
            let d_tx = db_tx.clone();
            let thread_line_re = line_re.clone(); 
            let thread_path_re = path_re.clone();
            let thread_columns = columns.clone();
            let t_stats = db_stats.clone();

            parser_handles.push(thread::spawn(move || {
                run_db_parser(rx, r_tx, d_tx, thread_line_re, thread_path_re, thread_columns, t_stats)
            }));
        }
        println!("Starting DB Ingestion Scan...");
    }

    let splicer = IoSplicer::new(config, splicer_stats.clone(), splicer_tx, recycle_rx);

    // Input Aggregation
    if cli.data_stdin {
        splicer.run_stream(io::stdin())?;
    } else {
        let mut path_iterators: Vec<Box<dyn Iterator<Item = PathBuf> + Send>> = Vec::new();

        if !cli.inputs.is_empty() {
             for path in cli.inputs {
                 if path.is_dir() {
                     let recursive = !cli.no_recursive;
                     let max_depth = if recursive { usize::MAX } else { 1 };
                     let walker = WalkDir::new(path).max_depth(max_depth);
                     let iter = walker.into_iter()
                         .filter_map(|e| e.ok())
                         .filter(|e| e.file_type().is_file())
                         .map(|e| e.path().to_path_buf());
                     path_iterators.push(Box::new(iter));
                 } else {
                     path_iterators.push(Box::new(std::iter::once(path)));
                 }
             }
        }

        if cli.files_from_stdin {
             let stdin_iter = io::stdin().lock().lines()
                 .filter_map(|l| l.ok())
                 .filter(|l| !l.trim().is_empty())
                 .map(|l| PathBuf::from(l.trim()));
             let paths: Vec<PathBuf> = stdin_iter.collect();
             path_iterators.push(Box::new(paths.into_iter()));
        }

        if let Some(list_path) = cli.file_list {
             let file = File::open(list_path).context("Cannot open file list")?;
             let buf = BufReader::new(file);
             let file_iter = buf.lines()
                 .filter_map(|l| l.ok())
                 .filter(|l| !l.trim().is_empty())
                 .map(|l| PathBuf::from(l.trim()));
             path_iterators.push(Box::new(file_iter));
        }

        if path_iterators.is_empty() {
            println!("No input sources provided. Defaulting to scanning current directory.");
            let walker = WalkDir::new(".").max_depth(if cli.no_recursive { 1 } else { usize::MAX });
            let iter = walker.into_iter()
                 .filter_map(|e| e.ok())
                 .filter(|e| e.file_type().is_file())
                 .map(|e| e.path().to_path_buf());
            path_iterators.push(Box::new(iter));
        }

        let unified_iter = path_iterators.into_iter().flatten();
        splicer.run(unified_iter)?;
    }

    drop(splicer);

    // Finalize
    for h in parser_handles { h.join().unwrap(); }
    drop(db_tx);
    
    // DB Finalization
    if let Some(h) = db_handle {
        let run_id = h.join().unwrap()?;
        println!("Run ID: {}", run_id);
        println!("Data Table: data_{}", run_id);
    }
    
    // Map Merging
    if !map_handles.is_empty() {
        println!("Merging results from {} threads...", map_handles.len());
        let mut final_map: BTreeMap<Vec<AggValue>, Vec<AggAccumulator>> = BTreeMap::new();
        
        for h in map_handles {
            let sub_map = h.join().unwrap();
            for (key, val) in sub_map {
                match final_map.entry(key) {
                    Entry::Vacant(e) => {
                        e.insert(val);
                    },
                    Entry::Occupied(mut e) => {
                        let existing = e.get_mut();
                        for (acc, other) in existing.iter_mut().zip(val.into_iter()) {
                            acc.merge(other);
                        }
                    }
                }
            }
        }
        
        print_map_results(final_map, map_specs.unwrap());
    }

    // Final Stats
    let duration = start_time.elapsed().as_secs_f64();
    let cpu_seconds = get_cpu_time_seconds(); // Get CPU time
    let files = splicer_stats.file_count.load(Ordering::Relaxed);
    let skipped = splicer_stats.skipped_count.load(Ordering::Relaxed);
    let bytes = db_stats.bytes_processed.load(Ordering::Relaxed);
    let matches = db_stats.matched_lines.load(Ordering::Relaxed);
    let total_recs = db_stats.committed_records.load(Ordering::Relaxed) + db_stats.mapped_records.load(Ordering::Relaxed);
    let parse_errors = db_stats.total_errors.load(Ordering::Relaxed);
    
    let mb_total = bytes as f64 / 1024.0 / 1024.0;
    let mb_rate = if duration > 0.0 { mb_total / duration } else { 0.0 };
    let files_rate = if duration > 0.0 { files as f64 / duration } else { 0.0 };
    let recs_rate = if duration > 0.0 { total_recs as f64 / duration } else { 0.0 };

    // CPU Stats
    let cpu_stats = if cpu_seconds > 0.0 {
        format!(" [CPU: {:.2}s]", cpu_seconds)
    } else {
        String::new()
    };

    println!("Done:  [Files: {}/{} ({:.0}/s)] [Data: {:.1}MB ({:.1}MB/s)] [Matches: {}] [Processed: {} ({:.0}/s)] [Parse Errors: {}]{}", 
                files, skipped, files_rate, mb_total, mb_rate, matches, total_recs, recs_rate, parse_errors, cpu_stats);

    println!("Timing: [Wall Time: {:.3}s] [CPU Time: {:.3}s]", duration, cpu_seconds);

    if parse_errors > 0 {
        println!("\n--- Parse Errors by Field Index ---");
        // FIXED: Explicit type annotation for DashMap iterator
        let mut err_list: Vec<(usize, usize)> = db_stats.error_counts.iter().map(|r| (*r.key(), *r.value())).collect();
        err_list.sort_by_key(|k| k.0);
        for (idx, count) in err_list {
            println!("Capture Group {}: {} errors", idx, count);
        }
    }

    Ok(())
}
