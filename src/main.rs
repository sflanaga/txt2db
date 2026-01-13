use anyhow::{Context, Result};
use clap::Parser;
use crossbeam_channel::{bounded, Receiver};
use regex::Regex;
use rusqlite::{params, Connection, types::ValueRef};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

// Import the local module
mod io_splicer;
use crate::io_splicer::{IoSplicer, SplicedChunk, SplicerConfig, SplicerStats};

#[derive(Parser, Debug)]
#[command(author, version, about)]
// term_width = 0 means "Auto-detect terminal width".
#[command(term_width = 0)] 
struct Cli {
    // --- Input Sources ---
    /// Files or directories to scan. If directories, they are walked recursively.
    #[arg(value_name = "INPUTS", help_heading = "Input Sources")]
    inputs: Vec<PathBuf>,

    /// Read list of files from Stdin.
    #[arg(long = "files-from-stdin", help_heading = "Input Sources")]
    files_from_stdin: bool,

    /// Read file list from a specific file.
    #[arg(long = "file-list", help_heading = "Input Sources")]
    file_list: Option<PathBuf>,

    /// Read content DATA directly from Stdin (no filename).
    #[arg(long = "data-stdin", help_heading = "Input Sources")]
    data_stdin: bool,


    // --- Parsing Options ---
    /// Regular Expression to parse lines.
    /// Capturing groups are extracted into columns.
    #[arg(short = 'r', long = "regex", help_heading = "Parsing Options")]
    regex: String,

    /// Optional: Regular Expression to parse File Paths.  
    /// If provided, files not matching this regex are ignored.  
    /// Capturing groups are extracted into columns.
    #[arg(long = "path-regex", help_heading = "Parsing Options", verbatim_doc_comment)]
    path_regex: Option<String>,

    /// File path filter (regex) for directory walking. 
    /// (Distinct from --path-regex, which extracts fields)
    #[arg(short = 'f', long = "filter", help_heading = "Parsing Options", verbatim_doc_comment)]
    filter_pattern: Option<String>,

    /// Disable recursive directory walking
    #[arg(long = "no-recursive", help_heading = "Parsing Options")]
    no_recursive: bool,

    /// Field mapping (e.g., "p1:host;l1:date"). 
    /// Prefixes: 'p' for path capture groups, 'l' for line capture groups.
    /// If omitted, defaults to pf_N (path) and lf_N (line) or f_N.
    #[arg(short = 'F', long = "fields", help_heading = "Parsing Options", verbatim_doc_comment)]
    field_map: Option<String>,


    // --- Database Options ---
    /// Database output file. Defaults to scan_HHMMSS.db
    #[arg(long, help_heading = "Database Options")]
    db_path: Option<String>,

    /// Enable optional tracking tables (files, matches) to store full context
    #[arg(long, help_heading = "Database Options")]
    track_matches: bool,

    /// Batch size for DB inserts
    #[arg(long, default_value = "1000", help_heading = "Database Options")]
    batch_size: usize,

    /// SQLite Cache Size in MB
    #[arg(long = "cache-mb", default_value_t = 100, help_heading = "Database Options")]
    cache_mb: i64,


    // --- SQL Hooks ---
    /// Execute SQL string BEFORE scanning starts
    #[arg(long = "pre-sql", help_heading = "SQL Hooks")]
    pre_sql: Option<String>,

    /// Execute SQL string AFTER scanning finishes
    #[arg(long = "post-sql", help_heading = "SQL Hooks")]
    post_sql: Option<String>,

    /// Execute SQL script from file BEFORE scanning starts
    #[arg(long = "pre-sql-file", help_heading = "SQL Hooks")]
    pre_sql_file: Option<PathBuf>,

    /// Execute SQL script from file AFTER scanning finishes
    #[arg(long = "post-sql-file", help_heading = "SQL Hooks")]
    post_sql_file: Option<PathBuf>,


    // --- Performance/System ---
    /// Stats ticker interval in milliseconds
    #[arg(long = "ticker", default_value_t = 1000, help_heading = "Performance")]
    ticker_interval: u64,

    /// Number of file splicer threads
    #[arg(short = 's', long = "splicers", help_heading = "Performance")]
    splicer_threads: Option<usize>,

    /// Number of regex parser threads
    #[arg(short = 'p', long = "parsers", help_heading = "Performance")]
    parser_threads: Option<usize>,
}

#[derive(Clone, Debug)]
enum FieldSource {
    Path(usize),
    Line(usize),
}

#[derive(Clone, Debug)]
struct ColumnDef {
    name: String,
    source: FieldSource,
}

enum DbRecord {
    Data {
        file_path: Option<Arc<PathBuf>>,
        offset: u64,
        line_content: String,
        fields: Vec<Option<String>>,
    },
}

#[derive(Default)]
struct DbStats {
    matched_lines: AtomicUsize,
    committed_records: AtomicUsize,
    bytes_processed: AtomicUsize,
}

struct RunMetadata {
    regex: String,
    command_args: String,
    created_at: String,
    cache_mb: i64,
    pre_sql: Vec<String>,
    post_sql: Vec<String>,
}

/// Helper to split SQL safely respecting quotes and comments
fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    
    // States
    let mut in_quote = false;
    let mut quote_char = '\0';
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(c) = chars.next() {
        current.push(c);

        if in_line_comment {
            if c == '\n' { in_line_comment = false; }
        } else if in_block_comment {
            if c == '*' && chars.peek() == Some(&'/') {
                current.push(chars.next().unwrap());
                in_block_comment = false;
            }
        } else if in_quote {
            if c == quote_char {
                // Check escape (doubled quote)
                if chars.peek() == Some(&quote_char) {
                    current.push(chars.next().unwrap());
                } else {
                    in_quote = false;
                }
            }
        } else {
            // Normal State
            match c {
                '\'' | '"' => {
                    in_quote = true;
                    quote_char = c;
                },
                '-' => {
                    if chars.peek() == Some(&'-') {
                        current.push(chars.next().unwrap());
                        in_line_comment = true;
                    }
                },
                '/' => {
                    if chars.peek() == Some(&'*') {
                        current.push(chars.next().unwrap());
                        in_block_comment = true;
                    }
                },
                ';' => {
                    // Split point!
                    let stmt = current.trim().to_string();
                    if !stmt.is_empty() {
                        stmts.push(stmt);
                    }
                    current = String::new();
                },
                _ => {}
            }
        }
    }
    
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        stmts.push(stmt);
    }
    stmts
}

fn execute_and_print_sql(conn: &Connection, sql_scripts: &[String], stage: &str) -> Result<()> {
    for (i, script) in sql_scripts.iter().enumerate() {
        if script.trim().is_empty() { continue; }
        
        let statements = split_sql_statements(script);
        
        if !statements.is_empty() {
            println!("--- [Executing {} SQL Block #{} ({} statements)] ---", stage, i+1, statements.len());
        }

        for stmt_sql in statements {
            let clean_sql = stmt_sql.trim_end_matches(';');
            
            let mut stmt = conn.prepare(clean_sql).context(format!("Failed to prepare SQL: {}", clean_sql))?;
            
            if stmt.column_count() > 0 {
                let col_count = stmt.column_count();
                let col_names: Vec<String> = (0..col_count).map(|i| stmt.column_name(i).unwrap_or("?").to_string()).collect();
                
                println!("> Query: {}", clean_sql);
                println!("{}", col_names.join("\t"));
                println!("{}", "-".repeat(col_names.len() * 10));

                let mut rows = stmt.query([])?;
                let mut row_count = 0;
                while let Some(row) = rows.next()? {
                    row_count += 1;
                    let values: Vec<String> = (0..col_count).map(|i| {
                        match row.get_ref(i).unwrap() {
                            ValueRef::Null => "NULL".to_string(),
                            ValueRef::Integer(i) => i.to_string(),
                            ValueRef::Real(f) => f.to_string(),
                            ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                            ValueRef::Blob(_) => "<BLOB>".to_string(),
                        }
                    }).collect();
                    println!("{}", values.join("\t"));
                }
                println!("({} rows)\n", row_count);
            } else {
                stmt.execute([])?;
            }
        }
    }
    Ok(())
}


fn get_iso_time() -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%d %H:%M:%S.%3N"])
        .output()
        .ok();
    
    if let Some(o) = output {
        let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
        if !s.is_empty() { return s; }
    }
    format!("{:?}", SystemTime::now())
}

fn main() -> Result<()> {
    let raw_args: Vec<String> = std::env::args().collect();
    let command_line = raw_args.join(" ");
    let cli = Cli::parse();

    // Collect Pre/Post SQL
    let mut pre_sql_scripts = Vec::new();
    if let Some(s) = &cli.pre_sql { pre_sql_scripts.push(s.clone()); }
    if let Some(p) = &cli.pre_sql_file { pre_sql_scripts.push(fs::read_to_string(p)?); }

    let mut post_sql_scripts = Vec::new();
    if let Some(s) = &cli.post_sql { post_sql_scripts.push(s.clone()); }
    if let Some(p) = &cli.post_sql_file { post_sql_scripts.push(fs::read_to_string(p)?); }


    // Setup Regexes
        // FIX: Use RegexBuilder to force multi-line mode.
    // This ensures '^' matches the start of any line in the chunk,
    // not just the start of the chunk itself.
    let line_re = regex::RegexBuilder::new(&cli.regex)
        .multi_line(true) // <--- CRITICAL FIX
        .build()
        .context("Invalid Line Regex")?;
    let path_re = if let Some(pr) = &cli.path_regex {
        Some(Regex::new(pr).context("Invalid Path Regex")?)
    } else {
        None
    };

    // Setup Columns
    let mut columns = Vec::new();
    if let Some(map_str) = &cli.field_map {
        for part in map_str.split(';') {
            let kv: Vec<&str> = part.split(':').collect();
            if kv.len() == 2 {
                let key = kv[0]; 
                let name = kv[1].to_string();
                if let Some(idx_str) = key.strip_prefix('p') {
                    let idx: usize = idx_str.parse().context("Invalid path index")?;
                    columns.push(ColumnDef { name, source: FieldSource::Path(idx) });
                } else if let Some(idx_str) = key.strip_prefix('l') {
                    let idx: usize = idx_str.parse().context("Invalid line index")?;
                    columns.push(ColumnDef { name, source: FieldSource::Line(idx) });
                } else {
                    let idx: usize = key.parse().context("Invalid index")?;
                    columns.push(ColumnDef { name, source: FieldSource::Line(idx) });
                }
            }
        }
    } else {
        if let Some(pre) = &path_re {
            for i in 1..pre.captures_len() {
                columns.push(ColumnDef { name: format!("pf_{}", i), source: FieldSource::Path(i) });
            }
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

    let db_filename = cli.db_path.clone().unwrap_or_else(|| {
        let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let seconds_in_day = secs % 86400;
        let hour = seconds_in_day / 3600;
        let minute = (seconds_in_day % 3600) / 60;
        let second = seconds_in_day % 60;
        format!("scan_{:02}{:02}{:02}.db", hour, minute, second)
    });
    println!("Database: {}", db_filename);

    let total_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let splicer_count = cli.splicer_threads.unwrap_or_else(|| std::cmp::max(1, total_cores / 2));
    let parser_count = cli.parser_threads.unwrap_or_else(|| std::cmp::max(1, total_cores.saturating_sub(splicer_count)));
    
    let path_filter = if let Some(pattern) = cli.filter_pattern {
        Some(Regex::new(&pattern).context("Invalid filter regex")?)
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
            let matches = mon_db.matched_lines.load(Ordering::Relaxed);
            let commits = mon_db.committed_records.load(Ordering::Relaxed);
            let bytes = mon_db.bytes_processed.load(Ordering::Relaxed);
            
            let files_d = files - last_files;
            let recs_d = commits - last_recs;
            let bytes_d = bytes - last_bytes;
            
            last_files = files;
            last_recs = commits;
            last_bytes = bytes;

            let mb_total = bytes as f64 / 1024.0 / 1024.0;
            let mb_rate = bytes_d as f64 / 1024.0 / 1024.0;
            let rate_factor = 1000.0 / tick_duration.as_millis() as f64;
            let display_mb_rate = mb_rate * rate_factor;
            let display_files_rate = files_d as f64 * rate_factor;
            let display_recs_rate = recs_d as f64 * rate_factor;

            println!("Stats: [Files: {}/{} ({:.0}/s)] [Data: {:.1}MB ({:.1}MB/s)] [Matches: {}] [DB Insert: {} ({:.0}/s)]", 
                files, skipped, display_files_rate, mb_total, display_mb_rate, matches, commits, display_recs_rate);
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

    // DB Worker
    let batch_size = cli.batch_size;
    let track_matches = cli.track_matches;
    let col_defs_for_db = columns.iter().map(|c| c.name.clone()).collect();
    let db_worker_stats = db_stats.clone();
    let db_splicer_stats = splicer_stats.clone();
    
    let db_handle = thread::spawn(move || {
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
    });

    // Parsers
    let mut handles = vec![];
    for _ in 0..parser_count {
        let rx = splicer_rx.clone();
        let r_tx = recycle_tx.clone();
        let d_tx = db_tx.clone();
        let thread_line_re = line_re.clone(); 
        let thread_path_re = path_re.clone();
        let thread_columns = columns.clone();
        let t_stats = db_stats.clone();

        handles.push(thread::spawn(move || {
            while let Ok(chunk) = rx.recv() {
                let mut data = chunk.data;
                t_stats.bytes_processed.fetch_add(data.len(), Ordering::Relaxed);

                let path_arc = chunk.file_path;
                let chunk_offset = chunk.offset;
                
                let mut should_process = true;
                let mut path_fields = Vec::with_capacity(thread_columns.len());

                if let Some(pre) = &thread_path_re {
                    if let Some(p) = &path_arc {
                        let p_str = p.to_string_lossy();
                        if let Some(caps) = pre.captures(&p_str) {
                             for col in &thread_columns {
                                 if let FieldSource::Path(idx) = col.source {
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
                    
                    for capture in thread_line_re.captures_iter(&s) {
                        t_stats.matched_lines.fetch_add(1, Ordering::Relaxed);
                        
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

                        let mut fields = Vec::with_capacity(thread_columns.len());
                        for col in &thread_columns {
                            match col.source {
                                FieldSource::Line(idx) => {
                                    fields.push(capture.get(idx).map(|m| m.as_str().to_string()));
                                },
                                FieldSource::Path(idx) => {
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
                        if d_tx.send(record).is_err() { break; }
                    }
                }
                data.clear();
                let _ = r_tx.send(data);
            }
        }));
    }

    println!("Starting DB Ingestion Scan...");
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

    for h in handles { h.join().unwrap(); }
    drop(db_tx);
    let run_id = db_handle.join().unwrap()?;

    // Final Stats
    let duration = start_time.elapsed().as_secs_f64();
    let files = splicer_stats.file_count.load(Ordering::Relaxed);
    let skipped = splicer_stats.skipped_count.load(Ordering::Relaxed);
    let bytes = db_stats.bytes_processed.load(Ordering::Relaxed);
    let matches = db_stats.matched_lines.load(Ordering::Relaxed);
    let commits = db_stats.committed_records.load(Ordering::Relaxed);
    let mb_total = bytes as f64 / 1024.0 / 1024.0;
    let mb_rate = if duration > 0.0 { mb_total / duration } else { 0.0 };
    let files_rate = if duration > 0.0 { files as f64 / duration } else { 0.0 };
    let recs_rate = if duration > 0.0 { commits as f64 / duration } else { 0.0 };

    println!("Done:  [Files: {}/{} ({:.0}/s)] [Data: {:.1}MB ({:.1}MB/s)] [Matches: {}] [DB Insert: {} ({:.0}/s)]", 
                files, skipped, files_rate, mb_total, mb_rate, matches, commits, recs_rate);
    println!("Run ID: {}", run_id);
    println!("Data Table: data_{}", run_id);

    Ok(())
}

fn run_db_worker(
    path: String, 
    rx: Receiver<DbRecord>, 
    batch_size: usize,
    track_matches: bool,
    columns: Vec<String>,
    stats: Arc<DbStats>,
    splicer_stats: Arc<SplicerStats>,
    meta: RunMetadata,
) -> Result<i64> {
    let mut conn = Connection::open(path)?;
    
    // Performance Tunings
    conn.execute_batch("
        PRAGMA synchronous = OFF;
        PRAGMA journal_mode = MEMORY;
        PRAGMA temp_store = 2;
    ")?;
    
    // Set Cache Size
    let cache_kib = meta.cache_mb * 1024;
    let cache_pragma = format!("PRAGMA cache_size = -{};", cache_kib); // Negative means KiB
    conn.execute(&cache_pragma, [])?;

    // --- PRE-RUN SQL ---
    if !meta.pre_sql.is_empty() {
        execute_and_print_sql(&conn, &meta.pre_sql, "PRE")?;
    }

    // 1. Setup Shared Tables
    conn.execute(
        "CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY,
            timestamp TEXT,
            command TEXT,
            regex TEXT,
            files_processed INTEGER DEFAULT 0,
            files_skipped INTEGER DEFAULT 0,
            bytes_processed INTEGER DEFAULT 0,
            match_count INTEGER DEFAULT 0,
            finished_at TEXT
        )", 
        [],
    )?;
    conn.execute("INSERT INTO runs (timestamp, command, regex) VALUES (?, ?, ?)",
        params![meta.created_at, meta.command_args, meta.regex])?;
    let run_id = conn.last_insert_rowid();

    let _ = conn.execute("ALTER TABLE files ADD COLUMN run_id INTEGER", []);
    conn.execute("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, run_id INTEGER, path TEXT)", [])?;
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_files_run_path ON files(run_id, path)", [])?;

    // 2. Setup Dynamic Tables
    let data_table_name = format!("data_{}", run_id);
    let matches_table_name = format!("matches_{}", run_id);

    if track_matches {
        conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, file_id INTEGER, offset INTEGER, content TEXT)", matches_table_name), [])?;
    }

    let mut col_defs = String::new();
    for col in &columns { col_defs.push_str(&format!(", {} TEXT", col)); }
    let match_id_col = if track_matches { ", match_id INTEGER" } else { "" };
    
    conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, run_id INTEGER, file_id INTEGER{}{})", 
        data_table_name, match_id_col, col_defs), [])?;

    // 3. Process Data
    let mut file_cache: HashMap<PathBuf, i64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);
    
    while let Ok(msg) = rx.recv() {
        batch.push(msg);
        if batch.len() >= batch_size {
            flush_batch(&mut conn, &mut batch, &mut file_cache, track_matches, &columns, &stats, &data_table_name, &matches_table_name, run_id)?;
        }
    }
    if !batch.is_empty() {
        flush_batch(&mut conn, &mut batch, &mut file_cache, track_matches, &columns, &stats, &data_table_name, &matches_table_name, run_id)?;
    }

    // Update Runs with Final Stats
    let final_files = splicer_stats.file_count.load(Ordering::Relaxed);
    let final_skipped = splicer_stats.skipped_count.load(Ordering::Relaxed);
    let final_bytes = stats.bytes_processed.load(Ordering::Relaxed);
    let final_matches = stats.matched_lines.load(Ordering::Relaxed);
    let finished_at = get_iso_time();

    conn.execute("UPDATE runs SET files_processed = ?, files_skipped = ?, bytes_processed = ?, match_count = ?, finished_at = ? WHERE id = ?",
        params![final_files, final_skipped, final_bytes, final_matches, finished_at, run_id])?;


    // --- CREATE LATEST VIEWS ---
    let _ = conn.execute("DROP VIEW IF EXISTS data", []);
    let create_view = format!("CREATE VIEW data AS SELECT * FROM {}", data_table_name);
    if let Err(e) = conn.execute(&create_view, []) {
        eprintln!("Warning: Could not create 'data' view: {}", e);
    }
    
    if track_matches {
        let _ = conn.execute("DROP VIEW IF EXISTS matches", []);
        let create_view = format!("CREATE VIEW matches AS SELECT * FROM {}", matches_table_name);
         let _ = conn.execute(&create_view, []);
    }

    // --- POST-RUN SQL ---
    if !meta.post_sql.is_empty() {
        execute_and_print_sql(&conn, &meta.post_sql, "POST")?;
    }

    Ok(run_id)
}

fn flush_batch(
    conn: &mut Connection, 
    batch: &mut Vec<DbRecord>, 
    file_cache: &mut HashMap<PathBuf, i64>,
    track_matches: bool,
    columns: &[String],
    stats: &Arc<DbStats>,
    data_table: &str,
    matches_table: &str,
    run_id: i64,
) -> Result<()> {
    let tx = conn.transaction()?;
    
    for record in batch.drain(..) {
        let DbRecord::Data { file_path, offset, line_content, fields } = record;
            
        let mut current_match_id = None;

        let file_id = if let Some(p) = &file_path {
            if let Some(&id) = file_cache.get(&**p) {
                id
            } else {
                let path_str = p.to_string_lossy();
                tx.execute("INSERT OR IGNORE INTO files (run_id, path) VALUES (?, ?)", params![run_id, path_str])?;
                let mut stmt = tx.prepare("SELECT id FROM files WHERE run_id = ? AND path = ?")?;
                let id: i64 = stmt.query_row(params![run_id, path_str], |row| row.get(0))?;
                file_cache.insert((**p).clone(), id);
                id
            }
        } else { 0 };

        if track_matches {
            let sql = format!("INSERT INTO {} (file_id, offset, content) VALUES (?, ?, ?)", matches_table);
            tx.execute(&sql, params![file_id, offset as i64, line_content])?;
            current_match_id = Some(tx.last_insert_rowid());
        }

        let mut place_holders = String::new();
        let mut values: Vec<String> = Vec::new();
        
        place_holders.push_str("?, ?, ");
        values.push(run_id.to_string());
        values.push(file_id.to_string());

        if track_matches {
            place_holders.push_str("?, ");
            values.push(current_match_id.unwrap().to_string());
        }

        for (i, field) in fields.iter().enumerate() {
            if i > 0 { place_holders.push_str(", "); }
            place_holders.push_str("?");
            values.push(field.clone().unwrap_or_default());
        }

        let match_col = if track_matches { "match_id, " } else { "" };
        let col_names = columns.join(", ");
        let sql = format!("INSERT INTO {} (run_id, file_id, {}{}) VALUES ({})", data_table, match_col, col_names, place_holders);
        let params_refs: Vec<&dyn rusqlite::ToSql> = values.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        tx.execute(&sql, &*params_refs)?;
        
        stats.committed_records.fetch_add(1, Ordering::Relaxed);
    }

    tx.commit()?;
    Ok(())
}
