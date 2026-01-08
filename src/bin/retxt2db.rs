use anyhow::{Context, Result};
use clap::Parser;
use crossbeam_channel::{bounded, Receiver};
use io_splicer_demo::{InputSource, IoSplicer, SplicedChunk, SplicerConfig, SplicerStats};
use regex::Regex;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// Target directory to scan (optional if --stdin used)
    #[arg(default_value = ".")]
    target_dir: PathBuf,

    /// Read list of files from Stdin instead of walking a directory
    #[arg(long = "stdin")]
    read_stdin_files: bool,

    /// Regular Expression to parse lines
    #[arg(short = 'r', long = "regex")]
    regex: String,

    /// Optional: Regular Expression to parse File Paths. 
    /// If provided, files not matching this regex are ignored.
    #[arg(long = "path-regex")]
    path_regex: Option<String>,

    /// File path filter (regex) for directory walking. 
    /// (Distinct from --path-regex, which extracts fields)
    #[arg(short = 'f', long = "filter")]
    filter_pattern: Option<String>,

    /// Disable recursive directory walking
    #[arg(long = "no-recursive")]
    no_recursive: bool,

    /// Database output file. Defaults to scan_HHMMSS.db
    #[arg(long)]
    db_path: Option<String>,

    /// Field mapping (e.g., "p1:host;l1:date"). 
    /// Prefixes: 'p' for path capture groups, 'l' for line capture groups.
    /// If omitted, defaults to pf_N (path) and lf_N (line) or f_N.
    #[arg(short = 'F', long = "fields")]
    field_map: Option<String>,

    /// Enable optional tracking tables (files, matches)
    #[arg(long)]
    track_matches: bool,

    /// Batch size for DB inserts
    #[arg(long, default_value = "1000")]
    batch_size: usize,

    #[arg(short = 's', long = "splicers")]
    splicer_threads: Option<usize>,

    #[arg(short = 'p', long = "parsers")]
    parser_threads: Option<usize>,
}

/// Helper to define where a DB column gets its data
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
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // 1. Setup Input Source
    let input_source = if cli.read_stdin_files {
        let mut files = Vec::new();
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            if let Ok(l) = line {
                if !l.trim().is_empty() {
                    files.push(PathBuf::from(l.trim()));
                }
            }
        }
        println!("Loaded {} files from stdin.", files.len());
        InputSource::FileList(files)
    } else {
        InputSource::Directory(cli.target_dir)
    };

    // 2. Setup Regexes
    let line_re = Regex::new(&cli.regex).context("Invalid Line Regex")?;
    
    let path_re = if let Some(pr) = &cli.path_regex {
        Some(Regex::new(pr).context("Invalid Path Regex")?)
    } else {
        None
    };

    // 3. Setup Field Mapping
    let mut columns = Vec::new();

    if let Some(map_str) = &cli.field_map {
        // Custom Mapping: "p1:host;l1:date"
        for part in map_str.split(';') {
            let kv: Vec<&str> = part.split(':').collect();
            if kv.len() == 2 {
                let key = kv[0]; // e.g., "p1" or "l2" or just "1"
                let name = kv[1].to_string();
                
                // Parse prefix
                if let Some(idx_str) = key.strip_prefix('p') {
                    // Path Group
                    let idx: usize = idx_str.parse().context("Invalid path index")?;
                    columns.push(ColumnDef { name, source: FieldSource::Path(idx) });
                } else if let Some(idx_str) = key.strip_prefix('l') {
                    // Line Group
                    let idx: usize = idx_str.parse().context("Invalid line index")?;
                    columns.push(ColumnDef { name, source: FieldSource::Line(idx) });
                } else {
                    // Legacy/Default: Assume Line if no prefix
                    let idx: usize = key.parse().context("Invalid index")?;
                    columns.push(ColumnDef { name, source: FieldSource::Line(idx) });
                }
            }
        }
    } else {
        // Default Auto-Mapping
        if let Some(pre) = &path_re {
            // Path Regex Exists: pf_1..N then lf_1..M
            for i in 1..pre.captures_len() {
                columns.push(ColumnDef { name: format!("pf_{}", i), source: FieldSource::Path(i) });
            }
            for i in 1..line_re.captures_len() {
                columns.push(ColumnDef { name: format!("lf_{}", i), source: FieldSource::Line(i) });
            }
            // If no groups found in line RE, maybe capture whole match?
             if line_re.captures_len() == 1 && columns.iter().all(|c| matches!(c.source, FieldSource::Path(_))) {
                 columns.push(ColumnDef { name: "lf_0".to_string(), source: FieldSource::Line(0) });
             }
        } else {
            // No Path Regex: f_1..N
            let cap_len = line_re.captures_len();
            for i in 1..cap_len {
                columns.push(ColumnDef { name: format!("f_{}", i), source: FieldSource::Line(i) });
            }
            if columns.is_empty() {
                 columns.push(ColumnDef { name: "f_0".to_string(), source: FieldSource::Line(0) });
            }
        }
    }

    // 4. Filename Logic
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
    
    // Explicitly defining path_filter here to ensure scope is correct for IoSplicer
    let path_filter = if let Some(pattern) = cli.filter_pattern {
        Some(Regex::new(&pattern).context("Invalid filter regex")?)
    } else {
        None
    };

    let config = SplicerConfig {
        chunk_size: 256 * 1024,
        max_buffer_size: 1024 * 1024,
        path_filter, // Now definitely in scope
        recursive: !cli.no_recursive,
        thread_count: splicer_count,
    };

    let splicer_stats = Arc::new(SplicerStats::default());
    let db_stats = Arc::new(DbStats::default());

    let (splicer_tx, splicer_rx) = bounded::<SplicedChunk>(256);
    let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(512);
    let (db_tx, db_rx) = bounded::<DbRecord>(4096);

    // Ticker
    let mon_splicer = splicer_stats.clone();
    let mon_db = db_stats.clone();
    thread::spawn(move || {
        let mut last_files = 0;
        let mut last_recs = 0;
        loop {
            thread::sleep(Duration::from_secs(1));
            let files = mon_splicer.file_count.load(Ordering::Relaxed);
            let matches = mon_db.matched_lines.load(Ordering::Relaxed);
            let commits = mon_db.committed_records.load(Ordering::Relaxed);
            
            let files_d = files - last_files;
            let recs_d = commits - last_recs;
            last_files = files;
            last_recs = commits;

            println!("Stats: [Files: {} ({}/s)] [Matches: {}] [DB Insert: {} ({}/s)]", 
                files, files_d, matches, commits, recs_d);
        }
    });

    let batch_size = cli.batch_size;
    let track_matches = cli.track_matches;
    let col_defs_for_db = columns.iter().map(|c| c.name.clone()).collect();
    let db_worker_stats = db_stats.clone();
    
    let db_handle = thread::spawn(move || {
        run_db_worker(db_filename, db_rx, batch_size, track_matches, col_defs_for_db, db_worker_stats)
    });

    // 5. Parsers
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
                let path_arc = chunk.file_path;
                let chunk_offset = chunk.offset;
                
                // Logic: Check Path Regex first (Optimization + Requirement)
                // If path_re is set, the file MUST match, or we skip the chunk.
                let mut should_process = true;
                let mut current_path_vals: HashMap<usize, String> = HashMap::new();

                if let Some(pre) = &thread_path_re {
                    if let Some(p) = &path_arc {
                        let path_str = p.to_string_lossy();
                        if let Some(caps) = pre.captures(&path_str) {
                            // Path matches! Extract needed fields
                             for col in &thread_columns {
                                 if let FieldSource::Path(idx) = col.source {
                                      let val = caps.get(idx).map(|m| m.as_str().to_string()).unwrap_or_default();
                                      current_path_vals.insert(idx, val);
                                 }
                             }
                        } else {
                            // Path regex provided, but did NOT match file.
                            should_process = false; 
                        }
                    } else {
                         // Path regex required, but no path info available.
                         should_process = false;
                    }
                }

                if should_process {
                    let s = String::from_utf8_lossy(&data);
                    
                    for capture in thread_line_re.captures_iter(&s) {
                        t_stats.matched_lines.fetch_add(1, Ordering::Relaxed);
                        
                        let full_match = capture.get(0).map(|m| m.as_str()).unwrap_or("").to_string();
                        let match_offset = chunk_offset + capture.get(0).map(|m| m.start()).unwrap_or(0) as u64;

                        let mut fields = Vec::with_capacity(thread_columns.len());
                        
                        for col in &thread_columns {
                            match col.source {
                                FieldSource::Line(idx) => {
                                    fields.push(capture.get(idx).map(|m| m.as_str().to_string()));
                                },
                                FieldSource::Path(idx) => {
                                    fields.push(current_path_vals.get(&idx).cloned());
                                }
                            }
                        }

                        let record = DbRecord::Data {
                            file_path: path_arc.clone(),
                            offset: match_offset,
                            line_content: full_match,
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
    splicer.run(input_source)?;
    drop(splicer);

    for h in handles { h.join().unwrap(); }
    
    drop(db_tx);
    db_handle.join().unwrap()?;

    println!("Done.");
    Ok(())
}

fn run_db_worker(
    path: String, 
    rx: Receiver<DbRecord>, 
    batch_size: usize,
    track_matches: bool,
    columns: Vec<String>,
    stats: Arc<DbStats>
) -> Result<()> {
    let mut conn = Connection::open(path)?;
    
    conn.execute_batch("
        PRAGMA synchronous = OFF;
        PRAGMA journal_mode = MEMORY;
    ")?;

    if track_matches {
        conn.execute("CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY, 
            path TEXT UNIQUE
        )", [])?;
        conn.execute("CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            offset INTEGER,
            content TEXT
        )", [])?;
    }

    let mut col_defs = String::new();
    for col in &columns {
        col_defs.push_str(&format!(", {} TEXT", col));
    }
    
    let match_id_col = if track_matches { ", match_id INTEGER" } else { "" };
    
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY{}{})", 
        match_id_col, col_defs
    );
    conn.execute(&create_sql, [])?;

    let mut file_cache: HashMap<PathBuf, i64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);
    
    while let Ok(msg) = rx.recv() {
        batch.push(msg);
        if batch.len() >= batch_size {
            flush_batch(&mut conn, &mut batch, &mut file_cache, track_matches, &columns, &stats)?;
        }
    }
    
    if !batch.is_empty() {
        flush_batch(&mut conn, &mut batch, &mut file_cache, track_matches, &columns, &stats)?;
    }

    Ok(())
}

fn flush_batch(
    conn: &mut Connection, 
    batch: &mut Vec<DbRecord>, 
    file_cache: &mut HashMap<PathBuf, i64>,
    track_matches: bool,
    columns: &[String],
    stats: &Arc<DbStats>
) -> Result<()> {
    let tx = conn.transaction()?;
    
    for record in batch.drain(..) {
        // FIXED: Irrefutable pattern error fixed by just destructing directly 
        let DbRecord::Data { file_path, offset, line_content, fields } = record;
            
        let mut current_match_id = None;

        if track_matches {
            let file_id = if let Some(p) = &file_path {
                if let Some(&id) = file_cache.get(&**p) {
                    id
                } else {
                    let path_str = p.to_string_lossy();
                    tx.execute("INSERT OR IGNORE INTO files (path) VALUES (?)", [&path_str])?;
                    
                    let mut stmt = tx.prepare("SELECT id FROM files WHERE path = ?")?;
                    let id: i64 = stmt.query_row([&path_str], |row| row.get(0))?;
                    file_cache.insert((**p).clone(), id);
                    id
                }
            } else {
                0
            };

            tx.execute(
                "INSERT INTO matches (file_id, offset, content) VALUES (?, ?, ?)",
                params![file_id, offset as i64, line_content],
            )?;
            current_match_id = Some(tx.last_insert_rowid());
        }

        let mut place_holders = String::new();
        let mut values: Vec<String> = Vec::new();
        
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
        
        let sql = format!("INSERT INTO data ({}{}) VALUES ({})", match_col, col_names, place_holders);
        
        let params_refs: Vec<&dyn rusqlite::ToSql> = values.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        tx.execute(&sql, &*params_refs)?;
        
        stats.committed_records.fetch_add(1, Ordering::Relaxed);
    }

    tx.commit()?;
    Ok(())
}

