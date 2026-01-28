#[cfg(feature = "duckdb")]
use anyhow::{Context, Result};
#[cfg(feature = "duckdb")]
use crossbeam_channel::Receiver;
#[cfg(feature = "duckdb")]
use std::collections::HashMap;
#[cfg(feature = "duckdb")]
use std::io::Write;
#[cfg(feature = "duckdb")]
use std::path::PathBuf;
#[cfg(feature = "duckdb")]
use std::sync::{atomic::Ordering, Arc};

#[cfg(feature = "duckdb")]
use crate::io_splicer::SplicerStats;
#[cfg(feature = "duckdb")]
use crate::output::{fmt_float, make_sink, OutputConfig};
#[cfg(feature = "duckdb")]
use crate::stats::{get_iso_time, DbStats, RunMetadata};
#[cfg(feature = "duckdb")]
use crate::database::split_sql_statements;

#[cfg(feature = "duckdb")]
use duckdb::{params, types::ValueRef, Appender, Connection};

#[cfg(feature = "duckdb")]
fn escape_sql(input: &str) -> String {
    input.replace('\'', "''")
}

#[cfg(feature = "duckdb")]
pub fn run_db_worker_duckdb(
    path: String,
    rx: Receiver<crate::database::DbRecord>,
    batch_size: usize,
    track_matches: bool,
    columns: Vec<String>,
    stats: Arc<DbStats>,
    splicer_stats: Arc<SplicerStats>,
    meta: RunMetadata,
    out_cfg: OutputConfig,
) -> Result<i64> {
    let mut conn = Connection::open(path)?;

    // Performance Tunings for DuckDB
    conn.execute("PRAGMA threads = 8", [])?;
    conn.execute("PRAGMA memory_limit = '1GB'", [])?;
    conn.execute("PRAGMA enable_optimizer = true", [])?;
    conn.execute("PRAGMA enable_profiling = false", [])?;
    
    // Additional performance tuning for bulk inserts
    conn.execute("PRAGMA wal_autocheckpoint = 0", [])?;  // Disable auto-checkpoint during load
    conn.execute("PRAGMA checkpoint_threshold = '1GB'", [])?;  // Larger checkpoint threshold

    // --- PRE-RUN SQL ---
    if !meta.pre_sql.is_empty() {
        let mut stdout = std::io::stdout();
        execute_and_print_sql_duckdb(&conn, &meta.pre_sql, "PRE", &out_cfg, &mut stdout)?;
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
    conn.execute(
        "INSERT INTO runs (timestamp, command, regex) VALUES (?, ?, ?)",
        params![meta.created_at, meta.command_args, meta.regex],
    )?;
    let run_id: i64 = conn.query_row("SELECT max(id) FROM runs", [], |row| row.get::<_, i64>(0))?;

    // Note: DuckDB doesn't support ALTER TABLE ADD COLUMN IF NOT EXISTS the same way
    // We'll handle this more gracefully
    let has_run_id: bool = conn.query_row(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'files' AND column_name = 'run_id'",
        [],
        |row| row.get(0).map(|count: i64| count > 0),
    ).unwrap_or(false);
    
    if has_run_id {
        // Column already exists
    } else {
        conn.execute("ALTER TABLE files ADD COLUMN run_id INTEGER", [])?;
    }
    
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, run_id INTEGER, path TEXT)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_run_path ON files(run_id, path)",
        [],
    )?;

    // 2. Setup Dynamic Tables
    let data_table_name = format!("data_{}", run_id);
    let matches_table_name = format!("matches_{}", run_id);

    if track_matches {
        conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, file_id INTEGER, offset INTEGER, content TEXT)", matches_table_name), [])?;
    }

    let mut col_defs = String::new();
    for col in &columns {
        col_defs.push_str(&format!(", {} TEXT", col));
    }
    let match_id_col = if track_matches {
        ", match_id INTEGER"
    } else {
        ""
    };

    conn.execute(
        &format!(
            "CREATE TABLE {} (id INTEGER PRIMARY KEY, run_id INTEGER, file_id INTEGER{}{})",
            data_table_name, match_id_col, col_defs
        ),
        [],
    )?;

    // 3. Process Data using DuckDB's Appender for better performance
    let mut file_cache: HashMap<PathBuf, i64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);

    while let Ok(msg) = rx.recv() {
        batch.push(msg);
        if batch.len() >= batch_size {
            flush_batch_duckdb(
                &mut conn,
                &mut batch,
                &mut file_cache,
                track_matches,
                &columns,
                &stats,
                run_id,
                &data_table_name,
                &matches_table_name,
            )?;
        }
    }
    if !batch.is_empty() {
        flush_batch_duckdb(
            &mut conn,
            &mut batch,
            &mut file_cache,
            track_matches,
            &columns,
            &stats,
            run_id,
            &data_table_name,
            &matches_table_name,
        )?;
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
        let create_view = format!(
            "CREATE VIEW matches AS SELECT * FROM {}",
            matches_table_name
        );
        let _ = conn.execute(&create_view, []);
    }

    // --- POST-RUN SQL ---
    if !meta.post_sql.is_empty() {
        let mut stdout = std::io::stdout();
        execute_and_print_sql_duckdb(&conn, &meta.post_sql, "POST", &out_cfg, &mut stdout)?;
    }

    Ok(run_id)
}

#[cfg(feature = "duckdb")]
fn flush_batch_duckdb(
    conn: &mut Connection,
    batch: &mut Vec<crate::database::DbRecord>,
    file_cache: &mut HashMap<PathBuf, i64>,
    track_matches: bool,
    columns: &[String],
    stats: &Arc<DbStats>,
    run_id: i64,
    data_table_name: &str,
    matches_table_name: &str,
) -> Result<()> {
    // Use Appender API for bulk inserts - no transaction needed, Appender handles it
    let mut data_appender = conn.appender(data_table_name)?;
    let mut matches_appender = if track_matches {
        Some(conn.appender(matches_table_name)?)
    } else {
        None
    };

    for record in batch.drain(..) {
        let crate::database::DbRecord::Data {
            file_path,
            offset,
            line_content,
            fields,
        } = record;

        // Handle file insertion with caching (still needs individual queries for deduplication)
        let file_id = if let Some(p) = &file_path {
            if let Some(&id) = file_cache.get(&**p) {
                id
            } else {
                let path_str = p.to_string_lossy();
                
                // Try to insert file (ignore duplicates)
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO files (run_id, path) VALUES (?, ?)",
                    params![run_id, path_str.as_ref()],
                );
                
                // Get the file ID
                let id: i64 = conn.query_row(
                    "SELECT id FROM files WHERE run_id = ? AND path = ?",
                    params![run_id, path_str.as_ref()],
                    |row| row.get(0),
                )?;
                file_cache.insert((**p).clone(), id);
                id
            }
        } else {
            0
        };

        // Use Appender for match insertion if tracking (much faster)
        let match_id: Option<i64> = if track_matches {
            if let Some(ref mut appender) = matches_appender {
                // Append to matches table
                appender.append_row(params![file_id, offset as i64, line_content.as_str()])?;
                // Note: We can't get the auto-generated ID easily with Appender
                // For now, set to None - if you need match_id in data table, we'd need a different approach
                None
            } else {
                None
            }
        } else {
            None
        };

        // Directly append to data table using Appender API
        if track_matches {
            let mut row_data: Vec<&dyn duckdb::ToSql> = Vec::with_capacity(3 + fields.len());
            row_data.push(&run_id);
            row_data.push(&file_id);
            row_data.push(&match_id);
            for field in &fields {
                row_data.push(field);
            }
            data_appender.append_row(duckdb::params_from_iter(row_data))?;
        } else {
            let mut row_data: Vec<&dyn duckdb::ToSql> = Vec::with_capacity(2 + fields.len());
            row_data.push(&run_id);
            row_data.push(&file_id);
            for field in &fields {
                row_data.push(field);
            }
            data_appender.append_row(duckdb::params_from_iter(row_data))?;
        }

        stats.committed_records.fetch_add(1, Ordering::Relaxed);
    }

    // Flush both appenders to commit all rows
    if let Some(mut appender) = matches_appender {
        appender.flush()?;
    }
    data_appender.flush()?;
    Ok(())
}

#[cfg(feature = "duckdb")]
fn execute_and_print_sql_duckdb(
    conn: &Connection,
    sql_scripts: &[String],
    stage: &str,
    out_cfg: &OutputConfig,
    writer: &mut dyn Write,
) -> Result<()> {
    for (i, script) in sql_scripts.iter().enumerate() {
        if script.trim().is_empty() {
            continue;
        }

        let statements = split_sql_statements(script);

        if !statements.is_empty() {
            writeln!(
                writer,
                "--- [Executing {} SQL Block #{} ({} statements)] ---",
                stage,
                i + 1,
                statements.len()
            )?;
        }

        for stmt_sql in statements {
            let clean_sql = stmt_sql.trim_end_matches(';');

            let mut stmt = conn
                .prepare(clean_sql)
                .context(format!("Failed to prepare SQL: {}", clean_sql))?;

            if stmt.column_count() > 0 {
                let col_count = stmt.column_count();
                let col_names: Vec<String> = (0..col_count)
                    .map(|i| stmt.column_name(i).map_or("?", |v| v).to_string())
                    .collect();

                writeln!(writer, "> Query: {}", clean_sql)?;
                let mut row_count = 0;
                {
                    let mut sink = make_sink(*out_cfg, writer)?;
                    sink.write_header(&col_names)?;

                    let mut rows = stmt.query([])?;
                    while let Some(row) = rows.next()? {
                        row_count += 1;
                        let values: Vec<String> = (0..col_count)
                            .map(|i| match row.get_ref(i).unwrap() {
                                ValueRef::Null => "NULL".to_string(),
                                ValueRef::BigInt(i) => i.to_string(),
                                ValueRef::Double(f) => fmt_float(f, out_cfg.sig_digits),
                                ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                                _ => "<BLOB>".to_string(),
                            })
                            .collect();
                        sink.write_row(&values)?;
                    }
                    sink.finish()?;
                }
                writeln!(writer, "({} rows)\n", row_count)?;
            } else {
                stmt.execute([])?;
            }
        }
    }
    Ok(())
}
