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
use crate::database::{split_sql_statements, ColumnDef, TypedValue};

#[cfg(feature = "duckdb")]
use duckdb::{params, types::ValueRef, Connection};

#[cfg(feature = "duckdb")]
#[allow(dead_code)]
fn escape_sql(input: &str) -> String {
    input.replace('\'', "''")
}

#[cfg(feature = "duckdb")]
pub fn run_db_worker_duckdb(
    path: String,
    rx: Receiver<crate::database::DbRecord>,
    batch_size: usize,
    track_matches: bool,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
    splicer_stats: Arc<SplicerStats>,
    meta: RunMetadata,
    out_cfg: OutputConfig,
    duckdb_threads: usize,
    duckdb_memory_limit: String,
) -> Result<i64> {
    let mut conn = Connection::open(path)?;

    // Performance Tunings for DuckDB
    conn.execute(&format!("PRAGMA threads = {}", duckdb_threads), [])?;
    conn.execute(&format!("PRAGMA memory_limit = '{}'", duckdb_memory_limit), [])?;

    // --- PRE-RUN SQL ---
    if !meta.pre_sql.is_empty() {
        let mut stdout = std::io::stdout();
        if let Err(e) = execute_and_print_sql_duckdb(&conn, &meta.pre_sql, "PRE", &out_cfg, &mut stdout) {
            eprintln!("PRE SQL error: {}", e);
        }
    }

    // 1. Setup Shared Tables - explicit IDs (no sequences)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS runs (
            id BIGINT PRIMARY KEY,
            timestamp TEXT,
            command TEXT,
            regex TEXT,
            files_processed BIGINT DEFAULT 0,
            files_skipped BIGINT DEFAULT 0,
            bytes_processed BIGINT DEFAULT 0,
            match_count BIGINT DEFAULT 0,
            finished_at TEXT
        )",
        [],
    ).or_else(|_| {
        // Table might already exist, that's fine
        Ok::<_, duckdb::Error>(0)
    })?;
    // Best-effort widen integer counters to BIGINT on existing DBs
    let _ = conn.execute("ALTER TABLE runs ALTER COLUMN files_processed SET DATA TYPE BIGINT", []);
    let _ = conn.execute("ALTER TABLE runs ALTER COLUMN files_skipped SET DATA TYPE BIGINT", []);
    let _ = conn.execute("ALTER TABLE runs ALTER COLUMN bytes_processed SET DATA TYPE BIGINT", []);
    let _ = conn.execute("ALTER TABLE runs ALTER COLUMN match_count SET DATA TYPE BIGINT", []);
    // Compute a new run_id and insert explicitly
    let run_id: i64 = conn
        .query_row("SELECT COALESCE(MAX(id), 0) + 1 FROM runs", [], |row| row.get::<_, i64>(0))?;
    conn.execute(
        "INSERT INTO runs (id, timestamp, command, regex) VALUES (?, ?, ?, ?)",
        params![run_id, meta.created_at, meta.command_args, meta.regex],
    )?;

    // Create files table (with run_id column included from the start)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (id BIGINT PRIMARY KEY, run_id BIGINT, path TEXT)",
        [],
    ).or_else(|_| Ok::<_, duckdb::Error>(0))?;
    
    let _ = conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_run_path ON files(run_id, path)",
        [],
    );

    // 2. Setup Dynamic Tables
    let data_table_name = format!("data_{}", run_id);
    let matches_table_name = format!("matches_{}", run_id);

    if track_matches {
        conn.execute(&format!("CREATE TABLE {} (id BIGINT PRIMARY KEY, file_id BIGINT, \"offset\" BIGINT, content TEXT)", matches_table_name), [])?;
    }

    // Build typed column definitions
    let mut col_defs = String::new();
    for col in &columns {
        col_defs.push_str(&format!(", \"{}\" {}", col.name, col.dtype.sql_type()));
    }
    let match_id_col = if track_matches {
        ", match_id BIGINT"
    } else {
        ""
    };

    conn.execute(
        &format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, run_id BIGINT, file_id BIGINT{}{})",
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
        if let Err(e) = execute_and_print_sql_duckdb(&conn, &meta.post_sql, "POST", &out_cfg, &mut stdout) {
            eprintln!("POST SQL error: {}", e);
        }
    }

    Ok(run_id)
}

#[cfg(feature = "duckdb")]
fn flush_batch_duckdb(
    conn: &mut Connection,
    batch: &mut Vec<crate::database::DbRecord>,
    file_cache: &mut HashMap<PathBuf, i64>,
    track_matches: bool,
    _columns: &[ColumnDef],
    stats: &Arc<DbStats>,
    run_id: i64,
    data_table_name: &str,
    matches_table_name: &str,
) -> Result<()> {
    // Initialize ID counters from current tables
    let mut data_id: i64 = conn
        .query_row(
            &format!("SELECT COALESCE(MAX(id), 0) FROM {}", data_table_name),
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let mut match_id_counter: i64 = if track_matches {
        conn
            .query_row(
                &format!("SELECT COALESCE(MAX(id), 0) FROM {}", matches_table_name),
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
    } else {
        0
    };

    // Create appenders
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

        // Handle file insertion with caching
        let file_id = if let Some(p) = &file_path {
            if let Some(&id) = file_cache.get(&**p) {
                id
            } else {
                let path_str = p.to_string_lossy();
                // Check if file exists, insert if not (manual id)
                let existing_id: Option<i64> = conn
                    .query_row(
                        "SELECT id FROM files WHERE run_id = ? AND path = ?",
                        params![run_id, path_str.as_ref()],
                        |row| row.get(0),
                    )
                    .ok();

                let id: i64 = if let Some(id) = existing_id {
                    id
                } else {
                    let next_id: i64 = conn
                        .query_row("SELECT COALESCE(MAX(id), 0) + 1 FROM files", [], |row| row.get(0))
                        .unwrap_or(1);
                    conn.execute(
                        "INSERT INTO files (id, run_id, path) VALUES (?, ?, ?)",
                        params![next_id, run_id, path_str.as_ref()],
                    )?;
                    next_id
                };
                file_cache.insert((**p).clone(), id);
                id
            }
        } else {
            0
        };

        // Append match if tracking (manual id)
        let mut current_match_id: Option<i64> = None;
        if track_matches {
            match_id_counter += 1;
            if let Some(ref mut app) = matches_appender {
                app.append_row(params![match_id_counter, file_id, offset as i64, line_content.as_str()])?;
                current_match_id = Some(match_id_counter);
            }
        }

        // Append data row with explicit id and dynamic fields
        // We need to convert TypedValue to owned values for the appender
        data_id += 1;
        
        // Convert TypedValue fields to bindable values
        // We store owned values to ensure they live long enough
        let mut string_values: Vec<String> = Vec::with_capacity(fields.len());
        let mut i64_values: Vec<i64> = Vec::with_capacity(fields.len());
        let mut f64_values: Vec<f64> = Vec::with_capacity(fields.len());
        let mut null_string_values: Vec<Option<String>> = Vec::with_capacity(fields.len());
        
        for field in &fields {
            match field {
                TypedValue::Null => {
                    null_string_values.push(None);
                    string_values.push(String::new());
                    i64_values.push(0);
                    f64_values.push(0.0);
                }
                TypedValue::String(s) => {
                    null_string_values.push(Some(String::new())); // placeholder
                    string_values.push(s.clone());
                    i64_values.push(0);
                    f64_values.push(0.0);
                }
                TypedValue::I64(v) => {
                    null_string_values.push(Some(String::new())); // placeholder
                    string_values.push(String::new());
                    i64_values.push(*v);
                    f64_values.push(0.0);
                }
                TypedValue::F64(v) => {
                    null_string_values.push(Some(String::new())); // placeholder
                    string_values.push(String::new());
                    i64_values.push(0);
                    f64_values.push(*v);
                }
            }
        }

        if track_matches {
            let mut row_data: Vec<&dyn duckdb::ToSql> = Vec::with_capacity(4 + fields.len());
            row_data.push(&data_id);
            row_data.push(&run_id);
            row_data.push(&file_id);
            let mid = current_match_id.unwrap();
            row_data.push(&mid);
            for (i, field) in fields.iter().enumerate() {
                match field {
                    TypedValue::Null => row_data.push(&null_string_values[i]),
                    TypedValue::String(_) => row_data.push(&string_values[i]),
                    TypedValue::I64(_) => row_data.push(&i64_values[i]),
                    TypedValue::F64(_) => row_data.push(&f64_values[i]),
                }
            }
            data_appender.append_row(row_data.as_slice())?;
        } else {
            let mut row_data: Vec<&dyn duckdb::ToSql> = Vec::with_capacity(3 + fields.len());
            row_data.push(&data_id);
            row_data.push(&run_id);
            row_data.push(&file_id);
            for (i, field) in fields.iter().enumerate() {
                match field {
                    TypedValue::Null => row_data.push(&null_string_values[i]),
                    TypedValue::String(_) => row_data.push(&string_values[i]),
                    TypedValue::I64(_) => row_data.push(&i64_values[i]),
                    TypedValue::F64(_) => row_data.push(&f64_values[i]),
                }
            }
            data_appender.append_row(row_data.as_slice())?;
        }

        stats.committed_records.fetch_add(1, Ordering::Relaxed);
    }

    // Flush appenders
    if let Some(mut app) = matches_appender { app.flush()?; }
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

            writeln!(writer, "> Query: {}", clean_sql)?;
            
            // Execute query and collect rows
            let mut rows = stmt.query([])?;
            let mut rows_buf: Vec<Vec<String>> = Vec::new();
            let mut col_count: usize = 0;
            
            while let Some(row) = rows.next()? {
                // Get column count from first row by probing
                if col_count == 0 {
                    let mut i = 0;
                    loop {
                        match row.get_ref(i) {
                            Ok(_) => i += 1,
                            Err(_) => break,
                        }
                    }
                    col_count = i;
                }
                
                let mut values: Vec<String> = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let s = match row.get_ref(i).unwrap() {
                        ValueRef::Null => "NULL".to_string(),
                        ValueRef::BigInt(i) => i.to_string(),
                        ValueRef::Double(f) => fmt_float(f, out_cfg.sig_digits),
                        ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                        _ => "<BLOB>".to_string(),
                    };
                    values.push(s);
                }
                rows_buf.push(values);
            }
            let row_count = rows_buf.len();
            
            // Get column names from the statement after rows iteration is complete
            let col_names: Vec<String> = (0..col_count)
                .map(|j| stmt.column_name(j)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| format!("col{}", j + 1)))
                .collect();
            {
                let mut sink = make_sink(*out_cfg, writer)?;
                sink.write_header(&col_names)?;
                for v in &rows_buf { sink.write_row(v)?; }
                sink.finish()?;
            }
            writeln!(writer, "({} rows)\n", row_count)?;
        }
    }
    Ok(())
}
