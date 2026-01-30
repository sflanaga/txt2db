#[cfg(feature = "sqlite")]
use anyhow::{Context, Result};
#[cfg(feature = "sqlite")]
use crossbeam_channel::Receiver;
#[cfg(feature = "sqlite")]
use log::warn;
#[cfg(feature = "sqlite")]
use std::collections::HashMap;
#[cfg(feature = "sqlite")]
use std::io::Write;
#[cfg(feature = "sqlite")]
use std::path::PathBuf;
#[cfg(feature = "sqlite")]
use std::sync::{atomic::Ordering, Arc};

#[cfg(feature = "sqlite")]
use crate::io_splicer::SplicerStats;
#[cfg(feature = "sqlite")]
use crate::output::{fmt_float, make_sink, OutputConfig};
#[cfg(feature = "sqlite")]
use crate::stats::{get_iso_time, DbStats, RunMetadata};
#[cfg(feature = "sqlite")]
use crate::database::{split_sql_statements, ColumnDef, TypedValue};

#[cfg(feature = "sqlite")]
use rusqlite::{params, types::ValueRef, Connection};

#[cfg(feature = "sqlite")]
#[allow(dead_code)]
pub struct SqliteConnection {
    conn: Connection,
}

#[cfg(feature = "sqlite")]
impl SqliteConnection {
    #[allow(dead_code)]
    pub fn new(path: &str, cache_mb: i64) -> Result<Self> {
        let conn = Connection::open(path)?;
        
        // Performance Tunings
        conn.execute_batch(
            "
            PRAGMA synchronous = OFF;
            PRAGMA journal_mode = MEMORY;
            PRAGMA temp_store = 2;
        ",
        )?;

        // Set Cache Size
        let cache_kib = cache_mb * 1024;
        let cache_pragma = format!("PRAGMA cache_size = -{};", cache_kib);
        conn.execute(&cache_pragma, [])?;
        
        Ok(SqliteConnection { conn })
    }
}

#[cfg(feature = "sqlite")]
pub fn run_db_worker_sqlite(
    path: String,
    rx: Receiver<crate::database::DbRecord>,
    batch_size: usize,
    track_matches: bool,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
    splicer_stats: Arc<SplicerStats>,
    meta: RunMetadata,
    out_cfg: OutputConfig,
) -> Result<i64> {
    let mut conn = Connection::open(path)?;

    // Performance Tunings
    conn.execute_batch(
        "
        PRAGMA synchronous = OFF;
        PRAGMA journal_mode = MEMORY;
        PRAGMA temp_store = 2;
    ",
    )?;

    // Set Cache Size
    let cache_kib = meta.cache_mb * 1024;
    let cache_pragma = format!("PRAGMA cache_size = -{};", cache_kib);
    conn.execute(&cache_pragma, [])?;

    // --- PRE-RUN SQL ---
    if !meta.pre_sql.is_empty() {
        let mut stdout = std::io::stdout();
        execute_and_print_sql_sqlite(&conn, &meta.pre_sql, "PRE", &out_cfg, &mut stdout)?;
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
    let run_id = conn.last_insert_rowid();

    let _ = conn.execute("ALTER TABLE files ADD COLUMN run_id INTEGER", []);
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, run_id INTEGER, path TEXT)",
        [],
    )?;
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_files_run_path ON files(run_id, path)",
        [],
    )?;

    // 2. Setup Dynamic Tables
    let data_table_name = format!("data_{}", run_id);
    let matches_table_name = format!("matches_{}", run_id);

    if track_matches {
        conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, file_id INTEGER, offset INTEGER, content TEXT)", matches_table_name), [])?;
    }

    // Build typed column definitions (SQLite uses REAL instead of DOUBLE)
    let mut col_defs = String::new();
    for col in &columns {
        let sql_type = match col.dtype {
            crate::config::DataType::String => "TEXT",
            crate::config::DataType::I64 => "INTEGER",
            crate::config::DataType::F64 => "REAL",
        };
        col_defs.push_str(&format!(", \"{}\" {}", col.name, sql_type));
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

    // 3. Process Data
    let mut file_cache: HashMap<PathBuf, i64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);

    while let Ok(msg) = rx.recv() {
        batch.push(msg);
        if batch.len() >= batch_size {
            flush_batch_sqlite(
                &mut conn,
                &mut batch,
                &mut file_cache,
                track_matches,
                &columns,
                &stats,
                &data_table_name,
                &matches_table_name,
                run_id,
            )?;
        }
    }
    if !batch.is_empty() {
        flush_batch_sqlite(
            &mut conn,
            &mut batch,
            &mut file_cache,
            track_matches,
            &columns,
            &stats,
            &data_table_name,
            &matches_table_name,
            run_id,
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
        warn!("Could not create 'data' view: {}", e);
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
        execute_and_print_sql_sqlite(&conn, &meta.post_sql, "POST", &out_cfg, &mut stdout)?;
    }

    Ok(run_id)
}

#[cfg(feature = "sqlite")]
fn execute_and_print_sql_sqlite(
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
                    .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
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
                                ValueRef::Integer(i) => i.to_string(),
                                ValueRef::Real(f) => fmt_float(f, out_cfg.sig_digits),
                                ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                                ValueRef::Blob(_) => "<BLOB>".to_string(),
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

#[cfg(feature = "sqlite")]
fn flush_batch_sqlite(
    conn: &mut Connection,
    batch: &mut Vec<crate::database::DbRecord>,
    file_cache: &mut HashMap<PathBuf, i64>,
    track_matches: bool,
    columns: &[ColumnDef],
    stats: &Arc<DbStats>,
    data_table: &str,
    matches_table: &str,
    run_id: i64,
) -> Result<()> {
    let tx = conn.transaction()?;

    // Build column names string once
    let col_names: String = columns.iter().map(|c| format!("\"{}\"", c.name)).collect::<Vec<_>>().join(", ");

    for record in batch.drain(..) {
        let crate::database::DbRecord::Data {
            file_path,
            offset,
            line_content,
            fields,
        } = record;

        let mut current_match_id = None;

        let file_id = if let Some(p) = &file_path {
            if let Some(&id) = file_cache.get(&**p) {
                id
            } else {
                let path_str = p.to_string_lossy();
                tx.execute(
                    "INSERT OR IGNORE INTO files (run_id, path) VALUES (?, ?)",
                    params![run_id, path_str],
                )?;
                let mut stmt = tx.prepare("SELECT id FROM files WHERE run_id = ? AND path = ?")?;
                let id: i64 = stmt.query_row(params![run_id, path_str], |row| row.get(0))?;
                file_cache.insert((**p).clone(), id);
                id
            }
        } else {
            0
        };

        if track_matches {
            let sql = format!(
                "INSERT INTO {} (file_id, offset, content) VALUES (?, ?, ?)",
                matches_table
            );
            tx.execute(&sql, params![file_id, offset as i64, line_content])?;
            current_match_id = Some(tx.last_insert_rowid());
        }

        // Build placeholders for typed values
        let field_placeholders: String = (0..fields.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
        
        let match_col = if track_matches { "match_id, " } else { "" };
        let match_placeholder = if track_matches { "?, " } else { "" };
        
        let sql = format!(
            "INSERT INTO {} (run_id, file_id, {}{}) VALUES (?, ?, {}{})",
            data_table, match_col, col_names, match_placeholder, field_placeholders
        );

        // Build params dynamically based on TypedValue types
        // We need to collect owned values first
        let mut string_vals: Vec<String> = Vec::new();
        let mut i64_vals: Vec<i64> = Vec::new();
        let mut f64_vals: Vec<f64> = Vec::new();
        
        for field in &fields {
            match field {
                TypedValue::Null => {
                    string_vals.push(String::new());
                    i64_vals.push(0);
                    f64_vals.push(0.0);
                }
                TypedValue::String(s) => {
                    string_vals.push(s.clone());
                    i64_vals.push(0);
                    f64_vals.push(0.0);
                }
                TypedValue::I64(v) => {
                    string_vals.push(String::new());
                    i64_vals.push(*v);
                    f64_vals.push(0.0);
                }
                TypedValue::F64(v) => {
                    string_vals.push(String::new());
                    i64_vals.push(0);
                    f64_vals.push(*v);
                }
            }
        }

        // Build the params vector
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        params_vec.push(Box::new(run_id));
        params_vec.push(Box::new(file_id));
        
        if track_matches {
            params_vec.push(Box::new(current_match_id.unwrap()));
        }

        for (i, field) in fields.iter().enumerate() {
            match field {
                TypedValue::Null => {
                    let null_val: Option<String> = None;
                    params_vec.push(Box::new(null_val));
                }
                TypedValue::String(_) => {
                    params_vec.push(Box::new(string_vals[i].clone()));
                }
                TypedValue::I64(_) => {
                    params_vec.push(Box::new(i64_vals[i]));
                }
                TypedValue::F64(_) => {
                    params_vec.push(Box::new(f64_vals[i]));
                }
            }
        }

        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();
        tx.execute(&sql, params_refs.as_slice())?;

        stats.committed_records.fetch_add(1, Ordering::Relaxed);
    }

    tx.commit()?;
    Ok(())
}
