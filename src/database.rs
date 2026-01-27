use anyhow::{Context, Result};
use crossbeam_channel::Receiver;
use rusqlite::{params, types::ValueRef, Connection};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{atomic::Ordering, Arc};

use crate::io_splicer::SplicerStats;
use crate::output::{fmt_float, make_sink, OutputConfig};
use crate::stats::{get_iso_time, DbStats, RunMetadata};

#[derive(Clone, Debug)]
pub enum FieldSource {
    Path(usize),
    Line(usize),
}

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub source: FieldSource,
}

pub enum DbRecord {
    Data {
        file_path: Option<Arc<PathBuf>>,
        offset: u64,
        line_content: String,
        fields: Vec<Option<String>>,
    },
}

/// Helper to split SQL safely respecting quotes and comments
pub fn split_sql_statements(sql: &str) -> Vec<String> {
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
            if c == '\n' {
                in_line_comment = false;
            }
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
                }
                '-' => {
                    if chars.peek() == Some(&'-') {
                        current.push(chars.next().unwrap());
                        in_line_comment = true;
                    }
                }
                '/' => {
                    if chars.peek() == Some(&'*') {
                        current.push(chars.next().unwrap());
                        in_block_comment = true;
                    }
                }
                ';' => {
                    // Split point!
                    let stmt = current.trim().to_string();
                    if !stmt.is_empty() {
                        stmts.push(stmt);
                    }
                    current = String::new();
                }
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

pub fn execute_and_print_sql(
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

pub fn run_db_worker(
    path: String,
    rx: Receiver<DbRecord>,
    batch_size: usize,
    track_matches: bool,
    columns: Vec<String>,
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
    let cache_pragma = format!("PRAGMA cache_size = -{};", cache_kib); // Negative means KiB
    conn.execute(&cache_pragma, [])?;

    // --- PRE-RUN SQL ---
    if !meta.pre_sql.is_empty() {
        let mut stdout = std::io::stdout();
        execute_and_print_sql(&conn, &meta.pre_sql, "PRE", &out_cfg, &mut stdout)?;
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

    // 3. Process Data
    let mut file_cache: HashMap<PathBuf, i64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);

    while let Ok(msg) = rx.recv() {
        batch.push(msg);
        if batch.len() >= batch_size {
            flush_batch(
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
        flush_batch(
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
        execute_and_print_sql(&conn, &meta.post_sql, "POST", &out_cfg, &mut stdout)?;
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
        let DbRecord::Data {
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
            if i > 0 {
                place_holders.push_str(", ");
            }
            place_holders.push_str("?");
            values.push(field.clone().unwrap_or_default());
        }

        let match_col = if track_matches { "match_id, " } else { "" };
        let col_names = columns.join(", ");
        let sql = format!(
            "INSERT INTO {} (run_id, file_id, {}{}) VALUES ({})",
            data_table, match_col, col_names, place_holders
        );
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            values.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        tx.execute(&sql, &*params_refs)?;

        stats.committed_records.fetch_add(1, Ordering::Relaxed);
    }

    tx.commit()?;
    Ok(())
}
