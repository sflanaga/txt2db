use anyhow::Result;
use crossbeam_channel::Receiver;
use std::path::PathBuf;
use std::sync::Arc;

use crate::io_splicer::SplicerStats;
use crate::output::OutputConfig;
use crate::stats::{DbStats, RunMetadata};
use crate::config::{DbBackend, DataType, FieldSource as ConfigFieldSource};

/// Typed value for database fields - supports NULL, String, I64, F64
#[derive(Clone, Debug)]
pub enum TypedValue {
    Null,
    String(String),
    I64(i64),
    F64(f64),
}

impl TypedValue {
    /// Parse a string into a TypedValue based on the target DataType
    pub fn parse(s: &str, dtype: DataType) -> Result<Self, String> {
        if s.is_empty() {
            return Ok(TypedValue::Null);
        }
        match dtype {
            DataType::String => Ok(TypedValue::String(s.to_string())),
            DataType::I64 => {
                s.parse::<i64>()
                    .map(TypedValue::I64)
                    .map_err(|_| format!("Cannot parse '{}' as integer", s))
            }
            DataType::F64 => {
                s.parse::<f64>()
                    .map(TypedValue::F64)
                    .map_err(|_| format!("Cannot parse '{}' as float", s))
            }
        }
    }

    /// Convert to string representation for display/legacy use
    #[allow(dead_code)]
    pub fn as_string(&self) -> Option<String> {
        match self {
            TypedValue::Null => None,
            TypedValue::String(s) => Some(s.clone()),
            TypedValue::I64(v) => Some(v.to_string()),
            TypedValue::F64(v) => Some(v.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub source: ConfigFieldSource,
    pub dtype: DataType,
}

impl ColumnDef {
    #[allow(dead_code)]
    pub fn new(name: String, source: ConfigFieldSource, dtype: DataType) -> Self {
        Self { name, source, dtype }
    }

    /// Create a string column (for backward compatibility)
    #[allow(dead_code)]
    pub fn new_string(name: String, source: ConfigFieldSource) -> Self {
        Self { name, source, dtype: DataType::String }
    }
}

pub enum DbRecord {
    Data {
        file_path: Option<Arc<PathBuf>>,
        offset: u64,
        line_content: String,
        fields: Vec<TypedValue>,
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

pub fn run_db_worker(
    path: String,
    rx: Receiver<DbRecord>,
    batch_size: usize,
    track_matches: bool,
    columns: Vec<ColumnDef>,
    stats: Arc<DbStats>,
    splicer_stats: Arc<SplicerStats>,
    meta: RunMetadata,
    out_cfg: OutputConfig,
    backend: DbBackend,
    duckdb_threads: usize,
    duckdb_memory_limit: String,
) -> Result<i64> {
    match backend {
        #[cfg(feature = "sqlite")]
        DbBackend::Sqlite => {
            crate::database_sqlite::run_db_worker_sqlite(
                path,
                rx,
                batch_size,
                track_matches,
                columns,
                stats,
                splicer_stats,
                meta,
                out_cfg,
            )
        }
        #[cfg(feature = "duckdb")]
        DbBackend::DuckDB => {
            crate::database_duckdb::run_db_worker_duckdb(
                path,
                rx,
                batch_size,
                track_matches,
                columns,
                stats,
                splicer_stats,
                meta,
                out_cfg,
                duckdb_threads,
                duckdb_memory_limit,
            )
        }
        #[allow(unreachable_patterns)]
        _ => anyhow::bail!("Database backend {:?} not enabled", backend),
    }
}

