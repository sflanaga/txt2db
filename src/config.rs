use bytesize::ByteSize;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Parse a size string with optional suffix, defaulting to bytes.
/// Supports: B, K, KB, KiB, M, MB, MiB, G, GB, GiB, etc. (case-insensitive)
pub fn parse_size_string(s: &str) -> Result<usize, String> {
    s.parse::<ByteSize>()
        .map(|bs| bs.as_u64() as usize)
        .map_err(|_| format!("Invalid size value: {} (e.g., '256KB', '1MB', '1048576')", s))
}

// =============================================================================
// Unified Field Specification Types
// =============================================================================

/// Data type for field values
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum DataType {
    #[default]
    String,  // s, string
    I64,     // i, int
    F64,     // f, float
}

impl DataType {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "s" | "string" | "str" => Ok(DataType::String),
            "i" | "int" | "i64" | "bigint" => Ok(DataType::I64),
            "f" | "float" | "f64" | "double" => Ok(DataType::F64),
            _ => Err(format!("Invalid type '{}': use s/string, i/int, or f/float", s)),
        }
    }

    pub fn sql_type(&self) -> &'static str {
        match self {
            DataType::String => "TEXT",
            DataType::I64 => "BIGINT",
            DataType::F64 => "DOUBLE",
        }
    }
}

/// Aggregation operation for map mode
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggOp {
    Key,     // k, key
    Sum,     // sum (not 's' to avoid conflict with string type)
    Avg,     // a, avg
    Count,   // c, count
    Max,     // x, max
    Min,     // n, min
}

impl AggOp {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "k" | "key" => Ok(AggOp::Key),
            "sum" => Ok(AggOp::Sum),
            "a" | "avg" | "average" => Ok(AggOp::Avg),
            "c" | "count" => Ok(AggOp::Count),
            "x" | "max" => Ok(AggOp::Max),
            "n" | "min" => Ok(AggOp::Min),
            _ => Err(format!("Invalid operation '{}': use k/key, sum, a/avg, c/count, x/max, n/min", s)),
        }
    }
}

/// Source of field data (line regex capture or path regex capture)
#[derive(Clone, Debug, PartialEq)]
pub enum FieldSource {
    Line(usize),   // l1, l2, ... (capture index from line regex)
    Path(usize),   // p1, p2, ... (capture index from path regex)
    None,          // _ (for count operation, no source needed)
}

impl FieldSource {
    pub fn from_str(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s == "_" {
            return Ok(FieldSource::None);
        }
        if let Some(idx_str) = s.strip_prefix('l').or_else(|| s.strip_prefix('L')) {
            let idx: usize = idx_str.parse()
                .map_err(|_| format!("Invalid line index '{}' in source '{}'", idx_str, s))?;
            Ok(FieldSource::Line(idx))
        } else if let Some(idx_str) = s.strip_prefix('p').or_else(|| s.strip_prefix('P')) {
            let idx: usize = idx_str.parse()
                .map_err(|_| format!("Invalid path index '{}' in source '{}'", idx_str, s))?;
            Ok(FieldSource::Path(idx))
        } else {
            // Default to line source if just a number
            let idx: usize = s.parse()
                .map_err(|_| format!("Invalid source '{}': use l1, p1, or _ for count", s))?;
            Ok(FieldSource::Line(idx))
        }
    }
}

/// Unified field specification for both DB and Map modes
/// Syntax: source:name[:type[:op]]
/// Examples:
///   l1:userid           -> line capture 1, name "userid", type string, no op
///   l1:userid:i         -> line capture 1, name "userid", type i64, no op
///   l1:bytes:i:sum      -> line capture 1, name "bytes", type i64, sum aggregation
///   _:rowcount:i:count  -> no source, name "rowcount", type i64, count aggregation
#[derive(Clone, Debug)]
pub struct FieldSpec {
    pub source: FieldSource,
    pub name: String,
    pub dtype: DataType,
    pub op: Option<AggOp>,
}

impl FieldSpec {
    /// Parse a single field spec from "source:name[:type[:op]]"
    pub fn from_str(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() < 2 {
            return Err(format!("Invalid field spec '{}': need at least source:name", s));
        }

        let source = FieldSource::from_str(parts[0])?;
        let name = parts[1].to_string();
        
        if name.is_empty() {
            return Err(format!("Empty field name in spec '{}'", s));
        }

        let dtype = if parts.len() >= 3 && !parts[2].is_empty() {
            DataType::from_str(parts[2])?
        } else {
            DataType::String
        };

        let op = if parts.len() >= 4 && !parts[3].is_empty() {
            Some(AggOp::from_str(parts[3])?)
        } else {
            None
        };

        // Validate: count operation requires None source
        if let Some(AggOp::Count) = op {
            if source != FieldSource::None {
                return Err(format!(
                    "Count operation in '{}' should use '_' as source (e.g., '_:rowcount:i:count')", s
                ));
            }
        }

        // Validate: None source requires count operation
        if source == FieldSource::None && op != Some(AggOp::Count) {
            return Err(format!(
                "Source '_' in '{}' is only valid with count operation", s
            ));
        }

        Ok(FieldSpec { source, name, dtype, op })
    }
}

/// Parse multiple field specs from semicolon-separated string
pub fn parse_field_specs(s: &str) -> Result<Vec<FieldSpec>, String> {
    // Remove all whitespace before parsing
    let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    
    let mut specs = Vec::new();
    for part in s.split(';') {
        if part.is_empty() {
            continue;
        }
        specs.push(FieldSpec::from_str(part)?);
    }
    if specs.is_empty() {
        return Err("No valid field specs found".to_string());
    }
    Ok(specs)
}

/// Validate field specs for DB mode (no aggregation ops allowed)
pub fn validate_field_specs_for_db(specs: &[FieldSpec]) -> Result<(), String> {
    for spec in specs {
        if spec.op.is_some() {
            return Err(format!(
                "Field '{}' has aggregation operation which is not allowed in DB mode. \
                Remove the operation or use 'map' subcommand for aggregation.",
                spec.name
            ));
        }
    }
    Ok(())
}

/// Validate field specs for Map mode (at least one key required)
pub fn validate_field_specs_for_map(specs: &[FieldSpec]) -> Result<(), String> {
    let has_key = specs.iter().any(|s| s.op == Some(AggOp::Key));
    if !has_key {
        return Err(
            "Map mode requires at least one key field (e.g., 'l1:name:s:key'). \
            Add ':key' or ':k' to at least one field spec.".to_string()
        );
    }
    Ok(())
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum OutFormat {
    Tsv,
    Csv,
    Box,
    Compact,
}

impl Default for OutFormat {
    fn default() -> Self {
        OutFormat::Box
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum DbBackend {
    #[value(name = "sqlite")]
    Sqlite,
    #[value(name = "duckdb")]
    DuckDB,
}

impl Default for DbBackend {
    fn default() -> Self {
        DbBackend::Sqlite
    }
}

impl std::fmt::Display for DbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbBackend::Sqlite => write!(f, "sqlite"),
            DbBackend::DuckDB => write!(f, "duckdb"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_INFO"), ")"), about)]
#[command(term_width = 0)]
#[command(args_conflicts_with_subcommands = false)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    // --- Input Sources ---
    /// Files or directories to scan. If directories, they are walked recursively.
    #[arg(value_name = "INPUTS", help_heading = "Input Sources", global = true)]
    pub inputs: Vec<PathBuf>,

    /// Read list of files from Stdin.
    #[arg(long = "files-from-stdin", help_heading = "Input Sources")]
    pub files_from_stdin: bool,

    /// Read file list from a specific file.
    #[arg(long = "file-list", help_heading = "Input Sources")]
    pub file_list: Option<PathBuf>,

    /// Read content DATA directly from Stdin (no filename).
    #[arg(long = "data-stdin", help_heading = "Input Sources")]
    pub data_stdin: bool,

    // --- Parsing ---
    /// Regular Expression to parse lines. Capturing groups are extracted into columns.
    #[arg(short = 'r', long = "regex", help_heading = "Parsing")]
    pub regex: String,

    /// Use PCRE2 regex engine instead of the default Rust regex engine.
    #[arg(long = "pcre2", help_heading = "Parsing")]
    pub use_pcre2: bool,

    /// Optional: Regular Expression to parse File Paths. Capturing groups are extracted into columns.
    #[arg(long = "path-regex", help_heading = "Parsing", verbatim_doc_comment)]
    pub path_regex: Option<String>,

    /// File path filter (regex) for directory walking.
    #[arg(short = 'f', long = "filter", help_heading = "Parsing", verbatim_doc_comment)]
    pub filter_pattern: Option<String>,

    /// Disable recursive directory walking
    #[arg(long = "no-recursive", help_heading = "Parsing")]
    pub no_recursive: bool,

    /// Field mapping: source:name[:type[:op]] separated by semicolons.
    /// Sources: l1,l2 (line captures), p1,p2 (path captures), _ (for count).
    /// Types: s/string (default), i/int, f/float.
    /// Ops (map mode only): k/key, sum, a/avg, c/count, x/max, n/min.
    /// Examples: "l1:host:s;l2:bytes:i" or "l1:host:s:key;l2:bytes:i:sum"
    #[arg(short = 'm', long = "map", help_heading = "Parsing", verbatim_doc_comment)]
    pub field_map: Option<String>,

    // --- Error Handling ---
    /// Print error location (File, Offset, Capture Group) to stderr as it happens.
    #[arg(short = 'e', long = "show-errors", help_heading = "Error Handling")]
    pub show_errors: bool,

    /// Stop processing immediately upon the first parse error.
    #[arg(short = 'E', long = "stop-on-error", help_heading = "Error Handling")]
    pub stop_on_error: bool,

    // --- Performance ---
    /// Stats ticker interval in milliseconds
    #[arg(long = "ticker", default_value_t = 1000, help_heading = "Performance")]
    pub ticker_interval: u64,

    /// Show verbose ticker stats including channel depths and additional metrics
    #[arg(long = "ticker-verbose", help_heading = "Performance")]
    pub ticker_verbose: bool,

    /// Number of file splicer threads
    #[arg(short = 's', long = "splicers", help_heading = "Performance")]
    pub splicer_threads: Option<usize>,

    /// Target chunk size for I/O splitting (e.g., '256KB', '1MB', or bytes without suffix)
    #[arg(long = "io-chunk-size", default_value = "256KB", help_heading = "Performance")]
    pub io_chunk_size: String,

    /// Max buffer size for long line handling (e.g., '1MB', '2048KB', must be > 2x chunk size)
    #[arg(long = "io-max-buffer", default_value = "1MB", help_heading = "Performance")]
    pub io_max_buffer: String,

    /// Comma separated list of operations to disable for benchmarking
    #[arg(long = "disable-operations", help_heading = "Performance", verbatim_doc_comment)]
    pub disable_operations: Option<String>,

    // --- Output ---
    /// Output format for results: tsv, csv, box, compact
    #[arg(long = "out-format", value_enum, default_value = "box", help_heading = "Output")]
    pub out_format: OutFormat,

    /// Significant digits for floating-point output
    #[arg(long = "sig-digits", default_value_t = 4, help_heading = "Output")]
    pub sig_digits: usize,

    /// For TSV output: expand tabs for aligned columns
    #[arg(long = "expand-tabs", help_heading = "Output")]
    pub expand_tabs: bool,

    /// Show detailed help with full documentation (renders README)
    #[arg(long = "long-help", help_heading = "Help")]
    pub long_help: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Database ingestion mode
    Db(DbOptions),
    /// Map/aggregation mode
    Map(MapOptions),
}

#[derive(Args, Debug)]
pub struct DbOptions {
    /// Database output file. Defaults to scan_HHMMSS.db or scan_HHMMSS.duckdb
    #[arg(long, help_heading = "Database")]
    pub db_path: Option<String>,

    /// Database backend to use
    #[arg(long = "db-backend", value_enum, default_value_t = DbBackend::Sqlite, help_heading = "Database")]
    pub db_backend: DbBackend,

    /// Enable optional tracking tables (files, matches) to store full context
    #[arg(long, help_heading = "Database")]
    pub track_matches: bool,

    /// Batch size for DB inserts
    #[arg(long, default_value = "1000", help_heading = "Database")]
    pub batch_size: usize,

    /// Capacity of the DB record channel (number of records)
    #[arg(long = "db-channel-size", default_value_t = 65536, help_heading = "Database")]
    pub db_channel_size: usize,

    /// SQLite cache size in MB (SQLite only)
    #[arg(long = "cache-mb", default_value_t = 100, help_heading = "Database")]
    pub cache_mb: i64,

    /// Number of threads for DuckDB internal operations (DuckDB only)
    #[arg(long = "duckdb-threads", default_value_t = 8, help_heading = "Database")]
    pub duckdb_threads: usize,

    /// Memory limit for DuckDB (e.g., '2GB', '512MB') (DuckDB only)
    #[arg(long = "duckdb-memory-limit", default_value = "1GB", help_heading = "Database")]
    pub duckdb_memory_limit: String,

    /// Number of regex parser threads
    #[arg(short = 'p', long = "parsers", help_heading = "Performance")]
    pub parser_threads: Option<usize>,

    /// Execute SQL string BEFORE scanning starts
    #[arg(long = "pre-sql", help_heading = "SQL Hooks")]
    pub pre_sql: Option<String>,

    /// Execute SQL string AFTER scanning finishes
    #[arg(long = "post-sql", help_heading = "SQL Hooks")]
    pub post_sql: Option<String>,

    /// Execute SQL script from file BEFORE scanning starts
    #[arg(long = "pre-sql-file", help_heading = "SQL Hooks")]
    pub pre_sql_file: Option<PathBuf>,

    /// Execute SQL script from file AFTER scanning finishes
    #[arg(long = "post-sql-file", help_heading = "SQL Hooks")]
    pub post_sql_file: Option<PathBuf>,
}

impl DbOptions {
    /// Validate backend-specific options
    pub fn validate(&self) {
        if matches!(self.db_backend, DbBackend::DuckDB) && self.cache_mb != 100 {
            eprintln!("Warning: --cache-mb is SQLite-specific and has no effect with --db-backend duckdb");
        }
        if matches!(self.db_backend, DbBackend::Sqlite) {
            if self.duckdb_threads != 8 {
                eprintln!("Warning: --duckdb-threads is DuckDB-specific and has no effect with SQLite backend");
            }
            if self.duckdb_memory_limit != "1GB" {
                eprintln!("Warning: --duckdb-memory-limit is DuckDB-specific and has no effect with SQLite backend");
            }
        }
    }
}

#[derive(Args, Debug)]
pub struct MapOptions {
    /// Number of mapper threads to use for aggregation
    #[arg(long = "map-threads", help_heading = "Performance")]
    pub map_threads: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DisableConfig {
    pub regex: bool,
    pub map_target: bool,
    pub map_write: bool,
}

impl DisableConfig {
    pub fn from_str(s: Option<&str>) -> Self {
        let mut cfg = DisableConfig::default();
        if let Some(s) = s {
            for part in s.split(',') {
                match part.trim() {
                    "regex" => cfg.regex = true,
                    "maptarget" => cfg.map_target = true,
                    "mapwrite" => cfg.map_write = true,
                    _ => {}
                }
            }
        }
        cfg
    }
}