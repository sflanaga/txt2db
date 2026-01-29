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

    /// Field mapping (e.g., "p1:host;l1:date"). Prefixes: 'p' for path, 'l' for line.
    #[arg(short = 'F', long = "fields", help_heading = "Parsing", verbatim_doc_comment)]
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
    /// Aggregation map definition (e.g. "1_k_i;2_k_s;5_s_i").
    /// Format: index_role_type separated by ;.
    /// Roles: k=Key, s=Sum, c=Count, x=Max, n=Min, a=Avg.
    /// Types: i=i64, u=u64, f=f64, s=String.
    #[arg(short = 'm', long = "map", help_heading = "Aggregation", verbatim_doc_comment)]
    pub map_def: String,

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