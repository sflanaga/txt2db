use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_INFO"), ")"), about)]
// term_width = 0 means "Auto-detect terminal width".
#[command(term_width = 0)] 
pub struct Cli {
    // --- Input Sources ---
    /// Files or directories to scan. If directories, they are walked recursively.
    #[arg(value_name = "INPUTS", help_heading = "Input Sources")]
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


    // --- Parsing Options ---
    /// Regular Expression to parse lines.
    /// Capturing groups are extracted into columns.
    #[arg(short = 'r', long = "regex", help_heading = "Parsing Options")]
    pub regex: String,

    /// Use PCRE2 regex engine instead of the default Rust regex engine.
    /// This may provide better compatibility for complex Perl-style patterns.
    #[arg(long = "pcre2", help_heading = "Parsing Options")]
    pub use_pcre2: bool,

    /// Optional: Regular Expression to parse File Paths.  
    /// If provided, files not matching this regex are ignored.  
    /// Capturing groups are extracted into columns.
    #[arg(long = "path-regex", help_heading = "Parsing Options", verbatim_doc_comment)]
    pub path_regex: Option<String>,

    /// File path filter (regex) for directory walking. 
    /// (Distinct from --path-regex, which extracts fields)
    #[arg(short = 'f', long = "filter", help_heading = "Parsing Options", verbatim_doc_comment)]
    pub filter_pattern: Option<String>,

    /// Disable recursive directory walking
    #[arg(long = "no-recursive", help_heading = "Parsing Options")]
    pub no_recursive: bool,

    /// Field mapping (e.g., "p1:host;l1:date"). 
    /// Prefixes: 'p' for path capture groups, 'l' for line capture groups.
    /// If omitted, defaults to pf_N (path) and lf_N (line) or f_N.
    #[arg(short = 'F', long = "fields", help_heading = "Parsing Options", verbatim_doc_comment)]
    pub field_map: Option<String>,
    
    /// Aggregation map definition (e.g. "1_k_i;2_k_s;5_s_i").
    /// Mutually exclusive with Database mode.
    /// Format: index_role_type separated by ;. 
    /// Roles: k=Key, s=Sum, c=Count, x=Max, n=Min, a=Avg.
    /// Types: i=i64, u=u64, f=f64, s=String.
    #[arg(short = 'm', long = "map", help_heading = "Parsing Options", verbatim_doc_comment)]
    pub map_def: Option<String>,

    /// Number of mapper threads to use for aggregation.
    /// Defaults to half of available CPUs if not set.
    #[arg(long = "map-threads", help_heading = "Performance", verbatim_doc_comment)]
    pub map_threads: Option<usize>,
    
    // --- Error Handling ---
    /// Print error location (File, Offset, Capture Group) to stderr as it happens.
    #[arg(short = 'e', long = "show-errors", help_heading = "Error Handling")]
    pub show_errors: bool,

    /// Stop processing immediately upon the first parse error.
    #[arg(short = 'E', long = "stop-on-error", help_heading = "Error Handling")]
    pub stop_on_error: bool,

    /// Enable manual internal profiling (Regex vs Parse vs Map time)
    #[arg(long = "profile", help_heading = "Performance")]
    pub profile: bool,
    
    /// Comma separated list of operations to disable for benchmarking:
    /// 'regex' (disable regex parsing), 'maptarget' (disable value parsing), 
    /// 'mapwrite' (disable map insertion).
    #[arg(long = "disable-operations", help_heading = "Performance", verbatim_doc_comment)]
    pub disable_operations: Option<String>,


    // --- Database Options ---
    /// Database output file. Defaults to scan_HHMMSS.db
    #[arg(long, help_heading = "Database Options")]
    pub db_path: Option<String>,

    /// Enable optional tracking tables (files, matches) to store full context
    #[arg(long, help_heading = "Database Options")]
    pub track_matches: bool,

    /// Batch size for DB inserts
    #[arg(long, default_value = "1000", help_heading = "Database Options")]
    pub batch_size: usize,

    /// SQLite Cache Size in MB
    #[arg(long = "cache-mb", default_value_t = 100, help_heading = "Database Options")]
    pub cache_mb: i64,


    // --- SQL Hooks ---
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


    // --- Performance/System ---
    /// Stats ticker interval in milliseconds
    #[arg(long = "ticker", default_value_t = 1000, help_heading = "Performance")]
    pub ticker_interval: u64,

    /// Number of file splicer threads
    #[arg(short = 's', long = "splicers", help_heading = "Performance")]
    pub splicer_threads: Option<usize>,

    /// Number of regex parser threads (Only used in DB mode)
    #[arg(short = 'p', long = "parsers", help_heading = "Performance")]
    pub parser_threads: Option<usize>,
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
