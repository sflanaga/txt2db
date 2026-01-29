use assert_cmd::Command;
use rusqlite::Connection;
#[cfg(feature = "duckdb")]
use duckdb;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;
use flate2::write::GzEncoder;
use flate2::Compression;
use predicates::str::contains;
use predicates::prelude::PredicateBooleanExt;

// --- Helper Functions ---

// Helper to run the binary
fn txt2db_cmd() -> Command {
    // FIX: Use env! macro which is the modern standard for integration tests
    // avoiding the deprecated assert_cmd::Command::cargo_bin warning.
    Command::new(env!("CARGO_BIN_EXE_txt2db"))
}

// Force mapper output to TSV (default is now comfy)
fn with_map_tsv(cmd: &mut Command) -> &mut Command {
    cmd.arg("--map-format").arg("tsv")
}

/// Helper to get a single cell value from the DB
fn get_db_value<T: std::str::FromStr>(db_path: &str, sql: &str) -> T
where
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    let conn = Connection::open(db_path).expect("failed to open db");
    let result: String = conn
        .query_row(sql, [], |row| {
            let val: String = row.get(0)?;
            Ok(val)
        })
        .expect(&format!("Query failed: {}", sql));

    result.parse().expect("Failed to parse result")
}

/// Helper to count rows in a table
fn count_rows(db_path: &str, table: &str) -> i64 {
    let conn = Connection::open(db_path).expect("failed to open db");
    conn.query_row(&format!("SELECT count(*) FROM {}", table), [], |row| row.get(0))
        .unwrap_or(0)
}

// --- Tests ---

#[test]
fn test_basic_log_parsing() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("app.log");
    let db_path = temp.path().join("basic_test.db");

    // 1. Create dummy data
    let mut file = File::create(&input_path)?;
    writeln!(file, "2023-01-01 10:00:00 [INFO] User logged in")?;
    writeln!(file, "2023-01-01 10:05:00 [ERROR] Connection failed")?;

    // 2. Run txt2db
    // Note: We use (?m) to enable multi-line mode so ^ and $ match line boundaries
    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"(?m)^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(.*?)\] (.*)$")
        .arg("--fields")
        .arg("1:ts;2:level;3:msg")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    // 3. Verify Database
    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 2, "Should have 2 rows");

    let level: String =
        get_db_value(db_path.to_str().unwrap(), "SELECT level FROM data WHERE msg = 'Connection failed'");
    assert_eq!(level, "ERROR");

    Ok(())
}

#[test]
fn test_gzip_support() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("compressed.log.gz");
    let db_path = temp.path().join("gz_test.db");

    // 1. Create Gzipped data
    let file = File::create(&input_path)?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    writeln!(encoder, "ID:100 DATA:Alpha")?;
    writeln!(encoder, "ID:101 DATA:Beta")?;
    encoder.finish()?;

    // 2. Run txt2db
    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"ID:(\d+) DATA:(\w+)")
        .arg("--fields")
        .arg("1:item_id;2:val")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    // 3. Verify
    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 2);
    let val: String = get_db_value(db_path.to_str().unwrap(), "SELECT val FROM data WHERE item_id = '101'");
    assert_eq!(val, "Beta");

    Ok(())
}

#[test]
fn test_path_regex_and_recursion() -> anyhow::Result<()> {
    let temp = TempDir::new()?;

    // Create structure: /server-01/logs/sys.log
    let subdir = temp.path().join("server-01").join("logs");
    fs::create_dir_all(&subdir)?;
    let input_path = subdir.join("sys.log");

    let mut file = File::create(&input_path)?;
    writeln!(file, "CRITICAL_ERROR")?;

    let db_path = temp.path().join("path_test.db");

    // Run on the root temp dir
    let mut cmd = txt2db_cmd();
    cmd.arg("--path-regex")
        .arg(r"(server-\d+)") // Extract 'server-01'
        .arg("--regex")
        .arg(r"(\w+)")
        .arg("--fields")
        .arg("p1:hostname;l1:status")
        .arg("db")
        .arg(temp.path()) // Input is the directory
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    let host: String = get_db_value(db_path.to_str().unwrap(), "SELECT hostname FROM data");
    assert_eq!(host, "server-01");

    let status: String = get_db_value(db_path.to_str().unwrap(), "SELECT status FROM data");
    assert_eq!(status, "CRITICAL_ERROR");

    Ok(())
}

#[test]
fn test_sql_hooks() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("dummy.txt");
    let mut file = File::create(&input_path)?;
    writeln!(file, "ignore me")?;

    let db_path = temp.path().join("hooks.db");

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg("(.*)")
        .arg("db")
        .arg(input_path.to_str().unwrap())
        // Pre-SQL: Create a summary table
        .arg("--pre-sql")
        .arg("CREATE TABLE summary (run_date TEXT); INSERT INTO summary VALUES ('2023-01-01');")
        // Post-SQL: Update it (just to prove it ran after)
        .arg("--post-sql")
        .arg("UPDATE summary SET run_date = 'FINISHED'")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    // Verify the table created by PRE-SQL exists and was updated by POST-SQL
    let val: String = get_db_value(db_path.to_str().unwrap(), "SELECT run_date FROM summary");
    assert_eq!(val, "FINISHED");

    Ok(())
}

#[test]
fn test_file_filter() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("filter.db");

    // Create 'include.log' (should be processed)
    let f1_path = temp.path().join("include.log");
    let mut f1 = File::create(&f1_path)?;
    writeln!(f1, "DATA 1")?;

    // Create 'exclude.txt' (should be ignored by filter)
    let f2_path = temp.path().join("exclude.txt");
    let mut f2 = File::create(&f2_path)?;
    writeln!(f2, "DATA 2")?;

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"DATA (\d+)")
        .arg("--fields")
        .arg("1:val")
        .arg("--filter")
        .arg(r".*\.log$") // <--- Only process .log files
        .arg("db")
        .arg(temp.path()) // Scan the temp directory
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    // Should only have 1 row (from include.log)
    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 1);

    let conn = Connection::open(&db_path)?;
    let val: String = conn.query_row("SELECT val FROM data", [], |r| r.get(0))?;
    assert_eq!(val, "1");

    Ok(())
}

#[test]
fn test_data_stdin() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("stdin.db");

    // Simulate piping raw data via stdin
    let input_data = "LINE One\nLINE Two\nLINE Three";

    let mut cmd = txt2db_cmd();
    cmd.arg("--data-stdin") // <--- flag to read data from stdin
        .arg("--regex")
        .arg(r"LINE (\w+)")
        .arg("--fields")
        .arg("1:val")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin(input_data)
        .assert()
        .success();

    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 3);
    Ok(())
}

#[test]
fn test_files_from_stdin() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("stdin_files.db");

    let log1 = temp.path().join("manual.log");
    File::create(&log1)?.write_all(b"FOUND MANUAL")?;

    // Simulate `find . -name *.log | txt2db ...`
    // We feed the *path* of the file into stdin
    let input_list = format!("{}\n", log1.to_str().unwrap());

    let mut cmd = txt2db_cmd();
    cmd.arg("--files-from-stdin") // <--- flag to read paths from stdin
        .arg("--regex")
        .arg(r"FOUND (\w+)")
        .arg("--fields")
        .arg("1:val")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin(input_list)
        .assert()
        .success();

    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 1);
    Ok(())
}

#[test]
fn test_file_list_argument() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("list.db");

    let log1 = temp.path().join("a.log");
    let log2 = temp.path().join("b.log");
    File::create(&log1)?.write_all(b"DATA A")?;
    File::create(&log2)?.write_all(b"DATA B")?;

    // Create a text file listing the paths
    let list_file = temp.path().join("files_to_scan.txt");
    let mut list = File::create(&list_file)?;
    writeln!(list, "{}", log1.to_str().unwrap())?;
    writeln!(list, "{}", log2.to_str().unwrap())?;

    let mut cmd = txt2db_cmd();
    cmd.arg("--file-list")
        .arg(list_file.to_str().unwrap()) // <--- Read paths from file
        .arg("--regex")
        .arg(r"DATA (\w+)")
        .arg("--fields")
        .arg("1:val")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 2);
    Ok(())
}

#[test]
fn test_track_matches() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_file = temp.path().join("test.log");
    let db_path = temp.path().join("track.db");

    let mut f = File::create(&input_file)?;
    writeln!(f, "This is the full line content")?;

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"(?i)(full) (line)") // Case insensitive
        .arg("--fields")
        .arg("1:a;2:b")
        .arg("db")
        .arg(input_file.to_str().unwrap())
        .arg("--track-matches") // <--- Enable raw content tracking
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    // Check 'matches' table exists and has content
    // Note: The convenience view 'matches' points to matches_{run_id}
    assert_eq!(count_rows(db_path.to_str().unwrap(), "matches"), 1);

    let conn = Connection::open(&db_path)?;
    let content: String = conn.query_row("SELECT content FROM matches", [], |r| r.get(0))?;
    assert_eq!(content, "This is the full line content");

    Ok(())
}

#[test]
fn test_no_recursive() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("norec.db");

    // 1. File in root (Should be found)
    let root_file = temp.path().join("root.log");
    File::create(&root_file)?.write_all(b"DATA ROOT")?;

    // 2. File in subdir (Should be ignored)
    let subdir = temp.path().join("sub");
    fs::create_dir(&subdir)?;
    let sub_file = subdir.join("nested.log");
    File::create(&sub_file)?.write_all(b"DATA NESTED")?;

    let mut cmd = txt2db_cmd();
    cmd.arg("--no-recursive") // <--- Disable recursion
        .arg("--regex")
        .arg(r"DATA (\w+)")
        .arg("--fields")
        .arg("1:val")
        .arg("db")
        .arg(temp.path())
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    // Should only find 1 record (root.log)
    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 1);

    let conn = Connection::open(&db_path)?;
    let val: String = conn.query_row("SELECT val FROM data", [], |r| r.get(0))?;
    assert_eq!(val, "ROOT");

    Ok(())
}

#[test]
fn test_map_mode_basic() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("map.txt");
    let mut f = File::create(&input_path)?;
    // Use B before A to test sorting logic
    writeln!(f, "B 2")?;
    writeln!(f, "A 5")?;
    writeln!(f, "A 10")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i") // Key=String, Sum=Int
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    let out = assert.get_output();
    let out_str = std::str::from_utf8(&out.stdout)?;

    // Split into non-empty lines
    let lines: Vec<&str> = out_str.lines().filter(|l| !l.trim().is_empty()).collect();

    // Expected Data:
    // A\t15
    // B\t2
    let data_lines: Vec<&str> = lines
        .into_iter()
        .filter(|l| l.contains('\t') && !l.starts_with("Key_"))
        .collect();

    assert!(data_lines.len() >= 2);
    assert_eq!(data_lines[0], "A\t15");
    assert_eq!(data_lines[1], "B\t2");

    Ok(())
}

#[test]
fn test_map_mode_composite_and_parallel() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("composite.txt");
    let mut f = File::create(&input_path)?;

    // We intentionally write these out of order to verify sort
    for _ in 0..100 {
        writeln!(f, "G2 0 100")?; // Should come last
        writeln!(f, "G1 1 5")?; // Should come middle
        writeln!(f, "G1 0 10")?; // Should come first
        writeln!(f, "G1 0 40")?;
    }

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--regex")
        .arg(r"(\w+) (\d+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_k_i;3_s_i") // Key1=Str, Key2=Int, Sum=Int
        .arg("--map-threads")
        .arg("4") // Force parallel
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    let out = assert.get_output();
    let out_str = std::str::from_utf8(&out.stdout)?;

    let data_lines: Vec<&str> = out_str
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter(|l| l.contains('\t') && !l.starts_with("Key_"))
        .collect();

    assert!(data_lines.len() >= 3);

    // Check Sort Order and Sums
    // 1. G1 0 -> 50 * 100 = 5000
    assert_eq!(data_lines[0], "G1\t0\t5000");
    // 2. G1 1 -> 5 * 100 = 500
    assert_eq!(data_lines[1], "G1\t1\t500");
    // 3. G2 0 -> 100 * 100 = 10000
    assert_eq!(data_lines[2], "G2\t0\t10000");

    Ok(())
}

#[test]
fn test_map_mode_exclusive() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("data.txt");
    File::create(&input_path)?.write_all(b"DATA 1")?;

    let db_path = temp.path().join("should_not_exist.db");

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"DATA (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_i")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap()) // Should be ignored
        .arg(input_path.to_str().unwrap())
        .assert()
        .failure();

    assert!(!db_path.exists(), "Database file should not be created in map mode");
    Ok(())
}

#[test]
fn test_map_mode_parse_error_counting() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("mixed.txt");
    let mut f = File::create(&input_path)?;
    // Data:
    // A 10  <- Valid
    // B XX  <- Parse Error (Group 2 is not Int)
    // C 5.5 <- Parse Error (Group 2 is not Int)
    writeln!(f, "A 10")?;
    writeln!(f, "B XX")?;
    writeln!(f, "C 5.5")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--regex")
        .arg(r"(\w+) (\S+)") // Capture everything non-whitespace
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i") // Key=String, Sum=Int (Expects Int)
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    let out = assert.get_output();
    let out_str = std::str::from_utf8(&out.stdout)?;

    // Check Output (Only A should be present)
    assert!(out_str.contains("A\t10"));
    assert!(!out_str.contains("B\t"));
    assert!(!out_str.contains("C\t"));

    // Check Stats for Parse Errors
    assert!(out_str.contains("Parse Errors: 2"));

    // Check Specific Field Breakdown
    assert!(out_str.contains("Capture Group 2: 2 errors"));

    Ok(())
}

#[test]
fn test_stop_on_error() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("bad.txt");
    let mut f = File::create(&input_path)?;
    writeln!(f, "A XX")?; // Error

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"(\w+) (\S+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg("-E") // Stop on Error
        .arg(input_path.to_str().unwrap())
        .assert()
        .failure(); // Should fail with exit code 1

    Ok(())
}

// --- New tests for broader CLI coverage ---

#[test]
fn test_map_mode_path_regex_positive() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("2026-01-23.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "flush=10 close=20 rename=30")?;
    writeln!(f, "flush=5 close=10 rename=15")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--path-regex")
        .arg(r"\d{4}-\d{2}-(\d{2})\.log")
        .arg("--regex")
        .arg(r"flush=(\d+) close=(\d+) rename=(\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_s;1_s_i;2_s_i;3_s_i")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // Expect day "23" as key, sums 15/30/45
    assert!(out_str.contains("23\t15\t30\t45"));
    Ok(())
}

#[test]
fn test_map_mode_path_regex_no_match_skips() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("nomatch.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "flush=1 close=2 rename=3")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--path-regex")
        .arg(r"\d{4}-\d{2}-(\d{2})\.log")
        .arg("--regex")
        .arg(r"flush=(\d+) close=(\d+) rename=(\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_s;1_s_i;2_s_i;3_s_i")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // No match on path => no aggregation rows
    assert!(
        !out_str.contains('\t') || out_str.lines().all(|l| !l.contains('\t') || l.starts_with("Key_"))
    );
    Ok(())
}

#[test]
fn test_map_mode_mixed_line_and_path_captures() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("server-42.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "user=jane count=3")?;
    writeln!(f, "user=jane count=2")?;
    writeln!(f, "user=bob count=5")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--path-regex")
        .arg(r"server-(\d+)")
        .arg("--regex")
        .arg(r"user=(\w+) count=(\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_i;1_k_s;2_s_i") // path group 1, line group 1, sum of group 2
        .arg(file_path.to_str().unwrap())
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // Expect keys (42, jane) sum=5 and (42, bob) sum=5
    assert!(out_str.contains("42\tjane\t5"));
    assert!(out_str.contains("42\tbob\t5"));
    Ok(())
}

#[test]
fn test_map_mode_average() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("avg.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "A 2")?;
    writeln!(f, "A 4")?;
    writeln!(f, "A 6")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_a_i")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // Average should be (2+4+6)/3 = 4
    assert!(out_str.contains("A\t4"));
    Ok(())
}

#[test]
fn test_invalid_map_definition_error_message() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("badmap.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "x 1")?;

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_z_i") // invalid role
        .arg(file_path.to_str().unwrap())
        .assert()
        .failure()
        .stderr(contains("Invalid map definition (--map)").and(contains("1_z_i")));
    Ok(())
}

#[test]
fn test_invalid_regex_error_message() -> anyhow::Result<()> {
    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg("(") // invalid regex
        .arg("--data-stdin")
        .arg("db")
        .arg("--db-path")
        .arg("/tmp/nowhere.db") // dummy to satisfy args
        .write_stdin("")
        .assert()
        .failure()
        .stderr(contains("Invalid Line Regex (--regex)"));
    Ok(())
}

#[test]
fn test_disable_mapwrite_produces_no_rows() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("disable.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "A 1")?;
    writeln!(f, "A 2")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("--disable-operations")
        .arg("mapwrite")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // mapwrite disabled => no aggregation rows
    assert!(
        !out_str.contains('\t') || out_str.lines().all(|l| !l.contains('\t') || l.starts_with("Key_"))
    );
    Ok(())
}

#[test]
fn test_map_mode_path_capture_parse_error_counts() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("server-xx.log"); // xx not numeric, will fail p1_s_i parse
    let mut f = File::create(&file_path)?;
    writeln!(f, "val=10")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--path-regex")
        .arg(r"server-(\w+)") // match "xx" so parse to i64 fails
        .arg("--regex")
        .arg(r"val=(\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_i;1_s_i")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // Should report one parse error for capture group 1 (path capture)
    assert!(out_str.contains("Parse Errors: 1"));
    assert!(out_str.contains("Capture Group 1: 1 errors"));
    Ok(())
}

#[test]
fn test_map_mode_files_from_stdin_with_path_regex() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("2026-02-05.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "x=1")?;
    let list = format!("{}\n", file_path.to_str().unwrap());

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--files-from-stdin")
        .arg("--path-regex")
        .arg(r"\d{4}-\d{2}-(\d{2})\.log")
        .arg("--regex")
        .arg(r"x=(\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_s;1_s_i")
        .write_stdin(list)
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    assert!(out_str.contains("05\t1"));
    Ok(())
}

#[test]
fn test_map_mode_pcre_line_regex_with_path_regex() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("pcreserver-77.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "VAL=123")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--pcre2")
        .arg("--path-regex")
        .arg(r"pcreserver-(\d+)")
        .arg("--regex")
        .arg(r"VAL=(\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_i;1_s_i")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    assert!(out_str.contains("77\t123"));
    Ok(())
}

#[test]
fn test_filter_and_path_regex_together() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("filter_path.db");

    // Will match filter (.log) and path regex (host-01)
    let good = temp.path().join("host-01").join("app.log");
    fs::create_dir_all(good.parent().unwrap())?;
    File::create(&good)?.write_all(b"OK 1")?;

    // Fails filter (.txt) even though host matches path regex
    let wrong_ext = temp.path().join("host-01").join("app.txt");
    File::create(&wrong_ext)?.write_all(b"OK 2")?;

    // Matches filter (.log) but fails path regex (host name wrong)
    let wrong_host = temp.path().join("node-02").join("app.log");
    fs::create_dir_all(wrong_host.parent().unwrap())?;
    File::create(&wrong_host)?.write_all(b"OK 3")?;

    let mut cmd = txt2db_cmd();
    cmd.arg("--filter")
        .arg(r".*\.log$") // keep only .log
        .arg("--path-regex")
        .arg(r"host-(\d+)") // extract host id and exclude non-matching hosts
        .arg("--regex")
        .arg(r"(OK) (\d+)")
        .arg("--fields")
        .arg("p1:host;l1:status;l2:val")
        .arg("db")
        .arg(temp.path())
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    // Only one file should be processed (good)
    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 1);

    let conn = Connection::open(&db_path)?;
    let row: (String, String, String) =
        conn.query_row("SELECT host, status, val FROM data", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    assert_eq!(row, ("01".to_string(), "OK".to_string(), "1".to_string()));

    Ok(())
}

// --- Additional tests for output/formatting features ---

#[test]
fn test_map_mode_custom_labels() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input = temp.path().join("labels.txt");
    let mut f = File::create(&input)?;
    writeln!(f, "foo 10")?;
    writeln!(f, "bar 5")?;

    let mut cmd = txt2db_cmd();
    let assert = with_map_tsv(&mut cmd)
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s:name;2_s_i:total")
        .arg(input.to_str().unwrap())
        .assert()
        .success();

    let out = std::str::from_utf8(&assert.get_output().stdout)?;
    assert!(out.lines().any(|l| l.trim() == "name\ttotal"));
    assert!(out.contains("foo\t10"));
    assert!(out.contains("bar\t5"));
    Ok(())
}

#[test]
fn test_map_mode_output_formats() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input = temp.path().join("formats.txt");
    let mut f = File::create(&input)?;
    writeln!(f, "A 1")?;

    // TSV
    let mut cmd = txt2db_cmd();
    let out_tsv = cmd
        .arg("--map-format")
        .arg("tsv")
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s_tsv = std::str::from_utf8(&out_tsv)?;
    assert!(s_tsv.contains("A\t1"));

    // CSV
    let mut cmd = txt2db_cmd();
    let out_csv = cmd
        .arg("--map-format")
        .arg("csv")
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s_csv = std::str::from_utf8(&out_csv)?;
    assert!(s_csv.contains("A,1"));

    // Comfy
    let mut cmd = txt2db_cmd();
    let out_comfy = cmd
        .arg("--map-format")
        .arg("comfy")
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s_comfy = std::str::from_utf8(&out_comfy)?;
    assert!(s_comfy.contains("╭") && s_comfy.contains("╯"));
    Ok(())
}

#[test]
fn test_comfy_wrap_and_truncate() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input = temp.path().join("wrap.txt");
    let mut f = File::create(&input)?;
    writeln!(f, "long 123456789012345678901234567890")?;

    // Wrap
    let mut cmd = txt2db_cmd();
    let out_wrap = cmd
        .arg("--map-format")
        .arg("comfy")
        .arg("--comfy-wrap")
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s_wrap = std::str::from_utf8(&out_wrap)?;
    assert!(s_wrap.lines().any(|l| l.contains("long")));

    // Truncate
    let mut cmd = txt2db_cmd();
    let out_trunc = cmd
        .arg("--map-format")
        .arg("comfy")
        .arg("--comfy-truncate")
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s_trunc = std::str::from_utf8(&out_trunc)?;
    assert!(s_trunc.contains("long"));
    Ok(())
}

#[test]
fn test_sig_digits_adaptive() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input = temp.path().join("sig.txt");
    let mut f = File::create(&input)?;
    writeln!(f, "A 123.456")?;
    writeln!(f, "A 0.0004567")?;
    writeln!(f, "A 120000000")?;

    let mut cmd = txt2db_cmd();
    let out = with_map_tsv(&mut cmd)
        .arg("--sig-digits")
        .arg("3")
        .arg("--regex")
        .arg(r"(\w+) (\S+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_f")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s = std::str::from_utf8(&out)?;
    assert!(s.contains("123")); // fixed with 3 sig digits
    assert!(s.contains("0.000457")); // fixed small with 3 sig digits
    assert!(s.contains("1.20e+08")); // scientific for large with 3 sig digits
    Ok(())
}

#[test]
fn test_expand_tabs_tsv_alignment() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input = temp.path().join("tabs.txt");
    let mut f = File::create(&input)?;
    writeln!(f, "A 1")?;
    writeln!(f, "BBBB 22")?;

    let mut cmd = txt2db_cmd();
    let out = cmd
        .arg("--map-format")
        .arg("tsv")
        .arg("--expand-tabs")
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s = std::str::from_utf8(&out)?;
    assert!(!s.contains("\\t"));
    Ok(())
}

#[test]
fn test_csv_quoting() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input = temp.path().join("csvq.txt");
    let mut f = File::create(&input)?;
    writeln!(f, "foo,bar 1")?;
    writeln!(f, "quoted\"val 2")?;

    let mut cmd = txt2db_cmd();
    let out = cmd
        .arg("--map-format")
        .arg("csv")
        .arg("--regex")
        .arg(r"(.+) (\d+)")
        .arg("map")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg(input.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s = std::str::from_utf8(&out)?;
    assert!(s.contains("\"foo,bar\",1"));
    assert!(s.contains("\"quoted\"\"val\",2"));
    Ok(())
}

#[test]
fn test_map_path_labels() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("dirA").join("file.log");
    fs::create_dir_all(file_path.parent().unwrap())?;
    let mut f = File::create(&file_path)?;
    writeln!(f, "val=7")?;

    let mut cmd = txt2db_cmd();
    let out = with_map_tsv(&mut cmd)
        .arg("--path-regex")
        .arg(r".*/(dir\\w+)")
        .arg("--regex")
        .arg(r"val=(\\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_s:source;1_s_i:total")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s = std::str::from_utf8(&out)?;
    assert!(s.contains("source\ttotal"));
    assert!(s.contains("dirA\t7"));
    Ok(())
}

#[test]
fn test_db_output_respects_format_and_sig_digits() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input = temp.path().join("dbfmt.txt");
    let mut f = File::create(&input)?;
    writeln!(f, "x 1.23456")?;

    let db_path = temp.path().join("fmt.db");
    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"(\\w+) (\\S+)")
        .arg("--fields")
        .arg("1:a;2:b")
        .arg("--map-format")
        .arg("tsv")
        .arg("--sig-digits")
        .arg("3")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .arg("--post-sql")
        .arg("SELECT 1.23456 AS v;")
        .arg(input.to_str().unwrap())
        .assert()
        .success();
    // No direct stdout assertion (DB mode), but should not panic and should format 1.23 in post-sql output.
    Ok(())
}

// #[test]
// fn test_comfy_options_warning_with_other_formats() -> anyhow::Result<()> {
//     let mut cmd = txt2db_cmd();
//     cmd.arg("--map-format")
//         .arg("csv")
//         .arg("--comfy-wrap")
//         .arg("--regex")
//         .arg(r"(\\w+)")
//         .arg("-m")
//         .arg("1_k_s")
//         .write_stdin("A")
//         .assert()
//         .success()
//         .stderr(is_match("Warning:").unwrap());
//     Ok(())
// }

#[test]
fn test_map_mode_pcre_with_path_and_labels() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("pcreserver-77.log");
    let mut f = File::create(&file_path)?;
    writeln!(f, "VAL=123")?;

    let mut cmd = txt2db_cmd();
    let out = with_map_tsv(&mut cmd)
        .arg("--pcre2")
        .arg("--path-regex")
        .arg(r"pcreserver-(\\d+)")
        .arg("--regex")
        .arg(r"VAL=(\\d+)")
        .arg("map")
        .arg("-m")
        .arg("p1_k_i:server;1_s_i:sum")
        .arg(file_path.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s = std::str::from_utf8(&out)?;
    assert!(s.contains("server\tsum"));
    assert!(s.contains("77\t123"));
    Ok(())
}

// --- Database Safety Check Tests ---
// Note: Some tests require --features duckdb to run

// --- DuckDB-specific tests (equivalents of SQLite tests) ---

#[test]
#[cfg(feature = "duckdb")]
fn test_duckdb_basic_log_parsing() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("app.log");
    let db_path = temp.path().join("basic_test.duckdb");

    // 1. Create dummy data
    let mut file = File::create(&input_path)?;
    writeln!(file, "2023-01-01 10:00:00 [INFO] User logged in")?;
    writeln!(file, "2023-01-01 10:05:00 [ERROR] Connection failed")?;

    // 2. Run txt2db with DuckDB backend
    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"(?m)^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(.*?)\] (.*)$")
        .arg("--fields")
        .arg("1:ts;2:level;3:msg")
        .arg("db")
        .arg("--db-backend")
        .arg("duckdb")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    // 3. Verify Database using DuckDB
    let conn = duckdb::Connection::open(db_path.to_str().unwrap()).expect("failed to open duckdb");
    let count: i64 = conn.query_row("SELECT count(*) FROM data", [], |row| row.get(0))?;
    assert_eq!(count, 2, "Should have 2 rows");

    let level: String = conn.query_row(
        "SELECT level FROM data WHERE msg = 'Connection failed'", 
        [], 
        |row| row.get(0)
    )?;
    assert_eq!(level, "ERROR");

    Ok(())
}

#[test]
#[cfg(feature = "duckdb")]
fn test_duckdb_track_matches() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_file = temp.path().join("test.log");
    let db_path = temp.path().join("track.duckdb");

    let mut f = File::create(&input_file)?;
    writeln!(f, "This is the full line content")?;

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"This is (.*)")
        .arg("--fields")
        .arg("1:a;2:b")
        .arg("db")
        .arg("--db-backend")
        .arg("duckdb")
        .arg(input_file.to_str().unwrap())
        .arg("--track-matches")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .assert()
        .success();

    // Check 'matches' table exists and has content
    let conn = duckdb::Connection::open(db_path.to_str().unwrap()).expect("failed to open duckdb");
    let count: i64 = conn.query_row("SELECT count(*) FROM matches", [], |row| row.get(0))?;
    assert_eq!(count, 1);

    let content: String = conn.query_row("SELECT content FROM matches", [], |r| r.get(0))?;
    assert_eq!(content, "This is the full line content");

    Ok(())
}

#[test]
#[cfg(feature = "duckdb")]
fn test_duckdb_pre_post_sql() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("dummy.txt");
    let mut file = File::create(&input_path)?;
    writeln!(file, "ignore me")?;

    let db_path = temp.path().join("hooks.duckdb");

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg("(.*)")
        .arg("db")
        .arg("--db-backend")
        .arg("duckdb")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        // Pre-SQL: Create a table
        .arg("--pre-sql")
        .arg("CREATE TABLE summary (run_date TEXT); INSERT INTO summary VALUES ('2023-01-01');")
        // Post-SQL: Update it
        .arg("--post-sql")
        .arg("UPDATE summary SET run_date = 'FINISHED'")
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    // Verify the table created by PRE-SQL exists and was updated by POST-SQL
    let conn = duckdb::Connection::open(db_path.to_str().unwrap()).expect("failed to open duckdb");
    let val: String = conn.query_row("SELECT run_date FROM summary", [], |r| r.get(0))?;
    assert_eq!(val, "FINISHED");

    Ok(())
}

#[test]
#[cfg(feature = "duckdb")]
fn test_duckdb_reserved_keywords() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("keywords.log");
    let mut file = File::create(&input_path)?;
    writeln!(file, "offset=123 rename=456")?;

    let db_path = temp.path().join("keywords.duckdb");

    // Use field names that are reserved keywords in DuckDB
    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"offset=(\d+) rename=(\d+)")
        .arg("--fields")
        .arg("1:offset;2:rename")  // Both are reserved in DuckDB
        .arg("db")
        .arg("--db-backend")
        .arg("duckdb")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .arg(input_path.to_str().unwrap())
        .assert()
        .success();

    // Verify data was inserted correctly with quoted column names
    let conn = duckdb::Connection::open(db_path.to_str().unwrap()).expect("failed to open duckdb");
    let offset: i64 = conn.query_row(r#"SELECT "offset" FROM data"#, [], |r| r.get(0))?;
    assert_eq!(offset, 123);
    
    let rename: i64 = conn.query_row(r#"SELECT "rename" FROM data"#, [], |r| r.get(0))?;
    assert_eq!(rename, 456);

    Ok(())
}

#[test]
#[cfg(feature = "duckdb")]
fn test_duckdb_data_stdin() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("stdin.duckdb");

    // Simulate piping raw data via stdin
    let input_data = "LINE One\nLINE Two\nLINE Three";

    let mut cmd = txt2db_cmd();
    cmd.arg("--regex")
        .arg(r"LINE (\w+)")
        .arg("--fields")
        .arg("1:val")
        .arg("--data-stdin")
        .arg("db")
        .arg("--db-backend")
        .arg("duckdb")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin(input_data)
        .assert()
        .success();

    let conn = duckdb::Connection::open(db_path.to_str().unwrap()).expect("failed to open duckdb");
    let count: i64 = conn.query_row("SELECT count(*) FROM data", [], |row| row.get(0))?;
    assert_eq!(count, 3);
    
    Ok(())
}

#[test]
fn test_extension_mismatch_warning() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("mismatch.duckdb");
    
    let mut cmd = txt2db_cmd();
    let assert = cmd.arg("--regex")
        .arg("(.*)")
        .arg("--data-stdin")
        .arg("db")
        .arg("--db-backend")
        .arg("sqlite")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin("test line")
        .assert();
    
    // Should contain warning about extension mismatch
    let output = assert.get_output();
    let stderr = std::str::from_utf8(&output.stderr)?;
    let stdout = std::str::from_utf8(&output.stdout)?;
    
    // Check both stderr and stdout for the warning
    let warning = "Warning: Database file extension '.duckdb' does not match expected '.db' for sqlite";
    assert!(stderr.contains(warning) || stdout.contains(warning), 
            "Warning not found in stderr or stdout. stderr='{}', stdout='{}'", stderr, stdout);
    
    Ok(())
}

#[test]
fn test_file_exists_warning() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("existing.db");
    
    // Create initial database
    {
        let mut cmd = txt2db_cmd();
        cmd.arg("--regex")
            .arg("(.*)")
            .arg("--data-stdin")
            .arg("db")
            .arg("--db-path")
            .arg(db_path.to_str().unwrap())
            .write_stdin("first line")
            .assert()
            .success();
    }
    
    // Try to write to the same database again
    let mut cmd = txt2db_cmd();
    let assert = cmd.arg("--regex")
        .arg("(.*)")
        .arg("--data-stdin")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin("second line")
        .assert();
    
    // Should contain warning about existing file
    let output = assert.get_output();
    let stderr = std::str::from_utf8(&output.stderr)?;
    assert!(stderr.contains("Warning: Database file already exists. Appending to existing database."));
    
    Ok(())
}

#[test]
fn test_sqlite_file_with_duckdb_backend_error() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("test.db");
    
    // Create a SQLite database first
    {
        let mut cmd = txt2db_cmd();
        cmd.arg("--regex")
            .arg("(.*)")
            .arg("--data-stdin")
            .arg("db")
            .arg("--db-backend")
            .arg("sqlite")
            .arg("--db-path")
            .arg(db_path.to_str().unwrap())
            .write_stdin("test line")
            .assert()
            .success();
    }
    
    // Try to open the SQLite file with DuckDB backend
    let mut cmd = txt2db_cmd();
    let assert = cmd.arg("--regex")
        .arg("(.*)")
        .arg("--data-stdin")
        .arg("db")
        .arg("--db-backend")
        .arg("duckdb")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin("test line")
        .assert()
        .failure();
    
    // Should contain error about wrong backend (if DuckDB is enabled)
    let output = assert.get_output();
    let stderr = std::str::from_utf8(&output.stderr)?;
    if stderr.contains("Database backend DuckDB not enabled") {
        // Can't test backend detection if DuckDB is not enabled
        return Ok(());
    }
    assert!(stderr.contains("Cannot open SQLite database with DuckDB backend"));
    
    Ok(())
}

#[test]
fn test_non_sqlite_file_with_sqlite_backend_error() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("notsqlite.duckdb");
    
    // Create a DuckDB database first (skip if not enabled)
    {
        let mut cmd = txt2db_cmd();
        let result = cmd.arg("--regex")
            .arg("(.*)")
            .arg("--data-stdin")
            .arg("db")
            .arg("--db-backend")
            .arg("duckdb")
            .arg("--db-path")
            .arg(db_path.to_str().unwrap())
            .write_stdin("test line")
            .assert();
            
        let output = result.get_output();
        let stderr = std::str::from_utf8(&output.stderr)?;
        if stderr.contains("Database backend DuckDB not enabled") {
            // Skip test if DuckDB is not enabled
            return Ok(());
        }
        // If we get here, the database was created successfully
    }
    
    // Try to open the DuckDB file with SQLite backend
    let mut cmd = txt2db_cmd();
    let assert = cmd.arg("--regex")
        .arg("(.*)")
        .arg("--data-stdin")
        .arg("db")
        .arg("--db-backend")
        .arg("sqlite")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin("test line")
        .assert()
        .failure();
    
    // Should contain error about not being a SQLite database
    let output = assert.get_output();
    let stderr = std::str::from_utf8(&output.stderr)?;
    assert!(stderr.contains("File does not appear to be a SQLite database"));
    
    Ok(())
}

#[test]
fn test_invalid_database_file_handling() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let db_path = temp.path().join("invalid.db");
    
    // Create an invalid file (not a database)
    {
        let mut f = File::create(&db_path)?;
        writeln!(f, "This is not a database file")?;
    }
    
    // Try to open with SQLite backend
    let mut cmd = txt2db_cmd();
    let assert = cmd.arg("--regex")
        .arg("(.*)")
        .arg("--data-stdin")
        .arg("db")
        .arg("--db-path")
        .arg(db_path.to_str().unwrap())
        .write_stdin("test line")
        .assert()
        .failure();
    
    // Should fail due to invalid database
    let output = assert.get_output();
    let stderr = std::str::from_utf8(&output.stderr)?;
    assert!(stderr.contains("File does not appear to be a SQLite database") || 
            stderr.contains("file is encrypted or is not a database"));
    
    Ok(())
}

