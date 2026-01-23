use assert_cmd::Command;
use rusqlite::Connection;
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

/// Helper to get a single cell value from the DB
fn get_db_value<T: std::str::FromStr>(db_path: &str, sql: &str) -> T 
where <T as std::str::FromStr>::Err: std::fmt::Debug 
{
    let conn = Connection::open(db_path).expect("failed to open db");
    let result: String = conn.query_row(sql, [], |row| {
        let val: String = row.get(0)?;
        Ok(val)
    }).expect(&format!("Query failed: {}", sql));
    
    result.parse().expect("Failed to parse result")
}

/// Helper to count rows in a table
fn count_rows(db_path: &str, table: &str) -> i64 {
    let conn = Connection::open(db_path).expect("failed to open db");
    conn.query_row(
        &format!("SELECT count(*) FROM {}", table),
        [],
        |row| row.get(0),
    ).unwrap_or(0)
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
       .arg("--db-path")
       .arg(db_path.to_str().unwrap())
       .arg(input_path.to_str().unwrap())
       .assert()
       .success();

    // 3. Verify Database
    assert_eq!(count_rows(db_path.to_str().unwrap(), "data"), 2, "Should have 2 rows");
    
    let level: String = get_db_value(db_path.to_str().unwrap(), "SELECT level FROM data WHERE msg = 'Connection failed'");
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
    cmd.arg(temp.path()) // Input is the directory
       .arg("--path-regex")
       .arg(r"(server-\d+)") // Extract 'server-01'
       .arg("--regex")
       .arg(r"(\w+)")
       .arg("--fields")
       .arg("p1:hostname;l1:status")
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
    cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg("(.*)")
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
    cmd.arg(temp.path()) // Scan the temp directory
       .arg("--regex").arg(r"DATA (\d+)")
       .arg("--fields").arg("1:val")
       .arg("--filter").arg(r".*\.log$") // <--- Only process .log files
       .arg("--db-path").arg(db_path.to_str().unwrap())
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
       .arg("--regex").arg(r"LINE (\w+)")
       .arg("--fields").arg("1:val")
       .arg("--db-path").arg(db_path.to_str().unwrap())
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
       .arg("--regex").arg(r"FOUND (\w+)")
       .arg("--fields").arg("1:val")
       .arg("--db-path").arg(db_path.to_str().unwrap())
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
    cmd.arg("--file-list").arg(list_file.to_str().unwrap()) // <--- Read paths from file
       .arg("--regex").arg(r"DATA (\w+)")
       .arg("--fields").arg("1:val")
       .arg("--db-path").arg(db_path.to_str().unwrap())
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
    cmd.arg(input_file.to_str().unwrap())
       .arg("--regex").arg(r"(?i)(full) (line)") // Case insensitive
       .arg("--fields").arg("1:a;2:b")
       .arg("--track-matches") // <--- Enable raw content tracking
       .arg("--db-path").arg(db_path.to_str().unwrap())
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
    cmd.arg(temp.path())
       .arg("--no-recursive") // <--- Disable recursion
       .arg("--regex").arg(r"DATA (\w+)")
       .arg("--fields").arg("1:val")
       .arg("--db-path").arg(db_path.to_str().unwrap())
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
    let assert = cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg(r"(\w+) (\d+)")
       .arg("-m").arg("1_k_s;2_s_i") // Key=String, Sum=Int
       .assert()
       .success();
    
    let out = assert.get_output();
    let out_str = std::str::from_utf8(&out.stdout)?;
    
    // Split into non-empty lines
    let lines: Vec<&str> = out_str.lines()
        .filter(|l| !l.trim().is_empty())
        .collect();

    // Verify ordering
    // 1. Header or "---" logic might be present, find data rows
    // Expected Data:
    // A	15
    // B	2
    
    let data_lines: Vec<&str> = lines.into_iter()
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
        writeln!(f, "G1 1 5")?;   // Should come middle
        writeln!(f, "G1 0 10")?;  // Should come first
        writeln!(f, "G1 0 40")?;
    }

    let mut cmd = txt2db_cmd();
    let assert = cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg(r"(\w+) (\d+) (\d+)")
       .arg("-m").arg("1_k_s;2_k_i;3_s_i") // Key1=Str, Key2=Int, Sum=Int
       .arg("--map-threads").arg("4") // Force parallel
       .assert()
       .success();

    let out = assert.get_output();
    let out_str = std::str::from_utf8(&out.stdout)?;
    
    let data_lines: Vec<&str> = out_str.lines()
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
    cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg(r"DATA (\d+)")
       .arg("-m").arg("1_k_i")
       .arg("--db-path").arg(db_path.to_str().unwrap()) // Should be ignored
       .assert()
       .success();

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
    let assert = cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg(r"(\w+) (\S+)") // Capture everything non-whitespace
       .arg("-m").arg("1_k_s;2_s_i") // Key=String, Sum=Int (Expects Int)
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
    cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg(r"(\w+) (\S+)")
       .arg("-m").arg("1_k_s;2_s_i")
       .arg("-E") // Stop on Error
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
    let assert = cmd
        .arg(file_path.to_str().unwrap())
        .arg("--path-regex")
        .arg(r"\d{4}-\d{2}-(\d{2})\.log")
        .arg("--regex")
        .arg(r"flush=(\d+) close=(\d+) rename=(\d+)")
        .arg("-m")
        .arg("p1_k_s;1_s_i;2_s_i;3_s_i")
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
    let assert = cmd
        .arg(file_path.to_str().unwrap())
        .arg("--path-regex")
        .arg(r"\d{4}-\d{2}-(\d{2})\.log")
        .arg("--regex")
        .arg(r"flush=(\d+) close=(\d+) rename=(\d+)")
        .arg("-m")
        .arg("p1_k_s;1_s_i;2_s_i;3_s_i")
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // No match on path => no aggregation rows
    assert!(!out_str.contains('\t') || out_str.lines().all(|l| !l.contains('\t') || l.starts_with("Key_")));
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
    let assert = cmd
        .arg(file_path.to_str().unwrap())
        .arg("--path-regex")
        .arg(r"server-(\d+)")
        .arg("--regex")
        .arg(r"user=(\w+) count=(\d+)")
        .arg("-m")
        .arg("p1_k_i;1_k_s;2_s_i") // path group 1, line group 1, sum of group 2
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
    let assert = cmd
        .arg(file_path.to_str().unwrap())
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("-m")
        .arg("1_k_s;2_a_i")
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
    cmd.arg(file_path.to_str().unwrap())
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("-m")
        .arg("1_z_i") // invalid role
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
        .arg("--db-path")
        .arg("/tmp/nowhere.db") // dummy to satisfy args
        .arg("--data-stdin")
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
    let assert = cmd
        .arg(file_path.to_str().unwrap())
        .arg("--regex")
        .arg(r"(\w+) (\d+)")
        .arg("-m")
        .arg("1_k_s;2_s_i")
        .arg("--disable-operations")
        .arg("mapwrite")
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    // mapwrite disabled => no aggregation rows
    assert!(!out_str.contains('\t') || out_str.lines().all(|l| !l.contains('\t') || l.starts_with("Key_")));
    Ok(())
}

#[test]
fn test_map_mode_path_capture_parse_error_counts() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let file_path = temp.path().join("server-xx.log"); // xx not numeric, will fail p1_s_i parse
    let mut f = File::create(&file_path)?;
    writeln!(f, "val=10")?;

    let mut cmd = txt2db_cmd();
    let assert = cmd
        .arg(file_path.to_str().unwrap())
        .arg("--path-regex")
        .arg(r"server-(\w+)") // match "xx" so parse to i64 fails
        .arg("--regex")
        .arg(r"val=(\d+)")
        .arg("-m")
        .arg("p1_k_i;1_s_i")
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
    let assert = cmd
        .arg("--files-from-stdin")
        .arg("--path-regex")
        .arg(r"\d{4}-\d{2}-(\d{2})\.log")
        .arg("--regex")
        .arg(r"x=(\d+)")
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
    let assert = cmd
        .arg(file_path.to_str().unwrap())
        .arg("--pcre2")
        .arg("--path-regex")
        .arg(r"pcreserver-(\d+)")
        .arg("--regex")
        .arg(r"VAL=(\d+)")
        .arg("-m")
        .arg("p1_k_i;1_s_i")
        .assert()
        .success();

    let out_str = std::str::from_utf8(&assert.get_output().stdout)?;
    assert!(out_str.contains("77\t123"));
    Ok(())
}
