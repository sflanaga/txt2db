use assert_cmd::Command;
use rusqlite::Connection;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;
use flate2::write::GzEncoder;
use flate2::Compression;

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
    writeln!(f, "A 5")?;
    writeln!(f, "A 10")?;
    writeln!(f, "B 2")?;

    let mut cmd = txt2db_cmd();
    let assert = cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg(r"(\w+) (\d+)")
       .arg("-m").arg("1_k_s;2_s_i") // Key=String, Sum=Int
       .assert()
       .success();
    
    let out = assert.get_output();
    let out_str = std::str::from_utf8(&out.stdout)?;
    
    // A -> 15, B -> 2
    assert!(out_str.contains("A\t15"));
    assert!(out_str.contains("B\t2"));
    
    Ok(())
}

#[test]
fn test_map_mode_composite_and_parallel() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("composite.txt");
    let mut f = File::create(&input_path)?;
    // Groups: G1, G1. Subgroups: 0, 1.
    // Key: (Group, Sub)
    // G1 0 10
    // G1 0 40 -> Sum 50
    // G1 1 5
    // G2 0 100
    for _ in 0..100 {
        writeln!(f, "G1 0 10")?;
        writeln!(f, "G1 0 40")?;
        writeln!(f, "G1 1 5")?;
        writeln!(f, "G2 0 100")?;
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
    
    // G1 0 -> 50 * 100 = 5000
    assert!(out_str.contains("G1\t0\t5000"));
    // G1 1 -> 5 * 100 = 500
    assert!(out_str.contains("G1\t1\t500"));
     // G2 0 -> 100 * 100 = 10000
    assert!(out_str.contains("G2\t0\t10000"));

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

