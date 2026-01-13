use assert_cmd::Command;
use rusqlite::Connection;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;
use flate2::write::GzEncoder;
use flate2::Compression;

// --- Helper Functions ---

/// Helper to get a single cell value from the DB
fn get_db_value<T: std::str::FromStr>(db_path: &std::path::Path, sql: &str) -> T 
where <T as std::str::FromStr>::Err: std::fmt::Debug 
{
    let conn = Connection::open(db_path).expect("failed to open db");
    let result: String = conn.query_row(sql, [], |row| {
        // We cast everything to string in the query for simplicity in tests, 
        // or just read the text column
        let val: String = row.get(0)?;
        Ok(val)
    }).expect(&format!("Query failed: {}", sql));
    
    result.parse().expect("Failed to parse result")
}

/// Helper to count rows in a table
fn count_rows(db_path: &std::path::Path, table: &str) -> i64 {
    let conn = Connection::open(db_path).expect("failed to open db");
    let count: i64 = conn.query_row(
        &format!("SELECT count(*) FROM {}", table),
        [],
        |row| row.get(0),
    ).unwrap_or(0);
    count
}

// --- Tests ---

#[test]
fn test_basic_log_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("app.log");
    let db_path = temp.path().join("basic_test.db");

    // 1. Create dummy data
    let mut file = File::create(&input_path)?;
    writeln!(file, "2023-01-01 10:00:00 [INFO] User logged in")?;
    writeln!(file, "2023-01-01 10:05:00 [ERROR] Connection failed")?;

    // 2. Run txt2db
    // Note: We use (?m) to enable multi-line mode so ^ and $ match line boundaries
    let mut cmd = Command::cargo_bin("txt2db")?;
    cmd.arg("--regex")
       .arg(r"(?m)^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(.*?)\] (.*)$")
       .arg("--fields")
       .arg("1:ts;2:level;3:msg")
       .arg("--db-path")
       .arg(db_path.to_str().unwrap())
       .arg(input_path.to_str().unwrap());

    cmd.assert().success();

    // 3. Verify Database
    assert_eq!(count_rows(&db_path, "data"), 2, "Should have 2 rows");
    
    let level: String = get_db_value(&db_path, "SELECT level FROM data WHERE msg = 'Connection failed'");
    assert_eq!(level, "ERROR");

    Ok(())
}

#[test]
fn test_gzip_support() -> Result<(), Box<dyn std::error::Error>> {
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
    let mut cmd = Command::cargo_bin("txt2db")?;
    cmd.arg("--regex")
       .arg(r"ID:(\d+) DATA:(\w+)")
       .arg("--fields")
       .arg("1:item_id;2:val") // Changed 'id' to 'item_id' to avoid conflict with PK
       .arg("--db-path")
       .arg(db_path.to_str().unwrap())
       .arg(input_path.to_str().unwrap());

    cmd.assert().success();

    // 3. Verify
    assert_eq!(count_rows(&db_path, "data"), 2);
    let val: String = get_db_value(&db_path, "SELECT val FROM data WHERE item_id = '101'");
    assert_eq!(val, "Beta");

    Ok(())
}

#[test]
fn test_path_regex_and_recursion() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    
    // Create structure: /server-01/logs/sys.log
    let subdir = temp.path().join("server-01").join("logs");
    fs::create_dir_all(&subdir)?;
    let input_path = subdir.join("sys.log");
    
    let mut file = File::create(&input_path)?;
    writeln!(file, "CRITICAL_ERROR")?;

    let db_path = temp.path().join("path_test.db");

    // Run on the root temp dir
    let mut cmd = Command::cargo_bin("txt2db")?;
    cmd.arg(temp.path()) // Input is the directory
       .arg("--path-regex")
       .arg(r"(server-\d+)") // Extract 'server-01'
       .arg("--regex")
       .arg(r"(\w+)")
       .arg("--fields")
       .arg("p1:hostname;l1:status")
       .arg("--db-path")
       .arg(db_path.to_str().unwrap());

    cmd.assert().success();

    let host: String = get_db_value(&db_path, "SELECT hostname FROM data");
    assert_eq!(host, "server-01");
    
    let status: String = get_db_value(&db_path, "SELECT status FROM data");
    assert_eq!(status, "CRITICAL_ERROR");

    Ok(())
}

#[test]
fn test_sql_hooks() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let input_path = temp.path().join("dummy.txt");
    let mut file = File::create(&input_path)?;
    writeln!(file, "ignore me")?;
    
    let db_path = temp.path().join("hooks.db");

    let mut cmd = Command::cargo_bin("txt2db")?;
    cmd.arg(input_path.to_str().unwrap())
       .arg("--regex").arg("(.*)")
       // Pre-SQL: Create a summary table
       .arg("--pre-sql")
       .arg("CREATE TABLE summary (run_date TEXT); INSERT INTO summary VALUES ('2023-01-01');")
       // Post-SQL: Update it (just to prove it ran after)
       .arg("--post-sql")
       .arg("UPDATE summary SET run_date = 'FINISHED'")
       .arg("--db-path")
       .arg(db_path.to_str().unwrap());

    cmd.assert().success();

    // Verify the table created by PRE-SQL exists and was updated by POST-SQL
    let val: String = get_db_value(&db_path, "SELECT run_date FROM summary");
    assert_eq!(val, "FINISHED");

    Ok(())
}


