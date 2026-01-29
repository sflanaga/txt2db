# txt2db

A command-line tool for extracting structured data from text files using regular expressions. It operates in two modes:

1. **DB Mode** (`db` subcommand) — Persistent storage in SQLite or DuckDB for complex queries
2. **Map Mode** (`map` subcommand) — Fast in-memory aggregation for quick "group by" style reports (up to 4x faster)

## Installation

```bash
cargo build --release --features "sqlite,duckdb"
./target/release/txt2db --help
```

See [BUILD_INSTRUCTIONS.md](BUILD_INSTRUCTIONS.md) for platform-specific setup and feature options.

## Common Options

Both modes share these options:

| Option | Description |
|--------|-------------|
| `--regex <PATTERN>` | Line-matching regex with capture groups |
| `--path-regex <PATTERN>` | Extract fields from file paths (files not matching are skipped) |
| `-m <SPEC>` | Field specification (see below) |
| `--filter <PATTERN>` | Filter which files to process during directory traversal |
| `--pcre2` | Use PCRE2 engine instead of Rust regex |
| `--no-recursive` | Don't recurse into subdirectories |
| `-s, --splicers <N>` | Number of file reader threads |

**Input sources:**
- File/directory paths as arguments
- `--files-from-stdin` — Read file paths from stdin
- `--file-list <FILE>` — Read file paths from a file
- `--data-stdin` — Read data directly from stdin

**Compression:** Automatically handles `.gz`, `.bz2`, and `.zst` files.

## Field Specification (`-m`)

The `-m` option defines how capture groups map to named, typed columns:

```
-m "SOURCE:NAME[:TYPE[:OP]];..."
```

| Component | Description |
|-----------|-------------|
| **SOURCE** | `l1`, `l2`... for line regex groups; `p1`, `p2`... for path regex groups |
| **NAME** | Column name |
| **TYPE** | `s`=String (default), `i`=i64, `f`=f64 |
| **OP** | Map mode only: `key`, `sum`, `count`, `max`, `min`, `avg` |

**Examples:**
```bash
# DB mode: extract timestamp, level, message
-m "l1:timestamp;l2:level;l3:msg"

# DB mode: typed columns (integer bytes)
-m "l1:path;l2:bytes:i;l3:status:i"

# Map mode: group by host, sum bytes
-m "p1:host:s:key;l1:bytes:i:sum"
```

---

# Map Mode

Use the `map` subcommand for fast in-memory aggregation without writing to a database. This is ideal for quick summaries like counting occurrences, summing values, or computing statistics grouped by key fields.

### Examples

**Count log levels:**
```bash
txt2db --regex '\[(\w+)\]' -m 'l1:level:s:key' map ./app.log
```

**Sum values by category:**
```bash
# Input: "category value" per line
txt2db --regex '(\w+) (\d+)' -m 'l1:cat:s:key;l2:val:i:sum' map ./data.txt
```

**Multiple aggregations:**
```bash
txt2db --regex '(\w+) (\d+)' \
  -m 'l1:cat:s:key;l2:sum:i:sum;l2:max:i:max;l2:min:i:min;l2:avg:f:avg' \
  map ./data.txt
```

**Group by path and line fields:**
```bash
txt2db \
  --path-regex 'server-(\d+)' \
  --regex 'user=(\w+) count=(\d+)' \
  -m 'p1:server:i:key;l1:user:s:key;l2:total:i:sum' \
  map ./logs/
```

## Map Output Formats

| Option | Description |
|--------|-------------|
| `--out-format tsv` | Tab-separated values |
| `--out-format csv` | Comma-separated values (with proper quoting) |
| `--out-format box` | Pretty table with borders (default) |
| `--expand-tabs` | Align TSV columns using elastic tabstops |
| `--sig-digits <N>` | Significant digits for floats (default: 4) |

## Map Performance Options

| Option | Description |
|--------|-------------|
| `--map-threads <N>` | Number of aggregation threads (default: half of CPUs) |
| `-e, --show-errors` | Print parse errors to stderr |
| `-E, --stop-on-error` | Stop on first parse error |

---

# DB Mode

Use the `db` subcommand to write extracted data to SQLite (default) or DuckDB for persistent storage and complex queries.

## Basic Usage

```bash
txt2db \
  --regex '^(\d{4}-\d{2}-\d{2}) \[(\w+)\] (.*)$' \
  -m 'l1:date;l2:level;l3:msg' \
  db --db-path output.db \
  ./logs/
```

## Typed Columns

Specify types for proper SQL column types:

```bash
txt2db \
  --regex 'status=(\d+) bytes=(\d+)' \
  -m 'l1:status:i;l2:bytes:i' \
  db --db-path output.db ./access.log
```

This creates INTEGER columns instead of TEXT, enabling proper numeric comparisons and aggregations in SQL.

## DuckDB Backend

Use `--db-backend duckdb` for DuckDB instead of SQLite:

```bash
txt2db \
  --regex '(\w+) (\d+)' \
  -m 'l1:key;l2:val:i' \
  db --db-backend duckdb --db-path output.duckdb ./data.txt
```

## SQL Hooks

Execute SQL before or after processing:

```bash
txt2db \
  --regex '...' \
  --pre-sql 'CREATE TABLE summary (key TEXT)' \
  --post-sql 'SELECT level, count(*) FROM data GROUP BY level' \
  --db-path output.db \
  ./logs/
```

| Option | Description |
|--------|-------------|
| `--pre-sql <SQL>` | Run SQL before processing |
| `--post-sql <SQL>` | Run SQL after processing |
| `--pre-sql-file <FILE>` | Run SQL from file before processing |
| `--post-sql-file <FILE>` | Run SQL from file after processing |

SELECT statements in hooks print results to stdout.

## Database Schema

**Shared tables:**
- `runs` — One row per execution with metadata
- `files` — Unique file paths per run

**Per-run tables:**
- `data_<run_id>` — Extracted fields
- `matches_<run_id>` — Raw line content (with `--track-matches`)

**Convenience views:**
- `data` — Points to latest `data_<run_id>`
- `matches` — Points to latest `matches_<run_id>`

## SQLite Performance Options

| Option | Description |
|--------|-------------|
| `-p, --parsers <N>` | Number of parser threads |
| `--batch-size <N>` | Records per transaction (default: 1000) |
| `--cache-mb <N>` | SQLite cache size in MB (default: 100) |
| `--track-matches` | Store full line content and byte offsets |

The database uses bulk-load settings (`synchronous=OFF`, `journal_mode=MEMORY`) for speed.

---

# Examples

## Quick error count by level
```bash
txt2db --regex '\[(\w+)\]' -m 'l1:level:s:key' map ./app.log
```

## Sum request bytes by endpoint
```bash
txt2db --regex 'GET (/\S+) .* (\d+)$' \
  -m 'l1:endpoint:s:key;l2:bytes:i:sum' \
  map access.log
```

## Load logs into SQLite for analysis
```bash
txt2db \
  --regex '^(\S+) - - \[([^\]]+)\] "(\w+) ([^"]+)" (\d+) (\d+)' \
  -m 'l1:ip;l2:time;l3:method;l4:path;l5:status:i;l6:bytes:i' \
  db --db-path access.db \
  ./access.log

sqlite3 access.db "SELECT status, count(*) FROM data GROUP BY status"
```

## Process compressed files from find
```bash
find /var/log -name '*.gz' | txt2db \
  --files-from-stdin \
  --regex 'ERROR (.*)' \
  -m 'l1:msg:s:key' \
  map
```

---

# SQL vs Map Mode: Performance Comparison

For aggregation queries, **map mode can be up to 4x faster** than loading data into a database and running SQL. Here's a comparison using a web access log analysis scenario.

## Scenario: Sum bytes by HTTP status code

Given an access log with lines like:
```
192.168.1.1 - - [01/Jan/2024:10:00:00] "GET /api/users HTTP/1.1" 200 1234
192.168.1.2 - - [01/Jan/2024:10:00:01] "GET /api/data HTTP/1.1" 500 567
```

### Approach 1: SQL with post-sql (slower, but data persists)

```bash
txt2db \
  --regex '"\w+ [^"]+" (\d+) (\d+)$' \
  -m 'l1:status:i;l2:bytes:i' \
  db --db-path access.db \
  --post-sql 'SELECT status, SUM(bytes) as total_bytes, COUNT(*) as requests 
              FROM data GROUP BY status ORDER BY status' \
  ./access.log
```

This approach:
1. Parses all lines and writes to SQLite
2. Runs the GROUP BY query
3. **Leaves `access.db` behind** for additional queries later

You can then run more queries:
```bash
sqlite3 access.db "SELECT status, AVG(bytes) FROM data GROUP BY status"
sqlite3 access.db "SELECT * FROM data WHERE status >= 500"
```

### Approach 2: Map mode (faster, no database file)

```bash
txt2db \
  --regex '"\w+ [^"]+" (\d+) (\d+)$' \
  -m 'l1:status:i:key;l2:bytes:i:sum' \
  map ./access.log
```

This approach:
1. Parses lines and aggregates in memory using parallel hash maps
2. Outputs results directly to stdout
3. **No database file is created** — results are ephemeral

### When to use each

| Use Case | Recommended Mode |
|----------|------------------|
| One-off aggregation query | **Map mode** — faster, no cleanup |
| Multiple different queries on same data | **DB mode** — parse once, query many times |
| Need to join with other data | **DB mode** — use SQL joins |
| Exploratory analysis | **DB mode** — iterate on queries |
| CI/CD metrics extraction | **Map mode** — fast, no artifacts |
| Very large datasets (>RAM) | **DB mode** — disk-backed storage |

---

# Notes

- **Encoding:** Uses `from_utf8_lossy`; invalid UTF-8 bytes are replaced.
- **Chunk boundaries:** The reader splits on newlines when possible, but very long lines may be force-split.
- **Map vs DB:** Map mode is faster for simple aggregations; DB mode is better when you need to run multiple queries or preserve data.
- **Type conversion errors:** Invalid values (e.g., "abc" for an integer field) result in NULL and increment the parse error count.

---

# CLI Reference

Complete reference for all command-line options.

## Global Options

These options apply to both `db` and `map` subcommands.

### Input Sources

| Option | Description |
|--------|-------------|
| `<INPUTS>` | Files or directories to process. Directories are walked recursively by default. |
| `--files-from-stdin` | Read file paths from stdin (one per line). Useful with `find \| txt2db ...` |
| `--file-list <FILE>` | Read file paths from a text file (one per line). |
| `--data-stdin` | Read data content directly from stdin instead of files. |

**Example: Process files from find**
```bash
find /var/log -name "*.log" -mtime -1 | txt2db --files-from-stdin --regex '...' map
```

### Parsing Options

| Option | Description |
|--------|-------------|
| `-r, --regex <PATTERN>` | **Required.** Regular expression with capture groups to extract fields from each line. |
| `--path-regex <PATTERN>` | Extract fields from file paths. Files not matching are skipped entirely. |
| `-m <SPEC>` | Field specification mapping captures to named columns. See [Field Specification](#field-specification--m). |
| `-f, --filter <PATTERN>` | Regex filter for file paths during directory walking. Only matching files are processed. |
| `--no-recursive` | Don't recurse into subdirectories when given a directory input. |
| `--pcre2` | Use PCRE2 regex engine instead of Rust regex. Enables features like lookahead/lookbehind. |

**Example: Extract host from path and parse log lines**
```bash
txt2db \
  --path-regex 'logs/([^/]+)/' \
  --regex '(\d{4}-\d{2}-\d{2}) \[(\w+)\] (.*)' \
  -m 'p1:host;l1:date;l2:level;l3:msg' \
  db --db-path logs.db /var/logs/
```

### Error Handling

| Option | Description |
|--------|-------------|
| `-e, --show-errors` | Print parse errors to stderr as they occur, showing file, offset, and capture group. |
| `-E, --stop-on-error` | Stop processing immediately on the first parse error. Exits with code 1. |

**Example: Debug regex issues**
```bash
txt2db --regex '(\d+)' -m 'l1:num:i' -e map data.txt 2>errors.log
```

### Output Options

| Option | Description |
|--------|-------------|
| `--out-format <FMT>` | Output format: `box` (default), `tsv`, `csv`, `compact`. |
| `--sig-digits <N>` | Significant digits for floating-point numbers (default: 4). |
| `--expand-tabs` | For TSV output, expand tabs to align columns visually. |

**Output formats:**
- **box** — Pretty table with Unicode borders (default)
- **tsv** — Tab-separated values, suitable for piping to other tools
- **csv** — Comma-separated with proper quoting
- **compact** — Minimal whitespace table

### Performance Options

| Option | Description |
|--------|-------------|
| `-s, --splicers <N>` | Number of file reader threads. Default: auto-detected based on CPU cores. |
| `--io-chunk-size <SIZE>` | Target chunk size for I/O splitting (default: 256KB). Accepts suffixes: KB, MB. |
| `--io-max-buffer <SIZE>` | Max buffer for long lines (default: 1MB). Must be > 2x chunk size. |
| `--ticker <MS>` | Stats ticker interval in milliseconds (default: 1000). |
| `--ticker-verbose` | Show detailed stats including channel depths. |
| `--disable-operations <OPS>` | Disable operations for benchmarking: `regex`, `maptarget`, `mapwrite`. |

---

## DB Subcommand Options

Options specific to `txt2db ... db [OPTIONS] <INPUTS>`.

### Database Options

| Option | Description |
|--------|-------------|
| `--db-path <PATH>` | Output database file. Default: `scan_HHMMSS.db` or `.duckdb`. |
| `--db-backend <BACKEND>` | Database backend: `sqlite` (default) or `duckdb`. |
| `--track-matches` | Store full line content and byte offsets in a `matches` table. |
| `--batch-size <N>` | Records per transaction (default: 1000). Higher = faster but more memory. |
| `--db-channel-size <N>` | Internal channel capacity (default: 65536). |

### SQLite-Specific Options

| Option | Description |
|--------|-------------|
| `--cache-mb <N>` | SQLite page cache size in MB (default: 100). |

### DuckDB-Specific Options

| Option | Description |
|--------|-------------|
| `--duckdb-threads <N>` | Internal DuckDB threads (default: 8). |
| `--duckdb-memory-limit <SIZE>` | DuckDB memory limit (default: 1GB). |

### SQL Hooks

| Option | Description |
|--------|-------------|
| `--pre-sql <SQL>` | Execute SQL before processing starts. |
| `--post-sql <SQL>` | Execute SQL after processing finishes. SELECT results print to stdout. |
| `--pre-sql-file <FILE>` | Execute SQL from file before processing. |
| `--post-sql-file <FILE>` | Execute SQL from file after processing. |

**Example: Run aggregation query after loading**
```bash
txt2db --regex 'status=(\d+)' -m 'l1:status:i' \
  db --db-path access.db \
  --post-sql 'SELECT status, COUNT(*) as cnt FROM data GROUP BY status ORDER BY cnt DESC' \
  ./access.log
```

### Parser Threads

| Option | Description |
|--------|-------------|
| `-p, --parsers <N>` | Number of regex parser threads. Default: auto-detected. |

---

## Map Subcommand Options

Options specific to `txt2db ... map [OPTIONS] <INPUTS>`.

| Option | Description |
|--------|-------------|
| `--map-threads <N>` | Number of aggregation threads. Default: half of CPU cores. |

---

## Field Specification Syntax (`-m`)

The `-m` option uses a unified syntax for both DB and Map modes:

```
-m "SOURCE:NAME[:TYPE[:OP]]; ..."
```

### Components

| Component | Values | Description |
|-----------|--------|-------------|
| **SOURCE** | `l1`, `l2`, ... | Line regex capture group (1-indexed) |
| | `p1`, `p2`, ... | Path regex capture group (1-indexed) |
| | `_` | No source (only valid with `count` operation) |
| **NAME** | any identifier | Column name in output |
| **TYPE** | `s`, `string` | String (default) |
| | `i`, `int` | 64-bit integer |
| | `f`, `float` | 64-bit float |
| **OP** | `k`, `key` | Group-by key (map mode only) |
| | `sum` | Sum values |
| | `a`, `avg` | Average |
| | `c`, `count` | Count rows |
| | `x`, `max` | Maximum |
| | `n`, `min` | Minimum |

### DB Mode Examples

```bash
# Basic: all strings
-m "l1:timestamp;l2:level;l3:message"

# Typed columns
-m "l1:ip;l2:status:i;l3:bytes:i;l4:duration:f"

# With path captures
-m "p1:hostname;p2:date;l1:level;l2:msg"
```

### Map Mode Examples

```bash
# Count by key
-m "l1:level:s:key"

# Sum with key
-m "l1:endpoint:s:key;l2:bytes:i:sum"

# Multiple aggregations
-m "l1:host:s:key;l2:bytes:i:sum;l2:bytes:i:max;l2:bytes:i:avg"

# Composite key (group by two fields)
-m "p1:server:s:key;l1:status:i:key;l2:bytes:i:sum"

# Count without source field
-m "l1:status:i:key;_:requests:i:count"
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Set logging level (e.g., `RUST_LOG=debug`). |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (invalid arguments, parse error with `-E`, etc.) |
