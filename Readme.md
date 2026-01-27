# txt2db

A command-line tool for extracting structured data from text files using regular expressions. It operates in two modes:

1. **Map Mode** (`-m`) — Fast in-memory aggregation for quick "group by" style reports
2. **SQLite Mode** — Persistent storage for complex queries and repeated analysis

## Installation

```bash
cargo build --release
./target/release/txt2db --help
```

## Common Options

Both modes share these options:

| Option | Description |
|--------|-------------|
| `--regex <PATTERN>` | Line-matching regex with capture groups |
| `--path-regex <PATTERN>` | Extract fields from file paths (files not matching are skipped) |
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

---

# Map Mode

Use `-m` (or `--map`) for fast aggregation without writing to a database. This is useful for quick summaries like counting occurrences, summing values, or computing statistics grouped by key fields.

## Map Definition Syntax

```
-m "INDEX_ROLE_TYPE[:LABEL];..."
```

- **INDEX**: Capture group number (1-based), or `pN` for path regex groups
- **ROLE**: `k`=Key, `s`=Sum, `c`=Count, `x`=Max, `n`=Min, `a`=Avg
- **TYPE**: `i`=i64, `u`=u64, `f`=f64, `s`=String
- **LABEL** (optional): Custom column header

### Examples

**Count log levels:**
```bash
txt2db --regex '\[(\w+)\]' -m '1_k_s' ./app.log
```

**Sum values by category:**
```bash
# Input: "category value" per line
txt2db --regex '(\w+) (\d+)' -m '1_k_s;2_s_i' ./data.txt
```

**Multiple aggregations:**
```bash
txt2db --regex '(\w+) (\d+)' -m '1_k_s;2_s_i;2_x_i;2_n_i;2_a_f' ./data.txt
```

**Group by path and line fields:**
```bash
txt2db \
  --path-regex 'server-(\d+)' \
  --regex 'user=(\w+) count=(\d+)' \
  -m 'p1_k_i:server;1_k_s:user;2_s_i:total' \
  ./logs/
```

## Map Output Formats

| Option | Description |
|--------|-------------|
| `--map-format tsv` | Tab-separated values |
| `--map-format csv` | Comma-separated values (with proper quoting) |
| `--map-format comfy` | Pretty table with borders (default) |
| `--expand-tabs` | Align TSV columns using elastic tabstops |
| `--sig-digits <N>` | Significant digits for floats (default: 4) |

## Map Performance Options

| Option | Description |
|--------|-------------|
| `--map-threads <N>` | Number of aggregation threads (default: half of CPUs) |
| `-e, --show-errors` | Print parse errors to stderr |
| `-E, --stop-on-error` | Stop on first parse error |

---

# SQLite Mode

Without `-m`, extracted data is written to a SQLite database for persistent storage and complex queries.

## Basic Usage

```bash
txt2db \
  --regex '^(\d{4}-\d{2}-\d{2}) \[(\w+)\] (.*)$' \
  --fields '1:date;2:level;3:msg' \
  --db-path output.db \
  ./logs/
```

## Field Mapping

The `--fields` option maps capture groups to column names:

```
--fields 'INDEX:NAME;...'
```

- Plain numbers (`1`, `2`) or `lN` reference line regex groups
- `pN` references path regex groups

```bash
txt2db \
  --path-regex 'server-(\d+)/(\d{4}-\d{2}-\d{2})' \
  --regex '\[(\w+)\] (.*)' \
  --fields 'p1:host;p2:log_date;l1:level;l2:msg' \
  ./logs/
```

If `--fields` is omitted, columns are auto-named `f_1`, `f_2`, etc. (or `pf_N`/`lf_N` when using path regex).

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
txt2db --regex '\[(\w+)\]' -m '1_k_s' ./app.log
```

## Sum request bytes by endpoint
```bash
txt2db --regex 'GET (/\S+) .* (\d+)$' -m '1_k_s:endpoint;2_s_i:bytes' access.log
```

## Load logs into SQLite for analysis
```bash
txt2db \
  --regex '^(\S+) - - \[([^\]]+)\] "(\w+) ([^"]+)" (\d+) (\d+)' \
  --fields '1:ip;2:time;3:method;4:path;5:status;6:bytes' \
  --db-path access.db \
  ./access.log

sqlite3 access.db "SELECT status, count(*) FROM data GROUP BY status"
```

## Process compressed files from find
```bash
find /var/log -name '*.gz' | txt2db \
  --files-from-stdin \
  --regex 'ERROR (.*)' \
  -m '1_k_s'
```

---

# Notes

- **Encoding:** Uses `from_utf8_lossy`; invalid UTF-8 bytes are replaced.
- **Chunk boundaries:** The reader splits on newlines when possible, but very long lines may be force-split.
- **Map vs SQLite:** Map mode is faster for simple aggregations; SQLite mode is better when you need to run multiple queries or preserve data.
