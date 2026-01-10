# txt2db

`txt2db` is a high-throughput **text/log ingestion** CLI that scans files, extracts fields using **regular expressions**, and loads the extracted results into a **SQLite** database for analysis.

It is designed for **bulk ingestion and post-processing** of large datasets (e.g., log archives), not low-latency “real-time” alerting.

## What it does

Given one or more input sources (directories, files, file lists, or stdin), `txt2db`:

1. **Reads file content** (optionally decompressing `.gz`, `.bz2`, and `.zst`)
2. **Splits input into newline-safe chunks** (so lines are not cut across chunk boundaries when possible)
3. **Applies a line regex** (`--regex`) to find matches in the text
4. **Extracts capture groups into columns** (optionally mapping names via `--fields`)
5. **Optionally extracts metadata from the file path** using `--path-regex`
6. **Batches inserts into SQLite** for performance
7. Tracks run metadata and creates convenience views pointing at the latest run

## What it is NOT

- **Not a CSV, JSON, XML, etc parser.**  
  It does not implement CSV rules (headers, quoting/escaping, delimiters). It treats input as plain text lines. You *can* parse simple delimiter-separated data with a regex, but that’s “regex parsing,” not CSV semantics.

## Key features (from the code)

- **Concurrent pipeline**
  - *Splicer threads* read/decompress files and emit chunks.
  - *Parser threads* run regex matching and build DB records.
  - A single *DB worker thread* batches and writes to SQLite.
- **Compression support**
  - `.gz` via `flate2`
  - `.bz2` via `bzip2`
  - `.zst` via `zstd`
- **Input flexibility**
  - Scan files/directories (optionally recursive)
  - Read file paths from stdin (`--files-from-stdin`)
  - Read file paths from a file (`--file-list`)
  - Read raw data from stdin (`--data-stdin`)
- **File filtering**
  - `--filter` filters *which files to read* during directory walking (distinct from `--path-regex`)
- **SQL hooks**
  - `--pre-sql` / `--pre-sql-file`
  - `--post-sql` / `--post-sql-file`
  - Statements are split safely (respects quotes and comments) before execution and prints query results for SELECTs.
- **Optional context tracking**
  - `--track-matches` stores matched line content and byte offsets in a per-run matches table.

## Installation

Build with Rust (edition 2021):

```bash
cargo build --release
```

Binary:

```bash
./target/release/txt2db --help
```

## Usage overview

At minimum you provide:

- `--regex <REGEX>`: the line-matching regex
- one or more inputs **or** an input mode like `--data-stdin`

### Basic: scan a directory of logs

```bash
txt2db \
  --regex 'ERROR (.*)' \
  /var/log/myapp/
```

### Limit which files are scanned (directory walk filter)

`--filter` applies to file paths discovered during directory traversal.

```bash
txt2db \
  --regex 'ERROR (.*)' \
  --filter '.*\.log(\.gz)?$' \
  ./logs/
```

### Parse typical log lines into fields

If your log lines look like:

`2026-01-10 12:34:56 [WARN] something happened`

You can extract timestamp, level, message:

```bash
txt2db \
  --regex '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] (.*)$' \
  --fields '1:ts;2:level;3:msg' \
  ./app.log
```

### Extract fields from the file path too (`--path-regex`)

This is useful when log files encode metadata in the path/name, e.g.:

`./logs/server-01/2026-01-10/app.log`

```bash
txt2db \
  --path-regex './logs/(server-\d+)/(\d{4}-\d{2}-\d{2})/.*' \
  --regex '^\[(\w+)\] (.*)$' \
  --fields 'p1:host;p2:log_date;l1:level;l2:msg' \
  ./logs/
```

Notes:
- `pN:` references capture groups from `--path-regex`
- `lN:` references capture groups from `--regex`

### Read file list from stdin (paths)

```bash
find ./logs -type f -name '*.log' | txt2db \
  --files-from-stdin \
  --regex 'ERROR (.*)'
```

### Read raw data from stdin (no filename)

```bash
cat ./app.log | txt2db --data-stdin --regex 'ERROR (.*)'
```

In this mode, there is no file path, so:
- `--path-regex` matching will not apply
- `pN:` fields cannot be populated

## Performance tuning options

- `-s, --splicers <N>`: number of file splicer (reader) threads  
- `-p, --parsers <N>`: number of regex parser threads  
- `--batch-size <N>`: records per SQLite transaction (default `1000`)
- `--cache-mb <N>`: SQLite cache size in MB (default `100`)

The DB worker sets performance-related PRAGMAs (per code):
- `synchronous = OFF`
- `journal_mode = MEMORY`
- `temp_store = 2`

These improve ingestion speed but are tuned for bulk-load scenarios.

## How the pipeline works (implementation summary)

### 1) Splicer (I/O + decompression + chunking)
- Walks inputs and opens files
- Detects compression by extension: `.gz`, `.bz2`, `.zst`
- Reads bytes and builds an internal buffer
- Emits chunks:
  - Prefer splitting at the last newline within a bounded search window
  - If no newline and buffer reaches `max_buffer_size`, it forces a split
- Each emitted chunk includes:
  - `data` (bytes)
  - `file_path` (optional, absent for `--data-stdin`)
  - `offset` (byte offset into the file)

### 2) Parser threads (regex matching)
- Convert chunk bytes to text using `String::from_utf8_lossy`
- Iterate matches using `captures_iter`
- For each match:
  - Find the full line boundaries around the match
  - Compute the match line’s byte offset (`chunk_offset + line_start`)
  - Extract line capture groups and optional path capture groups
  - Send a structured record to the DB worker

### 3) DB worker (batch + schema creation)
Each run inserts a row into `runs`, then creates per-run tables:
- `files` table stores `(run_id, path)` and is cached in-memory for speed
- `data_{run_id}` stores extracted fields
- Optional `matches_{run_id}` stores raw line content and offsets when `--track-matches` is enabled
- Creates a `data` VIEW pointing to the latest `data_{run_id}` table
- Updates the `runs` row with final stats (`files_processed`, `files_skipped`, `bytes_processed`, `match_count`, `finished_at`)

## Database layout

### Shared tables
- `runs`: one row per execution
- `files`: unique file paths per run (unique index on `(run_id, path)`)

### Per-run tables
- `data_<run_id>`: extracted field columns (your `--fields` mapping determines names)
- `matches_<run_id>`: (only with `--track-matches`) stores `(file_id, offset, content)`

### Convenience views
- `data`: points to `data_<latest_run_id>`
- `matches`: points to `matches_<latest_run_id>` (if enabled)

## Notes / Caveats

- **Encoding:** parsing uses `from_utf8_lossy`, so invalid UTF-8 bytes will be replaced. If your logs are not UTF-8, be aware of potential data changes.
- **Regex and chunk boundaries:** the splicer tries hard to split on newlines, but extremely long lines or data without newlines can be force-split; this may affect regex matches that rely on whole-line context.
- **Bulk-load settings:** SQLite PRAGMAs favor speed over durability during ingestion.

## License

Add your preferred license here.
