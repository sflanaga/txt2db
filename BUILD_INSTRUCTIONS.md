# Build Instructions for txt2db

## Features

txt2db supports multiple database backends via Cargo features:

| Feature | Description |
|---------|-------------|
| `sqlite` | SQLite backend (lightweight, widely compatible) |
| `duckdb` | DuckDB backend (faster for analytical queries) |

## Quick Build

```bash
# Build with all features (recommended)
cargo build --release --features "sqlite,duckdb"

# Build with SQLite only
cargo build --release --features sqlite

# Build with DuckDB only
cargo build --release --features duckdb
```
