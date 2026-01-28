# Build Instructions for txt2db with DuckDB Support

## Summary of Changes
- Added DuckDB as an alternative database backend to SQLite
- Added CLI option `--db-backend` to choose between sqlite and duckdb
- Created separate modules for SQLite and DuckDB implementations
- Database file extension now matches the backend (.db for SQLite, .duckdb for DuckDB)

## Building on WSL

### Prerequisites
```bash
# Install Rust if not already installed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Install required system packages
sudo apt update
sudo apt install build-essential pkg-config libssl-dev
```

### Build Commands

#### 1. Build with SQLite (default)
```bash
cd /mnt/c/ProgramData/bin/src/rust/txt2db
cargo build --release --features sqlite
```

#### 2. Build with DuckDB
```bash
cd /mnt/c/ProgramData/bin/src/rust/txt2db
cargo build --release --features duckdb
```

#### 3. Build with both SQLite and DuckDB support
```bash
cd /mnt/c/ProgramData/bin/src/rust/txt2db
cargo build --release --features "sqlite,duckdb"
```

### Running the Application

#### With SQLite (default)
```bash
./target/release/txt2db -r "(\w+)" /path/to/files
```

#### With DuckDB
```bash
./target/release/txt2db --db-backend duckdb -r "(\w+)" /path/to/files
```

### Performance Notes
- DuckDB provides better performance for analytical queries
- DuckDB supports parallel execution (configured with 4 threads)
- SQLite is still a good choice for simple use cases
- DuckDB files will have .duckdb extension, SQLite files have .db extension

### Testing
To test both backends:
```bash
# Create a test file
echo -e "name: John\nage: 30\nname: Jane\nage: 25" > test.txt

# Test with SQLite
./target/release/txt2db --db-backend sqlite -r "name: (\w+)\nage: (\d+)" test.txt

# Test with DuckDB  
./target/release/txt2db --db-backend duckdb -r "name: (\w+)\nage: (\d+)" test.txt
```

### Troubleshooting
If you encounter build errors:
1. Ensure all prerequisites are installed
2. Try `cargo clean` before rebuilding
3. Make sure you're using the latest Rust version: `rustup update`

The built binary will be located at `target/release/txt2db`
