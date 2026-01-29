use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::DbBackend;
use crate::output::OutputConfig;
use crate::stats::{DbStats, RunMetadata};
use crate::io_splicer::SplicerStats;

#[cfg(feature = "sqlite")]
use crate::database::DbRecord;

pub trait DatabaseConnection: Send + 'static {
    fn execute(&mut self, sql: &str, params: &[&dyn std::fmt::Debug]) -> Result<()>;
    fn execute_batch(&mut self, sql: &str) -> Result<()>;
    fn prepare(&mut self, sql: &str) -> Result<Box<dyn DatabaseStatement>>;
    fn last_insert_rowid(&self) -> i64;
    fn transaction(&mut self) -> Result<Box<dyn DatabaseTransaction>>;
}

pub trait DatabaseStatement {
    fn execute(&mut self, params: &[&dyn std::fmt::Debug]) -> Result<()>;
    fn query_row(&mut self, params: &[&dyn std::fmt::Debug], f: impl FnOnce(&dyn DatabaseRow) -> Result<()>) -> Result<()>;
}

pub trait DatabaseRow {
    fn get(&self, idx: usize) -> Result<i64>;
}

pub trait DatabaseTransaction {
    fn execute(&mut self, sql: &str, params: &[&dyn std::fmt::Debug]) -> Result<()>;
    fn prepare(&mut self, sql: &str) -> Result<Box<dyn DatabaseStatement>>;
    fn commit(self: Box<Self>) -> Result<()>;
}

pub fn create_database_connection(
    backend: DbBackend,
    path: &str,
    cache_mb: i64,
) -> Result<Box<dyn DatabaseConnection>> {
    match backend {
        #[cfg(feature = "sqlite")]
        DbBackend::Sqlite => {
            let conn = crate::database_sqlite::SqliteConnection::new(path, cache_mb)?;
            Ok(Box::new(conn))
        }
        #[cfg(feature = "duckdb")]
        DbBackend::DuckDB => {
            let conn = crate::database_duckdb::DuckDBConnection::new(path, cache_mb)?;
            Ok(Box::new(conn))
        }
        #[allow(unreachable_patterns)]
        _ => anyhow::bail!("Database backend {:?} not enabled", backend),
    }
}

#[cfg(feature = "sqlite")]
pub mod sqlite {
    use super::*;
    use rusqlite::{params, Connection, Statement, Transaction};
    use std::io::Write;

    pub struct SqliteConnection {
        conn: Connection,
    }

    impl SqliteConnection {
        pub fn new(path: &str, cache_mb: i64) -> Result<Self> {
            let mut conn = Connection::open(path)?;
            
            // Performance Tunings
            conn.execute_batch(
                "
                PRAGMA synchronous = OFF;
                PRAGMA journal_mode = MEMORY;
                PRAGMA temp_store = 2;
            ",
            )?;

            // Set Cache Size
            let cache_kib = cache_mb * 1024;
            let cache_pragma = format!("PRAGMA cache_size = -{};", cache_kib);
            conn.execute(&cache_pragma, [])?;
            
            // Validate connection with a simple query
            conn.query_row("SELECT 1", [], |_| Ok(()))?;
            
            Ok(SqliteConnection { conn })
        }
    }

    impl DatabaseConnection for SqliteConnection {
        fn execute(&mut self, sql: &str, params: &[&dyn std::fmt::Debug]) -> Result<()> {
            // Convert params to rusqlite format
            let rusqlite_params: Vec<&dyn rusqlite::ToSql> = params
                .iter()
                .map(|p| p as &dyn rusqlite::ToSql)
                .collect();
            self.conn.execute(sql, &*rusqlite_params)?;
            Ok(())
        }

        fn execute_batch(&mut self, sql: &str) -> Result<()> {
            self.conn.execute_batch(sql)?;
            Ok(())
        }

        fn prepare(&mut self, sql: &str) -> Result<Box<dyn DatabaseStatement>> {
            let stmt = self.conn.prepare(sql)?;
            Ok(Box::new(SqliteStatement { stmt }))
        }

        fn last_insert_rowid(&self) -> i64 {
            self.conn.last_insert_rowid()
        }

        fn transaction(&mut self) -> Result<Box<dyn DatabaseTransaction>> {
            let tx = self.conn.transaction()?;
            Ok(Box::new(SqliteTransaction { tx }))
        }
    }

    pub struct SqliteStatement {
        stmt: Statement<'static>,
    }

    // SAFETY: We ensure the statement doesn't outlive the connection
    unsafe impl Send for SqliteStatement {}

    impl DatabaseStatement for SqliteStatement {
        fn execute(&mut self, params: &[&dyn std::fmt::Debug]) -> Result<()> {
            let rusqlite_params: Vec<&dyn rusqlite::ToSql> = params
                .iter()
                .map(|p| p as &dyn rusqlite::ToSql)
                .collect();
            self.stmt.execute(&*rusqlite_params)?;
            Ok(())
        }

        fn query_row(&mut self, params: &[&dyn std::fmt::Debug], f: impl FnOnce(&dyn DatabaseRow) -> Result<()>) -> Result<()> {
            let rusqlite_params: Vec<&dyn rusqlite::ToSql> = params
                .iter()
                .map(|p| p as &dyn rusqlite::ToSql)
                .collect();
            self.stmt.query_row(&*rusqlite_params, |row| {
                f(&SqliteRow { row }) as Result<()>
            })?;
            Ok(())
        }
    }

    pub struct SqliteRow<'a> {
        row: &'a rusqlite::Row<'a>,
    }

    impl DatabaseRow for SqliteRow<'_> {
        fn get(&self, idx: usize) -> Result<i64> {
            Ok(self.row.get(idx)?)
        }
    }

    pub struct SqliteTransaction {
        tx: Transaction<'static>,
    }

    // SAFETY: We ensure the transaction doesn't outlive the connection
    unsafe impl Send for SqliteTransaction {}

    impl DatabaseTransaction for SqliteTransaction {
        fn execute(&mut self, sql: &str, params: &[&dyn std::fmt::Debug]) -> Result<()> {
            let rusqlite_params: Vec<&dyn rusqlite::ToSql> = params
                .iter()
                .map(|p| p as &dyn rusqlite::ToSql)
                .collect();
            self.tx.execute(sql, &*rusqlite_params)?;
            Ok(())
        }

        fn prepare(&mut self, sql: &str) -> Result<Box<dyn DatabaseStatement>> {
            let stmt = self.tx.prepare(sql)?;
            Ok(Box::new(SqliteStatement { stmt }))
        }

        fn commit(self: Box<Self>) -> Result<()> {
            self.tx.commit()?;
            Ok(())
        }
    }
}

#[cfg(feature = "duckdb")]
pub mod duckdb {
    use super::*;
    use duckdb::{params, Connection, Statement, Transaction};

    pub struct DuckDBConnection {
        conn: Connection,
    }

    impl DuckDBConnection {
        pub fn new(path: &str, _cache_mb: i64) -> Result<Self> {
            let mut conn = Connection::open(path)?;
            
            // Performance Tunings for DuckDB
            conn.execute("PRAGMA threads = 4", [])?;
            conn.execute("PRAGMA memory_limit = '1GB'", [])?;
            
            // Validate connection with a simple query
            conn.query_row("SELECT 1", [], |_| Ok(()))?;
            
            Ok(DuckDBConnection { conn })
        }
    }

    impl DatabaseConnection for DuckDBConnection {
        fn execute(&mut self, sql: &str, params: &[&dyn std::fmt::Debug]) -> Result<()> {
            let duckdb_params: Vec<&dyn duckdb::ToSql> = params
                .iter()
                .map(|p| p as &dyn duckdb::ToSql)
                .collect();
            self.conn.execute(sql, &*duckdb_params)?;
            Ok(())
        }

        fn execute_batch(&mut self, sql: &str) -> Result<()> {
            self.conn.execute_batch(sql)?;
            Ok(())
        }

        fn prepare(&mut self, sql: &str) -> Result<Box<dyn DatabaseStatement>> {
            let stmt = self.conn.prepare(sql)?;
            Ok(Box::new(DuckDBStatement { stmt }))
        }

        fn last_insert_rowid(&self) -> i64 {
            self.conn.last_insert_rowid()
        }

        fn transaction(&mut self) -> Result<Box<dyn DatabaseTransaction>> {
            let tx = self.conn.transaction()?;
            Ok(Box::new(DuckDBTransaction { tx }))
        }
    }

    pub struct DuckDBStatement {
        stmt: Statement<'static>,
    }

    // SAFETY: We ensure the statement doesn't outlive the connection
    unsafe impl Send for DuckDBStatement {}

    impl DatabaseStatement for DuckDBStatement {
        fn execute(&mut self, params: &[&dyn std::fmt::Debug]) -> Result<()> {
            let duckdb_params: Vec<&dyn duckdb::ToSql> = params
                .iter()
                .map(|p| p as &dyn duckdb::ToSql)
                .collect();
            self.stmt.execute(&*duckdb_params)?;
            Ok(())
        }

        fn query_row(&mut self, params: &[&dyn std::fmt::Debug], f: impl FnOnce(&dyn DatabaseRow) -> Result<()>) -> Result<()> {
            let duckdb_params: Vec<&dyn duckdb::ToSql> = params
                .iter()
                .map(|p| p as &dyn duckdb::ToSql)
                .collect();
            self.stmt.query_row(&*duckdb_params, |row| {
                f(&DuckDBRow { row }) as Result<()>
            })?;
            Ok(())
        }
    }

    pub struct DuckDBRow<'a> {
        row: &'a duckdb::Row<'a>,
    }

    impl DatabaseRow for DuckDBRow<'_> {
        fn get(&self, idx: usize) -> Result<i64> {
            Ok(self.row.get(idx)?)
        }
    }

    pub struct DuckDBTransaction {
        tx: Transaction<'static>,
    }

    // SAFETY: We ensure the transaction doesn't outlive the connection
    unsafe impl Send for DuckDBTransaction {}

    impl DatabaseTransaction for DuckDBTransaction {
        fn execute(&mut self, sql: &str, params: &[&dyn std::fmt::Debug]) -> Result<()> {
            let duckdb_params: Vec<&dyn duckdb::ToSql> = params
                .iter()
                .map(|p| p as &dyn duckdb::ToSql)
                .collect();
            self.tx.execute(sql, &*duckdb_params)?;
            Ok(())
        }

        fn prepare(&mut self, sql: &str) -> Result<Box<dyn DatabaseStatement>> {
            let stmt = self.tx.prepare(sql)?;
            Ok(Box::new(DuckDBStatement { stmt }))
        }

        fn commit(self: Box<Self>) -> Result<()> {
            self.tx.commit()?;
            Ok(())
        }
    }
}
