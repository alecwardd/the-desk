//! Read-only SQLite connection pool.
//!
//! All 120 MCP tools share a single writer connection behind `Arc<Mutex<Database>>`.
//! A long-running research query holding that mutex blocks live market reads.
//! The database runs in WAL mode, which supports any number of concurrent
//! readers alongside a single writer, so read-only tools can each borrow a
//! dedicated `SQLITE_OPEN_READ_ONLY` connection from this pool instead of
//! contending on the writer mutex.
//!
//! The pool is bounded by a [`tokio::sync::Semaphore`]: at most `size`
//! connections are ever checked out. Connections are created lazily on first
//! use and returned to an idle stack on drop. If a connection is lost (e.g. a
//! blocking task panics), the next `acquire` transparently opens a replacement,
//! so the pool self-heals back up to capacity.

use std::sync::{Arc, Mutex};

use the_desk_backend::db::{Database, DbError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default number of read-only connections. WAL readers are cheap; 4 covers
/// concurrent research queries plus live snapshot reads with headroom.
pub(crate) const DEFAULT_READ_POOL_SIZE: usize = 4;

struct ReadPoolInner {
    path: String,
    idle: Mutex<Vec<Database>>,
    semaphore: Arc<Semaphore>,
}

/// A small pool of read-only [`Database`] connections.
#[derive(Clone)]
pub(crate) struct ReadPool {
    inner: Arc<ReadPoolInner>,
}

impl ReadPool {
    /// Create a pool of up to `size` read-only connections to `path`.
    ///
    /// No connections are opened eagerly; each is created on first checkout.
    /// `size` is clamped to at least 1.
    pub(crate) fn new(path: impl Into<String>, size: usize) -> Self {
        let size = size.max(1);
        Self {
            inner: Arc::new(ReadPoolInner {
                path: path.into(),
                idle: Mutex::new(Vec::with_capacity(size)),
                semaphore: Arc::new(Semaphore::new(size)),
            }),
        }
    }

    /// Borrow a read-only connection, awaiting a free slot if the pool is busy.
    ///
    /// Returns a [`ReadGuard`] that returns the connection to the pool on drop.
    pub(crate) async fn acquire(&self) -> Result<ReadGuard, DbError> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("read pool semaphore is never closed");

        // A permit guarantees a slot; reuse an idle connection or lazily open
        // one (also covers self-healing after a lost connection).
        let db = {
            let mut idle = self.inner.idle.lock().expect("read pool mutex poisoned");
            idle.pop()
        };
        let db = match db {
            Some(db) => db,
            None => Database::open_read_only(&self.inner.path)?,
        };

        Ok(ReadGuard {
            db: Some(db),
            inner: Arc::clone(&self.inner),
            _permit: permit,
        })
    }
}

/// RAII handle to a borrowed read-only connection.
///
/// On drop the connection is pushed back onto the idle stack. If the connection
/// was moved out (e.g. into a blocking task) and never restored, the slot is
/// simply freed and the next `acquire` opens a fresh connection.
pub(crate) struct ReadGuard {
    db: Option<Database>,
    inner: Arc<ReadPoolInner>,
    _permit: OwnedSemaphorePermit,
}

impl ReadGuard {
    /// Move the connection out so it can run inside `spawn_blocking`.
    ///
    /// Pair with [`ReadGuard::restore`] to return it to the pool.
    pub(crate) fn take(&mut self) -> Database {
        self.db.take().expect("read connection present")
    }

    /// Return a connection previously removed with [`ReadGuard::take`].
    pub(crate) fn restore(&mut self, db: Database) {
        self.db = Some(db);
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        if let Some(db) = self.db.take() {
            if let Ok(mut idle) = self.inner.idle.lock() {
                idle.push(db);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use the_desk_backend::db::RiskConfigRecord;

    /// Create a temp database file (with schema) and a distinctive committed
    /// risk-config row the read pool can verify it observes.
    fn seed_db() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("read_pool.db");
        let path_str = path.to_string_lossy().into_owned();
        let db = Database::open(&path_str).expect("open writer");
        db.save_risk_config(&RiskConfigRecord {
            r_value_points: 123.0,
            ..RiskConfigRecord::default()
        })
        .expect("seed risk config");
        drop(db);
        (dir, path_str)
    }

    #[tokio::test]
    async fn pool_reads_committed_writer_data() {
        let (_dir, path) = seed_db();
        let pool = ReadPool::new(path, 3);

        let mut guard = pool.acquire().await.expect("acquire read conn");
        let db = guard.take();
        let config = db.load_risk_config().expect("load risk config");
        guard.restore(db);
        assert_eq!(
            config.r_value_points, 123.0,
            "read connection should see committed writer data"
        );
    }

    #[tokio::test]
    async fn pool_returns_connections_to_idle_stack() {
        let (_dir, path) = seed_db();
        let pool = ReadPool::new(path, 2);

        // Exhaust then release; connections must return to the idle stack so a
        // later acquire reuses them rather than opening unboundedly.
        {
            let _g1 = pool.acquire().await.expect("first");
            let _g2 = pool.acquire().await.expect("second");
        }
        let idle_after = pool.inner.idle.lock().unwrap().len();
        assert_eq!(idle_after, 2, "both connections returned to the pool");

        // Reacquire drains from the idle stack.
        let _g = pool.acquire().await.expect("reacquire");
        assert_eq!(pool.inner.idle.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn read_only_connection_rejects_writes() {
        let (_dir, path) = seed_db();
        let db = Database::open_read_only(&path).expect("open read only");
        // SQLITE_OPEN_READ_ONLY + PRAGMA query_only must reject any write.
        let result = db.save_risk_config(&RiskConfigRecord {
            r_value_points: 999.0,
            ..RiskConfigRecord::default()
        });
        assert!(
            result.is_err(),
            "read-only connection must reject writes, got: {result:?}"
        );
    }
}
