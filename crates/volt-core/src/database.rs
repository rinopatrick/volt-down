use crate::models::{DownloadStatus, DownloadTask};
use crate::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path).map_err(|e| crate::VoltError::Database(e.to_string()))?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS downloads (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                filename TEXT,
                save_path TEXT,
                total_size INTEGER,
                downloaded INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                chunks INTEGER DEFAULT 4,
                created_at TEXT,
                updated_at TEXT,
                error TEXT,
                metadata TEXT,
                proxy_url TEXT,
                cookies TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                download_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                start_byte INTEGER NOT NULL,
                end_byte INTEGER NOT NULL,
                downloaded INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (download_id) REFERENCES downloads(id)
            );",
        )
        .map_err(|e| crate::VoltError::Database(e.to_string()))?;

        // Backward-compatible migrations for existing databases.
        // Ignore errors when columns already exist.
        let _ = conn.execute("ALTER TABLE downloads ADD COLUMN metadata TEXT", []);
        let _ = conn.execute("ALTER TABLE downloads ADD COLUMN proxy_url TEXT", []);
        let _ = conn.execute("ALTER TABLE downloads ADD COLUMN cookies TEXT", []);
        Ok(())
    }

    pub fn upsert_download(&self, task: &DownloadTask) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO downloads (id, url, filename, save_path, total_size, downloaded, status, chunks, created_at, updated_at, error, metadata, proxy_url, cookies)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(id) DO UPDATE SET
                url=excluded.url,
                filename=excluded.filename,
                save_path=excluded.save_path,
                total_size=excluded.total_size,
                downloaded=excluded.downloaded,
                status=excluded.status,
                chunks=excluded.chunks,
                updated_at=excluded.updated_at,
                error=excluded.error,
                metadata=excluded.metadata,
                proxy_url=excluded.proxy_url,
                cookies=excluded.cookies",
            params![
                task.id, task.url, task.filename, task.save_path,
                task.total_size.map(|v| v as i64), task.downloaded as i64,
                task.status.to_string(), task.chunks as i64,
                task.created_at.to_rfc3339(), task.updated_at.to_rfc3339(),
                task.error.as_ref(), task.metadata.as_ref().map(|m| m.to_string()),
                task.proxy_url.as_ref(), task.cookies.as_ref()
            ],
        ).map_err(|e| crate::VoltError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn update_download(&self, task: &DownloadTask) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE downloads SET
                filename = ?1,
                save_path = ?2,
                total_size = ?3,
                downloaded = ?4,
                status = ?5,
                chunks = ?6,
                updated_at = ?7,
                error = ?8,
                metadata = ?9
             WHERE id = ?10",
            params![
                task.filename,
                task.save_path,
                task.total_size.map(|v| v as i64),
                task.downloaded as i64,
                task.status.to_string(),
                task.chunks as i64,
                task.updated_at.to_rfc3339(),
                task.error.as_ref(),
                task.metadata.as_ref().map(|m| m.to_string()),
                task.id
            ],
        )
        .map_err(|e| crate::VoltError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn update_status(
        &self,
        id: &str,
        status: &str,
        downloaded: i64,
        total: Option<i64>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE downloads SET status = ?1, downloaded = ?2, total_size = COALESCE(?3, total_size), updated_at = CURRENT_TIMESTAMP WHERE id = ?4",
            params![status, downloaded, total, id],
        ).map_err(|e| crate::VoltError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn delete_download(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM downloads WHERE id = ?1", params![id])
            .map_err(|e| crate::VoltError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn get_download(&self, id: &str) -> Result<Option<DownloadTask>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, url, filename, save_path, total_size, downloaded, status, chunks, created_at, updated_at, error, metadata, proxy_url, cookies FROM downloads WHERE id = ?1"
        ).map_err(|e| crate::VoltError::Database(e.to_string()))?;

        let item = stmt
            .query_row(params![id], |row| {
                let status_str: String = row.get(6)?;
                let meta_str: Option<String> = row.get(11)?;
                Ok(DownloadTask {
                    id: row.get(0)?,
                    url: row.get(1)?,
                    filename: row.get(2)?,
                    save_path: row.get(3)?,
                    total_size: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                    downloaded: row.get::<_, i64>(5)? as u64,
                    status: status_str.parse().unwrap_or(DownloadStatus::Pending),
                    chunks: row.get::<_, i64>(7)? as usize,
                    created_at: row
                        .get::<_, String>(8)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    updated_at: row
                        .get::<_, String>(9)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    error: row.get(10)?,
                    metadata: meta_str.and_then(|s| serde_json::from_str(&s).ok()),
                    speed_limit_bps: None,
                    proxy_url: row.get(12)?,
                    cookies: row.get(13)?,
                })
            })
            .optional()
            .map_err(|e| crate::VoltError::Database(e.to_string()))?;

        Ok(item)
    }

    pub fn list_downloads(&self) -> Result<Vec<DownloadTask>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, url, filename, save_path, total_size, downloaded, status, chunks, created_at, updated_at, error, metadata, proxy_url, cookies FROM downloads ORDER BY created_at DESC"
        ).map_err(|e| crate::VoltError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                let status_str: String = row.get(6)?;
                let meta_str: Option<String> = row.get(11)?;
                Ok(DownloadTask {
                    id: row.get(0)?,
                    url: row.get(1)?,
                    filename: row.get(2)?,
                    save_path: row.get(3)?,
                    total_size: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                    downloaded: row.get::<_, i64>(5)? as u64,
                    status: status_str.parse().unwrap_or(DownloadStatus::Pending),
                    chunks: row.get::<_, i64>(7)? as usize,
                    created_at: row
                        .get::<_, String>(8)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    updated_at: row
                        .get::<_, String>(9)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    error: row.get(10)?,
                    metadata: meta_str.and_then(|s| serde_json::from_str(&s).ok()),
                    speed_limit_bps: None,
                    proxy_url: row.get(12)?,
                    cookies: row.get(13)?,
                })
            })
            .map_err(|e| crate::VoltError::Database(e.to_string()))?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row.map_err(|e| crate::VoltError::Database(e.to_string()))?);
        }
        Ok(items)
    }

    /// Load downloads that are not in a terminal state (completed / cancelled)
    pub fn list_incomplete(&self) -> Result<Vec<DownloadTask>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, url, filename, save_path, total_size, downloaded, status, chunks, created_at, updated_at, error, metadata, proxy_url, cookies
             FROM downloads
             WHERE status NOT IN ('completed', 'cancelled')
             ORDER BY created_at ASC"
        ).map_err(|e| crate::VoltError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                let status_str: String = row.get(6)?;
                let meta_str: Option<String> = row.get(11)?;
                Ok(DownloadTask {
                    id: row.get(0)?,
                    url: row.get(1)?,
                    filename: row.get(2)?,
                    save_path: row.get(3)?,
                    total_size: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                    downloaded: row.get::<_, i64>(5)? as u64,
                    status: status_str.parse().unwrap_or(DownloadStatus::Pending),
                    chunks: row.get::<_, i64>(7)? as usize,
                    created_at: row
                        .get::<_, String>(8)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    updated_at: row
                        .get::<_, String>(9)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    error: row.get(10)?,
                    metadata: meta_str.and_then(|s| serde_json::from_str(&s).ok()),
                    speed_limit_bps: None,
                    proxy_url: row.get(12)?,
                    cookies: row.get(13)?,
                })
            })
            .map_err(|e| crate::VoltError::Database(e.to_string()))?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row.map_err(|e| crate::VoltError::Database(e.to_string()))?);
        }
        Ok(items)
    }
}
