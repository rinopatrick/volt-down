use std::collections::VecDeque;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::database::Database;
use crate::download::{DownloadConfig, DownloadEngine};
use crate::models::*;
use crate::Result;
use crate::VoltError;

const DEFAULT_MAX_CONCURRENT: usize = 3;
const DB_SYNC_INTERVAL_MS: u64 = 5000;

pub struct DownloadQueue {
    engine: Arc<DownloadEngine>,
    pending: RwLock<VecDeque<DownloadTask>>,
    active: DashMap<String, ActiveDownload>,
    finished: DashMap<String, DownloadTask>,
    max_concurrent: usize,
    progress_tx: mpsc::Sender<ProgressEvent>,
    semaphore: Arc<Semaphore>,
    db: Option<Arc<Database>>,
}

struct ActiveDownload {
    cancel_token: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
    task: Arc<RwLock<DownloadTask>>,
}

impl DownloadQueue {
    pub fn new(
        config: Option<DownloadConfig>,
        max_concurrent: Option<usize>,
        progress_tx: mpsc::Sender<ProgressEvent>,
        db: Option<Arc<Database>>,
    ) -> Result<Self> {
        let engine = Arc::new(DownloadEngine::new(config)?);
        let max_concurrent = max_concurrent.unwrap_or(DEFAULT_MAX_CONCURRENT);
        Ok(Self {
            engine,
            pending: RwLock::new(VecDeque::new()),
            active: DashMap::new(),
            finished: DashMap::new(),
            max_concurrent,
            progress_tx,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            db,
        })
    }

    /// Load incomplete downloads from DB and re-queue them
    pub async fn load_and_resume(&self) -> Result<usize> {
        let Some(ref db) = self.db else {
            return Ok(0);
        };
        let tasks = db.list_incomplete()?;
        let count = tasks.len();
        if count > 0 {
            info!("Loaded {} incomplete downloads from database", count);
        }
        let mut pending = self.pending.write().await;
        for mut task in tasks {
            // Reset downloading tasks to pending since they were interrupted
            if task.status == DownloadStatus::Downloading {
                task.status = DownloadStatus::Pending;
            }
            pending.push_back(task);
        }
        drop(pending);
        // Kick off processing
        for _ in 0..self.max_concurrent {
            self.process_queue().await;
        }
        Ok(count)
    }

    pub async fn add(&self, mut task: DownloadTask) -> Result<String> {
        if self.active.contains_key(&task.id)
            || self.pending.read().await.iter().any(|t| t.id == task.id)
        {
            return Err(VoltError::AlreadyExists(task.id));
        }

        let id = task.id.clone();
        task.status = DownloadStatus::Pending;

        if let Some(ref db) = self.db {
            if let Err(e) = db.upsert_download(&task) {
                warn!("Failed to persist new download: {}", e);
            }
        }

        self.pending.write().await.push_back(task);
        self.process_queue().await;
        Ok(id)
    }

    pub async fn pause(&self, id: &str) -> Result<()> {
        let mut persisted = false;
        if let Some(active) = self.active.get(id) {
            active.cancel_token.cancel();
            let mut t = active.task.write().await;
            t.status = DownloadStatus::Paused;
            t.updated_at = chrono::Utc::now();
            if let Some(ref db) = self.db {
                let _ = db.update_download(&t);
            }
            persisted = true;
            info!("Paused download {}", id);
        } else {
            let mut pending = self.pending.write().await;
            if let Some(task) = pending.iter_mut().find(|t| t.id == id) {
                task.status = DownloadStatus::Paused;
                task.updated_at = chrono::Utc::now();
                if let Some(ref db) = self.db {
                    let _ = db.update_download(task);
                }
                persisted = true;
                info!("Paused pending download {}", id);
            }
        }
        if !persisted {
            // Maybe it's already finished but paused/failed
            if let Some(mut task) = self.finished.remove(id).map(|(_, t)| t) {
                task.status = DownloadStatus::Paused;
                task.updated_at = chrono::Utc::now();
                if let Some(ref db) = self.db {
                    let _ = db.update_download(&task);
                }
                self.finished.insert(id.to_string(), task);
            }
        }
        Ok(())
    }

    pub async fn resume(&self, id: &str) -> Result<()> {
        // Check pending first
        let mut pending = self.pending.write().await;
        if let Some(task) = pending.iter_mut().find(|t| t.id == id) {
            if task.status == DownloadStatus::Paused || task.status == DownloadStatus::Failed {
                task.status = DownloadStatus::Pending;
                task.error = None;
                task.updated_at = chrono::Utc::now();
                if let Some(ref db) = self.db {
                    let _ = db.update_download(task);
                }
                drop(pending);
                self.process_queue().await;
                return Ok(());
            }
        }
        drop(pending);
        // Check active (paused but handle not yet finished) — abort old handle and re-queue
        if let Some((_, active)) = self.active.remove(id) {
            let (_should_resume, task) = {
                let mut t = active.task.write().await;
                let resumable =
                    t.status == DownloadStatus::Paused || t.status == DownloadStatus::Failed;
                let task_clone = if resumable {
                    t.status = DownloadStatus::Pending;
                    t.error = None;
                    Some(t.clone())
                } else {
                    None
                };
                (resumable, task_clone)
            };
            if let Some(mut task) = task {
                active.handle.abort();
                task.updated_at = chrono::Utc::now();
                if let Some(ref db) = self.db {
                    let _ = db.update_download(&task);
                }
                self.pending.write().await.push_back(task);
                self.process_queue().await;
                return Ok(());
            }
            // Put back if not resumable
            self.active.insert(id.to_string(), active);
        }
        // Also check finished (paused/cancelled tasks moved there after handle completes)
        if let Some((_, mut task)) = self.finished.remove(id) {
            if task.status == DownloadStatus::Paused || task.status == DownloadStatus::Failed {
                task.status = DownloadStatus::Pending;
                task.error = None;
                task.updated_at = chrono::Utc::now();
                if let Some(ref db) = self.db {
                    let _ = db.update_download(&task);
                }
                self.pending.write().await.push_back(task);
                self.process_queue().await;
                return Ok(());
            }
            // Put it back if not resumable
            self.finished.insert(id.to_string(), task);
        }
        Ok(())
    }

    pub async fn cancel(&self, id: &str) -> Result<()> {
        // Cancel active download
        if let Some(active) = self.active.get(id) {
            active.cancel_token.cancel();
            let mut t = active.task.write().await;
            t.status = DownloadStatus::Cancelled;
            t.updated_at = chrono::Utc::now();
            if let Some(ref db) = self.db {
                let _ = db.update_download(&t);
            }
        }
        // Cancel pending download — move to finished so it remains queryable
        let mut pending = self.pending.write().await;
        if let Some(pos) = pending.iter().position(|t| t.id == id) {
            let mut task = pending.remove(pos).unwrap();
            task.status = DownloadStatus::Cancelled;
            task.updated_at = chrono::Utc::now();
            if let Some(ref db) = self.db {
                let _ = db.update_download(&task);
            }
            drop(pending);
            self.finished.insert(id.to_string(), task);
        }
        Ok(())
    }

    pub async fn get_active(&self) -> Vec<DownloadTask> {
        // Move finished downloads to history so they remain queryable
        let mut finished: Vec<(String, DownloadTask)> = Vec::new();
        for entry in self.active.iter() {
            let is_finished = entry.value().handle.is_finished();
            let _status = entry.value().task.read().await.status.clone();
            if is_finished {
                finished.push((entry.key().clone(), entry.value().task.read().await.clone()));
            }
        }
        for (id, task) in finished {
            self.active.remove(&id);
            // Persist final state before moving to finished
            if let Some(ref db) = self.db {
                let _ = db.update_download(&task);
            }
            self.finished.entry(id).or_insert(task);
        }

        let mut result = Vec::new();
        for entry in self.active.iter() {
            result.push(entry.value().task.read().await.clone());
        }
        result
    }

    pub async fn get_finished(&self) -> Vec<DownloadTask> {
        self.finished
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub async fn get_task(&self, id: &str) -> Option<DownloadTask> {
        if let Some(entry) = self.active.get(id) {
            return Some(entry.value().task.read().await.clone());
        }
        let pending = self.pending.read().await;
        if let Some(task) = pending.iter().find(|t| t.id == id) {
            return Some(task.clone());
        }
        drop(pending);
        if let Some(entry) = self.finished.get(id) {
            return Some(entry.value().clone());
        }
        None
    }

    pub async fn get_pending(&self) -> Vec<DownloadTask> {
        self.pending.read().await.iter().cloned().collect()
    }

    pub async fn process_queue(&self) {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => return, // Max concurrent reached
        };

        let task = {
            let mut pending = self.pending.write().await;
            pending.pop_front()
        };

        let Some(task) = task else {
            return;
        };

        let id = task.id.clone();
        let id_for_active = id.clone();
        let cancel_token = CancellationToken::new();
        let progress_tx = self.progress_tx.clone();
        let engine = self.engine.clone();
        let child_cancel = cancel_token.child_token();
        let db = self.db.clone();

        let task_arc = Arc::new(RwLock::new(task));
        let task_for_async = task_arc.clone();

        // Local channel to bridge progress events into the shared task_arc
        let (local_tx, mut local_rx) = mpsc::channel::<ProgressEvent>(100);
        let bridge_task = task_arc.clone();
        let bridge_progress_tx = progress_tx.clone();
        tokio::spawn(async move {
            while let Some(evt) = local_rx.recv().await {
                {
                    let mut t = bridge_task.write().await;
                    t.downloaded = evt.downloaded;
                    if evt.total_size.is_some() {
                        t.total_size = evt.total_size;
                    }
                    t.status = evt.status.clone();
                }
                let _ = bridge_progress_tx.send(evt).await;
            }
        });

        // Periodic DB sync for active task progress
        let db_sync_handle = if let Some(ref db) = db {
            let sync_task = task_arc.clone();
            let sync_db = db.clone();
            let sync_id = id.clone();
            Some(tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(tokio::time::Duration::from_millis(DB_SYNC_INTERVAL_MS));
                loop {
                    ticker.tick().await;
                    let t = sync_task.read().await;
                    if t.status == DownloadStatus::Completed
                        || t.status == DownloadStatus::Cancelled
                    {
                        break;
                    }
                    let _ = sync_db.update_download(&t);
                }
                // Final flush
                let t = sync_task.read().await;
                let _ = sync_db.update_download(&t);
                info!("DB sync loop ended for {}", sync_id);
            }))
        } else {
            None
        };

        let handle = tokio::spawn(async move {
            let _permit = permit; // Hold permit until done
            let mut local_task = { task_for_async.read().await.clone() };
            local_task.status = DownloadStatus::Downloading;
            local_task.updated_at = chrono::Utc::now();
            // reflect initial status quickly
            {
                *task_for_async.write().await = local_task.clone();
            }

            // Persist downloading status
            if let Some(ref db) = db {
                let _ = db.update_download(&local_task);
            }

            match engine
                .download(&mut local_task, local_tx, child_cancel)
                .await
            {
                Ok(()) => {
                    let mut t = task_for_async.write().await;
                    let preserve =
                        t.status == DownloadStatus::Paused || t.status == DownloadStatus::Cancelled;
                    if preserve {
                        let saved = t.status.clone();
                        *t = local_task;
                        t.status = saved;
                    } else {
                        *t = local_task;
                    }
                    t.updated_at = chrono::Utc::now();
                    // Persist final state
                    if let Some(ref db) = db {
                        let _ = db.update_download(&t);
                    }
                    info!("Download {} finished with status {:?}", id, t.status);
                }
                Err(e) => {
                    let mut t = task_for_async.write().await;
                    if t.status != DownloadStatus::Paused && t.status != DownloadStatus::Cancelled {
                        error!("Download {} failed: {}", id, e);
                        local_task.status = DownloadStatus::Failed;
                        local_task.error = Some(e.to_string());
                        local_task.updated_at = chrono::Utc::now();
                        *t = local_task;
                        if let Some(ref db) = db {
                            let _ = db.update_download(&t);
                        }
                    }
                }
            }
            // Stop DB sync
            if let Some(h) = db_sync_handle {
                h.abort();
            }
        });

        self.active.insert(
            id_for_active,
            ActiveDownload {
                cancel_token,
                handle,
                task: task_arc,
            },
        );
    }
}
