use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::fs::OpenOptions;
use tokio::io::SeekFrom;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::header::{self, HeaderMap, HeaderValue};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::error::ErrorKind;
use crate::models::*;
use crate::Result;
use crate::VoltError;

const CHUNK_SIZE_MIN: u64 = 1024 * 1024; // 1 MB minimum chunk
const CONCURRENT_CHUNKS_DEFAULT: usize = 4;
const MAX_CONCURRENT_CHUNKS: usize = 16;
const PROGRESS_INTERVAL_MS: u64 = 1000;
const STATE_SAVE_INTERVAL_MS: u64 = 10000;

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub chunks: usize,
    pub speed_limit_bps: Option<u64>,
    pub user_agent: String,
    pub stealth: bool,
    pub proxy_url: Option<String>,
    pub cookies: Option<String>,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            chunks: CONCURRENT_CHUNKS_DEFAULT,
            speed_limit_bps: None,
            user_agent: format!(
                "VoltDown/{} (https://github.com/pat/volt-down)",
                env!("CARGO_PKG_VERSION")
            ),
            stealth: false,
            proxy_url: None,
            cookies: None,
        }
    }
}

// ─── Resume state structures ───────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct PartState {
    chunks: Vec<ChunkProgress>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ChunkProgress {
    index: usize,
    start: u64,
    end: Option<u64>,
    downloaded: u64,
}

impl ChunkProgress {
    fn total(&self) -> u64 {
        self.end
            .map(|e| e.saturating_sub(self.start).saturating_add(1))
            .unwrap_or(0)
    }

    fn is_complete(&self) -> bool {
        match self.end {
            Some(_) => self.downloaded >= self.total(),
            None => false, // unknown size — never pre-mark complete
        }
    }
}

#[derive(Debug, Clone)]
struct ChunkRange {
    index: usize,
    start: u64,
    end: Option<u64>,
}

// ─── Engine ────────────────────────────────────────────────────────

pub struct DownloadEngine {
    client: reqwest::Client,
    config: DownloadConfig,
}

impl DownloadEngine {
    pub fn new(config: Option<DownloadConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();
        let client = Self::build_client(&config)?;
        Ok(Self { client, config })
    }

    fn build_client(cfg: &DownloadConfig) -> Result<reqwest::Client> {
        let mut headers = HeaderMap::new();

        let ua = if cfg.stealth {
            Self::random_user_agent()
        } else {
            cfg.user_agent.clone()
        };
        headers.insert(
            header::USER_AGENT,
            HeaderValue::from_str(&ua).map_err(|e| VoltError::Unknown(e.to_string()))?,
        );

        if cfg.stealth {
            headers.insert(header::ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"));
            headers.insert(
                header::ACCEPT_LANGUAGE,
                HeaderValue::from_static("en-US,en;q=0.5"),
            );
            headers.insert(
                header::ACCEPT_ENCODING,
                HeaderValue::from_static("gzip, deflate, br"),
            );
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("max-age=0"));
            headers.insert(
                header::UPGRADE_INSECURE_REQUESTS,
                HeaderValue::from_static("1"),
            );
            headers.insert("Sec-Fetch-Dest", HeaderValue::from_static("document"));
            headers.insert("Sec-Fetch-Mode", HeaderValue::from_static("navigate"));
            headers.insert("Sec-Fetch-Site", HeaderValue::from_static("none"));
            headers.insert("DNT", HeaderValue::from_static("1"));
        }

        if let Some(ref cookies) = cfg.cookies {
            if !cookies.is_empty() {
                headers.insert(
                    header::COOKIE,
                    HeaderValue::from_str(cookies)
                        .map_err(|e| VoltError::Unknown(format!("Invalid cookie header: {}", e)))?,
                );
            }
        }

        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(
            "Keep-Alive",
            HeaderValue::from_static("timeout=60, max=1000"),
        );

        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .pool_max_idle_per_host(MAX_CONCURRENT_CHUNKS)
            .pool_idle_timeout(Duration::from_secs(300))
            .tcp_keepalive(Duration::from_secs(60))
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(10));

        if let Some(ref proxy_url) = cfg.proxy_url {
            if !proxy_url.is_empty() {
                let proxy = reqwest::Proxy::all(proxy_url)
                    .map_err(|e| VoltError::Unknown(format!("Invalid proxy URL: {}", e)))?;
                builder = builder.proxy(proxy);
            }
        } else {
            builder = builder.no_proxy();
        }

        builder.build().map_err(|e| VoltError::Http(e.to_string()))
    }

    /// Returns a client for the given task, using per-task proxy/cookies if provided.
    fn client_for_task(&self, task: &DownloadTask) -> Result<reqwest::Client> {
        let needs_override = task.proxy_url.is_some() || task.cookies.is_some();
        if !needs_override {
            return Ok(self.client.clone());
        }
        let mut cfg = self.config.clone();
        if task.proxy_url.is_some() {
            cfg.proxy_url = task.proxy_url.clone();
        }
        if task.cookies.is_some() {
            cfg.cookies = task.cookies.clone();
        }
        Self::build_client(&cfg)
    }

    fn random_user_agent() -> String {
        let uas = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ];
        use std::time::{SystemTime, UNIX_EPOCH};
        let idx = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as usize)
            % uas.len();
        uas[idx].to_string()
    }

    /// Fetch file metadata (size, range support) without downloading
    pub async fn probe(&self, url: &str) -> Result<FileInfo> {
        self.probe_with_client(&self.client, url).await
    }

    async fn probe_with_client(&self, client: &reqwest::Client, url: &str) -> Result<FileInfo> {
        let response = client
            .head(url)
            .send()
            .await
            .map_err(|e| VoltError::Http(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(VoltError::Http(format!(
                "HTTP {} for {}",
                status.as_u16(),
                url
            )));
        }

        let total_size = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        let accepts_ranges = response
            .headers()
            .get(header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("bytes"))
            .unwrap_or(false);

        let filename = Self::extract_filename(&response, url);

        Ok(FileInfo {
            url: url.to_string(),
            filename,
            total_size,
            accepts_ranges,
            content_type: response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string()),
        })
    }

    fn extract_filename(response: &reqwest::Response, url: &str) -> String {
        if let Some(cd) = response.headers().get(header::CONTENT_DISPOSITION) {
            if let Ok(cd_str) = cd.to_str() {
                if let Some(fname) = cd_str.split("filename=").nth(1) {
                    let name = fname.trim().trim_matches('"').trim_matches('\'');
                    if !name.is_empty() {
                        return name.to_string();
                    }
                }
            }
        }

        if let Ok(parsed) = url::Url::parse(url) {
            if let Some(mut segments) = parsed.path_segments() {
                if let Some(last) = segments.next_back() {
                    if !last.is_empty() {
                        return last.to_string();
                    }
                }
            }
        }

        "download.bin".to_string()
    }

    /// Start a download with full chunked + resume support
    pub async fn download(
        &self,
        task: &mut DownloadTask,
        progress_tx: mpsc::Sender<ProgressEvent>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        let client = self.client_for_task(task)?;
        let info = self.probe_with_client(&client, &task.url).await?;
        task.total_size = info.total_size;
        // Preserve user-provided filename; only overwrite if current name was derived from URL
        let url_fallback = task
            .url
            .split('/')
            .next_back()
            .unwrap_or("download")
            .to_string();
        if task.filename == url_fallback
            || task.filename == "download"
            || task.filename == "download.bin"
        {
            task.filename = info.filename;
        }

        let save_path = PathBuf::from(&task.save_path).join(&task.filename);
        let part_path = save_path.with_extension("part");
        let state_path = format!("{}.state", part_path.display());
        let supports_resume = info.accepts_ranges && info.total_size.is_some();

        // Always ensure the .part file exists — even for streaming (no total_size)
        if let Some(size) = info.total_size {
            Self::preallocate_or_create(&part_path, size).await?;
        } else {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&part_path)
                .await
                .map_err(VoltError::Io)?;
        }

        let chunks = if supports_resume {
            self.calculate_chunks(info.total_size.unwrap(), task.chunks)
        } else {
            vec![ChunkRange {
                index: 0,
                start: 0,
                end: info.total_size,
            }]
        };

        let num_chunks = chunks.len();
        task.chunks = num_chunks;
        info!(
            "Starting download '{}' with {} chunks (resume={})",
            task.filename, num_chunks, supports_resume
        );

        // ── Load or init resume state ──────────────────────────────
        let mut part_state = if supports_resume {
            Self::load_state(&state_path).await.unwrap_or_default()
        } else {
            PartState::default()
        };

        // Always initialise chunk progress — covers both resumable and streaming paths
        if part_state.chunks.len() != num_chunks {
            if !part_state.chunks.is_empty() {
                warn!("State chunk count mismatch, resetting resume state");
            }
            part_state.chunks = chunks
                .iter()
                .map(|c| ChunkProgress {
                    index: c.index,
                    start: c.start,
                    end: c.end,
                    downloaded: 0,
                })
                .collect();
        }

        let already_downloaded: u64 = part_state.chunks.iter().map(|c| c.downloaded).sum();
        task.downloaded = already_downloaded;
        if already_downloaded > 0 {
            info!(
                "Resuming download: {} bytes already present",
                already_downloaded
            );
        }

        // Each chunk gets its own independent file handle — eliminates mutex contention
        // POSIX allows multiple fds to the same file with independent offsets

        let part_state = Arc::new(RwLock::new(part_state));
        let downloaded_total = Arc::new(RwLock::new(already_downloaded));
        let _start_time = Instant::now();

        // ── Progress reporter ─────────────────────────────────────
        let progress_handle = {
            let task_id = task.id.clone();
            let total_size = task.total_size;
            let downloaded = downloaded_total.clone();
            let tx = progress_tx.clone();
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_millis(PROGRESS_INTERVAL_MS));
                let mut last_downloaded = already_downloaded;
                loop {
                    ticker.tick().await;
                    let current = *downloaded.read().await;
                    let speed = ((current.saturating_sub(last_downloaded)) as f64)
                        / PROGRESS_INTERVAL_MS as f64
                        * 1000.0;
                    last_downloaded = current;

                    let _ = tx
                        .send(ProgressEvent {
                            download_id: task_id.clone(),
                            downloaded: current,
                            total_size,
                            speed_bps: speed,
                            status: DownloadStatus::Downloading,
                        })
                        .await;
                }
            })
        };

        // ── Periodic state saver (only for resumable downloads) ────
        let state_path_clone = state_path.clone();
        let state_saver_handle = {
            let state = part_state.clone();
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_millis(STATE_SAVE_INTERVAL_MS));
                loop {
                    ticker.tick().await;
                    let s = state.read().await.clone();
                    if let Err(e) = Self::save_state(&state_path_clone, &s).await {
                        warn!("Failed to save state: {}", e);
                    }
                }
            })
        };

        // ── Filter incomplete chunks ───────────────────────────────
        let state_snapshot = part_state.read().await.clone();
        let incomplete_chunks: Vec<ChunkRange> = chunks
            .into_iter()
            .filter(|c| {
                state_snapshot
                    .chunks
                    .get(c.index)
                    .map(|p| !p.is_complete())
                    .unwrap_or(true)
            })
            .collect();

        // ── Download chunks (dynamic worker pool + retry) ──────────
        let effective_speed_limit = task.speed_limit_bps.or(self.config.speed_limit_bps);
        let per_chunk_speed_limit = effective_speed_limit.map(|l| l / num_chunks as u64);
        let url = task.url.clone();
        let max_workers = self.config.chunks.clamp(1, MAX_CONCURRENT_CHUNKS);
        const MAX_CHUNK_RETRIES: u8 = 3;

        let mut work_queue: std::collections::VecDeque<ChunkRange> =
            incomplete_chunks.into_iter().collect();
        let mut retry_counts: std::collections::HashMap<usize, u8> =
            std::collections::HashMap::new();
        let mut active = FuturesUnordered::new();

        let client_for_spawn = client.clone();
        let url_for_spawn = url.clone();
        let part_path_for_spawn = part_path.clone();
        let downloaded_total_for_spawn = downloaded_total.clone();
        let part_state_for_spawn = part_state.clone();
        let cancel_token_for_spawn = cancel_token.clone();
        let spawn_worker = move |chunk: ChunkRange| {
            let c = client_for_spawn.clone();
            tokio::spawn(Self::download_chunk(
                c,
                url_for_spawn.clone(),
                chunk,
                part_path_for_spawn.clone(),
                downloaded_total_for_spawn.clone(),
                part_state_for_spawn.clone(),
                cancel_token_for_spawn.child_token(),
                per_chunk_speed_limit,
            ))
        };

        // Seed initial workers — always keep concurrency at max until queue drains
        for _ in 0..max_workers.min(work_queue.len()) {
            if let Some(chunk) = work_queue.pop_front() {
                active.push(spawn_worker(chunk));
            }
        }
        let mut all_ok = true;
        let first_error = Arc::new(Mutex::new(None));
        while let Some(result) = active.next().await {
            match result {
                Ok((_chunk, Ok(()))) => {
                    // Keep worker busy
                    if let Some(next_chunk) = work_queue.pop_front() {
                        active.push(spawn_worker(next_chunk));
                    }
                }
                Ok((chunk, Err(e))) => {
                    let kind = e.kind();
                    let chunk_idx = chunk.index;
                    if kind == ErrorKind::Cancelled {
                        // User cancelled — finish gracefully
                        if work_queue.is_empty() && active.is_empty() {
                            break;
                        }
                        continue;
                    }
                    if first_error.lock().unwrap().is_none() {
                        *first_error.lock().unwrap() = Some(e.clone());
                    }
                    if kind == ErrorKind::Permanent {
                        error!("Chunk {} permanent failure (no retry): {}", chunk_idx, e);
                        all_ok = false;
                        // Keep worker busy with next chunk
                        if let Some(next_chunk) = work_queue.pop_front() {
                            active.push(spawn_worker(next_chunk));
                        }
                        continue;
                    }
                    // Transient — retry
                    let retries = retry_counts.entry(chunk_idx).or_insert(0);
                    if *retries < MAX_CHUNK_RETRIES {
                        *retries += 1;
                        warn!(
                            "Chunk {} transient failure (attempt {}/{}), retrying: {}",
                            chunk_idx, *retries, MAX_CHUNK_RETRIES, e
                        );
                        // Re-queue same chunk
                        work_queue.push_back(chunk);
                        // Keep worker busy with retry or next chunk
                        if let Some(next_chunk) = work_queue.pop_front() {
                            active.push(spawn_worker(next_chunk));
                        }
                    } else {
                        error!(
                            "Chunk {} failed after {} retries: {}",
                            chunk_idx, MAX_CHUNK_RETRIES, e
                        );
                        all_ok = false;
                        if first_error.lock().unwrap().is_none() {
                            *first_error.lock().unwrap() = Some(e.clone());
                        }
                    }
                }
                Err(e) => {
                    error!("Chunk task panicked: {}", e);
                    all_ok = false;
                }
            }
        }

        progress_handle.abort();
        state_saver_handle.abort();

        // Final state flush
        let final_state = part_state.read().await.clone();
        if supports_resume {
            let _ = Self::save_state(&state_path, &final_state).await;
        }

        if cancel_token.is_cancelled() {
            task.downloaded = *downloaded_total.read().await;
            return Ok(());
        }

        if !all_ok {
            task.status = DownloadStatus::Failed;
            task.downloaded = *downloaded_total.read().await;
            if let Some(err) = first_error.lock().unwrap().take() {
                return Err(err);
            }
            return Err(VoltError::Unknown("One or more chunks failed".into()));
        }

        // Verify completeness for resumable downloads
        if supports_resume {
            let all_complete = final_state.chunks.iter().all(|c| c.is_complete());
            if !all_complete {
                task.status = DownloadStatus::Failed;
                task.downloaded = *downloaded_total.read().await;
                return Err(VoltError::Unknown(
                    "Download incomplete after all chunks finished".into(),
                ));
            }
        }

        // ── Finalize ──────────────────────────────────────────────
        let _ = tokio::fs::remove_file(&state_path).await;
        tokio::fs::rename(&part_path, &save_path)
            .await
            .map_err(VoltError::Io)?;

        task.status = DownloadStatus::Completed;
        task.downloaded = task.total_size.unwrap_or(0);

        let _ = progress_tx
            .send(ProgressEvent {
                download_id: task.id.clone(),
                downloaded: task.downloaded,
                total_size: task.total_size,
                speed_bps: 0.0,
                status: DownloadStatus::Completed,
            })
            .await;

        info!("Download completed: {}", save_path.display());
        Ok(())
    }

    fn calculate_chunks(&self, total_size: u64, desired_chunks: usize) -> Vec<ChunkRange> {
        let chunks = desired_chunks.clamp(1, MAX_CONCURRENT_CHUNKS);
        let chunk_size = (total_size / chunks as u64).max(CHUNK_SIZE_MIN);
        let mut ranges = Vec::new();
        let mut start = 0u64;
        let mut index = 0usize;

        while start < total_size {
            let end = (start + chunk_size - 1).min(total_size.saturating_sub(1));
            ranges.push(ChunkRange {
                index,
                start,
                end: Some(end),
            });
            start = end + 1;
            index += 1;
        }

        ranges
    }

    async fn preallocate_or_create(path: &Path, size: u64) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(VoltError::Io)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await
            .map_err(VoltError::Io)?;

        let meta = file.metadata().await.map_err(VoltError::Io)?;
        if meta.len() < size {
            file.set_len(size).await.map_err(VoltError::Io)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn download_chunk(
        client: reqwest::Client,
        url: String,
        chunk: ChunkRange,
        part_path: PathBuf, // Each chunk opens its own fd — no mutex contention
        downloaded_total: Arc<RwLock<u64>>,
        state: Arc<RwLock<PartState>>,
        cancel: tokio_util::sync::CancellationToken,
        speed_limit_bps: Option<u64>,
    ) -> (ChunkRange, Result<()>) {
        let chunk_clone = chunk.clone();

        // Cancel guard #1: before file open
        if cancel.is_cancelled() {
            return (chunk_clone, Ok(()));
        }

        // Open independent file handle for this chunk
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&part_path)
            .await
            .map_err(VoltError::Io);
        let mut file = match file {
            Ok(f) => f,
            Err(e) => {
                if cancel.is_cancelled() {
                    return (chunk_clone, Ok(()));
                }
                return (chunk_clone, Err(e));
            }
        };

        // Compute resume offset from persisted state
        let resume_offset = {
            let st = state.read().await;
            let progress = match st.chunks.get(chunk.index) {
                Some(p) => p,
                None => {
                    return (
                        chunk_clone,
                        Err(VoltError::Unknown(format!(
                            "Chunk {} not found in state",
                            chunk.index
                        ))),
                    )
                }
            };
            if progress.is_complete() {
                return (chunk_clone, Ok(()));
            }
            chunk.start + progress.downloaded
        };

        // Cancel guard #2: before seek
        if cancel.is_cancelled() {
            return (chunk_clone, Ok(()));
        }

        // Seek to correct position immediately
        if let Err(e) = file
            .seek(SeekFrom::Start(resume_offset))
            .await
            .map_err(VoltError::Io)
        {
            if cancel.is_cancelled() {
                return (chunk_clone, Ok(()));
            }
            return (chunk_clone, Err(e));
        }

        let mut request = client.get(&url);
        let range_end = chunk.end.map(|e| format!("{}", e)).unwrap_or_default();
        request = request.header(
            header::RANGE,
            format!("bytes={}-{}", resume_offset, range_end),
        );

        // Cancel guard #3: before HTTP request
        if cancel.is_cancelled() {
            return (chunk_clone, Ok(()));
        }

        let mut response = match request
            .send()
            .await
            .map_err(|e| VoltError::Http(e.to_string()))
        {
            Ok(r) => r,
            Err(e) => {
                if cancel.is_cancelled() {
                    return (chunk_clone, Ok(()));
                }
                return (chunk_clone, Err(e));
            }
        };
        let status = response.status();

        if !status.is_success() && status.as_u16() != 206 {
            if cancel.is_cancelled() {
                return (chunk_clone, Ok(()));
            }
            return (
                chunk_clone,
                Err(VoltError::Http(
                    response.error_for_status().unwrap_err().to_string(),
                )),
            );
        }

        let mut local_downloaded = 0u64;
        let chunk_start_time = Instant::now();

        loop {
            if cancel.is_cancelled() {
                return (chunk_clone, Ok(()));
            }
            match response.chunk().await {
                Ok(None) => break,
                Ok(Some(bytes)) => {
                    if cancel.is_cancelled() {
                        return (chunk_clone, Ok(()));
                    }
                    if let Err(e) = file.write_all(&bytes).await.map_err(VoltError::Io) {
                        if cancel.is_cancelled() {
                            return (chunk_clone, Ok(()));
                        }
                        return (chunk_clone, Err(e));
                    }

                    local_downloaded += bytes.len() as u64;

                    {
                        let mut total = downloaded_total.write().await;
                        *total += bytes.len() as u64;
                    }

                    {
                        let mut st = state.write().await;
                        if let Some(progress) = st.chunks.get_mut(chunk.index) {
                            progress.downloaded += bytes.len() as u64;
                        }
                    }

                    // ── Speed limit throttle ──────────────────────────
                    if let Some(limit) = speed_limit_bps {
                        if limit > 0 {
                            let elapsed = chunk_start_time.elapsed().as_secs_f64();
                            let expected = (elapsed * limit as f64) as u64;
                            if local_downloaded > expected {
                                let overshoot = local_downloaded - expected;
                                let sleep_secs = overshoot as f64 / limit as f64;
                                if sleep_secs > 0.001 {
                                    if cancel.is_cancelled() {
                                        return (chunk_clone, Ok(()));
                                    }
                                    tokio::time::sleep(Duration::from_secs_f64(sleep_secs)).await;
                                }
                            }
                        }
                    }
                }
                Err(_e) if cancel.is_cancelled() => return (chunk_clone, Ok(())),
                Err(e) => return (chunk_clone, Err(VoltError::Http(e.to_string()))),
            }
        }

        debug!(
            "Chunk {} done: +{} bytes from offset {}",
            chunk_clone.index, local_downloaded, resume_offset
        );
        (chunk_clone, Ok(()))
    }

    // ── State persistence ─────────────────────────────────────────

    async fn load_state(path: &str) -> Result<PartState> {
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(VoltError::Io)?;
        serde_json::from_str(&content).map_err(|e| VoltError::Unknown(e.to_string()))
    }

    async fn save_state(path: &str, state: &PartState) -> Result<()> {
        let json =
            serde_json::to_string_pretty(state).map_err(|e| VoltError::Unknown(e.to_string()))?;
        tokio::fs::write(path, json).await.map_err(VoltError::Io)
    }
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub url: String,
    pub filename: String,
    pub total_size: Option<u64>,
    pub accepts_ranges: bool,
    pub content_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculate_chunks_respects_bounds_and_coverage() {
        let engine = DownloadEngine::new(None).expect("engine");
        let total = 10 * 1024 * 1024;
        let chunks = engine.calculate_chunks(total, 4);
        assert!(!chunks.is_empty());
        assert!(chunks.len() <= MAX_CONCURRENT_CHUNKS);
        assert_eq!(chunks.first().map(|c| c.start), Some(0));
        assert_eq!(chunks.last().and_then(|c| c.end), Some(total - 1));
        for window in chunks.windows(2) {
            assert_eq!(window[0].end.unwrap_or(0) + 1, window[1].start);
        }
    }

    #[test]
    fn calculate_chunks_clamps_desired_chunk_count() {
        let engine = DownloadEngine::new(None).expect("engine");
        let total = 128 * 1024 * 1024;
        let chunks = engine.calculate_chunks(total, 10_000);
        assert!(chunks.len() <= MAX_CONCURRENT_CHUNKS);
    }
}
