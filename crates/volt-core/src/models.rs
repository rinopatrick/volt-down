use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    Pending,
    Downloading,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for DownloadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadStatus::Pending => write!(f, "pending"),
            DownloadStatus::Downloading => write!(f, "downloading"),
            DownloadStatus::Paused => write!(f, "paused"),
            DownloadStatus::Completed => write!(f, "completed"),
            DownloadStatus::Failed => write!(f, "failed"),
            DownloadStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for DownloadStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "pending" => Ok(DownloadStatus::Pending),
            "downloading" => Ok(DownloadStatus::Downloading),
            "paused" => Ok(DownloadStatus::Paused),
            "completed" => Ok(DownloadStatus::Completed),
            "failed" => Ok(DownloadStatus::Failed),
            "cancelled" => Ok(DownloadStatus::Cancelled),
            _ => Err(format!("Unknown status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    pub id: String,
    pub url: String,
    pub filename: String,
    pub save_path: String,
    pub total_size: Option<u64>,
    pub downloaded: u64,
    pub status: DownloadStatus,
    pub chunks: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub error: Option<String>,
    pub metadata: Option<serde_json::Value>,
    #[serde(skip)]
    pub speed_limit_bps: Option<u64>,
    pub proxy_url: Option<String>,
    pub cookies: Option<String>,
}

impl DownloadTask {
    pub fn new(url: String, filename: String, save_path: String, chunks: usize) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            url,
            filename,
            save_path,
            total_size: None,
            downloaded: 0,
            status: DownloadStatus::Pending,
            chunks,
            created_at: now,
            updated_at: now,
            error: None,
            metadata: None,
            speed_limit_bps: None,
            proxy_url: None,
            cookies: None,
        }
    }

    pub fn progress_percent(&self) -> f64 {
        match self.total_size {
            Some(total) if total > 0 => (self.downloaded as f64 / total as f64) * 100.0,
            _ => 0.0,
        }
    }
}

/// Auto-categorize download by file extension into folder
pub fn auto_categorize(filename: &str) -> &'static str {
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm" | "m4v" | "3gp" | "mpg" | "mpeg" => {
            "Videos"
        }
        "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" | "m4a" | "opus" => "Audio",
        "pdf" | "doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx" | "txt" | "rtf" | "epub"
        | "mobi" | "csv" => "Documents",
        "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" | "tgz" | "tbz" => "Archives",
        "jpg" | "jpeg" | "png" | "gif" | "bmp" | "svg" | "webp" | "ico" | "tiff" | "tif" => {
            "Images"
        }
        "exe" | "msi" | "dmg" | "pkg" | "deb" | "rpm" | "appimage" | "sh" | "bat" | "cmd" => {
            "Programs"
        }
        _ => "Others",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkState {
    pub id: i64,
    pub download_id: String,
    pub chunk_index: usize,
    pub start_byte: u64,
    pub end_byte: u64,
    pub downloaded: u64,
    pub status: ChunkStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChunkStatus {
    Pending,
    Downloading,
    Completed,
    Failed,
}

impl std::fmt::Display for ChunkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkStatus::Pending => write!(f, "pending"),
            ChunkStatus::Downloading => write!(f, "downloading"),
            ChunkStatus::Completed => write!(f, "completed"),
            ChunkStatus::Failed => write!(f, "failed"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    pub download_id: String,
    pub downloaded: u64,
    pub total_size: Option<u64>,
    pub speed_bps: f64,
    pub status: DownloadStatus,
}
