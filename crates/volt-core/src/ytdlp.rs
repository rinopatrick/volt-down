use std::path::PathBuf;
use std::process::Command;

/// Uses yt-dlp (sidecar) to extract direct video URLs and metadata
pub struct YtDlp;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct VideoFormat {
    pub format_id: String,
    pub url: String,
    pub ext: String,
    pub quality: Option<serde_json::Value>,
    pub filesize: Option<u64>,
    pub audio_ext: Option<String>,
    pub video_ext: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct VideoInfo {
    pub id: String,
    pub title: String,
    pub webpage_url: String,
    pub duration: Option<u64>,
    pub thumbnail: Option<String>,
    pub formats: Vec<VideoFormat>,
}

impl YtDlp {
    pub fn new() -> Self {
        Self
    }

    /// Extract video info JSON from URL using yt-dlp
    pub fn extract(&self, url: &str) -> anyhow::Result<VideoInfo> {
        let ytdlp_path = Self::find_ytdlp();

        let output = Command::new(&ytdlp_path)
            .args([
                "--no-warnings",
                "--dump-single-json",
                "--no-playlist",
                "-f",
                "best[filesize<500M]/best",
                url,
            ])
            .output()
            .map_err(|e| anyhow::anyhow!("Failed to run yt-dlp: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("yt-dlp error: {}", stderr));
        }

        let json_str = String::from_utf8_lossy(&output.stdout);
        let info: VideoInfo = serde_json::from_str(&json_str)
            .map_err(|e| anyhow::anyhow!("JSON parse error: {}", e))?;

        Ok(info)
    }

    /// List all available formats
    pub fn list_formats(&self, url: &str) -> anyhow::Result<Vec<VideoFormat>> {
        let info = self.extract(url)?;
        Ok(info.formats)
    }

    /// Get best direct download URL
    pub fn get_best_url(&self, url: &str) -> anyhow::Result<String> {
        let info = self.extract(url)?;
        info.formats
            .into_iter()
            .find(|f| !f.url.is_empty())
            .map(|f| f.url)
            .ok_or_else(|| anyhow::anyhow!("No downloadable format found"))
    }

    fn find_ytdlp() -> PathBuf {
        // Check sidecar first (bundled binary)
        let sidecar = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|p| p.join("yt-dlp")));

        if let Some(ref path) = sidecar {
            if path.exists() {
                return path.clone();
            }
        }

        // Check system PATH
        if let Ok(path) = which::which("yt-dlp") {
            return path;
        }

        // Fallback to bundled in data dir
        let data_dir = dirs::data_dir().unwrap_or_else(|| PathBuf::from("."));
        let bundled = data_dir.join("voltdown").join("yt-dlp");
        if bundled.exists() {
            return bundled;
        }

        // Last resort — return "yt-dlp" and let it fail clearly
        PathBuf::from("yt-dlp")
    }
}

impl Default for YtDlp {
    fn default() -> Self {
        Self::new()
    }
}
