use axum::extract::State;
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use volt_core::{
    Database, DownloadConfig, DownloadQueue, DownloadStatus, DownloadTask, ProgressEvent, YtDlp,
};

#[derive(Parser)]
#[command(name = "voltdown")]
#[command(about = "VoltDown CLI — lightweight download manager")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a new download
    Add {
        url: String,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(short, long, default_value = "4")]
        threads: usize,
        #[arg(short, long)]
        filename: Option<String>,
        /// Speed limit in KB/s (e.g. 500 = 500 KB/s)
        #[arg(short, long)]
        speed: Option<u64>,
        /// Enable stealth mode (random browser headers)
        #[arg(long)]
        stealth: bool,
        /// Auto-categorize into folders (Videos, Audio, Documents, etc.)
        #[arg(long)]
        auto_categorize: bool,
        /// Proxy URL (e.g. http://127.0.0.1:8080 or socks5://127.0.0.1:1080)
        #[arg(long)]
        proxy: Option<String>,
        /// Cookies for site login (semicolon-separated, e.g. "session=abc; user=john")
        #[arg(long)]
        cookies: Option<String>,
    },
    /// List active and pending downloads
    List,
    /// Pause a download
    Pause { id: String },
    /// Resume a download
    Resume { id: String },
    /// Cancel a download
    Cancel { id: String },
    /// Extract video URL using yt-dlp
    Extract { url: String },
    /// Start local HTTP API server
    Server {
        #[arg(short, long, default_value = "62831")]
        port: u16,
    },
}

fn db_path() -> PathBuf {
    let base = dirs::data_dir().unwrap_or_else(|| PathBuf::from("."));
    base.join("volt-down").join("volt-down.db")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Add {
            url,
            output,
            threads,
            filename,
            speed,
            stealth,
            auto_categorize,
            proxy,
            cookies,
        } => {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ProgressEvent>(100);
            let config = DownloadConfig {
                chunks: threads,
                speed_limit_bps: speed.map(|kb| kb * 1024),
                user_agent: format!("VoltDown/{}", env!("CARGO_PKG_VERSION")),
                stealth,
                proxy_url: proxy.clone(),
                cookies: cookies.clone(),
            };

            let db_path = db_path();
            if let Some(parent) = db_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            let db = Arc::new(Database::new(&db_path)?);
            let queue = Arc::new(DownloadQueue::new(Some(config), None, tx, Some(db))?);

            let save_path = output.unwrap_or_else(|| PathBuf::from("."));
            let save_str = save_path.to_string_lossy().to_string();
            let fname =
                filename.unwrap_or_else(|| url.split('/').last().unwrap_or("download").to_string());

            let mut final_save = save_str.clone();
            if auto_categorize {
                let category = volt_core::auto_categorize(&fname);
                let base = std::path::PathBuf::from(&save_str);
                final_save = base.join(category).to_string_lossy().to_string();
                std::fs::create_dir_all(&final_save)?;
                println!("📁 Auto-categorized: {}/", category);
            }

            let mut task = DownloadTask::new(url, fname, final_save, threads);
            task.proxy_url = proxy;
            task.cookies = cookies;
            let id = task.id.clone();
            queue.add(task).await?;

            info!("Download added: id={}", id);
            println!("✓ Queued download {} — starting...", id);

            // Simple progress loop
            loop {
                let mut got_event = false;
                while let Ok(evt) = rx.try_recv() {
                    got_event = true;
                    let pct = match evt.total_size {
                        Some(t) if t > 0 => (evt.downloaded as f64 / t as f64) * 100.0,
                        _ => 0.0,
                    };
                    println!(
                        "Progress: {:.1}% | {} / {} bytes | {:.1} MB/s",
                        pct,
                        evt.downloaded,
                        evt.total_size.unwrap_or(0),
                        evt.speed_bps / 1_048_576.0
                    );
                    if evt.status == volt_core::DownloadStatus::Completed
                        || evt.status == volt_core::DownloadStatus::Failed
                    {
                        println!("Status: {:?}", evt.status);
                        return Ok(());
                    }
                }
                if !got_event {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
        Commands::List => match reqwest::get("http://127.0.0.1:62831/api/downloads").await {
            Ok(resp) => {
                let data: serde_json::Value = resp.json().await?;
                let active = data
                    .get("active")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let pending = data
                    .get("pending")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                println!("{:<36} {:<12} {:<8} {}", "ID", "STATUS", "PROG%", "URL");
                for item in active.iter().chain(pending.iter()) {
                    let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                    let status = item.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                    let pct = item
                        .get("progress_percent")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let url = item.get("url").and_then(|v| v.as_str()).unwrap_or("?");
                    println!(
                        "{:<36} {:<12} {:<7.1}% {}",
                        id,
                        status,
                        pct,
                        &url[..url.len().min(50)]
                    );
                }
                if active.is_empty() && pending.is_empty() {
                    println!("No downloads.");
                }
            }
            Err(_) => {
                eprintln!("✗ Server not running. Start with: voltdown server");
                std::process::exit(1);
            }
        },
        Commands::Pause { id } => {
            let client = reqwest::Client::new();
            match client
                .post("http://127.0.0.1:62831/api/pause")
                .json(&json!({"id": id}))
                .send()
                .await
            {
                Ok(resp) => {
                    let data: serde_json::Value = resp.json().await?;
                    if data.get("error").is_some() {
                        eprintln!("✗ {}", data["error"]);
                    } else {
                        println!("✓ Paused {}", id);
                    }
                }
                Err(_) => {
                    eprintln!("✗ Server not running. Start with: voltdown server");
                    std::process::exit(1);
                }
            }
        }
        Commands::Resume { id } => {
            let client = reqwest::Client::new();
            match client
                .post("http://127.0.0.1:62831/api/resume")
                .json(&json!({"id": id}))
                .send()
                .await
            {
                Ok(resp) => {
                    let data: serde_json::Value = resp.json().await?;
                    if data.get("error").is_some() {
                        eprintln!("✗ {}", data["error"]);
                    } else {
                        println!("✓ Resumed {}", id);
                    }
                }
                Err(_) => {
                    eprintln!("✗ Server not running. Start with: voltdown server");
                    std::process::exit(1);
                }
            }
        }
        Commands::Cancel { id } => {
            let client = reqwest::Client::new();
            match client
                .post("http://127.0.0.1:62831/api/cancel")
                .json(&json!({"id": id}))
                .send()
                .await
            {
                Ok(resp) => {
                    let data: serde_json::Value = resp.json().await?;
                    if data.get("error").is_some() {
                        eprintln!("✗ {}", data["error"]);
                    } else {
                        println!("✓ Cancelled {}", id);
                    }
                }
                Err(_) => {
                    eprintln!("✗ Server not running. Start with: voltdown server");
                    std::process::exit(1);
                }
            }
        }
        Commands::Extract { url } => {
            let ytdlp = YtDlp::new();
            match ytdlp.extract(&url) {
                Ok(info) => {
                    println!("Title: {}", info.title);
                    println!("Duration: {:?}s", info.duration);
                    println!("Formats:");
                    for f in info.formats.iter().take(10) {
                        println!(
                            "  [{}] {} | {} | {:?}",
                            f.format_id,
                            f.ext,
                            f.quality
                                .as_ref()
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "?".into()),
                            f.filesize.map(|s| format!("{:.2}MB", s as f64 / 1e6))
                        );
                    }
                }
                Err(e) => {
                    error!("Extraction failed: {}", e);
                    eprintln!("✗ yt-dlp error: {}", e);
                }
            }
        }
        Commands::Server { port } => {
            println!("🚀 Starting VoltDown API server on port {}", port);
            run_server(port).await?;
        }
    }

    Ok(())
}

async fn run_server(port: u16) -> anyhow::Result<()> {
    use axum::{
        response::Json,
        routing::{get, post},
        Router,
    };
    use serde_json::json;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ProgressEvent>(100);
    // Drain progress events so channel never blocks senders
    tokio::spawn(async move {
        while let Some(evt) = rx.recv().await {
            tracing::debug!(
                "Progress: id={} status={:?} downloaded={} total={:?} speed={:.1} MB/s",
                evt.download_id,
                evt.status,
                evt.downloaded,
                evt.total_size,
                evt.speed_bps / 1_048_576.0
            );
        }
    });

    let db_path = db_path();
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let db = Arc::new(Database::new(&db_path)?);
    let queue = Arc::new(DownloadQueue::new(None, None, tx, Some(db.clone()))?);

    // Auto-resume incomplete downloads
    match queue.load_and_resume().await {
        Ok(count) => {
            if count > 0 {
                println!("↻ Auto-resumed {} incomplete downloads", count);
            }
        }
        Err(e) => {
            eprintln!("⚠ Failed to load incomplete downloads: {}", e);
        }
    }

    // Background worker: continuously poll for pending tasks
    let worker_queue = queue.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(500));
        loop {
            ticker.tick().await;
            worker_queue.process_queue().await;
        }
    });

    let app = Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/api/downloads", get(list_downloads))
        .route("/api/download", post(add_download))
        .route("/api/batch", post(batch_download))
        .route("/api/download/:id", get(get_download))
        .route("/api/pause", post(pause_download))
        .route("/api/resume", post(resume_download))
        .route("/api/cancel", post(cancel_download))
        .with_state(queue);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn list_downloads(
    State(queue): State<Arc<DownloadQueue>>,
) -> axum::response::Json<serde_json::Value> {
    let active = queue.get_active().await;
    let pending = queue.get_pending().await;
    let finished = queue.get_finished().await;
    let (completed, failed): (Vec<_>, Vec<_>) = finished
        .into_iter()
        .partition(|t| t.status == DownloadStatus::Completed);
    axum::response::Json(json!({
        "active": active,
        "pending": pending,
        "completed": completed,
        "failed": failed,
    }))
}

async fn add_download(
    State(queue): State<Arc<DownloadQueue>>,
    axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
) -> axum::response::Json<serde_json::Value> {
    let url = body.get("url").and_then(|v| v.as_str()).unwrap_or("");
    if url.is_empty() {
        return axum::response::Json(json!({ "error": "missing url" }));
    }

    let threads = body.get("threads").and_then(|v| v.as_u64()).unwrap_or(4) as usize;
    let save_path = body
        .get("save_path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let filename = body
        .get("filename")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| url.split('/').last().unwrap_or("download").to_string());
    let speed_kb = body.get("speed").and_then(|v| v.as_u64());
    let auto_categorize = body
        .get("auto_categorize")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let proxy_url = body
        .get("proxy")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let cookies = body
        .get("cookies")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut final_save_path = save_path.clone();
    if auto_categorize {
        let category = volt_core::auto_categorize(&filename);
        let base = std::path::PathBuf::from(&save_path);
        final_save_path = base.join(category).to_string_lossy().to_string();
        // Ensure category dir exists
        let _ = tokio::fs::create_dir_all(&final_save_path).await;
    }

    let mut task = DownloadTask::new(url.to_string(), filename, final_save_path, threads);
    if let Some(kb) = speed_kb {
        task.speed_limit_bps = Some(kb * 1024);
    }
    task.proxy_url = proxy_url;
    task.cookies = cookies;
    let id = task.id.clone();

    match queue.add(task).await {
        Ok(_) => axum::response::Json(json!({ "id": id, "status": "queued" })),
        Err(e) => axum::response::Json(json!({ "error": e.to_string() })),
    }
}

async fn batch_download(
    State(queue): State<Arc<DownloadQueue>>,
    axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
) -> axum::response::Json<serde_json::Value> {
    let urls = body
        .get("urls")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let threads = body.get("threads").and_then(|v| v.as_u64()).unwrap_or(4) as usize;
    let save_path = body
        .get("save_path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let auto_categorize = body
        .get("auto_categorize")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let speed_kb = body.get("speed").and_then(|v| v.as_u64());
    let proxy_url = body
        .get("proxy")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let cookies = body
        .get("cookies")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut queued = Vec::new();
    let mut errors = Vec::new();

    for item in urls.iter() {
        let url = item.as_str().unwrap_or("");
        if url.is_empty() {
            continue;
        }
        let filename = url.split('/').last().unwrap_or("download").to_string();

        let mut final_save_path = save_path.clone();
        if auto_categorize {
            let category = volt_core::auto_categorize(&filename);
            let base = std::path::PathBuf::from(&save_path);
            final_save_path = base.join(category).to_string_lossy().to_string();
            let _ = tokio::fs::create_dir_all(&final_save_path).await;
        }

        let mut task = DownloadTask::new(url.to_string(), filename, final_save_path, threads);
        if let Some(kb) = speed_kb {
            task.speed_limit_bps = Some(kb * 1024);
        }
        task.proxy_url = proxy_url.clone();
        task.cookies = cookies.clone();
        let id = task.id.clone();
        match queue.add(task).await {
            Ok(_) => queued.push(id),
            Err(e) => errors.push(json!({"url": url, "error": e.to_string()})),
        }
    }

    axum::response::Json(json!({
        "queued": queued.len(),
        "ids": queued,
        "errors": errors,
    }))
}

async fn get_download(
    axum::extract::Path(id): axum::extract::Path<String>,
    State(queue): State<Arc<DownloadQueue>>,
) -> axum::response::Json<serde_json::Value> {
    match queue.get_task(&id).await {
        Some(task) => axum::response::Json(json!(task)),
        None => axum::response::Json(json!({ "error": "not found" })),
    }
}

async fn pause_download(
    State(queue): State<Arc<DownloadQueue>>,
    axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
) -> axum::response::Json<serde_json::Value> {
    let id = body.get("id").and_then(|v| v.as_str()).unwrap_or("");
    if id.is_empty() {
        return axum::response::Json(json!({ "error": "missing id" }));
    }
    match queue.pause(id).await {
        Ok(_) => axum::response::Json(json!({ "id": id, "status": "paused" })),
        Err(e) => axum::response::Json(json!({ "error": e.to_string() })),
    }
}

async fn resume_download(
    State(queue): State<Arc<DownloadQueue>>,
    axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
) -> axum::response::Json<serde_json::Value> {
    let id = body.get("id").and_then(|v| v.as_str()).unwrap_or("");
    if id.is_empty() {
        return axum::response::Json(json!({ "error": "missing id" }));
    }
    match queue.resume(id).await {
        Ok(_) => axum::response::Json(json!({ "id": id, "status": "resumed" })),
        Err(e) => axum::response::Json(json!({ "error": e.to_string() })),
    }
}

async fn cancel_download(
    State(queue): State<Arc<DownloadQueue>>,
    axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
) -> axum::response::Json<serde_json::Value> {
    let id = body.get("id").and_then(|v| v.as_str()).unwrap_or("");
    if id.is_empty() {
        return axum::response::Json(json!({ "error": "missing id" }));
    }
    match queue.cancel(id).await {
        Ok(_) => axum::response::Json(json!({ "id": id, "status": "cancelled" })),
        Err(e) => axum::response::Json(json!({ "error": e.to_string() })),
    }
}
