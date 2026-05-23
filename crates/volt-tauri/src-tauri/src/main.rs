use std::path::PathBuf;
use std::sync::Arc;

use tauri::Manager;
use tauri::State;
use tokio::sync::{mpsc, Mutex};
use tracing::info;
use volt_core::queue::DownloadQueue;
use volt_core::DownloadTask;

type QueueState = Arc<Mutex<DownloadQueue>>;

#[derive(serde::Serialize)]
struct DownloadView {
    id: String,
    filename: String,
    url: String,
    total_size: Option<u64>,
    downloaded: u64,
    status: String,
    speed_bps: f64,
    progress_percent: f64,
    created_at: String,
}

#[tauri::command]
async fn add_download(url: String, queue: State<'_, QueueState>) -> Result<String, String> {
    info!("Adding download: {}", url);

    let filename = url.split('/').last().unwrap_or("download.bin").to_string();
    let save_path = dirs::download_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .to_string_lossy()
        .to_string();

    let task = DownloadTask::new(url, filename, save_path, 4);
    let id = task.id.clone();

    let q = queue.lock().await;
    q.add(task).await.map_err(|e| e.to_string())?;
    Ok(id)
}

#[tauri::command]
async fn pause_download(id: String, queue: State<'_, QueueState>) -> Result<(), String> {
    let q = queue.lock().await;
    q.pause(&id).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn resume_download(id: String, queue: State<'_, QueueState>) -> Result<(), String> {
    let q = queue.lock().await;
    q.resume(&id).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn cancel_download(id: String, queue: State<'_, QueueState>) -> Result<(), String> {
    let q = queue.lock().await;
    q.cancel(&id).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn get_downloads(queue: State<'_, QueueState>) -> Result<Vec<DownloadView>, String> {
    let q = queue.lock().await;
    let mut views = Vec::new();

    for task in q.get_pending().await {
        views.push(DownloadView {
            id: task.id.clone(),
            filename: task.filename.clone(),
            url: task.url.clone(),
            total_size: task.total_size,
            downloaded: task.downloaded,
            status: task.status.to_string(),
            speed_bps: 0.0,
            progress_percent: task.progress_percent(),
            created_at: task.created_at.to_rfc3339(),
        });
    }

    // TODO: merge active downloads when state tracking is improved
    Ok(views)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn main() {
    tracing_subscriber::fmt::init();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let (progress_tx, mut progress_rx) =
                mpsc::channel::<volt_core::models::ProgressEvent>(100);

            let queue = Arc::new(Mutex::new(
                DownloadQueue::new(None, Some(3), progress_tx, None)
                    .map_err(|e| e.to_string())
                    .expect("Failed to create download queue"),
            ));

            app.manage(queue.clone() as QueueState);

            // Background task to log progress
            tauri::async_runtime::spawn(async move {
                while let Some(event) = progress_rx.recv().await {
                    info!(
                        "Progress: {} — {:.1}% at {:.0} B/s",
                        event.download_id,
                        event
                            .total_size
                            .map(|t| (event.downloaded as f64 / t as f64) * 100.0)
                            .unwrap_or(0.0),
                        event.speed_bps
                    );
                }
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            add_download,
            pause_download,
            resume_download,
            cancel_download,
            get_downloads
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
