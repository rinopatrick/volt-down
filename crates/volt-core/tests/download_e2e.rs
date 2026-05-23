use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use volt_core::{DownloadEngine, DownloadTask};

async fn spawn_range_server(payload: Arc<Vec<u8>>) -> io::Result<(String, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accept = listener.accept() => {
                    let Ok((mut stream, _)) = accept else { break; };
                    let data = payload.clone();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 8192];
                        let Ok(n) = stream.read(&mut buf).await else { return; };
                        if n == 0 {
                            return;
                        }
                        let req = String::from_utf8_lossy(&buf[..n]);
                        let mut lines = req.lines();
                        let first = lines.next().unwrap_or_default().to_string();
                        let is_head = first.starts_with("HEAD ");
                        let is_get = first.starts_with("GET ");

                        let mut range_start = None;
                        let mut range_end = None;
                        for line in req.lines() {
                            let lower = line.to_ascii_lowercase();
                            if lower.starts_with("range:") {
                                if let Some(spec) = line.split('=').nth(1) {
                                    let mut parts = spec.trim().split('-');
                                    range_start = parts.next().and_then(|v| v.parse::<usize>().ok());
                                    range_end = parts.next().and_then(|v| v.parse::<usize>().ok());
                                }
                            }
                        }

                        let full_len = data.len();
                        if is_head {
                            let headers = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                                full_len
                            );
                            let _ = stream.write_all(headers.as_bytes()).await;
                            return;
                        }

                        if is_get {
                            let (start, end, status) = if let Some(start) = range_start {
                                let end = range_end.unwrap_or(full_len.saturating_sub(1)).min(full_len.saturating_sub(1));
                                (start.min(full_len.saturating_sub(1)), end, "206 Partial Content")
                            } else {
                                (0usize, full_len.saturating_sub(1), "200 OK")
                            };

                            let body = &data[start..=end];
                            let headers = if status.starts_with("206") {
                                format!(
                                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {}-{}/{}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                                    body.len(), start, end, full_len
                                )
                            } else {
                                format!(
                                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                                    body.len()
                                )
                            };
                            let _ = stream.write_all(headers.as_bytes()).await;
                            let _ = stream.write_all(body).await;
                            return;
                        }

                        let _ = stream
                            .write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                            .await;
                    });
                }
            }
        }
    });

    Ok((format!("http://{addr}/file.bin"), shutdown_tx))
}

#[tokio::test]
async fn download_completes_and_writes_expected_file() {
    let payload = Arc::new(
        (0..200_000u32)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>(),
    );
    let (url, shutdown) = spawn_range_server(payload.clone()).await.expect("server");

    let tmp = tempfile::tempdir().expect("tempdir");
    let save_dir = tmp.path().to_string_lossy().to_string();
    let mut task = DownloadTask::new(url, "artifact.bin".to_string(), save_dir.clone(), 4);

    let engine = DownloadEngine::new(None).expect("engine");
    let (progress_tx, _progress_rx) = mpsc::channel(64);
    let cancel = tokio_util::sync::CancellationToken::new();

    engine
        .download(&mut task, progress_tx, cancel)
        .await
        .expect("download ok");

    let out = tokio::fs::read(tmp.path().join("artifact.bin"))
        .await
        .expect("read output");
    assert_eq!(out, *payload);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn resume_continues_from_saved_part_state() {
    let payload = Arc::new(
        (0..180_000u32)
            .map(|i| (i % 199) as u8)
            .collect::<Vec<u8>>(),
    );
    let (url, shutdown) = spawn_range_server(payload.clone()).await.expect("server");

    let tmp = tempfile::tempdir().expect("tempdir");
    let save_dir = tmp.path().to_string_lossy().to_string();

    let target = tmp.path().join("resume.bin");
    let part = target.with_extension("part");
    let state = format!("{}.state", part.display());

    let already = 50_000usize;
    tokio::fs::write(&part, &payload[..already])
        .await
        .expect("write part");
    tokio::fs::write(
        &state,
        format!(
            "{{\"chunks\":[{{\"index\":0,\"start\":0,\"end\":{},\"downloaded\":{}}}]}}",
            payload.len() - 1,
            already
        ),
    )
    .await
    .expect("write state");

    let mut task = DownloadTask::new(url, "resume.bin".to_string(), save_dir, 1);
    let engine = DownloadEngine::new(None).expect("engine");
    let (progress_tx, _progress_rx) = mpsc::channel(64);
    let cancel = tokio_util::sync::CancellationToken::new();

    engine
        .download(&mut task, progress_tx, cancel)
        .await
        .expect("resume download ok");

    let out = tokio::fs::read(&target).await.expect("read resumed output");
    assert_eq!(out, *payload);

    let _ = shutdown.send(());
}
