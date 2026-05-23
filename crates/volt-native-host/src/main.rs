use std::io::{self, Read, Write};

#[derive(serde::Deserialize)]
struct ExtMessage {
    #[serde(rename = "type")]
    msg_type: String,
    url: String,
    #[allow(dead_code)]
    filename: Option<String>,
    #[allow(dead_code)]
    referrer: Option<String>,
    #[allow(dead_code)]
    timestamp: u64,
}

#[derive(serde::Serialize)]
struct Response {
    success: bool,
    message: String,
}

fn main() {
    loop {
        // Chrome native messaging: 4-byte length prefix (little-endian)
        let mut len_bytes = [0u8; 4];
        if io::stdin().read_exact(&mut len_bytes).is_err() {
            break; // Chrome closed connection
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut msg_bytes = vec![0u8; len];
        if io::stdin().read_exact(&mut msg_bytes).is_err() {
            break;
        }

        let msg: ExtMessage = match serde_json::from_slice(&msg_bytes) {
            Ok(m) => m,
            Err(e) => {
                send_response(false, &format!("Parse error: {}", e));
                continue;
            }
        };

        if msg.msg_type == "download" {
            // For MVP: forward to voltdown CLI via HTTP or socket
            // In production: use IPC (Unix socket/named pipe)
            match forward_to_app(&msg.url) {
                Ok(()) => send_response(true, "Download queued"),
                Err(e) => send_response(false, &format!("Forward failed: {}", e)),
            }
        } else {
            send_response(false, "Unknown message type");
        }
    }
}

fn forward_to_app(url: &str) -> Result<(), String> {
    // Try HTTP localhost first (app runs local API on 62831)
    // Fallback: store to shared file for app to poll
    let client = reqwest::blocking::Client::new();
    match client
        .post("http://127.0.0.1:62831/api/download")
        .json(&serde_json::json!({ "url": url }))
        .timeout(std::time::Duration::from_secs(2))
        .send()
    {
        Ok(_) => Ok(()),
        Err(_) => {
            // Fallback: append to pending file
            let pending = dirs::data_dir()
                .unwrap_or_else(|| std::path::PathBuf::from("."))
                .join("voltdown")
                .join("pending.json");

            std::fs::create_dir_all(pending.parent().unwrap()).map_err(|e| e.to_string())?;

            let mut list = if pending.exists() {
                let content = std::fs::read_to_string(&pending).unwrap_or_else(|_| "[]".into());
                serde_json::from_str(&content).unwrap_or_else(|_| vec![])
            } else {
                Vec::new()
            };

            list.push(serde_json::json!({
                "url": url,
                "timestamp": chrono::Utc::now().timestamp()
            }));

            std::fs::write(&pending, serde_json::to_string(&list).unwrap())
                .map_err(|e| e.to_string())?;

            Ok(())
        }
    }
}

fn send_response(success: bool, message: &str) {
    let resp = Response {
        success,
        message: message.to_string(),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let len = json.len() as u32;
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let _ = handle.write_all(&len.to_le_bytes());
    let _ = handle.write_all(json.as_bytes());
    let _ = handle.flush();
}
