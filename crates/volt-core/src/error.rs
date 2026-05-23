use thiserror::Error;

#[derive(Error, Debug)]
pub enum VoltError {
    #[error("HTTP request failed: {0}")]
    Http(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    #[error("Server does not support range requests (resume impossible)")]
    NoRangeSupport,

    #[error("Download already exists: {0}")]
    AlreadyExists(String),

    #[error("Queue full (max {0} concurrent)")]
    QueueFull(usize),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl Clone for VoltError {
    fn clone(&self) -> Self {
        match self {
            VoltError::Http(s) => VoltError::Http(s.clone()),
            VoltError::Io(e) => VoltError::Io(std::io::Error::new(e.kind(), e.to_string())),
            VoltError::InvalidUrl(s) => VoltError::InvalidUrl(s.clone()),
            VoltError::NoRangeSupport => VoltError::NoRangeSupport,
            VoltError::AlreadyExists(s) => VoltError::AlreadyExists(s.clone()),
            VoltError::QueueFull(n) => VoltError::QueueFull(*n),
            VoltError::Database(s) => VoltError::Database(s.clone()),
            VoltError::Unknown(s) => VoltError::Unknown(s.clone()),
        }
    }
}

pub type Result<T> = std::result::Result<T, VoltError>;

/// Classification of download errors for smart retry / fail-fast logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Retryable: timeouts, 5xx, connection resets, DNS failures.
    Transient,
    /// Do not retry: 4xx (except 429), disk full, malformed response.
    Permanent,
    /// User cancelled / paused — error should be silently swallowed.
    Cancelled,
}

impl VoltError {
    /// Classify this error into Transient / Permanent / Cancelled.
    pub fn kind(&self) -> ErrorKind {
        match self {
            // IO errors
            VoltError::Io(e) => {
                use std::io::ErrorKind as IoKind;
                match e.kind() {
                    IoKind::ConnectionRefused
                    | IoKind::ConnectionReset
                    | IoKind::ConnectionAborted
                    | IoKind::NotConnected
                    | IoKind::TimedOut
                    | IoKind::UnexpectedEof => ErrorKind::Transient,
                    IoKind::PermissionDenied
                    | IoKind::NotFound
                    | IoKind::AlreadyExists
                    | IoKind::InvalidInput
                    | IoKind::WriteZero => ErrorKind::Permanent,
                    _ => ErrorKind::Transient, // conservative: unknown IO = retry once
                }
            }
            // HTTP errors
            VoltError::Http(msg) => {
                // Try to extract status code from message like "404 Not Found" or "status: 500"
                let code = msg.split_whitespace().find_map(|s| s.parse::<u16>().ok());
                if let Some(code) = code {
                    if code == 429 || code == 408 || (500..600).contains(&code) {
                        return ErrorKind::Transient;
                    }
                    if (400..500).contains(&code) {
                        return ErrorKind::Permanent;
                    }
                }
                // Check error text for transient indicators
                let txt = msg.to_lowercase();
                if txt.contains("timeout")
                    || txt.contains("timed out")
                    || txt.contains("connection reset")
                    || txt.contains("connection refused")
                    || txt.contains("broken pipe")
                    || txt.contains("dns")
                    || txt.contains("temporary")
                {
                    return ErrorKind::Transient;
                }
                ErrorKind::Transient
            }
            // Everything else treated as permanent (fail fast)
            VoltError::InvalidUrl(_) => ErrorKind::Permanent,
            VoltError::NoRangeSupport => ErrorKind::Permanent,
            VoltError::AlreadyExists(_) => ErrorKind::Permanent,
            VoltError::QueueFull(_) => ErrorKind::Transient,
            VoltError::Database(_) => ErrorKind::Transient,
            VoltError::Unknown(msg) => {
                let m = msg.to_lowercase();
                if m.contains("cancel") || m.contains("abort") {
                    ErrorKind::Cancelled
                } else {
                    ErrorKind::Transient
                }
            }
        }
    }
}
