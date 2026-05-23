# VoltDown

High-performance Rust download manager with chunked concurrency, resume support, and stealth mode.

## Features

- **Chunked downloads** — splits files into concurrent chunks with dynamic worker pools (`FuturesUnordered`)
- **Resume / pause** — persists `.part.state` to resume interrupted downloads
- **Speed limiting** — per-download or global bandwidth throttle (`bytes/s`)
- **Stealth mode** — rotating browser User-Agents + realistic HTTP headers
- **Fail-fast errors** — non-2xx URLs (e.g. 404) fail immediately without pointless retries
- **Chunk retry** — transient network errors retried up to 3×; permanent errors abort fast
- **REST API** — HTTP server for queue management, progress streaming, and control
- **Tauri GUI** — cross-platform desktop UI (work in progress)
- **Chrome extension** — send downloads from browser via native messaging

## Quick Start

```bash
cargo build --release
./target/release/voltdown server --port 62831
```

## API

### `POST /api/download`

Start a download.

```json
{
  "url": "https://example.com/file.zip",
  "save_path": "/tmp",
  "speed_limit_bps": 1048576,
  "stealth": false
}
```

Response:

```json
{ "id": "<uuid>", "status": "queued" }
```

### `POST /api/pause`

Pause an active download.

```json
{ "id": "<uuid>" }
```

### `POST /api/resume`

Resume a paused download.

```json
{ "id": "<uuid>" }
```

### `POST /api/cancel`

Cancel and remove a download.

```json
{ "id": "<uuid>" }
```

### `GET /api/downloads`

List all downloads grouped by status (`active`, `pending`, `completed`, `failed`).

### `GET /api/download/:id`

Get details for a single download.

## Architecture

| Crate | Role |
|-------|------|
| `volt-core` | Download engine, queue, database, REST API |
| `volt-tauri` | Desktop GUI (Tauri) |
| `volt-ext` | Chrome extension |
| `volt-native-host` | Native messaging bridge |

## Testing

A local Python test server is included in `/tmp/test_server.py` for end-to-end verification:

```bash
python3 /tmp/test_server.py
```

Supports Range requests, throttling, and 404 simulation.

## Release & CI

- CI workflow: [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
  - `cargo check --workspace`
  - `cargo test --workspace` (includes integration tests in `crates/volt-core/tests`)
- Release workflow: [`.github/workflows/release.yml`](.github/workflows/release.yml)
  - Triggered by tags `v*.*.*`
  - Builds `voltdown` and `voltdown-native` for Linux/macOS/Windows
  - Runs smoke tests via `--help`
  - Uploads packaged artifacts to GitHub Release

Local quality gate before pushing:

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
```
