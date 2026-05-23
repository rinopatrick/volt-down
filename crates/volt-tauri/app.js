const { invoke } = window.__TAURI__.core;

let downloads = new Map();
let pollInterval = null;

function init() {
    document.getElementById('addBtn').addEventListener('click', addDownload);
    document.getElementById('urlInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') addDownload();
    });
    document.getElementById('pauseAllBtn').addEventListener('click', pauseAll);
    document.getElementById('resumeAllBtn').addEventListener('click', resumeAll);
    document.getElementById('clearCompletedBtn').addEventListener('click', clearCompleted);

    startPolling();
}

async function addDownload() {
    const input = document.getElementById('urlInput');
    const url = input.value.trim();
    if (!url) return;

    try {
        const id = await invoke('add_download', { url });
        console.log('Added download:', id);
        input.value = '';
        updateStatus('Download added');
    } catch (e) {
        updateStatus('Error: ' + e, true);
    }
}

async function pauseDownload(id) {
    try {
        await invoke('pause_download', { id });
        updateStatus('Paused');
    } catch (e) {
        updateStatus('Error: ' + e, true);
    }
}

async function resumeDownload(id) {
    try {
        await invoke('resume_download', { id });
        updateStatus('Resumed');
    } catch (e) {
        updateStatus('Error: ' + e, true);
    }
}

async function cancelDownload(id) {
    try {
        await invoke('cancel_download', { id });
        downloads.delete(id);
        render();
        updateStatus('Cancelled');
    } catch (e) {
        updateStatus('Error: ' + e, true);
    }
}

async function pauseAll() {
    for (const [id, dl] of downloads) {
        if (dl.status === 'downloading' || dl.status === 'pending') {
            await pauseDownload(id);
        }
    }
}

async function resumeAll() {
    for (const [id, dl] of downloads) {
        if (dl.status === 'paused' || dl.status === 'failed') {
            await resumeDownload(id);
        }
    }
}

function clearCompleted() {
    for (const [id, dl] of downloads) {
        if (dl.status === 'completed' || dl.status === 'cancelled') {
            downloads.delete(id);
        }
    }
    render();
}

function startPolling() {
    pollInterval = setInterval(async () => {
        try {
            const list = await invoke('get_downloads');
            let totalSpeed = 0;
            for (const dl of list) {
                downloads.set(dl.id, dl);
                if (dl.status === 'downloading') {
                    totalSpeed += dl.speed_bps || 0;
                }
            }
            render();
            document.getElementById('speedText').textContent = totalSpeed > 0
                ? formatSpeed(totalSpeed) + ' total'
                : '';
        } catch (e) {
            console.error('Poll error:', e);
        }
    }, 500);
}

function render() {
    const container = document.getElementById('downloadsList');
    if (downloads.size === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <p>No active downloads</p>
                <p class="hint">Paste a URL above to start downloading</p>
            </div>
        `;
        return;
    }

    container.innerHTML = '';
    const sorted = Array.from(downloads.values()).sort((a, b) =>
        new Date(b.created_at) - new Date(a.created_at)
    );

    for (const dl of sorted) {
        const el = document.createElement('div');
        el.className = 'download-item';
        const pct = dl.total_size ? ((dl.downloaded / dl.total_size) * 100).toFixed(1) : 0;
        el.innerHTML = `
            <div class="download-header">
                <span class="filename" title="${escapeHtml(dl.filename)}">${escapeHtml(dl.filename)}</span>
                <span class="status ${dl.status}">${dl.status}</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: ${pct}%"></div>
            </div>
            <div class="download-meta">
                <span>${formatBytes(dl.downloaded)} / ${dl.total_size ? formatBytes(dl.total_size) : '?'}</span>
                <span>${pct}% ${dl.speed_bps ? '· ' + formatSpeed(dl.speed_bps) : ''}</span>
            </div>
            <div class="download-actions">
                ${dl.status === 'downloading' || dl.status === 'pending'
                    ? `<button onclick="window.pauseDownload('${dl.id}')">Pause</button>`
                    : dl.status === 'paused' || dl.status === 'failed'
                    ? `<button onclick="window.resumeDownload('${dl.id}')">Resume</button>`
                    : ''}
                <button onclick="window.cancelDownload('${dl.id}')">Remove</button>
            </div>
        `;
        container.appendChild(el);
    }
}

function updateStatus(text, isError = false) {
    const el = document.getElementById('statusText');
    el.textContent = text;
    el.style.color = isError ? '#f87171' : '#555';
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatSpeed(bps) {
    return formatBytes(bps) + '/s';
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Expose for inline onclick handlers
window.pauseDownload = pauseDownload;
window.resumeDownload = resumeDownload;
window.cancelDownload = cancelDownload;

init();
