document.addEventListener('DOMContentLoaded', () => {
  const statusEl = document.getElementById('status');
  const queueEl = document.getElementById('queue');
  const interceptToggle = document.getElementById('intercept');

  // Load settings
  chrome.storage.sync.get({ intercept: true }, (s) => {
    if (interceptToggle) interceptToggle.checked = s.intercept;
  });

  // Save settings
  if (interceptToggle) {
    interceptToggle.addEventListener('change', (e) => {
      chrome.storage.sync.set({ intercept: e.target.checked });
    });
  }

  // Check native host
  chrome.runtime.sendNativeMessage('com.voltdown.native', { type: 'ping' }, (resp) => {
    if (chrome.runtime.lastError) {
      statusEl.textContent = 'Native host: disconnected ⚠️';
      statusEl.style.color = '#ff6b6b';
    } else {
      statusEl.textContent = 'Native host: connected ✅';
      statusEl.style.color = '#51cf66';
    }
  });

  // Load pending queue
  chrome.runtime.sendMessage({ action: 'getPending' }, (data) => {
    if (!data || !data.pending || data.pending.length === 0) {
      queueEl.innerHTML = '<li class="empty">No pending downloads</li>';
      return;
    }
    queueEl.innerHTML = '';
    data.pending.forEach(item => {
      const li = document.createElement('li');
      li.textContent = item.url;
      queueEl.appendChild(li);
    });
  });

  // Open dashboard
  document.getElementById('openApp')?.addEventListener('click', () => {
    chrome.tabs.create({ url: 'http://127.0.0.1:62831' });
  });
});
