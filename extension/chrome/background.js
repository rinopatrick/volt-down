// VoltDown Chrome Extension — Background Service Worker (MV3)

const NATIVE_HOST = 'com.voltdown.native';
let port = null;

// ===== Native Messaging =====
function connectNative() {
  try {
    port = chrome.runtime.connectNative(NATIVE_HOST);
    port.onDisconnect.addListener(() => {
      console.log('Native host disconnected');
      port = null;
    });
    port.onMessage.addListener((msg) => {
      console.log('From native:', msg);
    });
  } catch (e) {
    console.error('Native connect failed:', e);
  }
}

function sendToVoltDown(url, filename, referrer) {
  const msg = {
    type: 'download',
    url: url,
    filename: filename || null,
    referrer: referrer || null,
    timestamp: Date.now()
  };

  if (port) {
    try {
      port.postMessage(msg);
      return true;
    } catch (e) {
      console.error('Post failed, falling back:', e);
    }
  }

  // Fallback: store for app to poll
  chrome.storage.local.get({ pending: [] }, (data) => {
    data.pending.push(msg);
    chrome.storage.local.set({ pending: data.pending });
  });

  return false;
}

// ===== Context Menu =====
chrome.runtime.onInstalled.addListener(() => {
  connectNative();

  chrome.contextMenus.create({
    id: 'voltdown-download',
    title: 'Download with VoltDown',
    contexts: ['link', 'audio', 'video']
  });

  chrome.contextMenus.create({
    id: 'voltdown-page',
    title: 'Download page media with VoltDown',
    contexts: ['page']
  });
});

chrome.contextMenus.onClicked.addListener((info, tab) => {
  if (info.menuItemId === 'voltdown-download') {
    const url = info.linkUrl || info.srcUrl || info.pageUrl;
    sendToVoltDown(url, null, tab?.url);
  }
  if (info.menuItemId === 'voltdown-page') {
    // Trigger content script scan
    chrome.tabs.sendMessage(tab.id, { action: 'scanMedia' });
  }
});

// ===== Download Interception =====
chrome.downloads.onDeterminingFilename.addListener((item, suggest) => {
  // Check if user wants VoltDown to handle this
  chrome.storage.sync.get({ intercept: true }, (settings) => {
    if (settings.intercept && item.url && !item.url.startsWith('blob:')) {
      // Cancel browser download and send to VoltDown
      chrome.downloads.cancel(item.id);
      sendToVoltDown(item.url, item.filename, item.referrer);
    }
    suggest();
  });
});

// ===== Message Handling =====
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'addDownload') {
    const ok = sendToVoltDown(request.url, request.filename, sender.tab?.url);
    sendResponse({ success: ok });
  }
  if (request.action === 'getPending') {
    chrome.storage.local.get({ pending: [] }, (data) => {
      sendResponse({ pending: data.pending });
    });
    return true; // async
  }
  if (request.action === 'clearPending') {
    chrome.storage.local.set({ pending: [] });
    sendResponse({ cleared: true });
  }
});

// Auto-connect on startup
connectNative();
