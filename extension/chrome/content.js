// VoltDown Content Script — Media detection & badge

(function() {
  'use strict';

  let mediaCount = 0;

  function countMedia() {
    const videos = document.querySelectorAll('video[src], video source[src]');
    const audios = document.querySelectorAll('audio[src], audio source[src]');
    const iframes = document.querySelectorAll('iframe[src*="youtube"], iframe[src*="vimeo"]');
    mediaCount = videos.length + audios.length + iframes.length;

    // Send count to popup
    chrome.runtime.sendMessage({
      action: 'mediaCount',
      count: mediaCount,
      url: location.href
    }).catch(() => {});
  }

  function scanMedia() {
    const results = [];

    // Direct video/audio tags
    document.querySelectorAll('video, audio').forEach(el => {
      const src = el.src || el.querySelector('source')?.src;
      if (src) {
        results.push({
          url: src,
          type: el.tagName.toLowerCase(),
          title: document.title,
          size: null
        });
      }
    });

    // HLS/DASH manifests in network (best effort via performance entries)
    performance.getEntriesByType('resource').forEach(r => {
      const url = r.name;
      if (url.match(/\.(m3u8|mpd)(\?|$)/i)) {
        results.push({
          url: url,
          type: 'stream-manifest',
          title: document.title,
          size: null
        });
      }
    });

    // YouTube specific (basic)
    if (location.hostname.includes('youtube.com') && window.ytInitialPlayerResponse) {
      const formats = window.ytInitialPlayerResponse.streamingData?.formats || [];
      formats.forEach(f => {
        results.push({
          url: f.url || f.signatureCipher,
          type: 'youtube',
          quality: f.qualityLabel,
          title: document.title
        });
      });
    }

    return results;
  }

  // Initial scan
  setTimeout(countMedia, 1500);

  // Re-scan on DOM changes
  const observer = new MutationObserver(() => countMedia());
  observer.observe(document.body, { childList: true, subtree: true });

  // Listen for background messages
  chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === 'scanMedia') {
      sendResponse({ media: scanMedia() });
    }
  });
})();
