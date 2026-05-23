#!/usr/bin/env python3
"""
VoltDown Clipboard Monitor
Watches clipboard for URLs and auto-queues them to VoltDown server.
Requires: pip install pyperclip
Usage: python3 clipboard-monitor.py
"""
import time
import json
import re
import urllib.request

SERVER = "http://127.0.0.1:62831"
URL_RE = re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+')


def get_clipboard():
    try:
        import pyperclip
        return pyperclip.paste() or ""
    except Exception:
        return ""


def queue_download(url: str):
    try:
        data = json.dumps({"url": url, "auto_categorize": True}).encode()
        req = urllib.request.Request(
            f"{SERVER}/api/download",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"  → Queued: {url}")
    except Exception as e:
        print(f"  → Failed: {url} ({e})")


def main():
    print("⚡ VoltDown Clipboard Monitor")
    print(f"   Server: {SERVER}")
    print("   Copy a URL to clipboard to auto-queue it. Ctrl+C to exit.\n")

    last = ""
    seen = set()

    while True:
        try:
            text = get_clipboard()
            if text and text != last:
                last = text
                urls = URL_RE.findall(text)
                for url in urls:
                    if url not in seen:
                        seen.add(url)
                        print(f"[CLIPBOARD] {url}")
                        queue_download(url)
            time.sleep(0.8)
        except KeyboardInterrupt:
            print("\nStopped.")
            break


if __name__ == "__main__":
    main()
