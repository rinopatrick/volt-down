#!/usr/bin/env bash
# Install VoltDown Native Messaging Host for Chrome/Chromium
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Detect OS
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    HOST_DIR="$HOME/.config/google-chrome/NativeMessagingHosts"
    CHROMIUM_DIR="$HOME/.config/chromium/NativeMessagingHosts"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    HOST_DIR="$HOME/Library/Application Support/Google/Chrome/NativeMessagingHosts"
    CHROMIUM_DIR="$HOME/Library/Application Support/Chromium/NativeMessagingHosts"
else
    echo "Unsupported OS: $OSTYPE"
    exit 1
fi

# Build native host if not exists
NATIVE_BIN="$PROJECT_ROOT/target/release/voltdown-native"
if [[ ! -f "$NATIVE_BIN" ]]; then
    echo "Building voltdown-native..."
    cd "$PROJECT_ROOT"
    cargo build --release -p volt-native-host
fi

# Create host dirs
mkdir -p "$HOST_DIR"
mkdir -p "$CHROMIUM_DIR"

# Generate manifest
MANIFEST='{
  "name": "com.voltdown.native",
  "description": "VoltDown Native Messaging Host",
  "path": "'"$NATIVE_BIN"'",
  "type": "stdio",
  "allowed_origins": [
    "chrome-extension://*/"
  ]
}'

# Write manifest
echo "$MANIFEST" > "$HOST_DIR/com.voltdown.native.json"
echo "$MANIFEST" > "$CHROMIUM_DIR/com.voltdown.native.json"

echo "✅ Native host installed:"
echo "   Chrome:  $HOST_DIR/com.voltdown.native.json"
echo "   Chromium: $CHROMIUM_DIR/com.voltdown.native.json"
echo "   Binary:  $NATIVE_BIN"
