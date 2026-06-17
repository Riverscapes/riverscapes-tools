#!/usr/bin/env bash
set -euo pipefail

# Step 1 workflow: download BLM ownership source data and unzip it.
URL="${BLM_OWNERSHIP_URL:-https://www.arcgis.com/sharing/rest/content/items/6bf2e737c59d4111be92420ee5ab0b46/data}"
DATA_DIR="${BLM_OWNERSHIP_DATA_DIR:-$HOME/GISData}"
ZIP_PATH="${BLM_OWNERSHIP_ZIP_PATH:-$DATA_DIR/data.zip}"
UNZIP_DIR="${BLM_OWNERSHIP_UNZIP_DIR:-$DATA_DIR/blm_ownership}"

mkdir -p "$DATA_DIR"
mkdir -p "$UNZIP_DIR"

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

cd "$TMP_DIR"
echo "Downloading source data from: $URL"
wget -O data "$URL"

mv data data.zip
mv data.zip "$ZIP_PATH"

echo "Unzipping into: $UNZIP_DIR"
unzip -o "$ZIP_PATH" -d "$UNZIP_DIR"

echo "Done"
echo "ZIP: $ZIP_PATH"
echo "UNZIPPED: $UNZIP_DIR"
