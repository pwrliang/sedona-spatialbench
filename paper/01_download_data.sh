#!/bin/bash
set -e
source config.env

echo "=== Step 1: Downloading Data (SF${SCALE_FACTOR}) ==="

# Install downloader prerequisite
pip install -q huggingface-hub

# Define directories
TARGET_DIR="benchmark-data-sf${SCALE_FACTOR}"
TEMP_HF_DIR="hf-data"

if [ -d "$TARGET_DIR" ] && [ "$(ls -A $TARGET_DIR)" ]; then
    echo "Data directory $TARGET_DIR already exists and is not empty. Skipping download."
    echo "To force redownload, delete the directory: rm -rf $TARGET_DIR"
else
    echo "Downloading data from HF: ${HF_DATASET}/${HF_DATA_VERSION}..."

    # Run the Python download logic inline
    python -c "
from huggingface_hub import snapshot_download
import os
import shutil

sf = '${SCALE_FACTOR}'
hf_sf = 'sf0.1' if sf == '0.1' else f'sf{sf}'
repo_id = '${HF_DATASET}'
version = '${HF_DATA_VERSION}'

print(f'Fetching {hf_sf}...')
snapshot_download(
    repo_id=repo_id,
    repo_type='dataset',
    local_dir='${TEMP_HF_DIR}',
    allow_patterns=[f'{version}/{hf_sf}/**'],
)
"

    # Organize data as the benchmark script expects it
    echo "Organizing data..."
    mkdir -p "$TARGET_DIR"

    # Determine source folder name logic based on YAML
    if [ "$SCALE_FACTOR" = "0.1" ]; then
        HF_SF="sf0.1"
    else
        HF_SF="sf${SCALE_FACTOR}"
    fi

    # Move files
    cp -r "${TEMP_HF_DIR}/${HF_DATA_VERSION}/${HF_SF}/"* "$TARGET_DIR/"

    # Cleanup temp
    rm -rf "$TEMP_HF_DIR"

    echo "Data ready in $TARGET_DIR"
    du -sh "$TARGET_DIR"
fi
