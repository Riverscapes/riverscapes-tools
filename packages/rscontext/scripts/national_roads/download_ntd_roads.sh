#!/usr/bin/env bash
# Download all NTD transportation shapefiles from the USGS S3 bucket.
# Usage: ./download_ntd_roads.sh <output_dir>
# Requires: aws cli (no credentials needed — bucket is public)

aws s3 sync \
    s3://prd-tnm/StagedProducts/Tran/Shape/ \
    /Users/philipbailey/GISData/roads/ntd_download \
    --no-sign-request \
    --exclude "*" \
    --include "*.zip"
