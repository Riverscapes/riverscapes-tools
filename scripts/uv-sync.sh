#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status
uv sync

# We always need to reinstall GDAL to match the system version because it isn't in the pyproject.toml
uv pip install GDAL==$(gdal-config --version)