#!/bin/bash

# fix to use git internally thanks to error: "detected dubious ownership in repository"
git config --global --add safe.directory /workspaces/riverscapes-tools

uv sync
# We need to install whatever of GDAL matches the system version
uv pip install gdal==$(gdal-config --version)
