#! /bin/bash
set -eu

# On OSX you must have run `brew install gdal` so that the header files are findable 
python3 --version
python3 -m venv .venv
# Make sure pip is at a good version
.venv/bin/python3 -m ensurepip --upgrade
.venv/bin/pip3 install --upgrade pip
.venv/bin/pip3 install --upgrade setuptools

# Cython and numpy need to go in before an explicit GDAL install
.venv/bin/pip3 --timeout=120 install \
  Cython==3.0.8 \
  setuptools

# Need numpy before GDAL
.venv/bin/pip3 install GDAL==$(gdal-config --version)

# Now install everything else
.venv/bin/pip3 --timeout=120 install -r requirements.txt

# Install our packages as being editable
.venv/bin/pip3 install -e ./lib/commons
.venv/bin/pip3 install -e ./packages/rscontext
.venv/bin/pip3 install -e ./packages/vbet
.venv/bin/pip3 install -e ./packages/brat
.venv/bin/pip3 install -e ./packages/rme
.venv/bin/pip3 install -e ./packages/hand
.venv/bin/pip3 install -e ./packages/rcat
.venv/bin/pip3 install -e ./packages/confinement
.venv/bin/pip3 install -e ./packages/hydro