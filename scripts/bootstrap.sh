#! /bin/bash
set -eu

# On OSX you must have run `brew install gdal` so that the header files are findable 
python3 -m virtualenv .venv
# Make sure pip is at a good version
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip --timeout=120 install \
  Cython==0.29.7 \
  numpy==1.16.3 \
  shapely==1.7.1 \
  scipy==1.5.1 \
  --no-binary shapely

# Need numpy before GDAL
.venv/bin/pip install \
  GDAL==$(gdal-config --version) \
  --global-option=build_ext \
  --global-option="-I/usr/include/gdal"


# Now install everything else
.venv/bin/pip --timeout=120 install -r requirements.txt

# Install our packages as being editable
.venv/bin/pip install -e ./lib/commons
.venv/bin/pip install -e ./packages/rscontext
.venv/bin/pip install -e ./packages/vbet
.venv/bin/pip install -e ./packages/brat