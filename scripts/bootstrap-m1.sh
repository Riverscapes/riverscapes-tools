#! /bin/bash
set -eu

# On OSX you must have run `brew install gdal` so that the header files are findable 
python3 --version
python3 -m venv .venv
# Make sure pip is at a good version
.venv/bin/python -m pip install --upgrade pip

# Make sure these versions match what you have in `brew info numpy` etc
.venv/bin/pip --timeout=120 install \
  Cython==0.29.32 \
  numpy==1.23.5

# Need numpy before GDAL
.venv/bin/pip install GDAL==$(gdal-config --version)

# Now install everything else
.venv/bin/pip --timeout=120 install -r requirements.m1.txt

# On M1 shapely dies for no reason. This can be removed when it is fixed
# https://github.com/Riverscapes/riverscapes-tools/issues/628
.venv/bin/pip install --force-reinstall git+https://github.com/shapely/shapely@maint-1.8

# Link up rscommons so we can find it in other places on our machine
.venv/bin/pip install -e ./lib/commons

# Iterate the string array using for loop
ORIGPWD=`pwd`
for tooldir in packages/* ; do 
  # Install our packages as being editable
  .venv/bin/pip install -e $tooldir
  cd $tooldir
  ln -sf ../../.venv
  cd $ORIGPWD
done
