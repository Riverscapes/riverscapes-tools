#!/bin/bash
set -eu

# Link up any of our AWS config or our .riverscapes file with the root
# (If they exist)
declare -a CONFPATHS=(".riverscapes" ".aws")

## now loop through the above array
for CPATH in "${CONFPATHS[@]}"
do
  if test -f "/homecopy/$CPATH"; then
      ln -s /homecopy/$CPATH ~/$CPATH
  fi
  if test -d "/homecopy/$CPATH"; then
      ln -s /homecopy/$CPATH ~/$CPATH
  fi  
done



# On OSX you must have run `brew install gdal` so that the header files are findable 
# Make sure pip is at a good version
python3 -m pip install --upgrade pip
pip --timeout=120 install \
  Cython==0.29.23 \
  numpy>=1.21.0 \
  scipy>=1.5.1

# Need numpy before GDAL
pip install GDAL==$(gdal-config --version)

# Now install everything else
pip --timeout=120 install -r requirements.txt

# Install our packages as being editable
pip install -e ./lib/commons

# Install each of our tools and make them executable
for d in ./packages/* ; do
    if [ -d "$d" ]; then
      pip install -e $d
    fi
done
