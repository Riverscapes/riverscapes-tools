#!/bin/bash
set -eu
ORIGPWD=`pwd`

echo "Publishing $ORIGPWD/docs"
cd $ORIGPWD/docs
bundle install --jobs 4 --retry 3 --verbose

echo "Now looping over derivative sites..."
for docsfolder in $ORIGPWD/packages/*/docs ; do 
  echo "Publishing $docsfolder"
  if [ -d "$docsfolder" ]; then
    cd $docsfolder
    bundle install --jobs 4 --retry 3 --verbose
  fi; 
done
echo "Complete"
