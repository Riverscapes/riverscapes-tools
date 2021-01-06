#!/bin/bash
set -eu
ORIGPWD=`pwd`

echo "Publishing $ORIGPWD/docs"
cd $ORIGPWD/docs
bundle exec jekyll build --verbose

echo "Now looping over derivative sites..."
for docsfolder in $ORIGPWD/packages/*/docs ; do 
  echo "Publishing $docsfolder"
  if [ -d "$docsfolder" ]; then
    cd $docsfolder
    bundle exec jekyll build --verbose
  fi; 
done
echo "Complete"
