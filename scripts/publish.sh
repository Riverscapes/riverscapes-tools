#!/bin/bash
set -eu
ORIGPWD=`pwd`

function jekyllbuild () {
  cd $1
  bundle install
  bundle exec jekyll build
}

jekyllbuild $ORIGPWD/docs

for file in $ORIGPWD/packages/*/docs ; do 
  if [[ -d "$file" && ! -L "$file" ]]; then
    jekyllbuild $file
  fi; 
done

cd $ORIGPWD
git checkout gh-pages
rm -fr site
mv PUBLIC site
git add site
git commit -m "Publish"
git push