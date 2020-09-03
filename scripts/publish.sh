#!/bin/bash
set -eu
ORIGPWD=`pwd`

function jekyllbuild () {
  cd $1
  bundle install
  bundle exec jekyll build
}

jekyllbuild $ORIGPWD/docs
echo "HERE: $ORIGPWD, $ORIGPWD/_site"
mv $ORIGPWD/docs/_site $ORIGPWD/


for file in $ORIGPWD/packages/*/docs ; do 
  if [[ -d "$file" && ! -L "$file" ]]; then
    echo "$file is a directory"; 
  fi; 
done

# # Now for each known site build it and move it
# for i in "${docfolders[@]}"
# do
#    :
#    SITE=$PWD/packages/$i/docs
#    jekyllbuild $SITE
#    mv $SITE/_site $PWD/_site/$i
#    # do whatever on $i
# done