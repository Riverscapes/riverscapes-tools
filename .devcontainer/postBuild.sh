#!/bin/bash

# fix to use git internally thanks to error: "detected dubious ownership in repository"
git config --global --add safe.directory /workspaces/riverscapes-tools

# Link up rscommons so we can find it everywhere
pip install -e ./lib/commons

# Iterate over the tools and install them. This should catch anything the requirements.txt file missed
# and it al
for tooldir in packages/* ; do 
  # Install our packages as being editable
  pip install -e $tooldir
done

