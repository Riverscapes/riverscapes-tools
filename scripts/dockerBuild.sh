#!/bin/bash
set -eu
IFS=$'\n\t'

# This overly complicated setup retrieves the Account number into a variable
# This has no bearing on what actually gets uploaded
IMGNAME=riverscapes/riverscapes-tools

# Build us a fresh copy of the docker image. If there's nothing to update then this will be short
# Otherwise it will be long
docker build \
  --build-arg CACHEBREAKER="$(date)" \
  -t $IMGNAME . | tee docker_build.log
