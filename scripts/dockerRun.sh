#!/bin/bash
set -eu
IFS=$'\n\t'

# This overly complicated setup retrieves the Account number into a variable
# This has no bearing on what actually gets uploaded
# ./scripts/dockerRun.sh /Users/matt/Work/data/bratDockerShare/
IMGNAME=cybercastor/rstools

docker run \
  --env-file .env.docker \
  --env SHELL_SCRIPT="$(<$2)" \
  --env RS_CONFIG="$(<~/.riverscapes)" \
  --mount type=bind,source=$1,target=/efsshare \
  --mount type=bind,source=$1,target=/task \
  --mount type=bind,source=$1,target=/usr/local/data \
  -it $IMGNAME:latest \
  /bin/bash | tee docker_run.log
