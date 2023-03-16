#!/bin/bash
set -eu
IFS=$'\n\t'

# This overly complicated setup retrieves the Account number into a variable
# This has no bearing on what actually gets uploaded
# ./scripts/dockerRun.sh /Users/matt/Work/data/cybercastor/
IMGNAME=cybercastor/riverscapestools

cd ../../

docker run \
  --env-file ./lib/cybercastor/dockerScripts/.env.test \
  --mount type=bind,source=$1,target=/efsshare \
  --mount type=bind,source=$1,target=/task \
  --mount type=bind,source=$1,target=/usr/local/data \
  --mount type=bind,source=$(pwd),target=/usr/local/src/riverscapes-tools \
  -it \
  --entrypoint /bin/bash \
  $IMGNAME:latest | tee docker_run.log

# After you load into the container you can run the following to test the script
# Just CD into /usr/local/src/riverscapes-tools/scripts/automation and run whatver you want