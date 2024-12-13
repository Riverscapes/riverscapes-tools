#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${BRAT_ID?}")
(: "${BEAVERACTIVITY_ID?}")
(: "${RS_API_URL?}")
(: "${VISIBILITY?}")
# These are machine credentials for the API which will allow the CLI to delegate uploading to either a specific user or an org
(: "${RS_CLIENT_ID?}")
(: "${RS_CLIENT_SECRET?}")

# Turn off the set -u option once we've checked all the mandatory variables
set +u

if [ -z "$USER_ID" ] && [ -z "$ORG_ID" ]; then
  echo "Error: Neither USER_ID nor ORG_ID environment variables are set. You need one of them."
  exit 1
elif [ -n "$USER_ID" ] && [ -n "$ORG_ID" ]; then
  echo "Error: Both USER_ID and ORG_ID environment variables are set. Not a valid case."
  exit 1
fi

cat<<EOF
  ____  _____         _______  __      __     _      _____ _____       _______ _____ ____  _   _ 
 |  _ \|  __ \     /\|__   __| \ \    / /\   | |    |_   _|  __ \   /\|__   __|_   _/ __ \| \ | |
 | |_) | |__) |   /  \  | |     \ \  / /  \  | |      | | | |  | | /  \  | |    | || |  | |  \| |
 |  _ <|  _  /   / /\ \ | |      \ \/ / /\ \ | |      | | | |  | |/ /\ \ | |    | || |  | | . ` |
 | |_) | | \ \  / ____ \| |       \  / ____ \| |____ _| |_| |__| / ____ \| |   _| || |__| | |\  |
 |____/|_|  \_\/_/    \_\_|        \/_/    \_\______|_____|_____/_/    \_\_|  |_____\____/|_| \_|
                                                                                                                                                                                   
EOF

echo "TAGS: $TAGS"
echo "BRAT_ID: $BRAT_ID"
echo "BEAVERACTIVITY_ID: $BEAVERACTIVITY_ID"
echo "VISIBILITY: $VISIBILITY"
if [ -n "$USER_ID" ]; then
  echo "USER_ID: $USER_ID"
elif [ -n "$ORG_ID" ]; then
  echo "ORG_ID: $ORG_ID"
fi

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
BRAT_DIR=$DATA_DIR/brat/data
BEAVERACTIVITY_DIR=$DATA_DIR/beaver_activity/data

cd /usr/local/src/riverscapes-tools
pip install -e packages/brat
cd /usr/local/src

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $BRAT_DIR --id "$BRAT_ID" \
  --no-input --no-ui --verbose

rscli download $BEAVERACTIVITY_DIR --id "$BEAVERACTIVITY_ID"\
  --no-input --no-ui --verbose

echo "======================  Initial Disk space usage ======================="
df -h


try() {

  ##########################################################################################
  # Now Run BRAT Validation
  ##########################################################################################
  cd /usr/local/src/riverscapes-tools/packages/brat
  python3 -m sqlbrat.capacity_validation \
    $HUC \
    $BRAT_DIR/outputs/brat.gpkg \
    $BEAVERACTIVITY_DIR/outputs/beaver_activity.gpkg \
    --verbose

  cd /usr/local/src/riverscapes-tools/packages/brat
  python3 -m sqlbrat.brat_rs \
    $BRAT_DIR/project.rs.xml \
    $RS_CONTEXT_DIR/project.rs.xml,$HYDRO_DIR/project.rs.xml,$ANTHRO_DIR/project.rs.xml,$VBET_DIR/project.rs.xml

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $BRAT_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USER_ID" ]; then
    rscli upload . --user $USER_ID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORG_ID" ]; then
    rscli upload . --org $ORG_ID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  # Cleanup
  echo "<<PROCESS COMPLETE>>\n\n"

}
try || {
  # Emergency Cleanup
  echo "<<BRAT PROCESS ENDED WITH AN ERROR>>\n\n"
  exit 1
}
