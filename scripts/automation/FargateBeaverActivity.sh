#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${VBET_ID?}")
(: "${QRIS_ID?}")
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
    
 ____  ____    __  _  _  ____  ____      __    ___  ____  ____  _  _  ____  ____  _  _ 
(  _ \( ___)  /__\( \/ )( ___)(  _ \    /__\  / __)(_  _)(_  _)( \/ )(_  _)(_  _)( \/ )
 ) _ < )__)  /(__)\\  /  )__)  )   /   /(__)\( (__   )(   _)(_  \  /  _)(_   )(   \  / 
(____/(____)(__)(__)\/  (____)(_)\_)  (__)(__)\___) (__) (____)  \/  (____) (__)  (__) 
          
EOF

echo "TAGS: $TAGS"
echo "VBET_ID: $VBET_ID"
echo "QRIS_ID: $QRIS_ID"
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
VBET_DIR=$DATA_DIR/vbet/data
QRIS_DIR=$DATA_DIR/qris/data
BEAVER_ACTIVITY_DIR=$DATA_DIR/output

cd /usr/local/src/riverscapes-tools
pip install -e packages/brat
cd /usr/local/src

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $VBET_DIR --id "$VBET_ID"\
  --file-filter "(vbet\.gpkg|vbet_intermediates\.gpkg)" \
  --no-input --no-ui --verbose

rscli download $QRIS_DIR --id "$QRIS_ID" \
  --no-input --no-ui --verbose

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  cd $QRIS_DIR
  ls
  cd context
  ls

  ##########################################################################################
  # Now Run Beaver Activity
  ##########################################################################################
  beaver_activity $HUC \ 
    $QRIS_DIR/context/feature_classes.gpkg/WBDHU10 \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/vbet_dgos \
    $VBET_DIR/outputs/vbet.gpkg/vbet_igos \
    $QRIS_DIR \
    $BEAVER_ACTIVITY_DIR \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/brat

  python3 -m beaver_sign.beaver_act_rs \
    $BEAVER_ACTIVITY_DIR/project.rs.xml\
    $VBET_DIR/project.rs.xml,$QRIS_DIR/project.rs.xml

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $BEAVER_ACTIVITY_DIR

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
  echo "<<BEAVER ACTIVITY PROCESS ENDED WITH AN ERROR>>\n\n"
  exit 1
}
