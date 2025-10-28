#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${NATIONAL_BLM_CONTEXT_ID?}")
(: "${RSCONTEXT_ID?}")
(: "${VBET_ID?}")
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

   ___    __             ___            _            _   
  / __\  / /   /\/\     / __\___  _ __ | |_ _____  _| |_ 
 /__\// / /   /    \   / /  / _ \| '_ \| __/ _ \ \/ / __|
/ \/  \/ /___/ /\/\ \ / /__| (_) | | | | ||  __/>  <| |_ 
\_____/\____/\/    \/ \____/\___/|_| |_|\__\___/_/\_\\__|
                                                        
EOF

echo "TAGS: $TAGS"
echo "NATIONAL_BLM_CONTEXT_ID: $NATIONAL_BLM_CONTEXT_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
echo "VBET_ID: $VBET_ID"
echo "RS_API_URL: $RS_API_URL"
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
NATIONAL_BLM_CONTEXT_DIR=/blm_context/national
RS_CONTEXT_DIR=$DATA_DIR/rs_context/rs_context_$RSCONTEXT_ID
VBET_DIR=$DATA_DIR/vbet/vbet_$VBET_ID
BLM_CONTEXT_DIR=$DATA_DIR/blm_context/output

cd /usr/local/src/riverscapes-tools
pip install -e packages/blm_context
cd /usr/local/src

##########################################################################################
# First Get the National BLM Context, RS_Context, and VBET inputs
##########################################################################################

# Get the RSCli of the projects we need to make this happen
rscli download $NATIONAL_BLM_CONTEXT_DIR --id $NATIONAL_BLM_CONTEXT_ID \
  --no-input --no-ui --verbose

rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hillshade|hydrology|project_bounds.geojson|rscontext_metrics.json)" \
  --no-input --no-ui --verbose
  
rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "(vbet.gpkg)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run BLM Context
##########################################################################################
try() {

  cd /usr/local/src/riverscapes-tools/packages/blm_context
  python3 -m blm_context.blm_context $HUC \
    $NATIONAL_BLM_CONTEXT_DIR \
    $RS_CONTEXT_DIR \
    $VBET_DIR \
    $BLM_CONTEXT_DIR \
    --meta "Runner=Cybercastor" \
    --verbose
    
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/blm_context
  python3 -m blm_context.blm_context_rs \
    $BLM_CONTEXT_DIR/project.rs.xml \
    $RS_CONTEXT_DIR/project.rs.xml,$NATIONAL_BLM_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml

  # python3 -m blm_context.blm_context_metrics \
  #   $VBET_DIR \
  #   $BLM_CONTEXT_DIR

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $BLM_CONTEXT_DIR

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

  echo "<<BLM CONTEXT PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  echo "<<BLM CONTEXT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
