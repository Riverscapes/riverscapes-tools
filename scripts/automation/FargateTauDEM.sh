#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'


# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${RSCONTEXT_ID?}")
(: "${CHANNELAREA_ID?}")
(: "${VISIBILITY?}")
(: "${RS_API_URL?}")
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

████████╗ █████╗ ██╗   ██╗██████╗ ███████╗███╗   ███╗
╚══██╔══╝██╔══██╗██║   ██║██╔══██╗██╔════╝████╗ ████║
   ██║   ███████║██║   ██║██║  ██║█████╗  ██╔████╔██║
   ██║   ██╔══██║██║   ██║██║  ██║██╔══╝  ██║╚██╔╝██║
   ██║   ██║  ██║╚██████╔╝██████╔╝███████╗██║ ╚═╝ ██║
   ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝     ╚═╝
EOF

echo "TAGS: $TAGS"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
echo "CHANNELAREA_ID: $CHANNELAREA_ID"
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
RS_CONTEXT_DIR=$DATA_DIR/rs_context/rs_context_$RSCONTEXT_ID
CHANNELAREA_DIR=$DATA_DIR/channel_area/channel_area_$CHANNELAREA_ID
TAUDEM_DIR=$DATA_DIR/output

##########################################################################################
# First Get RS_Context and Channel Area inputs
##########################################################################################

# TODO: Remove once RSCLI download is fixed
[[ -d $RS_CONTEXT_DIR ]] && rm -r $RS_CONTEXT_DIR
[[ -d $CHANNELAREA_DIR ]] && rm -r $CHANNELAREA_DIR

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(slope|hillshade|dem|hydrology|project_bounds.geojson)" \
  --no-input --no-ui --verbose

rscli download $CHANNELAREA_DIR --id $CHANNELAREA_ID \
  --file-filter "channel_area\.gpkg" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run TauDEM
##########################################################################################
try() {

taudem $HUC \
  $CHANNELAREA_DIR/outputs/channel_area.gpkg/channel_area \
  $RS_CONTEXT_DIR/topography/dem.tif \
  $TAUDEM_DIR \
  --meta "Runner=Cybercastor" \
  --verbose
if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/taudem
python3 -m taudem.taudem_rs \
  $TAUDEM_DIR/project.rs.xml \
  $RS_CONTEXT_DIR/project.rs.xml,$CHANNELAREA_DIR/project.rs.xml

echo "======================  Final Disk space usage ======================="
df -h

echo "======================  Upload to the warehouse ======================="

# Upload the HUC into the warehouse
cd $TAUDEM_DIR

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

echo "<<PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  echo "<<TAUDEM PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
