#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# download confinement package - delete later when we have a docker image
pip3 install -e /usr/local/src/riverscapes-tools/packages/confinement

# These environment variables need to be present before the script starts
(: "${VBET_ID?}")
(: "${RSCONTEXT_ID?}")
(: "${TAGS?}")
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
    ______                     ______  __                                                    __      
   /      \                   /      \|  \                                                  |  \     
  |  ▓▓▓▓▓▓\ ______  _______ |  ▓▓▓▓▓▓\\▓▓_______   ______  ______ ____   ______  _______  _| ▓▓_    
  | ▓▓   \▓▓/      \|       \| ▓▓_  \▓▓  \       \ /      \|      \    \ /      \|       \|   ▓▓ \   
  | ▓▓     |  ▓▓▓▓▓▓\ ▓▓▓▓▓▓▓\ ▓▓ \   | ▓▓ ▓▓▓▓▓▓▓\  ▓▓▓▓▓▓\ ▓▓▓▓▓▓\▓▓▓▓\  ▓▓▓▓▓▓\ ▓▓▓▓▓▓▓\\▓▓▓▓▓▓   
  | ▓▓   __| ▓▓  | ▓▓ ▓▓  | ▓▓ ▓▓▓▓   | ▓▓ ▓▓  | ▓▓ ▓▓    ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓    ▓▓ ▓▓  | ▓▓ | ▓▓ __  
  | ▓▓__/  \ ▓▓__/ ▓▓ ▓▓  | ▓▓ ▓▓     | ▓▓ ▓▓  | ▓▓ ▓▓▓▓▓▓▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓▓▓▓▓▓▓ ▓▓  | ▓▓ | ▓▓|  \ 
   \▓▓    ▓▓\▓▓    ▓▓ ▓▓  | ▓▓ ▓▓     | ▓▓ ▓▓  | ▓▓\▓▓     \ ▓▓ | ▓▓ | ▓▓\▓▓     \ ▓▓  | ▓▓  \▓▓  ▓▓ 
    \▓▓▓▓▓▓  \▓▓▓▓▓▓ \▓▓   \▓▓\▓▓      \▓▓\▓▓   \▓▓ \▓▓▓▓▓▓▓\▓▓  \▓▓  \▓▓ \▓▓▓▓▓▓▓\▓▓   \▓▓   \▓▓▓▓  
                                                                                                     
EOF

echo "VBET_ID: $VBET_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
echo "TAGS: $TAGS"
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
VBET_DIR=$DATA_DIR/vbet/vbet_$VBET_ID
CONFINEMENT_DIR=$DATA_DIR/output/confinement

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hydro_derivatives.gpkg|project_bounds.geojson)" \
  --no-input --no-ui --verbose

# Go get vbet result for this to work
rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "(vbet\.gpkg|dem_hillshade.tif|vbet_intermediates\.gpkg|vbet_inputs\.gpkg)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run Confinement
##########################################################################################

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  confinement $HUC \
    $VBET_DIR/inputs/vbet_inputs.gpkg/flowlines_vaa \
    $VBET_DIR/inputs/vbet_inputs.gpkg/channel_area_polygons \
    $VBET_DIR/outputs/vbet.gpkg/vbet_full \
    $CONFINEMENT_DIR \
    $VBET_DIR/inputs/dem_hillshade.tif \
    vbet_level_path \
    ValleyBottom \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/vbet_dgos \
    $VBET_DIR/outputs/vbet.gpkg/vbet_igos \
    --buffer 15.0 \
    --segmented_network $RS_CONTEXT_DIR/hydrology/hydro_derivatives.gpkg/network_intersected_300m \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/confinement
  python3 -m confinement.confinement_rs \
    $CONFINEMENT_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the confinement project into the warehouse.
  cd $CONFINEMENT_DIR

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
  echo "<<CONFINEMENT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
