#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

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

if [ -z "$USERID" ] && [ -z "$ORGID" ]; then
  echo "Error: Neither USERID nor ORGID environment variables are set. You need one of them."
  exit 1
elif [ -n "$USERID" ] && [ -n "$ORGID" ]; then
  echo "Error: Both USERID and ORGID environment variables are set. Not a valid case."
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
if [-n "$USERID"]; then
  echo "USERID: $USERID"
elif [-n "$ORGID"]; then
  echo "ORGID: $ORGID"
fi

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context/data
VBET_DIR=$DATA_DIR/vbet/data
CONFINEMENT_DIR=$DATA_DIR/output

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hydrology\.gpkg|project_bounds.geojson)" \
  --no-input --no-ui --verbose

# Go get vbet result for this to work
rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "(vbet\.gpkg|vbet_inputs\.gpkg)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run Confinement
##########################################################################################

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  confinement $HUC \
    $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
    $VBET_DIR/inputs/vbet_inputs.gpkg/channel_area_polygons \
    $VBET_DIR/outputs/vbet.gpkg/vbet_full \
    $CONFINEMENT_DIR \
    vbet_level_path \
    ValleyBottom \
    --buffer 15.0 \
    --segmented_network $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/gnat
  /usr/local/venv/bin/python -m gnat.confinement_rs \
    $CONFINEMENT_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the confinement project into the warehouse.
  cd $CONFINEMENT_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
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
