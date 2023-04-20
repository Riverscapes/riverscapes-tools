#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${VBET_ID?}")
(: "${RSCONTEXT_ID?}")
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
________ _____                                                                                 
___  __ \___(_)___   _______ _____________________________ _________ _____ ________            
__  /_/ /__  / __ | / /_  _ \__  ___/__  ___/_  ___/_  __ `/___  __ \_  _ \__  ___/            
_  _, _/ _  /  __ |/ / /  __/_  /    _(__  ) / /__  / /_/ / __  /_/ //  __/_(__  )             
/_/ |_|  /_/   _____/  \___/ /_/     /____/  \___/  \__,_/  _  .___/ \___/ /____/              
                                                            /_/                                
______  ___      _____         _____            __________                 _____               
___   |/  /_____ __  /____________(_)_______    ___  ____/_______ _______ ____(_)_______ _____ 
__  /|_/ / _  _ \_  __/__  ___/__  / _  ___/    __  __/   __  __ \__  __ `/__  / __  __ \_  _ \
_  /  / /  /  __// /_  _  /    _  /  / /__      _  /___   _  / / /_  /_/ / _  /  _  / / //  __/
/_/  /_/   \___/ \__/  /_/     /_/   \___/      /_____/   /_/ /_/ _\__, /  /_/   /_/ /_/ \___/ 
                                                                  /____/                                       
EOF

echo "TAGS: $TAGS"
echo "VBET_ID: $VBET_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
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
RS_CONTEXT_DIR=$DATA_DIR/rs_context/data
VBET_DIR=$DATA_DIR/vbet/data
RME_DIR=$DATA_DIR/output

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hydrology\.gpkg|nhd_data.sqlite|dem.tif|precipitation.tif|ecoregions|transportation|project_bounds.geojson)" \
  --no-input --no-ui --verbose

rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "(vbet\.gpkg|intermediates\.gpkg)" \
  --no-input --no-ui --verbose

echo "======================  Initial Disk space usage ======================="
df -h

##########################################################################################
# Now Run Riverscapes Metric Engine
##########################################################################################

try() {

  gnat $HUC \
    $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network \
    $RS_CONTEXT_DIR/hydrology/nhd_data.sqlite/NHDPlusFlowlineVAA \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/vbet_dgos \
    $VBET_DIR/outputs/vbet.gpkg/vbet_igos \
    $VBET_DIR/outputs/vbet.gpkg/vbet_centerlines \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/climate/precipitation.tif \
    $RS_CONTEXT_DIR/transportation/roads.shp \
    $RS_CONTEXT_DIR/transportation/railways.shp \
    $RS_CONTEXT_DIR/ecoregions/ecoregions.shp \
    $RME_DIR \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/rme
  /usr/local/venv/bin/python -m rme.rme_rs \
    $RME_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $RME_DIR
  
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
  echo "<<METRIC ENGINE PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
