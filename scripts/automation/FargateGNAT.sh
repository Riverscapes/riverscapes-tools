#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'
# Set -x will echo every command to the console
set -x

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${VBET_ID?}")
(: "${RSCONTEXT_ID?}")
(: "${VISIBILITY?}")
(: "${APIURL?}")
# These are machine credentials for the API which will allow the CLI to delegate uploading to either a specific user or an org
(: "${MACHINE_CLIENT?}")
(: "${MACHINE_SECRET?}")

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
_____________   ________________
__  ____/__  | / /__    |__  __/
_  / __ __   |/ /__  /| |_  /   
/ /_/ / _  /|  / _  ___ |  /    
\____/  /_/ |_/  /_/  |_/_/     
                                                                                                 
EOF

echo "TAGS: $TAGS"
echo "VBET_ID: $VBET_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context/data
VBET_DIR=$DATA_DIR/vbet/data
GNAT_DIR=$DATA_DIR/output

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
# Now Run GNAT
##########################################################################################

try() {

  gnat $HUC \
    $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network \
    $RS_CONTEXT_DIR/hydrology/nhd_data.sqlite/NHDPlusFlowlineVAA \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/vbet_dgos \
    $VBET_DIR/outputs/vbet.gpkg/segmentation_points \
    $VBET_DIR/outputs/vbet.gpkg/vbet_centerlines \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/climate/precipitation.tif \
    $RS_CONTEXT_DIR/transportation/roads.shp \
    $RS_CONTEXT_DIR/transportation/railways.shp \
    $RS_CONTEXT_DIR/ecoregions/ecoregions.shp \
    $GNAT_DIR \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/gnat
  /usr/local/venv/bin/python -m gnat.gnat_rs \
    $GNAT_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $GNAT_DIR
  
  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose \
        --api-url $APIURL \
        --client-id $MACHINE_CLIENT \
        --client-secret $MACHINE_SECRET
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose \
        --api-url $APIURL \
        --client-id $MACHINE_CLIENT \
        --client-secret $MACHINE_SECRET
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  echo "<<PROCESS COMPLETE>>"

}
try || {
  echo "<<GNAT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
