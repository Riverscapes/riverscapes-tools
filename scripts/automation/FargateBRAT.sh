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
      ██████╗ ██████╗  █████╗ ████████╗  
      ██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝  
      ██████╔╝██████╔╝███████║   ██║     
      ██╔══██╗██╔══██╗██╔══██║   ██║     
      ██████╔╝██║  ██║██║  ██║   ██║     
      ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝     
EOF

echo "TAGS: $TAGS"
echo "VBET_ID: $VBET_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
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
BRAT_DIR=$DATA_DIR/output

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id "$RSCONTEXT_ID" \
  --file-filter "(dem_hillshade|slope|dem|hydrology|existing_veg|historic_veg|transportation|ownership|project_bounds.geojson)" \
  --no-input --no-ui --verbose

# Go get vbet result for this to work
rscli download $VBET_DIR --id "$VBET_ID"\
  --file-filter "vbet\.gpkg" \
  --no-input --no-ui --verbose

echo "======================  Initial Disk space usage ======================="
df -h


try() {

  ##########################################################################################
  # Now Run BRAT Build
  ##########################################################################################
  bratbuild $HUC \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/topography/slope.tif \
    $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
    $RS_CONTEXT_DIR/hydrology/hydro_derivatives.gpkg/network_intersected \
    $RS_CONTEXT_DIR/vegetation/existing_veg.tif \
    $RS_CONTEXT_DIR/vegetation/historic_veg.tif \
    $VBET_DIR/outputs/vbet.gpkg/vbet_full \
    $RS_CONTEXT_DIR/transportation/roads.shp \
    $RS_CONTEXT_DIR/transportation/railways.shp \
    $RS_CONTEXT_DIR/transportation/canals.shp \
    $RS_CONTEXT_DIR/ownership/ownership.shp \
    30 \
    100 \
    100 \
    $BRAT_DIR \
    --reach_codes 33400,33600,33601,33603,46000,46003,46006,46007 \
    --canal_codes 33600,33601,33603 \
    --peren_codes 46006,55800,33400 \
    --flow_areas $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDArea \
    --waterbodies $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDWaterbody \
    --max_waterbody 0.001 \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  # # Upload the HUC into the warehouse. This is useful
  # # Since BRAT RUn might fail
  # cd $BRAT_DIR

  # # If this is a user upload then we need to use the user's id
  # if [ -n "$USER_ID" ]; then
  #   rscli upload . --user $USER_ID \
  #       --tags "$TAGS" \
  #       --visibility $VISIBILITY \
  #       --no-input --no-ui --verbose
  # # If this is an org upload, we need to specify the org ID
  # elif [ -n "$ORG_ID" ]; then
  #   rscli upload . --org $ORG_ID \
  #       --tags "$TAGS" \
  #       --visibility $VISIBILITY \
  #       --no-input --no-ui --verbose
  # else
  #   echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
  #   exit 1
  # fi

  # if [[ $? != 0 ]]; then return 1; fi
  
  ##########################################################################################
  # Now Run BRAT Run
  ##########################################################################################
  bratrun $BRAT_DIR --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/brat
  python3 -m sqlbrat.brat_rs \
    $BRAT_DIR/project.rs.xml \
    $RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml

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
