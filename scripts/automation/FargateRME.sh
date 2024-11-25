#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${VBET_ID?}")
(: "${RSCONTEXT_ID?}")
(: "${CONFINEMENT_ID?}")
(: "${ANTHRO_ID?}")
(: "${RCAT_ID?}")
(: "${BRAT_ID?}")
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
      ___           ___           ___     
     /\  \         /\__\         /\  \    
    /::\  \       /::|  |       /::\  \   
   /:/\:\  \     /:|:|  |      /:/\:\  \  
  /::\~\:\  \   /:/|:|__|__   /::\~\:\  \ 
 /:/\:\ \:\__\ /:/ |::::\__\ /:/\:\ \:\__\
 \/_|::\/:/  / \/__/~~/:/  / \:\~\:\ \/__/
    |:|::/  /        /:/  /   \:\ \:\__\  
    |:|\/__/        /:/  /     \:\ \/__/  
    |:|  |         /:/  /       \:\__\    
     \|__|         \/__/         \/__/     
                                                                                                 
EOF

echo "TAGS: $TAGS"
echo "VBET_ID: $VBET_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
echo "CONFINEMENT_ID: $CONFINEMENT_ID"
echo "ANTHRO_ID: $ANTHRO_ID"
echo "RCAT_ID: $RCAT_ID"
# echo "BRAT_ID: $BRAT_ID"
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
VBET_DIR=$DATA_DIR/vbet/vbet_$VBET_ID
CONFINEMENT_DIR=$DATA_DIR/confinement/confinement_$CONFINEMENT_ID
ANTHRO_DIR=$DATA_DIR/anthro/anthro_$ANTHRO_ID
RCAT_DIR=$DATA_DIR/rcat/rcat_$RCAT_ID
BRAT_DIR=$DATA_DIR/brat/brat_$BRAT_ID
RME_DIR=$DATA_DIR/output/rme

cd /usr/local/src/riverscapes-tools
pip install -e packages/brat
cd /usr/local/src

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(nhdplushr\.gpkg|hydro_derivatives\.gpkg|dem.tif|dem_hillshade.tif|political_boundaries|project_bounds.geojson)" \
  --no-input --no-ui --verbose

rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "(vbet\.gpkg|intermediates\.gpkg)" \
  --no-input --no-ui --verbose

rscli download $CONFINEMENT_DIR --id $CONFINEMENT_ID \
  --file-filter "(confinement\.gpkg)" \
  --no-input --no-ui --verbose

rscli download $ANTHRO_DIR --id $ANTHRO_ID \
  --file-filter "(anthro\.gpkg)" \
  --no-input --no-ui --verbose

rscli download $RCAT_DIR --id $RCAT_ID \
  --file-filter "(rcat\.gpkg)" \
  --no-input --no-ui --verbose

rscli download $BRAT_DIR --id $BRAT_ID \
  --file-filter "(brat\.gpkg)" \
  --no-input --no-ui --verbose

echo "======================  Initial Disk space usage ======================="
df -h

##########################################################################################
# Now Run RME
##########################################################################################

echo "======================  Running RME ======================="
try() {

  rme $HUC \
    $RS_CONTEXT_DIR/hydrology/hydro_derivatives.gpkg/network_intersected \
    $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDPlusFlowlineVAA \
    $RS_CONTEXT_DIR/political_boundaries/counties.shp \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/vbet_dgos \
    $VBET_DIR/outputs/vbet.gpkg/vbet_igos \
    $VBET_DIR/outputs/vbet.gpkg/vbet_centerlines \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
    $RME_DIR \
    --confinement_dgos $CONFINEMENT_DIR/outputs/confinement.gpkg/confinement_dgos \
    --anthro_dgos $ANTHRO_DIR/outputs/anthro.gpkg/vwDgos \
    --rcat_dgos $RCAT_DIR/outputs/rcat.gpkg/vwDgos \
    --brat_dgos $BRAT_DIR/outputs/brat.gpkg/vwDgos \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Augmenting the RME project ======================="
  cd /usr/local/src/riverscapes-tools/packages/rme
  python3 -m rme.rme_rs \
    $RME_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml",$CONFINEMENT_DIR/project.rs.xml,$ANTHRO_DIR/project.rs.xml,$RCAT_DIR/project.rs.xml,$BRAT_DIR/project.rs.xml \
     # Add these back in when we want confinement and brat: $CONFINEMENT_DIR/project.rs.xml,$BRAT_DIR/project.rs.xml
  if [[ $? != 0 ]]; then return 1; fi

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
  echo "<<RME PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
