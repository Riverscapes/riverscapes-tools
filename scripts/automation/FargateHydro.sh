#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")
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

__/\\\________/\\\_______________________/\\\______________________________        
 _\/\\\_______\/\\\______________________\/\\\______________________________       
  _\/\\\_______\/\\\____/\\\__/\\\________\/\\\______________________________      
   _\/\\\\\\\\\\\\\\\___\//\\\/\\\_________\/\\\___/\\/\\\\\\\______/\\\\\____     
    _\/\\\/////////\\\____\//\\\\\_____/\\\\\\\\\__\/\\\/////\\\___/\\\///\\\__    
     _\/\\\_______\/\\\_____\//\\\_____/\\\////\\\__\/\\\___\///___/\\\__\//\\\_   
      _\/\\\_______\/\\\__/\\_/\\\_____\/\\\__\/\\\__\/\\\_________\//\\\__/\\\__  
       _\/\\\_______\/\\\_\//\\\\/______\//\\\\\\\/\\_\/\\\__________\///\\\\\/___ 
        _\///________\///___\////_________\///////\//__\///_____________\/////_____
                                                                        
EOF

echo "TAGS: $TAGS"
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
RS_CONTEXT_DIR=$DATA_DIR/rs_context/rs_context_$RSCONTEXT_ID
VBET_DIR=$DATA_DIR/vbet/vbet_$VBET_ID
HYDRO_DIR=$DATA_DIR/output/hydro

##########################################################################################
# First Get RS_Context and VBET inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(dem|dem_hillshade|hydro_derivatives.gpkg|project_bounds.geojson)" \
  --no-input --no-ui --verbose
  
rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "(vbet.gpkg|vbet_intermediates.gpkg)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run Hydro
##########################################################################################
try() {

  cd /usr/local/src/riverscapes-tools/packages/hydro
  python3 -m hydro.hydro $HUC \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
    $VBET_DIR/outputs/vbet.gpkg/vbet_igos \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/vbet_dgos \
    $RS_CONTEXT_DIR/hydrology/hydro_derivatives.gpkg/network_segmented \
    $HYDRO_DIR \
    --meta "Runner=Cybercastor" \
    --verbose
    
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/hydro
  python3 -m hydro.hydro_rs \
    $HYDRO_DIR/project.rs.xml \
    $RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml

  python3 -m hydro.hydro_metrics \
    $HYDRO_DIR \
    $VBET_DIR

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $HYDRO_DIR

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
  echo "<<HYDRO PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
