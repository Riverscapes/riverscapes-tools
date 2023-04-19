#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${RSCONTEXT_ID?}")
(: "${CHANNELAREA_ID?}")
(: "${TAUDEM_ID?}")
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

    ▄   ███   ▄███▄     ▄▄▄▄▀   
     █  █  █  █▀   ▀ ▀▀▀ █      
█     █ █ ▀ ▄ ██▄▄       █      
 █    █ █  ▄▀ █▄   ▄▀   █       
  █  █  ███   ▀███▀    ▀        
   █▐                           
   ▐                            

EOF

echo "TAGS: $TAGS"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
echo "CHANNELAREA_ID: $CHANNELAREA_ID"
echo "TAUDEM_ID: $TAUDEM_ID"
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
TAUDEM_DIR=$DATA_DIR/taudem/taudem_$TAUDEM_ID
VBET_DIR=$DATA_DIR/output/vbet
VBET_TEMP=$DATA_DIR/vbet_temp

##########################################################################################
# First Get RS_Context, ChannelArea and Taudem inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hillshade|slope|dem|hydrology|project_bounds.geojson)" \
  --no-input --no-ui --verbose
  
rscli download $CHANNELAREA_DIR --id $CHANNELAREA_ID \
  --no-input --no-ui --verbose

rscli download $TAUDEM_DIR --id $TAUDEM_ID \
  --file-filter "(twi.tif|pitfill.tif|dinfflowdir_ang.tif|dinfflowdir_slp.tif)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run VBET
##########################################################################################
try() {

  vbet $HUC \
    $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDFlowline \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/topography/slope.tif \
    $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
    $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDPlusCatchment \
    $CHANNELAREA_DIR/outputs/channel_area.gpkg/channel_area \
    $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDPlusFlowlineVAA \
    $VBET_DIR \
    --pitfill $TAUDEM_DIR/intermediates/pitfill.tif \
    --dinfflowdir_ang $TAUDEM_DIR/intermediates/dinfflowdir_ang.tif \
    --dinfflowdir_slp $TAUDEM_DIR/outputs/dinfflowdir_slp.tif \
    --twi_raster $TAUDEM_DIR/outputs/twi.tif \
    --reach_codes 33400,46000,46003,46006,46007,55800 \
    --mask $RS_CONTEXT_DIR/hydrology/hydro_derivatives.gpkg/processing_extent \
    --meta "Runner=Cybercastor" \
    --verbose \
    --temp_folder $VBET_TEMP
    
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/vbet
  python3 -m vbet.vbet_rs \
    $VBET_DIR/project.rs.xml \
    $RS_CONTEXT_DIR/project.rs.xml,$TAUDEM_DIR/project.rs.xml,$CHANNELAREA_DIR/project.rs.xml

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $VBET_DIR

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
  echo "<<RS CONTEXT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
