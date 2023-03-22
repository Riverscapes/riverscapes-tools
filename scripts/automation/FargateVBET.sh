#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'
# Set -x will echo every command to the console
set -x

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${RSCONTEXT_ID?}")
(: "${CHANNELAREA_ID?}")
(: "${TAUDEM_ID?}")


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

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context/data
CHANNEL_AREA_DIR=$DATA_DIR/channel_area/data
TAUDEM_DIR=$DATA_DIR/taudem/data
VBET_DIR=$DATA_DIR/output

##########################################################################################
# First Get RS_Context, ChannelArea and Taudem inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hillshade|slope|dem|hydrology|project_bounds.geojson)" \
  --no-input --no-ui --verbose
  
rscli download $CHANNEL_AREA_DIR --id $CHANNELAREA_ID \
  --no-input --no-ui --verbose

rscli download $TAUDEM_DIR --id $TAUDEM_ID \
  --file-filter "(twi.tif|pitfill.tif|dinfflowdir_ang.tif|dinfflowdir_slp.tif)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run VBET
##########################################################################################
try() {

  vbet $HUC \
    "APRIL_2022" \
    $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/topography/slope.tif \
    $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
    $RS_CONTEXT_DIR/hydrology/NHDPlusCatchment.shp \
    $CHANNEL_AREA_DIR/outputs/channel_area.gpkg/channel_area \
    $RS_CONTEXT_DIR/hydrology/nhd_data.sqlite/NHDPlusFlowlineVAA \
    $VBET_DIR \
    --pitfill $TAUDEM_DIR/intermediates/pitfill.tif \
    --dinfflowdir_ang $TAUDEM_DIR/intermediates/dinfflowdir_ang.tif \
    --dinfflowdir_slp $TAUDEM_DIR/outputs/dinfflowdir_slp.tif \
    --twi_raster $TAUDEM_DIR/outputs/twi.tif \
    --reach_codes 33400,46000,46003,46006,46007,55800 \
    --mask $RS_CONTEXT_DIR/hydrology/WBDHU8.shp \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/vbet
  /usr/local/venv/bin/python -m vbet.vbet_rs \
    $VBET_DIR/project.rs.xml \
    $RS_CONTEXT_DIR/project.rs.xml,$TAUDEM_DIR/project.rs.xml,$CHANNEL_AREA_DIR/project.rs.xml

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $VBET_DIR

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
  # Emergency Cleanup
  echo "<<RS CONTEXT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
