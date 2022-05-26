#!/bin/bash
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")

echo "HUC: $HUC"

# Drop into our venv immediately
source /usr/local/venv/bin/activate

# Define some folders that we can easily clean up later
RSC_TASK_DIR=/data/rs_context/$HUC
VBET_TASK_DIR=/data/vbet/$HUC

##########################################################################################
# First Run RS_Context
##########################################################################################

rscontext $HUC \
  /efsshare/NationalDatasets/landfire/200/us_200evt.tif \
  /efsshare/NationalDatasets/landfire/200/us_200bps.tif \
  /efsshare/NationalDatasets/ownership/surface_management_agency.shp \
  /efsshare/NationalDatasets/ownership/FairMarketValue.tif \
  /efsshare/NationalDatasets/ecoregions/us_eco_l3_state_boundaries.shp \
  /efsshare/download/prism \
  $RSC_TASK_DIR \
  /efsshare/download \
  --verbose

echo "<<RS_CONTEXT COMPLETE>>"

##########################################################################################
# Now Run VBET
##########################################################################################

vbet $HUC \
  $RSC_TASK_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
  $RSC_TASK_DIR/hydrology/NHDArea.shp \
  $RSC_TASK_DIR/topography/slope.tif \
  $RSC_TASK_DIR/topography/hand.tif \
  $RSC_TASK_DIR/topography/dem_hillshade.tif \
  $VBET_TASK_DIR \
  --verbose

echo "<<VBET COMPLETE>>"
