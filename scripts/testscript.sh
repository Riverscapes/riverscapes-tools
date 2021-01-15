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
  /shared/NationalDatasets/landfire/200/us_200evt.tif \
  /shared/NationalDatasets/landfire/200/us_200bps.tif \
  /shared/NationalDatasets/ownership/surface_management_agency.shp \
  /shared/NationalDatasets/ownership/FairMarketValue.tif \
  /shared/NationalDatasets/ecoregions/us_eco_l3_state_boundaries.shp \
  /shared/download/prism \
  $RSC_TASK_DIR \
  /shared/download/ \
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
