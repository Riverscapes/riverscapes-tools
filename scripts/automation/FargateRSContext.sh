#!/bin/bash
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${TAGS?}")

echo "$RS_CONFIG" > /root/.riverscapes

cat<<EOF
      ██████╗ ███████╗   ██████╗ ██████╗ ███╗   ██╗████████╗███████╗██╗  ██╗████████╗  
      ██╔══██╗██╔════╝  ██╔════╝██╔═══██╗████╗  ██║╚══██╔══╝██╔════╝╚██╗██╔╝╚══██╔══╝  
      ██████╔╝███████╗  ██║     ██║   ██║██╔██╗ ██║   ██║   █████╗   ╚███╔╝    ██║     
      ██╔══██╗╚════██║  ██║     ██║   ██║██║╚██╗██║   ██║   ██╔══╝   ██╔██╗    ██║     
      ██║  ██║███████║  ╚██████╗╚██████╔╝██║ ╚████║   ██║   ███████╗██╔╝ ██╗   ██║     
      ╚═╝  ╚═╝╚══════╝   ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚═╝  ╚═╝   ╚═╝     
EOF

echo "HUC: $HUC"
echo "PROGRAM: $PROGRAM"
echo "TAGS: $TAGS"

# Drop into our venv immediately
source /usr/local/venv/bin/activate


echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
TASK_DIR=/usr/local/data/rs_context/$HUC
TASK_OUTPUT=$TASK_DIR/output
TASK_DOWNLOAD=$TASK_DIR/scratch

echo "======================  Disk space usage ======================="
df -h

echo "======================  Starting RSContext ======================="
##########################################################################################
# First Run RS_Context
##########################################################################################
try() {
  rscontext $HUC \
    /shared/NationalDatasets/landfire/200/us_200evt.tif \
    /shared/NationalDatasets/landfire/200/us_200bps.tif \
    /shared/NationalDatasets/ownership/surface_management_agency.shp \
    /shared/NationalDatasets/ownership/FairMarketValue.tif \
    /shared/NationalDatasets/ecoregions/us_eco_l3_state_boundaries.shp \
    /shared/download/prism \
    $TASK_OUTPUT \
    /shared/download/ \
    --parallel \
    --temp_folder $TASK_DOWNLOAD \
    --meta Runner=Cybercastor \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="
  # Upload the HUC into the warehouse
  cd $TASK_OUTPUT
  rscli upload . --replace --tags "$TAGS"  --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi
  
  # Cleanup
  cd /usr/local/
  rm -fr $TASK_DIR

}
try || {
  # Emerfency Cleanup
  cd /usr/local/
  rm -fr $TASK_DIR
  echo "<<PROCESS COMPLETE WITH ERROR>>"
  exit 1
}
