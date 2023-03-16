#!/bin/bash
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${TAGS?}")
(: "${VISIBILITY?}")

if [ -n "$USERID" ] && [ -n "$ORGID" ]; then
  echo "Error: Both USERID and ORGID environment variables are set"
  exit 1
fi

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
DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context/$HUC
RSCONTEXT_SCRATCH=$DATA_DIR/rs_context_scratch/$HUC

echo "======================  Disk space usage ======================="
df -h

echo "======================  Starting RSContext ======================="
##########################################################################################
# First Run RS_Context
##########################################################################################
try() {
  rscontext $HUC \
    /efsshare/NationalDatasets/landfire/220/ \
    /efsshare/NationalDatasets/ownership/surface_management_agency.shp \
    /efsshare/NationalDatasets/ownership/FairMarketValue.tif \
    /efsshare/NationalDatasets/ecoregions/us_eco_l4_state_boundaries.shp \
    /efsshare/NationalDatasets/political_boundaries/cb_2021_us_state_500k.shp \
    /efsshare/NationalDatasets/political_boundaries/cb_2021_us_county_500k.shp \
    /efsshare/NationalDatasets/geology/SGMC_geology.shp \
    /efsshare/download/prism \
    $RS_CONTEXT_DIR \
    /efsshare/download \
    --parallel \
    --temp_folder $RSCONTEXT_SCRATCH \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="
  # Upload the HUC into the warehouse
  cd $RS_CONTEXT_DIR

  if [ -n "$USER" ]; then
    rscli upload . --org $ORGID --tags "$TAGS" --visibility $VISIBILITY --no-input --no-ui --verbose 
  fi
  if [ -n "$ORG" ]; then
    rscli upload . --org $USERID --tags "$TAGS" --visibility $VISIBILITY --no-input --no-ui --verbose 
  fi

  if [[ $? != 0 ]]; then return 1; fi

}
try || {
  echo "<<PROCESS COMPLETE WITH ERROR>>"
  exit 1
}
