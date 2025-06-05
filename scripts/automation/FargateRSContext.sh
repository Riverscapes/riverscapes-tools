#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${TAGS?}")
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
      ██████╗ ███████╗   ██████╗ ██████╗ ███╗   ██╗████████╗███████╗██╗  ██╗████████╗  
      ██╔══██╗██╔════╝  ██╔════╝██╔═══██╗████╗  ██║╚══██╔══╝██╔════╝╚██╗██╔╝╚══██╔══╝  
      ██████╔╝███████╗  ██║     ██║   ██║██╔██╗ ██║   ██║   █████╗   ╚███╔╝    ██║     
      ██╔══██╗╚════██║  ██║     ██║   ██║██║╚██╗██║   ██║   ██╔══╝   ██╔██╗    ██║     
      ██║  ██║███████║  ╚██████╗╚██████╔╝██║ ╚████║   ██║   ███████╗██╔╝ ██╗   ██║     
      ╚═╝  ╚═╝╚══════╝   ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚═╝  ╚═╝   ╚═╝     
EOF

echo "HUC: $HUC"
echo "TAGS: $TAGS"
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
RS_CONTEXT_DIR=$DATA_DIR/rs_context/$HUC
RSCONTEXT_SCRATCH=$DATA_DIR/rs_context_scratch/$HUC

echo "DATA_DIR: $DATA_DIR"
echo "RS_CONTEXT_DIR: $RS_CONTEXT_DIR"
echo "RSCONTEXT_SCRATCH: $RSCONTEXT_SCRATCH"

echo "======================  Disk space usage ======================="
df -h

echo "======================  Starting RSContext ======================="
##########################################################################################
# First Run RS_Context
##########################################################################################
try() {
  rscontext $HUC \
    /efsshare/NationalDatasets/landfire/240/ \
    /efsshare/NationalDatasets/ownership/surface_management_agency.shp \
    /efsshare/NationalDatasets/ownership/FairMarketValue.tif \
    /efsshare/NationalDatasets/ecoregions/us_eco_l4_state_boundaries.shp \
    /efsshare/NationalDatasets/political_boundaries/cb_2021_us_state_500k.shp \
    /efsshare/NationalDatasets/political_boundaries/cb_2021_us_county_500k.shp \
    /efsshare/NationalDatasets/geology/SGMC_Geology.shp \
    /efsshare/download/prism \
    /efsshare/USACENationalDams/usace_national_dams.gpkg \
    /efsshare/NHDPlusV2/nhdplusv2.gpkg \
    $RS_CONTEXT_DIR \
    /efsshare/download \
    --parallel \
    --temp_folder $RSCONTEXT_SCRATCH \
    --meta "Runner=Cybercastor" \
    --verbose

  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/rscontext
  python3 -m rscontext.rs_context_metrics \
    $RS_CONTEXT_DIR

  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="
  # Upload the HUC into the warehouse
  cd $RS_CONTEXT_DIR

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

}
try || {
  echo "<<RS CONTEXT PROCESS COMPLETE WITH ERROR>>"
  exit 1
}
