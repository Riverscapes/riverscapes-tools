#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${TAGS?}")
(: "${VISIBILITY?}")
(: "${RS_API_URL?}")
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
      ██████╗ ███████╗ ██████╗ ██████╗ ███╗   ██╗████████╗███████╗██╗  ██╗████████╗  
      ██╔══██╗██╔════╝██╔════╝██╔═══██╗████╗  ██║╚══██╔══╝██╔════╝╚██╗██╔╝╚══██╔══╝  
      ██████╔╝███████╗██║     ██║   ██║██╔██╗ ██║   ██║   █████╗   ╚███╔╝    ██║     
      ██╔══██╗╚════██║██║     ██║   ██║██║╚██╗██║   ██║   ██╔══╝   ██╔██╗    ██║     
      ██║  ██║███████║╚██████╗╚██████╔╝██║ ╚████║   ██║   ███████╗██╔╝ ██╗   ██║     
      ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚═╝  ╚═╝   ╚═╝     


      ▄█▄     ▄  █ ██      ▄      ▄   ▄███▄   █         ██   █▄▄▄▄ ▄███▄   ██   
      █▀ ▀▄  █   █ █ █      █      █  █▀   ▀  █         █ █  █  ▄▀ █▀   ▀  █ █  
      █   ▀  ██▀▀█ █▄▄█ ██   █ ██   █ ██▄▄    █         █▄▄█ █▀▀▌  ██▄▄    █▄▄█ 
      █▄  ▄▀ █   █ █  █ █ █  █ █ █  █ █▄   ▄▀ ███▄      █  █ █  █  █▄   ▄▀ █  █ 
      ▀███▀     █     █ █  █ █ █  █ █ ▀███▀       ▀        █   █   ▀███▀      █ 
              ▀     █  █   ██ █   ██                     █   ▀              █  
                    ▀                                    ▀                  ▀   

                ████████╗ █████╗ ██╗   ██╗██████╗ ███████╗███╗   ███╗
                ╚══██╔══╝██╔══██╗██║   ██║██╔══██╗██╔════╝████╗ ████║
                  ██║   ███████║██║   ██║██║  ██║█████╗  ██╔████╔██║
                  ██║   ██╔══██║██║   ██║██║  ██║██╔══╝  ██║╚██╔╝██║
                  ██║   ██║  ██║╚██████╔╝██████╔╝███████╗██║ ╚═╝ ██║
                  ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝     ╚═╝

EOF

echo "HUC: $HUC"
echo "TAGS: $TAGS"
echo "RS_API_URL: $RS_API_URL"
echo "VISIBILITY: $VISIBILITY"
if [-n "$USER_ID"]; then
  echo "USER_ID: $USER_ID"
elif [-n "$ORG_ID"]; then
  echo "ORG_ID: $ORG_ID"
fi

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data

RSCONTEXT_DIR=$DATA_DIR/rs_context/$HUC
RSCONTEXT_SCRATCH=$DATA_DIR/rs_context_scratch/$HUC
CHANNELAREA_DIR=$DATA_DIR/channel_area/$HUC
TAUDEM_DIR=$DATA_DIR/taudem/$HUC


##########################################################################################
# First Run RS_Context
##########################################################################################

try() {

rscontext $HUC \
  /efsshare/NationalDatasets/landfire/200/us_200evt.tif \
  /efsshare/NationalDatasets/landfire/200/us_200bps.tif \
  /efsshare/NationalDatasets/ownership/surface_management_agency.shp \
  /efsshare/NationalDatasets/ownership/FairMarketValue.tif \
  /efsshare/NationalDatasets/ecoregions/us_eco_l3_state_boundaries.shp \
  /efsshare/download/prism \
  $RSCONTEXT_DIR \
  /efsshare/download \
  --parallel \
  --temp_folder $RSCONTEXT_SCRATCH \
  --meta "Runner=Cybercastor" \
  --verbose

if [[ $? != 0 ]]; then return 1; fi
echo "<<RS_CONTEXT COMPLETE>>"

# Upload the HUC into the warehouse
cd $RSCONTEXT_DIR
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
echo "<<RS_CONTEXT UPLOAD COMPLETE>>"

# We need to conserve some space for the next run
rm -fr $RSCONTEXT_DIR/vegetation
rm -fr $RSCONTEXT_DIR/transportation
rm -fr $RSCONTEXT_SCRATCH

##########################################################################################
# Now Run Channel Area Tool
##########################################################################################

channel $HUC \
  $RSCONTEXT_DIR/hydrology/NHDFlowline.shp \
  $CHANNELAREA_DIR \
  --flowareas $RSCONTEXT_DIR/hydrology/NHDArea.shp \
  --waterbodies $RSCONTEXT_DIR/hydrology/NHDWaterbody.shp \
  --bankfull_function "0.177 * (a ** 0.397) * (p ** 0.453)" \
  --bankfull_function_params "a=TotDASqKm" \
  --reach_code_field FCode \
  --flowline_reach_codes "33400,46000,46003,46006,46007", \
  --flowarea_reach_codes "53700,46100,48400,31800,34300,34305,34306,4600,46003,46006,46007", \
  --waterbody_reach_codes "49300,3900,39001,39004,39005,39006,39009,39010,39011,39012,43600,43601,43603,43604,43605,43606,43607,43608,43609,43610,43611,43612,43613,43614,43615,43618,43619,43621,43623,43624,43625,43626,46600,46601,46602", \
  --prism_data $RSCONTEXT_DIR/climate/precipitation.tif \
  --huc8boundary $RSCONTEXT_DIR/hydrology/WBDHU8.shp \
  --meta "Runner=Cybercastor" \
  --verbose

if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/channel
/usr/local/venv/bin/python -m channel.channel_rs \
  $CHANNELAREA_DIR/project.rs.xml \
  $RSCONTEXT_DIR/project.rs.xml

# Upload the HUC into the warehouse
cd $CHANNELAREA_DIR

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

echo "<<Channel Area COMPLETE>>"

##########################################################################################
# Now Run TauDEM
##########################################################################################

taudem $HUC \
  $CHANNELAREA_DIR/outputs/channel_area.gpkg/channel_area \
  $RSCONTEXT_DIR/topography/dem.tif \
  $TAUDEM_DIR \
  --meta "Runner=Cybercastor" \
  --verbose
if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/taudem
/usr/local/venv/bin/python -m taudem.taudem_rs \
  $TAUDEM_DIR/project.rs.xml \
  $RSCONTEXT_DIR/project.rs.xml,$CHANNEL_DIR/project.rs.xml

# Upload the HUC into the warehouse
cd $TAUDEM_DIR

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

echo "<<TauDEM COMPLETE>>"

echo "======================  Final Disk space usage ======================="
df -h

echo "<<PROCESS COMPLETE>>"

}
try || {
  echo "<<PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
