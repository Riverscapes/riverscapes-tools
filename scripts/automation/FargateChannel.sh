#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'
# set -x

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${RSCONTEXT_ID?}")
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
▄█▄     ▄  █ ██      ▄      ▄   ▄███▄   █         ██   █▄▄▄▄ ▄███▄   ██   
█▀ ▀▄  █   █ █ █      █      █  █▀   ▀  █         █ █  █  ▄▀ █▀   ▀  █ █  
█   ▀  ██▀▀█ █▄▄█ ██   █ ██   █ ██▄▄    █         █▄▄█ █▀▀▌  ██▄▄    █▄▄█ 
█▄  ▄▀ █   █ █  █ █ █  █ █ █  █ █▄   ▄▀ ███▄      █  █ █  █  █▄   ▄▀ █  █ 
▀███▀     █     █ █  █ █ █  █ █ ▀███▀       ▀        █   █   ▀███▀      █ 
         ▀     █  █   ██ █   ██                     █   ▀              █  
              ▀                                    ▀                  ▀   
EOF

echo "TAGS: $TAGS"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
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
CHANNELAREA_DIR=$DATA_DIR/output/channelarea

##########################################################################################
# First Get RS_Context inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hillshade|slope|dem|climate|hydrology|project_bounds.geojson)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run Channel Area Tool
##########################################################################################
try() {

channel $HUC \
  $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDFlowline \
  $CHANNELAREA_DIR \
  --flowareas $RS_CONTEXT_DIR/hydrology/hydro_derivatives.gpkg/NHDAreaSplit \
  --waterbodies $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDWaterbody \
  --bankfull_function "0.177 * (a ** 0.397) * (p ** 0.453)" \
  --bankfull_function_params "a=TotDASqKm" \
  --reach_code_field FCode \
  --flowline_reach_codes "33400,46000,46003,46006,46007", \
  --flowarea_reach_codes "53700,46100,48400,31800,34300,34305,34306,4600,46003,46006,46007","43100" \
  --waterbody_reach_codes "49300,3900,39001,39004,39005,39006,39009,39010,39011,39012,43600,43601,43603,43604,43605,43606,43607,43608,43609,43610,43611,43612,43613,43614,43615,43618,43619,43621,43623,43624,43625,43626,46600,46601,46602", \
  --prism_data $RS_CONTEXT_DIR/climate/precipitation.tif \
  --huc8boundary $RS_CONTEXT_DIR/hydrology/nhdplushr.gpkg/WBDHU8 \
  --meta "Runner=Cybercastor" \
  --verbose

if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/channel
python3 -m channel.channel_rs \
  $CHANNELAREA_DIR/project.rs.xml \
  $RS_CONTEXT_DIR/project.rs.xml

echo "======================  Final Disk space usage ======================="
df -h

echo "======================  Upload to the warehouse ======================="

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

echo "<<PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  echo "<<CHANNEL AREA PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
