
#!/bin/bash
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${RSCONTEXT_TAGS?}")
(: "${CHANNEL_TAGS?}")

echo "$RS_CONFIG" > /root/.riverscapes

cat<<EOF
▄█▄     ▄  █ ██      ▄      ▄   ▄███▄   █         ██   █▄▄▄▄ ▄███▄   ██   
█▀ ▀▄  █   █ █ █      █      █  █▀   ▀  █         █ █  █  ▄▀ █▀   ▀  █ █  
█   ▀  ██▀▀█ █▄▄█ ██   █ ██   █ ██▄▄    █         █▄▄█ █▀▀▌  ██▄▄    █▄▄█ 
█▄  ▄▀ █   █ █  █ █ █  █ █ █  █ █▄   ▄▀ ███▄      █  █ █  █  █▄   ▄▀ █  █ 
▀███▀     █     █ █  █ █ █  █ █ ▀███▀       ▀        █   █   ▀███▀      █ 
         ▀     █  █   ██ █   ██                     █   ▀              █  
              ▀                                    ▀                  ▀   
EOF

echo "HUC: $HUC"
echo "PROGRAM: $PROGRAM"
echo "RSCONTEXT_TAGS: $RSCONTEXT_TAGS"
echo "CHANNEL_TAGS: $CHANNEL_TAGS"

# Drop into our venv immediately
source /usr/local/venv/bin/activate

# Install latest pip dependencies
pip --timeout=120 install -r /usr/local/requirements.txt
pip install -e /usr/local/src/riverscapes-tools/packages/channel


echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
TASK_DIR=/usr/local/data/channel/$HUC
RS_CONTEXT_DIR=$TASK_DIR/rs_context
TASK_OUTPUT=$TASK_DIR/output

##########################################################################################
# First Get RS_Context inputs
##########################################################################################

# Get the RSCli project we need to make this happe-9
rscli download $RS_CONTEXT_DIR --type "RSContext" --meta "huc8=$HUC" \
  --file-filter "(hillshade|slope|dem|climate|hydrology)" \
  --tags "$RSCONTEXT_TAGS" --no-input --verbose --program "$PROGRAM"

##########################################################################################
# Now Run Channel Area Tool
##########################################################################################
try() {

channel $HUC \
  $RS_CONTEXT_DIR/hydrology/NHDFlowline.shp \
  $RS_CONTEXT_DIR/hydrology/NHDArea.shp \
  $RS_CONTEXT_DIR/hydrology/NHDWaterbody.shp \
  $TASK_OUTPUT \
  --bankfull_function "0.177 * (a ** 0.397) * (p ** 0.453)" \
  --bankfull_function_params "a=TotDASqKm" \
  --reach_code_field FCode \
  --flowline_reach_codes "33400,46000,46003,46006,46007", \
  --flowarea_reach_codes "53700,46100,48400,31800,34300,34305,34306,4600,46003,46006,46007", \
  --waterbody_reach_codes "49300,3900,39001,39004,39005,39006,39009,39010,39011,39012,43600,43601,43603,43604,43605,43606,43607,43608,43609,43610,43611,43612,43613,43614,43615,43618,43619,43621,43623,43624,43625,43626,46600,46601,46602", \
  --prism_data $RS_CONTEXT_DIR/climate/precipitation.tif \
  --huc8boundary $RS_CONTEXT_DIR/hydrology/WBDHU8.shp \
  --meta "Runner=Cybercastor" \
  --verbose

if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/channel
/usr/local/venv/bin/python -m channel.channel_rs \
  $TASK_OUTPUT/project.rs.xml \
  $RS_CONTEXT_DIR/project.rs.xml

echo "======================  Final Disk space usage ======================="
df -h

echo "======================  Upload to the warehouse ======================="

# Upload the HUC into the warehouse
cd $TASK_OUTPUT
rscli upload . --replace --tags "$CHANNEL_TAGS" --no-input --verbose --program "$PROGRAM"
if [[ $? != 0 ]]; then return 1; fi

# Cleanup
cd /usr/local/
rm -fr $TASK_DIR

echo "<<PROCESS COMPLETE>>"


}
try || {
  # Emergency Cleanup
  cd /usr/local/
  rm -fr $TASK_DIR
  echo "<<RS CONTEXT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
