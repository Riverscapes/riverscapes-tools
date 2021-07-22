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
echo "PROGRAM: $PROGRAM"
echo "TAGS: $TAGS"

# Drop into our venv immediately
source /usr/local/venv/bin/activate
pip --timeout=120 install -r /usr/local/requirements.txt
pip install -e /usr/local/src/riverscapes-tools/packages/taudem
pip install -e /usr/local/src/riverscapes-tools/packages/channel

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
RSC_TASK_DIR=/usr/local/data/rs_context/$HUC
RSC_TASK_OUTPUT=$RSC_TASK_DIR/output
RSC_TASK_DOWNLOAD=$RSC_TASK_DIR/scratch

CHANNEL_TASK_DIR=/usr/local/data/channel/$HUC
CHANNEL_TASK_OUTPUT=$CHANNEL_TASK_DIR/output

TAUDEM_TASK_DIR=/usr/local/data/taudem/$HUC
TAUDEM_TASK_OUTPUT=$TAUDEM_TASK_DIR/output

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
  $RSC_TASK_OUTPUT \
  /shared/download/ \
  --parallel \
  --temp_folder $RSC_TASK_DOWNLOAD \
  --meta "Runner=Cybercastor" \
  --verbose

if [[ $? != 0 ]]; then return 1; fi
echo "<<RS_CONTEXT COMPLETE>>"

# Upload the HUC into the warehouse
cd $RSC_TASK_OUTPUT
rscli upload . --replace --tags "$TAGS"  --no-input --verbose --program "$PROGRAM"
if [[ $? != 0 ]]; then return 1; fi
echo "<<RS_CONTEXT UPLOAD COMPLETE>>"

# We need to conserver some space for the next run
rm -fr $RSC_TASK_OUTPUT/vegetation
rm -fr $RSC_TASK_OUTPUT/transportation

##########################################################################################
# Now Run Channel Area Tool
##########################################################################################

channel $HUC \
  $RSC_TASK_OUTPUT/hydrology/NHDFlowline.shp \
  $RSC_TASK_OUTPUT/hydrology/NHDArea.shp \
  $CHANNEL_TASK_OUTPUT \
  --bankfull_function "0.177 * (a ** 0.397) * (p ** 0.453)" \
  --bankfull_function_params "a=TotDASqKm" \
  --reach_code_field FCode \
  --reach_codes "46003,46006,46007" \
  --prism_data $RSC_TASK_OUTPUT/climate/precipitation.tif \
  --huc8boundary $RSC_TASK_OUTPUT/hydrology/WBDHU8.shp \
  --meta "Runner=Cybercastor" \
  --verbose

if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/channel
/usr/local/venv/bin/python -m channel.channel_rs \
  $CHANNEL_TASK_OUTPUT/project.rs.xml \
  $RSC_TASK_OUTPUT/project.rs.xml

# Upload the HUC into the warehouse
cd $CHANNEL_TASK_OUTPUT
rscli upload . --replace --tags "$TAGS" --no-input --verbose --program "$PROGRAM"
if [[ $? != 0 ]]; then return 1; fi

echo "<<TauChannel Area COMPLETE>>"


##########################################################################################
# Now Run TauDEM
##########################################################################################

taudem $HUC \
  $CHANNEL_TASK_OUTPUT/outputs/channel_area.gpkg/channel_areas \
  $RSC_TASK_OUTPUT/topography/dem.tif \
  $TAUDEM_TASK_OUTPUT \
  --hillshade $RSC_TASK_OUTPUT/topography/dem_hillshade.tif
  --meta "Runner=Cybercastor" \
  --verbose
if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/taudem
/usr/local/venv/bin/python -m taudem.taudem_rs \
  $TAUDEM_TASK_OUTPUT/project.rs.xml \
  $RSC_TASK_OUTPUT/project.rs.xml,$CHANNEL_TASK_OUTPUT/project.rs.xml

# Upload the HUC into the warehouse
cd $TAUDEM_TASK_OUTPUT
rscli upload . --replace --tags "$TAGS" --no-input --verbose --program "$PROGRAM"
if [[ $? != 0 ]]; then return 1; fi


echo "<<TauDEM COMPLETE>>"

echo "======================  Final Disk space usage ======================="
df -h


# Cleanup
cd /usr/local/
rm -fr $RSC_TASK_DIR
rm -fr $CHANNEL_TASK_DIR
rm -fr $TAUDEM_TASK_DIR

echo "<<PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  cd /usr/local/
rm -fr $RSC_TASK_DIR
rm -fr $CHANNEL_TASK_DIR
rm -fr $TAUDEM_TASK_DIR
  echo "<<PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
