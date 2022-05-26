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
DATA_DIR=/usr/local/data

RSCONTEXT_DIR=$DATA_DIR/rs_context/$HUC
RSCONTEXT_SCRATCH=$DATA_DIR/rs_context_scratch/$HUC
CHANNEL_DIR=$DATA_DIR/channel/$HUC
TAUDEM_DIR=$DATA_DIR/taudem/$HUC

DOWNLOAD_DIR=/data/download

##########################################################################################
# First Run RS_Context
##########################################################################################

try() {

rscontext $HUC \
  /shaefssharered/NationalDatasets/landfire/200/us_200evt.tif \
  /efsshare/NationalDatasets/landfire/200/us_200bps.tif \
  /efsshare/NationalDatasets/ownership/surface_management_agency.shp \
  /efsshare/NationalDatasets/ownership/FairMarketValue.tif \
  /efsshare/NationalDatasets/ecoregions/us_eco_l3_state_boundaries.shp \
  /efsshare/download/prism \
  $RSCONTEXT_DIR \
  $DOWNLOAD_DIR \
  --parallel \
  --temp_folder $RSCONTEXT_SCRATCH \
  --meta "Runner=Cybercastor" \
  --verbose

if [[ $? != 0 ]]; then return 1; fi
echo "<<RS_CONTEXT COMPLETE>>"

# Upload the HUC into the warehouse
cd $RSCONTEXT_DIR
rscli upload . --replace --tags "$TAGS"  --no-input --verbose --program "$PROGRAM"
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
  $CHANNEL_DIR \
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
  $CHANNEL_DIR/project.rs.xml \
  $RSCONTEXT_DIR/project.rs.xml

# Upload the HUC into the warehouse
cd $CHANNEL_DIR
rscli upload . --replace --tags "$TAGS" --no-input --verbose --program "$PROGRAM"
if [[ $? != 0 ]]; then return 1; fi

echo "<<Channel Area COMPLETE>>"


##########################################################################################
# Now Run TauDEM
##########################################################################################

taudem $HUC \
  $CHANNEL_DIR/outputs/channel_area.gpkg/channel_area \
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
rscli upload . --replace --tags "$TAGS" --no-input --verbose --program "$PROGRAM"
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
