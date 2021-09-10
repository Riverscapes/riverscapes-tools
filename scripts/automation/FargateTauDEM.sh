#!/bin/bash
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${RSCONTEXT_TAGS?}")
(: "${CHANNEL_TAGS?}")
(: "${TAUDEM_TAGS?}")

echo "$RS_CONFIG" > /root/.riverscapes

cat<<EOF

████████╗ █████╗ ██╗   ██╗██████╗ ███████╗███╗   ███╗
╚══██╔══╝██╔══██╗██║   ██║██╔══██╗██╔════╝████╗ ████║
   ██║   ███████║██║   ██║██║  ██║█████╗  ██╔████╔██║
   ██║   ██╔══██║██║   ██║██║  ██║██╔══╝  ██║╚██╔╝██║
   ██║   ██║  ██║╚██████╔╝██████╔╝███████╗██║ ╚═╝ ██║
   ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝     ╚═╝
EOF

echo "HUC: $HUC"
echo "PROGRAM: $PROGRAM"
echo "RSCONTEXT_TAGS: $RSCONTEXT_TAGS"
echo "CHANNEL_TAGS: $CHANNEL_TAGS"
echo "TAUDEM_TAGS: $TAUDEM_TAGS"

# Drop into our venv immediately
source /usr/local/venv/bin/activate

# Install latest pip dependencies
pip --timeout=120 install -r /usr/local/requirements.txt
pip install -e /usr/local/src/riverscapes-tools/packages/taudem


echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context/$HUC
CHANNEL_DIR=$DATA_DIR/channel/$HUC
TAUDEM_DIR=$DATA_DIR/taudem/$HUC

##########################################################################################
# First Get RS_Context inputs
##########################################################################################

# Get the RSCli project we need to make this happe-9
rscli download $RS_CONTEXT_DIR --type "RSContext" --meta "huc=$HUC" \
  --file-filter "(hillshade|slope|dem|hand|hydrology)" \
  --tags "$RSCONTEXT_TAGS" --no-input --verbose --program "$PROGRAM"

##########################################################################################
# Now get ChannelArea inputs
##########################################################################################

# Get the RSCli project we need to make this happe-9
rscli download $CHANNEL_DIR --type "ChannelArea" --meta "huc=$HUC" \
  --file-filter "channel_area\.gpkg" \
  --tags "$CHANNEL_TAGS" --no-input --verbose --program "$PROGRAM"


##########################################################################################
# Now Run TauDEM
##########################################################################################
try() {

taudem $HUC \
  $CHANNEL_DIR/outputs/channel_area.gpkg/channel_area \
  $RS_CONTEXT_DIR/topography/dem.tif \
  $TAUDEM_DIR \
  --hillshade $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
  --meta "Runner=Cybercastor" \
  --verbose
if [[ $? != 0 ]]; then return 1; fi

cd /usr/local/src/riverscapes-tools/packages/taudem
/usr/local/venv/bin/python -m taudem.taudem_rs \
  $TAUDEM_DIR/project.rs.xml \
  $RS_CONTEXT_DIR/project.rs.xml,$CHANNEL_DIR/project.rs.xml

echo "======================  Final Disk space usage ======================="
df -h

echo "======================  Upload to the warehouse ======================="

# Upload the HUC into the warehouse
cd $TAUDEM_DIR
rscli upload . --replace --tags "$TAUDEM_TAGS" --no-input --verbose --program "$PROGRAM"
if [[ $? != 0 ]]; then return 1; fi

echo "<<PROCESS COMPLETE>>"


}
try || {
  # Emergency Cleanup
  echo "<<RS CONTEXT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
