#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'
# set -x

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${RS_TOPO_ID?}")
(: "${RS_API_URL?}")
# These are machine credentials for the API which will allow the CLI to delegate uploading to either a specific user or an org
(: "${RS_CLIENT_ID?}")
(: "${RS_CLIENT_SECRET?}")

# Turn off the set -u option once we've checked all the mandatory variables
set +u


cat<<EOF
CHaMP Metrics
EOF

echo "TAGS: $TAGS"
echo "RS_TOPO_ID: $RS_TOPO_ID"
echo "RS_API_URL: $RS_API_URL"

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RS_TOPO_DIR=$DATA_DIR/rs_topo/rs_topo_$RS_TOPO_ID

##########################################################################################
# First Get RS Topo inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RS_TOPO_DIR --id $RS_TOPO_ID --no-input --no-ui --verbose

##########################################################################################
# Now Run CHaMP Metrics Tool
##########################################################################################
try() {

cd /usr/local/src/riverscapes-tools/packages/champ_metrics
python3 -m champ_metrics.champ_metrics \
  $RS_TOPO_DIR/project.rs.xml \
  --verbose

if [[ $? != 0 ]]; then return 1; fi

echo "======================  Final Disk space usage ======================="
df -h

echo "======================  Upload to the warehouse ======================="

# Upload the HUC into the warehouse
cd $RS_TOPO_DIR

rscli upload .  \
      --tags "$TAGS" \
      --no-input --no-ui --verbose

if [[ $? != 0 ]]; then return 1; fi

echo "<<PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  echo "<<CHAMP METRICS PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
