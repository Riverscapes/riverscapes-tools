#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${TAGS?}")

(: "${RSCONTEXT_ID?}")
(: "${TAUDEM_ID?}")
(: "${VBET_ID?}")
(: "${ANTHRO_ID?}")
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
 .----------------.  .----------------.  .----------------.  .----------------. 
| .--------------. || .--------------. || .--------------. || .--------------. |
| |  _______     | || |     ______   | || |      __      | || |  _________   | |
| | |_   __ \    | || |   .' ___  |  | || |     /  \     | || | |  _   _  |  | |
| |   | |__) |   | || |  / .'   \_|  | || |    / /\ \    | || | |_/ | | \_|  | |
| |   |  __ /    | || |  | |         | || |   / ____ \   | || |     | |      | |
| |  _| |  \ \_  | || |  \ `.___.'\  | || | _/ /    \ \_ | || |    _| |_     | |
| | |____| |___| | || |   `._____.'  | || ||____|  |____|| || |   |_____|    | |
| |              | || |              | || |              | || |              | |
| '--------------' || '--------------' || '--------------' || '--------------' |
 '----------------'  '----------------'  '----------------'  '----------------' 
EOF

echo "TAGS: $TAGS"
echo "VBET_ID: $VBET_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"
echo "TAUDEM_ID: $TAUDEM_ID"
echo "ANTHRO_ID: $ANTHRO_ID"
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
RSCONTEXT_DIR=$DATA_DIR/rs_context/rs_context_$RSCONTEXT_ID
TAUDEM_DIR=$DATA_DIR/taudem/tau_$TAUDEM_ID
ANTHRO_DIR=$DATA_DIR/anthro/anthro_$ANTHRO_ID
VBET_DIR=$DATA_DIR/vbet/vbet_$VBET_ID
RCAT_DIR=$DATA_DIR/output/rcat

# build cython module and reinstall rcat
cd /usr/local/src/riverscapes-tools/packages/rcat/rcat/lib/accessibility
python3 setup.py build_ext --inplace
cd /usr/local/src/riverscapes-tools
pip install -e packages/rcat
cd /usr/local/src

##########################################################################################
# First Get RS_Context, taudem, antrho and vbet inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RSCONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(vegetation|nhdplushr.gpkg|project_bounds.geojson)" \
  --no-input --no-ui --verbose

# Go get vbet result for this to work
rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "vbet.gpkg" \
  --no-input --no-ui --verbose

# Go get taudem result for this to work
rscli download $TAUDEM_DIR --id $TAUDEM_ID \
  --file-filter "pitfill.tif" \
  --no-input --no-ui --verbose

# Go get antrho result for this to work
rscli download $ANTHRO_DIR --id $ANTHRO_ID \
  --file-filter "(anthro.gpkg|inputs.gpkg)" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run RCAT
##########################################################################################

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  rcat $HUC \
    $RSCONTEXT_DIR/vegetation/existing_veg.tif \
    $RSCONTEXT_DIR/vegetation/historic_veg.tif \
    $TAUDEM_DIR/intermediates/pitfill.tif \
    $ANTHRO_DIR/outputs/anthro.gpkg/vwIgos \
    $ANTHRO_DIR/inputs/inputs.gpkg/dgo \
    $ANTHRO_DIR/outputs/anthro.gpkg/vwReaches \
    $ANTHRO_DIR/inputs/inputs.gpkg/roads \
    $ANTHRO_DIR/inputs/inputs.gpkg/rails \
    $ANTHRO_DIR/inputs/inputs.gpkg/canals \
    $VBET_DIR/outputs/vbet.gpkg/vbet_full \
    $RCAT_DIR \
    --meta "Runner=Cybercastor" \
    --flow_areas $RSCONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDArea \
    --waterbodies $RSCONTEXT_DIR/hydrology/nhdplushr.gpkg/NHDWaterbody \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi


  cd /usr/local/src/riverscapes-tools/packages/rcat
  python3 -m rcat.rcat_rs \
    $RCAT_DIR/project.rs.xml \
    "$RSCONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml,$TAUDEM_DIR/project.rs.xml,$ANTHRO_DIR/project.rs.xml" \

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the output into the warehouse.
  cd $RCAT_DIR

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
  echo "<<RCAT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
