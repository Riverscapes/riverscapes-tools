#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'
# Set -x will echo every command to the console
set -x

# These environment variables need to be present before the script starts
(: "${TAGS?}")
(: "${VBET_ID?}")
(: "${RSCONTEXT_ID?}")
(: "${VISIBILITY?}")
(: "${APIURL?}")
# These are machine credentials for the API which will allow the CLI to delegate uploading to either a specific user or an org
(: "${MACHINE_CLIENT?}")
(: "${MACHINE_SECRET?}")

# Turn off the set -u option once we've checked all the mandatory variables
set +u

if [ -z "$USERID" ] && [ -z "$ORGID" ]; then
  echo "Error: Neither USERID nor ORGID environment variables are set. You need one of them."
  exit 1
elif [ -n "$USERID" ] && [ -n "$ORGID" ]; then
  echo "Error: Both USERID and ORGID environment variables are set. Not a valid case."
  exit 1
fi


cat<<EOF
      ▀███▀▀▀██▄ ▀████▀   ▀███▀███▀▀▀██▄
        ██   ▀██▄  ▀██     ▄█   ██    ▀██▄
        ██   ▄██    ██▄   ▄█    ██     ▀██
        ███████      ██▄  █▀    ██      ██
        ██  ██▄      ▀██ █▀     ██     ▄██
        ██   ▀██▄     ▄██▄      ██    ▄██▀
      ▄████▄ ▄███▄     ██     ▄████████▀
EOF

echo "VBET_ID: $VBET_ID"
echo "RSCONTEXT_ID: $RSCONTEXT_ID"

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RSCONTEXT_DIR=$DATA_DIR/rs_context/data
VBET_DIR=$DATA_DIR/vbet/data
RVD_DIR=$DATA_DIR/output

##########################################################################################
# First Get RS_Context and vbet inputs
##########################################################################################

# Get the RSCli project we need to make this happen
rscli download $RSCONTEXT_DIR --id $RSCONTEXT_ID \
  --file-filter "(hydrology|vegetation)" \
  --no-input --no-ui --verbose

# Go get vbet result for this to work
rscli download $VBET_DIR --id $VBET_ID \
  --file-filter "vbet.gpkg" \
  --no-input --no-ui --verbose

##########################################################################################
# Now Run RVD
##########################################################################################

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  rvd $HUC \
      $RSCONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
      $RSCONTEXT_DIR/vegetation/existing_veg.tif \
      $RSCONTEXT_DIR/vegetation/historic_veg.tif \
      $VBET_DIR/outputs/vbet.gpkg/vbet_full \
      $RVD_DIR \
      --reach_codes 33400,46003,46006,46007,55800 \
      --flow_areas $RSCONTEXT_DIR/hydrology/NHDArea.shp \
      --waterbodies $RSCONTEXT_DIR/hydrology/NHDWaterbody.shp \
      --meta "Runner=Cybercastor" \
      --verbose
  if [[ $? != 0 ]]; then return 1; fi


  cd /usr/local/src/riverscapes-tools/packages/rvd
  /usr/local/venv/bin/python -m rvd.rvd_rs \
    $RVD_DIR/project.rs.xml \
    "$RSCONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the output into the warehouse.
  cd $RVD_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose \
        --api-url $APIURL \
        --client-id $MACHINE_CLIENT \
        --client-secret $MACHINE_SECRET
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose \
        --api-url $APIURL \
        --client-id $MACHINE_CLIENT \
        --client-secret $MACHINE_SECRET
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  echo "<<PROCESS COMPLETE>>"

}
try || {
  echo "<<RVD PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
