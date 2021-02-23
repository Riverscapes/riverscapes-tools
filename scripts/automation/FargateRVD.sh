#!/bin/bash
set -eu
IFS=$'\n\t'
if [[ -v DEBUG ]];
then
    DEBUG_USE="--debug"
else
    DEBUG_USE=" "
fi

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${VBET_TAGS?}")
(: "${RSCONTEXT_TAGS?}")
(: "${RVD_TAGS?}")
(: "${DEBUG_USE?}")

echo "$RS_CONFIG" > /root/.riverscapes

cat<<EOF
      ▀███▀▀▀██▄ ▀████▀   ▀███▀███▀▀▀██▄
        ██   ▀██▄  ▀██     ▄█   ██    ▀██▄
        ██   ▄██    ██▄   ▄█    ██     ▀██
        ███████      ██▄  █▀    ██      ██
        ██  ██▄      ▀██ █▀     ██     ▄██
        ██   ▀██▄     ▄██▄      ██    ▄██▀
      ▄████▄ ▄███▄     ██     ▄████████▀
EOF

echo "HUC: $HUC"
echo "PROGRAM: $PROGRAM"
echo "VBET_TAGS: $VBET_TAGS"
echo "RSCONTEXT_TAGS: $RSCONTEXT_TAGS"
echo "RVD_TAGS: $RVD_TAGS"
echo "DEBUG_USE: $DEBUG_USE"
# sleep 1h

# Drop into our venv immediately
source /usr/local/venv/bin/activate
# Link it up
pip install -e /usr/local/src/riverscapes-tools/packages/rvd

TASK_DIR=/usr/local/data/rvd/$HUC
RS_CONTEXT_DIR=$TASK_DIR/rs_context
VBET_DIR=$TASK_DIR/vbet
RVD_DIR=$TASK_DIR/rvd

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --type "RSContext" --meta "huc8=$HUC" --tags "$RSCONTEXT_TAGS" \
  --file-filter "(hydrology|vegetation)" \
  --no-input --verbose --program "$PROGRAM"

# Go get vbet result for this to work
rscli download $VBET_DIR --type "VBET" --meta "huc8=$HUC" --tags "$VBET_TAGS" \
  --file-filter "vbet.gpkg" \
  --no-input --verbose --program "$PROGRAM"

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  rvd $HUC \
      $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
      $RS_CONTEXT_DIR/vegetation/existing_veg.tif \
      $RS_CONTEXT_DIR/vegetation/historic_veg.tif \
      $VBET_DIR/outputs/vbet.gpkg/vbet_50 \
      $RVD_DIR \
      --reach_codes 33400,46003,46006,46007,55800 \
      --flow_areas $RS_CONTEXT_DIR/hydrology/NHDArea.shp \
      --waterbodies $RS_CONTEXT_DIR/hydrology/NHDWaterbody.shp \
      --meta Runner=Cybercastor \
      --verbose $DEBUG_USE
  if [[ $? != 0 ]]; then return 1; fi


  cd /usr/local/src/riverscapes-tools/packages/rvd
  /usr/local/venv/bin/python -m rvd.rvd_rs \
    $RVD_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $RVD_DIR
  rscli upload . --tags "$RVD_TAGS" --replace --no-input --verbose --program "$PROGRAM"
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
  echo "<<RVD PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
