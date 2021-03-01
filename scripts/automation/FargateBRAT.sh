#!/bin/bash
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${VBET_TAGS?}")
(: "${RSCONTEXT_TAGS?}")
(: "${BRAT_TAGS?}")

echo "$RS_CONFIG" > /root/.riverscapes

cat<<EOF
      ██████╗ ██████╗  █████╗ ████████╗  
      ██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝  
      ██████╔╝██████╔╝███████║   ██║     
      ██╔══██╗██╔══██╗██╔══██║   ██║     
      ██████╔╝██║  ██║██║  ██║   ██║     
      ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝     
EOF

echo "HUC: $HUC"
echo "PROGRAM: $PROGRAM"
echo "VBET_TAGS: $VBET_TAGS"
echo "RSCONTEXT_TAGS: $RSCONTEXT_TAGS"
echo "BRAT_TAGS: $BRAT_TAGS"

# Drop into our venv immediately
source /usr/local/venv/bin/activate

TASK_DIR=/usr/local/data/brat/$HUC
RS_CONTEXT_DIR=$TASK_DIR/rs_context
VBET_DIR=$TASK_DIR/vbet
BRAT_DIR=$TASK_DIR/brat


# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --type "RSContext" --meta "huc8=$HUC" --tags "$RSCONTEXT_TAGS" \
  --no-input --verbose --program "$PROGRAM"

# Go get vbet result for this to work
rscli download $VBET_DIR --type "VBET" --meta "huc8=$HUC" --tags "$VBET_TAGS" --file-filter "vbet\.gpkg" \
  --no-input --verbose --program "$PROGRAM"

echo "======================  Initial Disk space usage ======================="
df -h


try() {

  ##########################################################################################
  # Now Run BRAT Build
  ##########################################################################################
  bratbuild $HUC \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/topography/slope.tif \
    $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
    $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
    $RS_CONTEXT_DIR/vegetation/existing_veg.tif \
    $RS_CONTEXT_DIR/vegetation/historic_veg.tif \
    $VBET_DIR/outputs/vbet.gpkg/vbet_50 \
    $RS_CONTEXT_DIR/transportation/roads.shp \
    $RS_CONTEXT_DIR/transportation/railways.shp \
    $RS_CONTEXT_DIR/transportation/canals.shp \
    $RS_CONTEXT_DIR/ownership/ownership.shp \
    30 \
    100 \
    100 \
    $BRAT_DIR \
    --reach_codes 33400,33600,33601,33603,46000,46003,46006,46007,55800 \
    --canal_codes 33600,33601,33603 \
    --peren_codes 46006 \
    --flow_areas $RS_CONTEXT_DIR/hydrology/NHDArea.shp \
    --waterbodies $RS_CONTEXT_DIR/hydrology/NHDWaterbody.shp \
    --max_waterbody 0.001 \
    --meta Runner=Cybercastor \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  # Upload the HUC into the warehouse. This is useful
  # Since BRAT RUn might fail
  cd $BRAT_DIR
  rscli upload . --tags "$BRAT_TAGS" --replace --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi
  
  ##########################################################################################
  # Now Run BRAT Run
  ##########################################################################################
  bratrun $BRAT_DIR --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/brat
  /usr/local/venv/bin/python -m sqlbrat.brat_rs \
    $BRAT_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $BRAT_DIR
  rscli upload . --tags "$BRAT_TAGS" --replace --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi

  # Cleanup
  cd /usr/local/
  rm -fr $TASK_DIR

  echo "<<PROCESS COMPLETE>>\n\n"



}
try || {
  # Emergency Cleanup
  cd /usr/local/
  rm -fr $TASK_DIR
  echo "<<BRAT PROCESS ENDED WITH AN ERROR>>\n\n"
  exit 1
}
