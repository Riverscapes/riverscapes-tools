#!/bin/bash
set -eu
IFS=$'\n\t'


# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${VBET_TAGS?}")
(: "${RSCONTEXT_TAGS?}")
(: "${GNAT_TAGS?}")

echo "$RS_CONFIG" > /root/.riverscapes

cat<<EOF
_____________   ________________
__  ____/__  | / /__    |__  __/
_  / __ __   |/ /__  /| |_  /   
/ /_/ / _  /|  / _  ___ |  /    
\____/  /_/ |_/  /_/  |_/_/     
                                                                                                 
EOF

echo "HUC: $HUC"
echo "PROGRAM: $PROGRAM"
echo "VBET_TAGS: $VBET_TAGS"
echo "RSCONTEXT_TAGS: $RSCONTEXT_TAGS"
echo "GNAT_TAGS: $GNAT_TAGS"

# Drop into our venv immediately
source /usr/local/venv/bin/activate

DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context/$HUC
VBET_DIR=$DATA_DIR/vbet/$HUC
GNAT_DIR=$DATA_DIR/gnat/$HUC

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --type "RSContext" --meta "huc=$HUC" --tags "$RSCONTEXT_TAGS" \
  --file-filter "(hydrology\.gpkg|nhd_data.sqlite|dem.tif|precipitation.tif|ecoregions|transportation|project_bounds.geojson)" \
  --no-input --verbose --program "$PROGRAM"

# Go get vbet result for this to work
rscli download $VBET_DIR --type "VBET" --meta "huc=$HUC" --tags "$VBET_TAGS" \
  --file-filter "(vbet\.gpkg|intermediates\.gpkg)" \
  --no-input --verbose --program "$PROGRAM"

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  gnat $HUC \
    $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network \
    $RS_CONTEXT_DIR/hydrology/nhd_data.sqlite/NHDPlusFlowlineVAA \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/segmented_vbet_polygons \
    $VBET_DIR/intermediates/vbet_intermediates.gpkg/segmentation_points \
    $VBET_DIR/outputs/vbet.gpkg/vbet_centerlines \
    $RS_CONTEXT_DIR/topography/dem.tif \
    $RS_CONTEXT_DIR/climate/precipitation.tif \
    $RS_CONTEXT_DIR/transportation/roads.shp \
    $RS_CONTEXT_DIR/transportation/railways.shp \
    $RS_CONTEXT_DIR/ecoregions/ecoregions.shp \
    $GNAT_DIR \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/gnat
  /usr/local/venv/bin/python -m gnat.gnat_rs \
    $GNAT_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $GNAT_DIR
  rscli upload . --tags "$GNAT_TAGS" --replace --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi


  echo "<<PROCESS COMPLETE>>"

}
try || {
  echo "<<GNAT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
