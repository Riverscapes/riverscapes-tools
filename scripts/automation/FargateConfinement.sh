#!/bin/bash
set -eu
IFS=$'\n\t'


# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${VBET_TAGS?}")
(: "${RSCONTEXT_TAGS?}")
(: "${CONFINEMENT_TAGS?}")

echo "$RS_CONFIG" > /root/.riverscapes

cat<<EOF
    ______                     ______  __                                                    __      
   /      \                   /      \|  \                                                  |  \     
  |  ▓▓▓▓▓▓\ ______  _______ |  ▓▓▓▓▓▓\\▓▓_______   ______  ______ ____   ______  _______  _| ▓▓_    
  | ▓▓   \▓▓/      \|       \| ▓▓_  \▓▓  \       \ /      \|      \    \ /      \|       \|   ▓▓ \   
  | ▓▓     |  ▓▓▓▓▓▓\ ▓▓▓▓▓▓▓\ ▓▓ \   | ▓▓ ▓▓▓▓▓▓▓\  ▓▓▓▓▓▓\ ▓▓▓▓▓▓\▓▓▓▓\  ▓▓▓▓▓▓\ ▓▓▓▓▓▓▓\\▓▓▓▓▓▓   
  | ▓▓   __| ▓▓  | ▓▓ ▓▓  | ▓▓ ▓▓▓▓   | ▓▓ ▓▓  | ▓▓ ▓▓    ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓    ▓▓ ▓▓  | ▓▓ | ▓▓ __  
  | ▓▓__/  \ ▓▓__/ ▓▓ ▓▓  | ▓▓ ▓▓     | ▓▓ ▓▓  | ▓▓ ▓▓▓▓▓▓▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓▓▓▓▓▓▓ ▓▓  | ▓▓ | ▓▓|  \ 
   \▓▓    ▓▓\▓▓    ▓▓ ▓▓  | ▓▓ ▓▓     | ▓▓ ▓▓  | ▓▓\▓▓     \ ▓▓ | ▓▓ | ▓▓\▓▓     \ ▓▓  | ▓▓  \▓▓  ▓▓ 
    \▓▓▓▓▓▓  \▓▓▓▓▓▓ \▓▓   \▓▓\▓▓      \▓▓\▓▓   \▓▓ \▓▓▓▓▓▓▓\▓▓  \▓▓  \▓▓ \▓▓▓▓▓▓▓\▓▓   \▓▓   \▓▓▓▓  
                                                                                                     
EOF

echo "HUC: $HUC"
echo "PROGRAM: $PROGRAM"
echo "VBET_TAGS: $VBET_TAGS"
echo "RSCONTEXT_TAGS: $RSCONTEXT_TAGS"
echo "CONFINEMENT_TAGS: $CONFINEMENT_TAGS"

# Drop into our venv immediately
source /usr/local/venv/bin/activate

DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context/$HUC
VBET_DIR=$DATA_DIR/vbet/$HUC
CONFINEMENT_DIR=$DATA_DIR/confinement/$HUC

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --type "RSContext" --meta "huc=$HUC" --tags "$RSCONTEXT_TAGS" \
  --file-filter "hydrology\.gpkg" \
  --no-input --verbose --program "$PROGRAM"

# Go get vbet result for this to work
rscli download $VBET_DIR --type "VBET" --meta "huc=$HUC" --tags "$VBET_TAGS" \
  --file-filter "vbet\.gpkg" \
  --no-input --verbose --program "$PROGRAM"

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  confinement $HUC \
    $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
    $VBET_DIR/outputs/vbet.gpkg/vbet_50 \
    $CONFINEMENT_DIR \
    BFwidth \
    ValleyBottom \
    --meta "Runner=Cybercastor" \
    --reach_codes 33400,46003,46006,46007,55800 \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/gnat
  /usr/local/venv/bin/python -m gnat.confinement_rs \
    $CONFINEMENT_DIR/project.rs.xml \
    "$RS_CONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $CONFINEMENT_DIR
  rscli upload . --tags "$CONFINEMENT_TAGS" --replace --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi


  echo "<<PROCESS COMPLETE>>"

}
try || {
  echo "<<CONFINEMENT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
