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

TASK_DIR=/usr/local/data/confinement/$HUC
RS_CONTEXT_DIR=$TASK_DIR/rs_context
VBET_DIR=$TASK_DIR/vbet
CONFINEMENT_DIR=$TASK_DIR/confinement

# Get the RSCli project we need to make this happen
rscli download $RS_CONTEXT_DIR --type "RSContext" --meta "huc8=$HUC" --tags "$RSCONTEXT_TAGS" \
  --file-filter "hydrology\.gpkg" \
  --no-input --verbose --program "$PROGRAM"

# Go get vbet result for this to work
rscli download $VBET_DIR --type "VBET" --meta "huc8=$HUC" --tags "$VBET_TAGS" \
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
    --meta Runner=Cybercastor \
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
  # Cleanup
  cd /usr/local/
  rm -fr $TASK_DIR

  echo "<<PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  cd /usr/local/
  rm -fr $TASK_DIR
  echo "<<CONFINEMENT PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
