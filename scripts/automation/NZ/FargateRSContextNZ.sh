#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${TAGS?}")
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
         _          _                  _             _            _           _            _     _      _            _                _                _         
        /\ \       / /\              /\ \           /\ \         /\ \     _  /\ \         /\ \ /_/\    /\ \         /\ \             /\ \     _      /\ \        
       /  \ \     / /  \            /  \ \         /  \ \       /  \ \   /\_\\_\ \       /  \ \\ \ \   \ \_\        \_\ \           /  \ \   /\_\   /  \ \       
      / /\ \ \   / / /\ \__        / /\ \ \       / /\ \ \     / /\ \ \_/ / //\__ \     / /\ \ \\ \ \__/ / /        /\__ \         / /\ \ \_/ / /__/ /\ \ \      
     / / /\ \_\ / / /\ \___\      / / /\ \ \     / / /\ \ \   / / /\ \___/ // /_ \ \   / / /\ \_\\ \__ \/_/        / /_ \ \       / / /\ \___/ //___/ /\ \ \     
    / / /_/ / / \ \ \ \/___/     / / /  \ \_\   / / /  \ \_\ / / /  \/____// / /\ \ \ / /_/_ \/_/ \/_/\__/\       / / /\ \ \     / / /  \/____/ \___\/ / / /     
   / / /__\/ /   \ \ \          / / /    \/_/  / / /   / / // / /    / / // / /  \/_// /____/\     _/\/__\ \     / / /  \/_/    / / /    / / /        / / /      
  / / /_____/_    \ \ \        / / /          / / /   / / // / /    / / // / /      / /\____\/    / _/_/\ \ \   / / /          / / /    / / /        / / /    _  
 / / /\ \ \ /_/\__/ / /       / / /________  / / /___/ / // / /    / / // / /      / / /______   / / /   \ \ \ / / /          / / /    / / /         \ \ \__/\_\ 
/ / /  \ \ \\ \/___/ /       / / /_________\/ / /____\/ // / /    / / //_/ /      / / /_______\ / / /    /_/ //_/ /          / / /    / / /           \ \___\/ / 
\/_/    \_\/ \_____\/        \/____________/\/_________/ \/_/     \/_/ \_\/       \/__________/ \/_/     \_\/ \_\/           \/_/     \/_/             \/___/_/  
                                                                                                                                                                 
EOF

echo "HUC: $HUC"
echo "TAGS: $TAGS"
echo "RS_API_URL: $RS_API_URL"
echo "VISIBILITY: $VISIBILITY"
if [ -n "$USER_ID" ]; then
  echo "USER_ID: $USER_ID"
elif [ -n "$ORG_ID" ]; then
  echo "ORG_ID: $ORG_ID"
fi


echo "======================  GDAL Version ======================="
gdal-config --version

cd /usr/local/src/riverscapes-tools
pip install -e packages/rscontext_nz
cd /usr/local/src

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RS_CONTEXT_DIR=$DATA_DIR/rs_context_nz/$HUC

echo "DATA_DIR: $DATA_DIR"
echo "RS_CONTEXT_DIR: $RS_CONTEXT_DIR"

echo "======================  Disk space usage ======================="
df -h

echo "======================  Starting RSContext ======================="
##########################################################################################
# First Run RS_Context
##########################################################################################
try() {
  rscontextnz $HUC \
    /efsshare/NationalDatasetsNZ/hydrography/hydrography.gpkg \
    /efsshare/NationalDatasetsNZ/topography/lidar.gpkg \
    /efsshare/NationalDatasetsNZ/topography/NORTH_ISLAND_8m.tif \
    /efsshare/NationalDatasetsNZ/topography/SOUTH_ISLAND_8m.tif \
    $RS_CONTEXT_DIR \
    --meta "Runner=Cybercastor" \
    --dem_resolution 8 \
    --verbose

  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="
  # Upload the HUC into the warehouse
  cd $RS_CONTEXT_DIR

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

}
try || {
  echo "<<RS CONTEXT NZ PROCESS COMPLETE WITH ERROR>>"
  exit 1
}
