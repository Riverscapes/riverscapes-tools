#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${AOI?}")
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

# Optional: CONFIG defaults to the CONUS profile if not provided
CONFIG="${CONFIG:-/usr/local/src/riverscapes-tools/packages/rs_context_neo/config/us_conus_aoi.json}"

cat<<EOF
      ██████╗ ███████╗     ██████╗ ██████╗ ███╗   ██╗████████╗███████╗██╗  ██╗████████╗    ███╗   ██╗███████╗ ██████╗ 
      ██╔══██╗██╔════╝    ██╔════╝██╔═══██╗████╗  ██║╚══██╔══╝██╔════╝╚██╗██╔╝╚══██╔══╝    ████╗  ██║██╔════╝██╔═══██╗
      ██████╔╝███████╗    ██║     ██║   ██║██╔██╗ ██║   ██║   █████╗   ╚███╔╝    ██║       ██╔██╗ ██║█████╗  ██║   ██║
      ██╔══██╗╚════██║    ██║     ██║   ██║██║╚██╗██║   ██║   ██╔══╝   ██╔██╗    ██║       ██║╚██╗██║██╔══╝  ██║   ██║
      ██║  ██║███████║    ╚██████╗╚██████╔╝██║ ╚████║   ██║   ███████╗██╔╝ ██╗   ██║       ██║ ╚████║███████╗╚██████╔╝
      ╚═╝  ╚═╝╚══════╝    ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚═╝  ╚═╝   ╚═╝       ╚═╝  ╚═══╝╚══════╝ ╚═════╝ 
EOF

echo "AOI: $AOI"
echo "CONFIG: $CONFIG"
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

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RS_CONTEXT_NEO_DIR=$DATA_DIR/rs_context_neo

echo "DATA_DIR: $DATA_DIR"
echo "RS_CONTEXT_NEO_DIR: $RS_CONTEXT_NEO_DIR"

echo "======================  Disk space usage ======================="
df -h

echo "======================  Starting RS Context Neo ======================="
##########################################################################################
# Run RS Context Neo
##########################################################################################
try() {
  cd /usr/local/src/riverscapes-tools/packages/rs_context_neo
  python3 -m rscontextneo.rs_context_neo \
    --config "$CONFIG" \
    --aoi "$AOI" \
    --output "$RS_CONTEXT_NEO_DIR" \
    --meta "Runner=Cybercastor" \
    --verbose

  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="
  # Upload the output into the warehouse
  cd "$RS_CONTEXT_NEO_DIR"

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
  echo "<<RS CONTEXT NEO PROCESS COMPLETE WITH ERROR>>"
  exit 1
}
