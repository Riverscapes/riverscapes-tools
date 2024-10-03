#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
# These are ids that we will use in rscli to download the necessary files
(: "${RCAT_ID?}")
(: "${RME_ID?}")
# These are properties we pass to rscli
(: "${TAGS?}")
(: "${RS_API_URL?}")
# These are machine credentials for the API which will allow the CLI to delegate uploading to either a specific user or an org
(: "${RS_CLIENT_ID?}")
(: "${RS_CLIENT_SECRET?}")

# Turn off the set -u option once we've checked all the mandatory variables
set +u

cat<<EOF

██████╗ ███╗   ███╗███████╗    ███████╗ ██████╗██████╗  █████╗ ██████╗ ███████╗██████╗ 
██╔══██╗████╗ ████║██╔════╝    ██╔════╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
██████╔╝██╔████╔██║█████╗      ███████╗██║     ██████╔╝███████║██████╔╝█████╗  ██████╔╝
██╔══██╗██║╚██╔╝██║██╔══╝      ╚════██║██║     ██╔══██╗██╔══██║██╔═══╝ ██╔══╝  ██╔══██╗
██║  ██║██║ ╚═╝ ██║███████╗    ███████║╚██████╗██║  ██║██║  ██║██║     ███████╗██║  ██║
╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝    ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚══════╝╚═╝  ╚═╝                                                                                       
                                                                                              
EOF

echo "TAGS: $TAGS"
echo "RME_ID: $RME_ID"
echo "RCAT_ID: $RCAT_ID"
echo "RS_API_URL: $RS_API_URL"

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data

RME_DIR=$DATA_DIR/rme/rme_$RME_ID
RCAT_DIR=$DATA_DIR/rcat/rcat_$RCAT_ID

##########################################################################################
# First Get RME and RCAT inputs
##########################################################################################

rscli download $RCAT_DIR --id $RCAT_ID \
  --file-filter "(rcat\.gpkg)" \
  --no-input --no-ui --verbose

rscli download $RME_DIR --id $RME_ID \
  --no-input --no-ui --verbose


##########################################################################################
# Now Run the RME Scraper script
##########################################################################################

try() {
  cd /usr/local/src/riverscapes-tools/lib/riverscapes/riverscapes
  
  # Pull the huc code out of the project XML
  HUC_CODE=$(grep 'Meta name="HUC"' $RME_DIR/project.rs.xml | head -n 1 | awk -F'[<>]' '{print $3}')
  (: "${HUC_CODE?}")
  echo "HUC_CODE: $HUC_CODE"
  
  python3 scrape_huc_statistics.py \
    $HUC_CODE \
    $RME_DIR/outputs/riverscapes_metrics.gpkg \
    $RCAT_DIR/outputs/rcat.gpkg \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $RME_DIR
  
  # We're re-uploading a project so there's no need to create a new one
  rscli upload . --tags "$TAGS" \
    --no-input \
    --no-delete \
    --no-ui \
    --verbose

  if [[ $? != 0 ]]; then return 1; fi

  echo "<<PROCESS COMPLETE>>"

}
try || {
  echo "<<RME_SCRAPE PROCESS ENDED WITH AN ERROR>>"
  exit 1
}
