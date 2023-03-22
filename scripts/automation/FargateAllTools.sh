#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
IFS=$'\n\t'
# Set -x will echo every command to the console
set -x

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${TAGS?}")
(: "${RS_API_URL?}")
# These are machine credentials for the API which will allow the CLI to delegate uploading to either a specific user or an org
(: "${RS_CLIENT_ID?}")
(: "${RS_CLIENT_SECRET?}")

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

   ▄████████  ▄█   ▄█    █▄     ▄████████    ▄████████    ▄████████  ▄████████    ▄████████    ▄███████▄    ▄████████    ▄████████ 
  ███    ███ ███  ███    ███   ███    ███   ███    ███   ███    ███ ███    ███   ███    ███   ███    ███   ███    ███   ███    ███ 
  ███    ███ ███▌ ███    ███   ███    █▀    ███    ███   ███    █▀  ███    █▀    ███    ███   ███    ███   ███    █▀    ███    █▀  
 ▄███▄▄▄▄██▀ ███▌ ███    ███  ▄███▄▄▄      ▄███▄▄▄▄██▀   ███        ███          ███    ███   ███    ███  ▄███▄▄▄       ███        
▀▀███▀▀▀▀▀   ███▌ ███    ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   ▀███████████ ███        ▀███████████ ▀█████████▀  ▀▀███▀▀▀     ▀███████████ 
▀███████████ ███  ███    ███   ███    █▄  ▀███████████          ███ ███    █▄    ███    ███   ███          ███    █▄           ███ 
  ███    ███ ███  ███    ███   ███    ███   ███    ███    ▄█    ███ ███    ███   ███    ███   ███          ███    ███    ▄█    ███ 
  ███    ███ █▀    ▀██████▀    ██████████   ███    ███  ▄████████▀  ████████▀    ███    █▀   ▄████▀        ██████████  ▄████████▀  
  ███    ███                                ███    ███                                                                             
    ███      ▄██████▄   ▄██████▄   ▄█          ▄████████                                                                           
▀█████████▄ ███    ███ ███    ███ ███         ███    ███                                                                           
   ▀███▀▀██ ███    ███ ███    ███ ███         ███    █▀                                                                            
    ███   ▀ ███    ███ ███    ███ ███         ███                                                                                  
    ███     ███    ███ ███    ███ ███       ▀███████████                                                                           
    ███     ███    ███ ███    ███ ███                ███                                                                           
    ███     ███    ███ ███    ███ ███▌    ▄    ▄█    ███                                                                           
   ▄████▀    ▀██████▀   ▀██████▀  █████▄▄██  ▄████████▀                                                                            
                                  ▀                                                                                                
                                     
EOF

echo "HUC: $HUC"
echo "TAGS: $TAGS"

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RSCONTEXT_DIR=$DATA_DIR/rs_context/$HUC
RSCONTEXT_SCRATCH_DIR=$DATA_DIR/rs_context_scratch/$HUC
CHANNELAREA_DIR=$DATA_DIR/channel_area/$HUC
TAUDEM_DIR=$DATA_DIR/taudem/$HUC

VBET_DIR=$DATA_DIR/vbet/$HUC
BRAT_DIR=$DATA_DIR/brat/$HUC
CONFINEMENT_DIR=$DATA_DIR/confinement/$HUC
RVD_DIR=$DATA_DIR/rvd/$HUC

echo "======================  Disk space usage ======================="
df -h

##########################################################################################
# First Run RS_Context
##########################################################################################

try() {

  rscontext $HUC \
    /efsshare/NationalDatasets/landfire/200/us_200evt.tif \
    /efsshare/NationalDatasets/landfire/200/us_200bps.tif \
    /efsshare/NationalDatasets/ownership/surface_management_agency.shp \
    /efsshare/NationalDatasets/ownership/FairMarketValue.tif \
    /efsshare/NationalDatasets/ecoregions/us_eco_l3_state_boundaries.shp \
    /efsshare/download/prism \
    $RSCONTEXT_DIR \
    /efsshare/download \
    --meta "Runner=Cybercastor" \
    --parallel \
    --temp_folder $RSCONTEXT_SCRATCH_DIR \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi
  echo "<<RS_CONTEXT COMPLETE>>"

  # Upload the HUC into the warehouse
  cd $RSCONTEXT_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi
  echo "<<RS_CONTEXT UPLOAD COMPLETE>>"

  # Clean up the scratch dir to save space
  rm -fr $RSCONTEXT_SCRATCH_DIR

  ##########################################################################################
  # Now Run Channel Area Tool
  ##########################################################################################

  channel $HUC \
    $RSCONTEXT_DIR/hydrology/NHDFlowline.shp \
    $CHANNELAREA_DIR \
    --flowareas $RSCONTEXT_DIR/hydrology/NHDArea.shp \
    --waterbodies $RSCONTEXT_DIR/hydrology/NHDWaterbody.shp \
    --bankfull_function "0.177 * (a ** 0.397) * (p ** 0.453)" \
    --bankfull_function_params "a=TotDASqKm" \
    --reach_code_field FCode \
    --flowline_reach_codes "33400,46000,46003,46006,46007", \
    --flowarea_reach_codes "53700,46100,48400,31800,34300,34305,34306,4600,46003,46006,46007", \
    --waterbody_reach_codes "49300,3900,39001,39004,39005,39006,39009,39010,39011,39012,43600,43601,43603,43604,43605,43606,43607,43608,43609,43610,43611,43612,43613,43614,43615,43618,43619,43621,43623,43624,43625,43626,46600,46601,46602", \
    --prism_data $RSCONTEXT_DIR/climate/precipitation.tif \
    --huc8boundary $RSCONTEXT_DIR/hydrology/WBDHU8.shp \
    --meta "Runner=Cybercastor" \
    --verbose

  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/channel
  /usr/local/venv/bin/python -m channel.channel_rs \
    $CHANNELAREA_DIR/project.rs.xml \
    $RSCONTEXT_DIR/project.rs.xml

  # Upload the HUC into the warehouse
  cd $CHANNELAREA_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  ##########################################################################################
  # Now Run TauDEM
  ##########################################################################################

  taudem $HUC \
    $CHANNELAREA_DIR/outputs/channel_area.gpkg/channel_area \
    $RSCONTEXT_DIR/topography/dem.tif \
    $TAUDEM_DIR \
    --hillshade $RSCONTEXT_DIR/topography/dem_hillshade.tif \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/taudem
  /usr/local/venv/bin/python -m taudem.taudem_rs \
    $TAUDEM_DIR/project.rs.xml \
    $RSCONTEXT_DIR/project.rs.xml,$CHANNEL_DIR/project.rs.xml

  # Upload the HUC into the warehouse
  cd $TAUDEM_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  ##########################################################################################
  # Now Run VBET
  ##########################################################################################

vbet $HUC \
  "APRIL_2022" \
  $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network \
  $RS_CONTEXT_DIR/topography/dem.tif \
  $RS_CONTEXT_DIR/topography/slope.tif \
  $RS_CONTEXT_DIR/topography/dem_hillshade.tif \
  $RS_CONTEXT_DIR/hydrology/NHDPlusCatchment.shp \
  $CHANNELAREA_DIR/outputs/channel_area.gpkg/channel_area \
  $RS_CONTEXT_DIR/hydrology/nhd_data.sqlite/NHDPlusFlowlineVAA \
  $VBET_DIR \
  --pitfill $TAUDEM_DIR/intermediates/pitfill.tif \
  --dinfflowdir_ang $TAUDEM_DIR/intermediates/dinfflowdir_ang.tif \
  --dinfflowdir_slp $TAUDEM_DIR/outputs/dinfflowdir_slp.tif \
  --twi_raster $TAUDEM_DIR/outputs/twi.tif \
  --reach_codes 33400,46000,46003,46006,46007,55800 \
  --mask $RS_CONTEXT_DIR/hydrology/WBDHU8.shp \
  --meta "Runner=Cybercastor" \
  --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/vbet
  /usr/local/venv/bin/python -m vbet.vbet_rs \
    $VBET_DIR/project.rs.xml \
    $RSCONTEXT_DIR/project.rs.xml,$TAUDEM_DIR/project.rs.xml,$CHANNELAREA_DIR/project.rs.xml

  # Upload the HUC into the warehouse
  cd $VBET_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  # Cleanup
  echo "<<RSC, CHANNEL AREA AND TAUDEM and VBET PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup. We need VBET for everything else so no point in continuing
  echo "<<RS CONTEXT PROCESS ENDED WITH AN ERROR>>"
  EXIT_CODE=1
  exit $EXIT_CODE
}

##########################################################################################
# Now Run BRAT
##########################################################################################
try() {

  ##########################################################################################
  # Now Run BRAT Build
  ##########################################################################################
  bratbuild $HUC \
    $RSCONTEXT_DIR/topography/dem.tif \
    $RSCONTEXT_DIR/topography/slope.tif \
    $RSCONTEXT_DIR/topography/dem_hillshade.tif \
    $RSCONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
    $RSCONTEXT_DIR/vegetation/existing_veg.tif \
    $RSCONTEXT_DIR/vegetation/historic_veg.tif \
    $VBET_DIR/outputs/vbet.gpkg/vbet_full \
    $RSCONTEXT_DIR/transportation/roads.shp \
    $RSCONTEXT_DIR/transportation/railways.shp \
    $RSCONTEXT_DIR/transportation/canals.shp \
    $RSCONTEXT_DIR/ownership/ownership.shp \
    30 \
    100 \
    100 \
    $BRAT_DIR \
    --reach_codes 33400,33600,33601,33603,46000,46003,46006,46007 \
    --canal_codes 33600,33601,33603 \
    --peren_codes 46006,55800,33400 \
    --flow_areas $RSCONTEXT_DIR/hydrology/NHDArea.shp \
    --waterbodies $RSCONTEXT_DIR/hydrology/NHDWaterbody.shp \
    --max_waterbody 0.001 \
    --meta "Runner=Cybercastor" \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi


  # Upload the HUC into the warehouse. This is useful
  # Since BRAT RUn might fail
  cd $BRAT_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi
  
  ##########################################################################################
  # Now Run BRAT Run
  ##########################################################################################
  bratrun $BRAT_DIR --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/brat
  /usr/local/venv/bin/python -m sqlbrat.brat_rs \
    $BRAT_DIR/project.rs.xml \
    "$RSCONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse
  cd $BRAT_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose --replace
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose --replace
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  echo "<<BRAT PROCESS COMPLETE>>\n\n"

}
try || {
  # We continue on to confinement
  echo "<<BRAT PROCESS ENDED WITH AN ERROR>>\n\n"
  EXIT_CODE=1
}


##########################################################################################
# Now Run Confinement
##########################################################################################
try() {

  confinement $HUC \
    $RSCONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
    $VBET_DIR/inputs/vbet_inputs.gpkg/channel_area_polygons \
    $VBET_DIR/outputs/vbet.gpkg/vbet_full \
    $CONFINEMENT_DIR \
    vbet_level_path \
    ValleyBottom \
    --buffer 15.0 \
    --segmented_network $RS_CONTEXT_DIR/hydrology/hydrology.gpkg/network_intersected_300m \
    --meta "Runner=Cybercastor" \
    --verbose

  if [[ $? != 0 ]]; then return 1; fi


  cd /usr/local/src/riverscapes-tools/packages/gnat
  /usr/local/venv/bin/python -m gnat.confinement_rs \
    $CONFINEMENT_DIR/project.rs.xml \
    "$RSCONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $CONFINEMENT_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi

  echo "<<CONFINEMENT PROCESS COMPLETE>>"

}
try || {
  # Fail but move on to RVD
  echo "<<CONFINEMENT PROCESS ENDED WITH AN ERROR>>"
  EXIT_CODE=1
}


##########################################################################################
# Now Run RVD
##########################################################################################
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

  # Upload the HUC into the warehouse.
  cd $RVD_DIR

  # If this is a user upload then we need to use the user's id
  if [ -n "$USERID" ]; then
    rscli upload . --user $USERID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  # If this is an org upload, we need to specify the org ID
  elif [ -n "$ORGID" ]; then
    rscli upload . --org $ORGID \
        --tags "$TAGS" \
        --visibility $VISIBILITY \
        --no-input --no-ui --verbose
  else
    echo "Error: Neither USER nor ORG environment variables are set. You need one of them."
    exit 1
  fi

  if [[ $? != 0 ]]; then return 1; fi
  echo "<<RVD PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  echo "<<RVD PROCESS ENDED WITH AN ERROR>>"
  EXIT_CODE=1
}

exit $EXIT_CODE