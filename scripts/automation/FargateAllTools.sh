#!/bin/bash
set -eu
IFS=$'\n\t'

# These environment variables need to be present before the script starts
(: "${HUC?}")
(: "${PROGRAM?}")
(: "${RS_CONFIG?}")
(: "${TAGS?}")

echo "$RS_CONFIG" > /root/.riverscapes

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
echo "PROGRAM: $PROGRAM"
echo "TAGS: $TAGS"


# Drop into our venv immediately
source /usr/local/venv/bin/activate

echo "======================  GDAL Version ======================="
gdal-config --version

# Define some folders that we can easily clean up later
DATA_DIR=/usr/local/data
RSCONTEXT_DIR=$DATA_DIR/rs_context/$HUC
RSCONTEXT_SCRATCH_DIR=$DATA_DIR/rs_context_scratch/$HUC
CHANNEL_DIR=$DATA_DIR/channel/$HUC
TAUDEM_DIR=$DATA_DIR/taudem/$HUC

VBET_DIR=$DATA_DIR/vbet/$HUC
BRAT_DIR=$DATA_DIR/brat/$HUC
CONFINEMENT_DIR=$DATA_DIR/confinement/$HUC
RVD_DIR=$DATA_DIR/rvd/$HUC

pip --timeout=120 install -r /usr/local/requirements.txt
pip install -e /usr/local/src/riverscapes-tools/packages/channel
pip install -e /usr/local/src/riverscapes-tools/packages/taudem

##########################################################################################
# First Run RS_Context
##########################################################################################

try() {

  rscontext $HUC \
    /shared/NationalDatasets/landfire/200/us_200evt.tif \
    /shared/NationalDatasets/landfire/200/us_200bps.tif \
    /shared/NationalDatasets/ownership/surface_management_agency.shp \
    /shared/NationalDatasets/ownership/FairMarketValue.tif \
    /shared/NationalDatasets/ecoregions/us_eco_l3_state_boundaries.shp \
    /shared/download/prism \
    $RSCONTEXT_DIR \
    /shared/download/ \
    --meta "Runner=Cybercastor" \
    --parallel \
    --temp_folder $RSCONTEXT_SCRATCH_DIR \
    --verbose
  if [[ $? != 0 ]]; then return 1; fi
  echo "<<RS_CONTEXT COMPLETE>>"

  # Upload the HUC into the warehouse
  cd $RSCONTEXT_DIR
  rscli upload . --replace --tags "$TAGS"  --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi
  echo "<<RS_CONTEXT UPLOAD COMPLETE>>"

  # Clean up the scratch dir to save space
  rm -fr $RSCONTEXT_SCRATCH_DIR

  ##########################################################################################
  # Now Run Channel Area Tool
  ##########################################################################################

  channel $HUC \
    $RSCONTEXT_DIR/hydrology/NHDFlowline.shp \
    $CHANNEL_DIR \
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
    $CHANNEL_DIR/project.rs.xml \
    $RSCONTEXT_DIR/project.rs.xml

  # Upload the HUC into the warehouse
  cd $CHANNEL_DIR
  rscli upload . --replace --tags "$TAGS" --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi

  ##########################################################################################
  # Now Run TauDEM
  ##########################################################################################

  taudem $HUC \
    $CHANNEL_DIR/outputs/channel_area.gpkg/channel_area \
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
  rscli upload . --replace --tags "$TAGS" --no-input --verbose --program "$PROGRAM"
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
  $CHANNEL_AREA_DIR/outputs/channel_area.gpkg/channel_area \
  $RS_CONTEXT_DIR/hydrology/nhd_data.sqlite/NHDPlusFlowlineVAA \
  $VBET_DIR \
  --pitfill $TAUDEM_DIR/intermediates/pitfill.tif \
  --dinfflowdir_ang $TAUDEM_DIR/intermediates/dinfflowdir_ang.tif \
  --dinfflowdir_slp $TAUDEM_DIR/outputs/dinfflowdir_slp.tif \
  --twi_raster $TAUDEM_DIR/outputs/twi.tif \
  --reach_codes 33400,46000,46003,46006,46007,55800 \
  --meta "Runner=Cybercastor" \
  --verbose
  if [[ $? != 0 ]]; then return 1; fi

  cd /usr/local/src/riverscapes-tools/packages/vbet
  /usr/local/venv/bin/python -m vbet.vbet_rs \
    $VBET_DIR/project.rs.xml \
    $RSCONTEXT_DIR/project.rs.xml,$TAUDEM_DIR/project.rs.xml,$CHANNEL_DIR/project.rs.xml

  # Upload the HUC into the warehouse
  cd $VBET_DIR
  rscli upload . --replace --tags "$TAGS" --no-input --verbose --program "$PROGRAM"
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
  rscli upload . --tags "$TAGS" --replace --no-input --verbose --program "$PROGRAM"
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
  rscli upload . --tags "$TAGS" --replace --no-input --verbose --program "$PROGRAM"
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
    $VBET_DIR/outputs/vbet.gpkg/vbet_full \
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
    "$RSCONTEXT_DIR/project.rs.xml,$VBET_DIR/project.rs.xml"

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the warehouse ======================="

  # Upload the HUC into the warehouse.
  cd $CONFINEMENT_DIR
  rscli upload . --tags "$TAGS" --replace --no-input --verbose --program "$PROGRAM"
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
      $VBET_DIR/outputs/vbet.gpkg/vbet_68 \
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
  rscli upload . --tags "$TAGS" --replace --no-input --verbose --program "$PROGRAM"
  if [[ $? != 0 ]]; then return 1; fi
  echo "<<RVD PROCESS COMPLETE>>"

}
try || {
  # Emergency Cleanup
  echo "<<RVD PROCESS ENDED WITH AN ERROR>>"
  EXIT_CODE=1
}

exit $EXIT_CODE