""" Build a BRAT project by segmenting a river network to a specified
    length and then extract the input values required to run the
    BRAT model for each reach segment from various GIS layers.

    Philip Bailey
    30 May 2019

    Returns:
        [type]: [description]
"""
import argparse
import os
import sys
import traceback
import datetime
import time
import json
from typing import List, Dict
from osgeo import ogr
from rscommons import GeopackageLayer
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.vector_ops import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.util import parse_metadata, pretty_duration
from rscommons.build_network import build_network
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields
from rscommons.line_attributes_to_dgo import line_attributes_to_dgo
from sqlbrat.utils.vegetation_summary import vegetation_summary, dgo_veg_summary
from sqlbrat.utils.vegetation_suitability import vegetation_suitability, output_vegetation_raster
from sqlbrat.utils.vegetation_fis import vegetation_fis
from sqlbrat.utils.combined_fis import combined_fis
from sqlbrat.utils.conservation import conservation
from sqlbrat.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),
    'INPUTS': RSLayer('Confinement', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'HYDRO_FLOWLINES': RSLayer('Hydro Flowlines', 'HYDRO_FLOWLINES', 'Vector', 'hydro_flowlines'),
        'HYDRO_IGOS': RSLayer('Hydro Integrated Geographic Objects', 'HYDRO_IGOS', 'Vector', 'hydro_igos'),
        'HYDRO_DGOS': RSLayer('Hyrdo Discrete Geographic Objects', 'HYDRO_DGOS', 'Vector', 'hydro_dgos'),
        'ANTHRO_FLOWLINES': RSLayer('Anthro Segmented Flowlines', 'ANTHRO_FLOWLINES', 'Vector', 'anthro_flowlines'),
        'ANTHRO_IGOS': RSLayer('Anthro Integrated Geographic Objects', 'ANTHRO_IGOS', 'Vector', 'anthro_igos'),
        'ANTHRO_DGOS': RSLayer('Anthro Discrete Geographic Objects', 'ANTHRO_DGOS', 'Vector', 'anthro_dgos'),
        'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'flowareas'),
        'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'waterbodies'),
        'VALLEY_BOTTOM': RSLayer('Valley Bottom', 'VALLEY_BOTTOM', 'Vector', 'valley_bottom'),
    }),
    'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/intermediates.gpkg', {
        'SEGMENTED_NETWORK': RSLayer('Segmented Network', 'SEGMENTED_NETWORK', 'Vector', 'segmented_network'),
    }),
    'EXVEG_SUIT': RSLayer('Existing Vegetation', 'EXVEG_SUIT', 'Raster', 'intermediates/existing_veg_suitability.tif'),
    'HISTVEG_SUIT': RSLayer('Historic Vegetation', 'HISTVEG_SUIT', 'Raster', 'intermediates/historic_veg_suitability.tif'),
    'OUTPUTS': RSLayer('BRAT', 'OUTPUTS', 'Geopackage', 'outputs/brat.gpkg', {
        'BRAT_GEOMETRY': RSLayer('BRAT Geometry', 'BRAT_GEOMETRY', 'Vector', 'ReachGeometry'),
        'BRAT': RSLayer('BRAT', 'BRAT_RESULTS', 'Vector', 'vwReaches'),
        'DGO_GEOM': RSLayer('Discrete Geographic Objects Geometry', 'DGO_GEOM', 'Vector', 'DGOGeometry'),
        'BRAT_DGOS': RSLayer('BRAT Discrete Geographic Objects', 'BRAT_DGOS', 'Vector', 'vwDgos'),
        'IGO_GEOM': RSLayer('Integrated Geographic Objects Geometry', 'IGO_GEOM', 'Vector', 'IGOGeometry'),
        'BRAT_IGOS': RSLayer('BRAT Integrated Geographic Objects', 'BRAT_IGOS', 'Vector', 'vwIgos'),
    }),
    'BRAT_RUN_REPORT': RSLayer('BRAT Report', 'BRAT_RUN_REPORT', 'HTMLFile', 'outputs/brat.html')
}

# Dictionary of fields that this process outputs, keyed by ShapeFile data type
output_fields = {
    ogr.OFTInteger: ['RiskID', 'LimitationID', 'OpportunityID'],
    ogr.OFTReal: ['iVeg100EX', 'iVeg_30EX', 'iVeg100HPE', 'iVeg_30HPE', 'iPC_LU',
                  'iPC_VLowLU', 'iPC_LowLU', 'iPC_ModLU', 'iPC_HighLU', 'iHyd_QLow',
                  'iHyd_Q2', 'iHyd_SPLow', 'iHyd_SP2', 'oVC_HPE', 'oVC_EX', 'oCC_HPE',
                  'mCC_HPE_CT', 'oCC_EX', 'mCC_EX_CT', 'mCC_HisDep']
}

Epochs = [
    # (epoch, prefix, LayerType, OrigId)
    ('Existing', 'EX', 'EXVEG_SUIT', 'EXVEG'),
    ('Historic', 'HPE', 'HISTVEG_SUIT', 'HISTVEG')
]


def brat_build(huc: int, hydro_flowlines: Path, hydro_igos: Path, hydro_dgos: Path,
               anthro_flowlines: Path, anthro_igos: Path, anthro_dgos: Path, hillshade: Path,
               existing_veg: Path, historical_veg: Path, output_folder: Path, streamside_buffer: float,
               riparian_buffer: float, reach_codes: List[str], canal_codes: List[str], peren_codes: List[str],
               flow_areas: Path, waterbodies: Path, max_waterbody: float, valley_bottom: Path,
               meta: Dict[str, str]):
    """Build a BRAT project by segmenting a reach network and copying
    all the necessary layers into the resultant BRAT project

    Arguments:
        huc {str} -- Watershed identifier
        flowlines {str} -- Path to the raw, original polyline flowline ShapeFile
        flow_areas {str} -- Path to the polygon ShapeFile that contains large river outlines
        waterbodies {str} -- Path to the polygon ShapeFile containing water bodies
        max_length {float} -- Maximum allowable flow line segment after segmentation
        min_length {float} -- Shortest allowable flow line segment after segmentation
        dem {str} -- Path to the DEM raster for the watershed
        slope {str} -- Path to the slope raster
        hillshade {str} -- Path to the DEM hillshade raster
        existing_veg {str} -- Path to the excisting vegetation raster
        historical_veg {str} -- Path to the historical vegetation raster
        output_folder {str} -- Output folder where the BRAT project will get created
        streamside_buffer {float} -- Streamside vegetation buffer (meters)
        riparian_buffer {float} -- Riparian vegetation buffer (meters)
        intermittent {bool} -- True to keep intermittent streams. False discard them.
        ephemeral {bool} -- True to keep ephemeral streams. False to discard them.
        max_waterbody {float} -- Area (sqm) of largest waterbody to be retained.
        valley_bottom {str} -- Path to valley bottom polygon layer.
        roads {str} -- Path to polyline roads ShapeFile
        rail {str} -- Path to polyline railway ShapeFile
        canals {str} -- Path to polyline canals ShapeFile
        ownership {str} -- Path to land ownership polygon ShapeFile
        elevation_buffer {float} -- Distance to buffer DEM when sampling elevation
        meta (Dict[str,str]): dictionary of riverscapes metadata key: value pairs
    """

    log = Logger("BRAT Build")
    log.info('HUC: {}'.format(huc))
    log.info('EPSG: {}'.format(cfg.OUTPUT_EPSG))

    augment_layermeta()

    start_time = time.time()

    project_name = 'BRAT for HUC {}'.format(huc)
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'BRAT', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/brat', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True),
        RSMeta('BRAT Version', cfg.version, locked=True),
        RSMeta('BRAT Timestamp', str(int(time.time())))
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Intermediates', 'Outputs'])

    log.info('Adding input rasters to project')
    _existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    _historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historical_veg)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    db_node, _db_path, *_ = project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(output_folder, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # Copy all the original vectors to the inputs geopackage. This will ensure on same spatial reference
    source_layers = {
        'HYDRO_FLOWLINES': hydro_flowlines,
        'HYDRO_IGOS': hydro_igos,
        'HYDRO_DGOS': hydro_dgos,
        'ANTHRO_FLOWLINES': anthro_flowlines,
        'ANTHRO_IGOS': anthro_igos,
        'ANTHRO_DGOS': anthro_dgos,
        'FLOW_AREA': flow_areas,
        'WATERBODIES': waterbodies,
        'VALLEY_BOTTOM': valley_bottom
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(source_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['BRAT_GEOMETRY'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, options=['FID=ReachID'], fields={
            'FCode': ogr.OFTInteger,
            'ReachCode': ogr.OFTString,
            'StreamName': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal,
            'WatershedID': ogr.OFTString,
            'level_path': ogr.OFTReal,
            'ownership': ogr.OFTString,
            'divergence': ogr.OFTReal,
            'stream_order': ogr.OFTInteger,
            'us_state': ogr.OFTString,
            'ecoregion_iii': ogr.OFTString,
            'ecoregion_iv': ogr.OFTString
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['DGO_GEOM'].rel_path, write=True) as dgo_lyr:
        dgo_lyr.create_layer(ogr.wkbPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=DGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTReal,
            'segment_area': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['IGO_GEOM'].rel_path, write=True) as igo_lyr:
        igo_lyr.create_layer(ogr.wkbPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTReal
        })

    db_metadata = {
        'BRAT_Build_DateTime': datetime.datetime.now().isoformat(),
        'Streamside_Buffer': str(streamside_buffer),
        'Riparian_Buffer': str(riparian_buffer),
        'Reach_Codes': ','.join(reach_codes),
        'Canal_Codes': ','.join(canal_codes),
        'Max_Waterbody': str(max_waterbody)
    }

    # Execute the SQL to create the lookup tables in the output geopackage
    watershed_name = create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'brat_schema.sql'))
    # Just for fun add the db metadata back to the xml
    project.add_metadata_simple(db_metadata)

    project.add_metadata([RSMeta('HUC8_Watershed', watershed_name)])

    # Copy the reaches into the output feature class layer, filtering by reach codes
    reach_geometry_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['BRAT_GEOMETRY'].rel_path)
    dgo_geometry_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['DGO_GEOM'].rel_path)
    igo_geometry_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['IGO_GEOM'].rel_path)
    # build_network(segmented_network_path, input_layers['FLOW_AREA'], reach_geometry_path, waterbodies_path=input_layers['WATERBODIES'], waterbody_max_size=max_waterbody, epsg=cfg.OUTPUT_EPSG, reach_codes=reach_codes, create_layer=False)
    copy_features_fields(input_layers['HYDRO_FLOWLINES'], reach_geometry_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['HYDRO_DGOS'], dgo_geometry_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['HYDRO_IGOS'], igo_geometry_path, epsg=cfg.OUTPUT_EPSG)

    # Check that there are features to process
    with GeopackageLayer(outputs_gpkg_path, 'ReachGeometry') as lyr:
        if lyr.ogr_layer.GetFeatureCount() == 0:
            log.info('No features to process; BRAT run complete')
            return

    # with GeopackageLayer(reach_geometry_path, write=True) as reach_lyr:
    #     for feat, *_ in reach_lyr.iterate_features('Add WatershedID to ReachGeometry'):
    #         feat.SetField('WatershedID', huc[:8])
    #         reach_lyr.ogr_layer.SetFeature(feat)

    with SQLiteCon(inputs_gpkg_path) as database:
        database.curs.execute("""CREATE VIEW vwHydroAnthro AS
                              SELECT H.fid, Slope, Length_m, DrainArea, QLow, Q2, SPLow, SP2, iPC_Road, iPC_RoadX, iPC_RoadVB, iPC_Rail, iPC_RailVB, iPC_DivPts, iPC_Privat, iPC_Canal, iPC_LU, iPC_VLowLU, iPC_LowLU, iPC_ModLU, iPC_HighLU, oPC_Dist
                              FROM hydro_flowlines H LEFT JOIN anthro_flowlines A ON H.fid = A.fid""")
        database.curs.execute("""CREATE VIEW vwHydroAnthroDGO AS
                              SELECT H.fid, Slope, Length_m, DrainArea, Qlow, Q2, SPLow, SP2, LUI, Road_len, Rail_len, Canal_len, RoadX_ct, DivPts_ct, Road_prim_len, Road_sec_len, Road_4wd_len
                              FROM hydro_dgos H LEFT JOIN anthro_dgos A ON H.fid = A.fid""")
        database.conn.commit()

    with SQLiteCon(outputs_gpkg_path) as database:
        # Data preparation SQL statements to handle any weird attributes
        database.curs.execute("""ATTACH DATABASE ? AS inputs""", (inputs_gpkg_path,))
        database.curs.execute("""INSERT INTO HydroAnthroReach SELECT * FROM inputs.vwHydroAnthro""")
        database.curs.execute("""INSERT INTO HydroAnthroDGO SELECT * FROM inputs.vwHydroAnthroDGO""")
        database.conn.commit()

        database.curs.execute("""INSERT INTO ReachAttributes (ReachID, WatershedID, ReachCode, StreamName)
                              SELECT ReachID, WatershedID, FCode, StreamName FROM ReachGeometry""")
        database.curs.execute("""INSERT INTO DGOAttributes (DGOID, FCode, level_path, seg_distance, centerline_length, segment_area) 
                              SELECT DGOID, FCode, level_path, seg_distance, centerline_length, segment_area FROM DGOGeometry""")
        database.curs.execute("""INSERT INTO IGOAttributes (IGOID, FCode, level_path, seg_distance) 
                              SELECT IGOID, FCode, level_path, seg_distance FROM IGOGeometry""")
        database.conn.commit()

        database.curs.execute("""SELECT ReachID FROM ReachAttributes""")
        for row in database.curs.fetchall():
            database.curs.execute(f"""UPDATE ReachAttributes SET (iGeo_Slope, iGeo_Len, iGeo_DA, iHyd_QLow, iHyd_Q2, iHyd_SPLow, iHyd_SP2, iPC_Road, iPC_RoadX, iPC_RoadVB, iPC_Rail, iPC_RailVB, iPC_DivPts, iPC_Privat, iPC_Canal, iPC_LU, iPC_VLowLU, iPC_LowLU, iPC_ModLU, iPC_HighLU, oPC_Dist) =
                                  (SELECT Slope, Length_m, DrainArea, QLow, Q2, SPLow, SP2, iPC_Road, iPC_RoadX, iPC_RoadVB, iPC_Rail, iPC_RailVB, iPC_DivPts, iPC_Privat, iPC_Canal, iPC_LU, iPC_VLowLU, iPC_LowLU, iPC_ModLU, iPC_HighLU, oPC_Dist FROM HydroAnthroReach WHERE ReachID = {row['ReachID']})
                                  WHERE ReachID = {row['ReachID']}""")
        database.conn.commit()
        database.curs.execute("""SELECT DGOID FROM DGOAttributes""")
        for row in database.curs.fetchall():
            database.curs.execute(f"""UPDATE DGOAttributes SET (iGeo_Slope, iGeo_DA, iHyd_QLow, iHyd_Q2, iHyd_SPLow, iHyd_SP2) =
                                  (SELECT Slope, DrainArea, Qlow, Q2, SPLow, SP2 FROM HydroAnthroDGO WHERE DGOID = {row['DGOID']})
                                  WHERE DGOID = {row['DGOID']}""")
        database.conn.commit()

        database.curs.execute(f'UPDATE ReachAttributes SET IsPeren = 1 WHERE (ReachCode IN ({", ".join(peren_codes)}))')
        database.curs.execute('UPDATE ReachAttributes SET iGeo_DA = 0 WHERE iGeo_DA IS NULL')
        database.conn.commit()

        database.curs.execute(f'UPDATE DGOAttributes SET WatershedID = {huc} WHERE WatershedID IS NULL')

        # Register vwReaches as a feature layer as well as its geometry column
        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwReaches', data_type, 'Reaches', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'ReachGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwReaches', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'ReachGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwDgos', data_type, 'DGOs', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'DGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwDgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'DGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwIgos', data_type, 'IGOs', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'IGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwIgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'IGOGeometry'""")

        database.conn.commit()

    # copy conflict attributes from reaches to dgos
    # copy_fields_lwa = {field: field for field in ['iPC_Road', 'iPC_RoadX', 'iPC_RoadVB', 'iPC_Rail', 'iPC_RailVB', 'iPC_DivPts', 'iPC_Privat', 'iPC_Canal', 'iPC_LU', 'iPC_VLowLU', 'iPC_LowLU', 'iPC_ModLU', 'iPC_HighLU', 'oPC_Dist']}
    # line_attributes_to_dgo(input_layers['ANTHRO_FLOWLINES'], input_layers['HYDRO_DGOS'], copy_fields_lwa, method='lwa', dgo_table=outputs_gpkg_path)

    # Calculate the vegetation cell counts for each epoch and buffer
    buffer_paths = []
    for label, veg_raster in [('Existing Veg', prj_existing_path), ('Historical Veg', prj_historic_path)]:
        for buffer in [streamside_buffer, riparian_buffer]:
            buffer_path = os.path.join(intermediates_gpkg_path, f'buffer_{int(buffer)}m')
            polygon_path = buffer_path if buffer_path in buffer_paths else None
            vegetation_summary(outputs_gpkg_path, '{} {}m'.format(label, buffer), veg_raster, buffer, polygon_path)
            buffer_paths.append(buffer_path)

    # add buffers to project
    for buffer in [streamside_buffer, riparian_buffer]:
        LayerTypes['INTERMEDIATES'].add_sub_layer(f'{int(buffer)}M_BUFFER', RSLayer(f'{int(buffer)}m Buffer', f'{int(buffer)}M_BUFFER', 'Vector', f'buffer_{int(buffer)}m'))
        LayerTypes['INTERMEDIATES'].sub_layers[f'{int(buffer)}M_BUFFER'].lyr_meta = [RSMeta('Description', f'A polygon of the input drainage network buffered by {int(buffer)} meters.'),
                                                                                     RSMeta('SourceUrl', '', RSMetaTypes.URL),
                                                                                     RSMeta('DataProductVersion', cfg.version),
                                                                                     RSMeta('DocsUrl', f'https://tools.riverscapes.net/brat/data/#{int(buffer)}M_BUFFER', RSMetaTypes.URL)]

    watershed, max_drainage_area, ecoregion = get_watershed_info(outputs_gpkg_path)

    # Calculate the vegetation and combined FIS for the existing and historical vegetation epochs
    for epoch, prefix, ltype, orig_id in Epochs:

        # Calculate the vegetation suitability for each buffer
        [vegetation_suitability(outputs_gpkg_path, buffer, prefix, ecoregion) for buffer in get_stream_buffers(outputs_gpkg_path)]

        # Run the vegetation and then combined FIS for this epoch
        vegetation_fis(outputs_gpkg_path, epoch, prefix)
        combined_fis(outputs_gpkg_path, epoch, prefix, max_drainage_area)

        orig_raster = os.path.join(project.project_dir, proj_nodes['Inputs'].find('Raster[@id="{}"]/Path'.format(orig_id)).text)
        _veg_suit_raster_node, veg_suit_raster = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes[ltype], None, True)
        output_vegetation_raster(outputs_gpkg_path, orig_raster, veg_suit_raster, epoch, prefix, ecoregion)

    # Calculate departure from historical conditions
    with SQLiteCon(outputs_gpkg_path) as database:
        log.info('Calculating departure from historic conditions')
        database.curs.execute('UPDATE ReachAttributes SET mCC_HisDep = mCC_HPE_CT - mCC_EX_CT WHERE (mCC_EX_CT IS NOT NULL) AND (mCC_HPE_CT IS NOT NULL)')
        database.conn.commit()

    conservation(outputs_gpkg_path)

    ellapsed_time = time.time() - start_time

    project.add_project_geopackage(proj_nodes['Intermediates'], LayerTypes['INTERMEDIATES'])
    project.add_metadata([
        RSMeta("BratBuildProcTimeS", "{:.2f}".format(ellapsed_time), RSMetaTypes.INT),
        RSMeta("BratBuildProcTimeHuman", pretty_duration(ellapsed_time))
    ])

    log.info('BRAT build completed successfully.')


def get_watershed_info(gpkg_path):
    """Query a BRAT database and get information about
    the watershed being run. Assumes that all watersheds
    except the one being run have been deleted.

    Arguments:
        database {str} -- Path to the BRAT SQLite database

    Returns:
        [tuple] -- WatershedID, max drainage area, EcoregionID with which
        the watershed is associated.
    """

    with SQLiteCon(gpkg_path) as database:
        database.curs.execute('SELECT WatershedID, MaxDrainage, EcoregionID FROM Watersheds')
        row = database.curs.fetchone()
        watershed = row['WatershedID']
        max_drainage = row['MaxDrainage']
        ecoregion = row['EcoregionID']

    log = Logger('BRAT Run')

    if not watershed:
        raise Exception('Missing watershed in BRAT datatabase {}'.format(database))

    if not max_drainage:
        log.warning('Missing max drainage for watershed {}'.format(watershed))

    if not ecoregion:
        raise Exception('Missing ecoregion for watershed {}'.format(watershed))

    return watershed, max_drainage, ecoregion


def get_stream_buffers(gpkg_path):
    """Get the list of buffers used to sample the vegetation.
    Assumes that the vegetation has already been sample and that
    the streamside and riparian buffers are the only values in
    the ReachVegetation database table.

    Arguments:
        database {str} -- Path to the BRAT database

    Returns:
        [list] -- all discrete vegetation buffers
    """

    with SQLiteCon(gpkg_path) as database:
        database.curs.execute('SELECT Buffer FROM ReachVegetation GROUP BY Buffer')
        return [row['Buffer'] for row in database.curs.fetchall()]


def augment_layermeta():
    """
    For RSContext we've written a JSON file with extra layer meta. We may use this pattern elsewhere but it's just here for now
    """
    with open(LYR_DESCRIPTIONS_JSON, 'r') as f:
        json_data = json.load(f)

    for k, lyr in LayerTypes.items():
        if lyr.sub_layers is not None:
            for h, sublyr in lyr.sub_layers.items():
                if h in json_data and len(json_data[h]) > 0:
                    sublyr.lyr_meta = [
                        RSMeta('Description', json_data[h][0]),
                        RSMeta('SourceUrl', json_data[h][1], RSMetaTypes.URL),
                        RSMeta('DataProductVersion', json_data[h][2]),
                        RSMeta('DocsUrl', 'https://tools.riverscapes.net/brat/data/#{}'.format(sublyr.id), RSMetaTypes.URL)
                    ]
        if k in json_data and len(json_data[k]) > 0:
            lyr.lyr_meta = [
                RSMeta('Description', json_data[k][0]),
                RSMeta('SourceUrl', json_data[k][1], RSMetaTypes.URL),
                RSMeta('DataProductVersion', json_data[k][2]),
                RSMeta('DocsUrl', 'https://tools.riverscapes.net/brat/data/#{}'.format(lyr.id), RSMetaTypes.URL)
            ]


def main():
    """ Main BRAT Build routine
    """

    parser = argparse.ArgumentParser(
        description='Build the inputs for an eventual brat_run:',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='huc input', type=str)

    parser.add_argument('hillshade', help='hillshade input', type=str)

    parser.add_argument('hydro_flowlines', help='hydro flowlines input', type=str)
    parser.add_argument('hydro_igos', help='hydro igos input', type=str)
    parser.add_argument('hydro_dgos', help='hydro dgos input', type=str)
    parser.add_argument('anthro_flowlines', help='anthro flowlines input', type=str)
    parser.add_argument('anthro_igos', help='anthro igos input', type=str)
    parser.add_argument('anthro_dgos', help='anthro dgos input', type=str)
    parser.add_argument('existing_veg', help='existing_veg input', type=str)
    parser.add_argument('historical_veg', help='historical_veg input', type=str)

    parser.add_argument('valley_bottom', help='Valley bottom shapeFile', type=str)

    parser.add_argument('streamside_buffer', help='streamside_buffer input', type=float)
    parser.add_argument('riparian_buffer', help='riparian_buffer input', type=float)

    parser.add_argument('output_folder', help='output_folder input', type=str)

    parser.add_argument('--reach_codes', help='Comma delimited reach codes (FCode) to retain when filtering features. Omitting this option retains all features.', type=str)
    parser.add_argument('--canal_codes', help='Comma delimited reach codes (FCode) representing canals. Omitting this option retains all features.', type=str)
    parser.add_argument('--peren_codes', help='Comma delimited reach codes (FCode) representing perennial features', type=str)
    parser.add_argument('--flow_areas', help='(optional) path to the flow area polygon feature class containing artificial paths', type=str)
    parser.add_argument('--waterbodies', help='(optional) waterbodies input', type=str)
    parser.add_argument('--max_waterbody', help='(optional) maximum size of small waterbody artificial flows to be retained', type=float)

    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)

    # Substitute patterns for environment varaibles
    args = dotenv.parse_args_env(parser)

    reach_codes = args.reach_codes.split(',') if args.reach_codes else None
    canal_codes = args.canal_codes.split(',') if args.canal_codes else None
    peren_codes = args.peren_codes.split(',') if args.peren_codes else None

    # Initiate the log file
    log = Logger("BRAT Build")
    log.setup(logPath=os.path.join(args.output_folder, "brat_build.log"), verbose=args.verbose)
    log.title('BRAT Build Tool For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'brat_build_memusage.log')
            retcode, max_obj = ThreadRun(brat_build, memfile,
                                         args.huc, args.hydro_flowlines, args.hydro_igos, args.hydro_dgos,
                                         args.anthro_flowlines, args.anthro_igos, args.anthro_dgos,
                                         args.hillshade, args.existing_veg, args.historical_veg, args.output_folder,
                                         args.streamside_buffer, args.riparian_buffer,
                                         reach_codes, canal_codes, peren_codes,
                                         args.flow_areas, args.waterbodies, args.max_waterbody,
                                         args.valley_bottom, meta
                                         )
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))
        else:
            brat_build(
                args.huc, args.hydro_flowlines, args.hydro_igos, args.hydro_dgos,
                args.anthro_flowlines, args.anthro_igos, args.anthro_dgos,
                args.hillshade, args.existing_veg, args.historical_veg, args.output_folder,
                args.streamside_buffer, args.riparian_buffer,
                reach_codes, canal_codes, peren_codes,
                args.flow_areas, args.waterbodies, args.max_waterbody,
                args.valley_bottom, meta
            )

    except Exception as ex:
        log.error(ex)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
