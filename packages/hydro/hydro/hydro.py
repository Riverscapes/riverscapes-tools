""" Build a Hydrologic Context project

Jordan Gilbert June 2024"""

import argparse
import os
import traceback
import sys
import time
import datetime
from typing import Dict
from osgeo import ogr

from rscommons.util import pretty_duration, parse_metadata
from rscommons import Logger, initGDALOGRErrors, dotenv
from rscommons import RSLayer, RSProject, ModelConfig
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons import GeopackageLayer
from rscommons.vector_ops import copy_feature_class
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions

from hydro.utils.feature_geometry import reach_geometry, dgo_geometry
from hydro.utils.hydrology import hydrology
from hydro.hydro_report import HydroReport
from hydro.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'DEM': RSLayer('Digital Elevation Model', 'DEM', 'Raster', 'inputs/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Hydrologic Inputs', 'INPUTS', 'Geopackage', 'inputs/hydro_inputs.gpkg', {
        'IGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'DGO': RSLayer('Discrete Geographic Objects', 'DGO', 'Vector', 'dgo'),
        'FLOWLINES': RSLayer('Segmented Flowlines', 'FLOWLINES', 'Vector', 'flowlines'),
    }),
    'OUTPUTS': RSLayer('Hydrologic Outputs', 'OUTPUTS', 'Geopackage', 'outputs/hydro.gpkg', {
        'HYDRO_GEOM_POINTS': RSLayer('Hydrologic IGO Point Geometry', 'HYDRO_GEOM_POINTS', 'Vector', 'IGOGeometry'),
        'HYDRO_POINTS': RSLayer('Hydrologic Output Points', 'ANTHRO_POINTS', 'Vector', 'vwIgos'),
        'HYDRO_GEOM_LINES': RSLayer('Hydrologic Reach Geometry', 'HYDRO_GEOM_LINES', 'Vector', 'ReachGeometry'),
        'HYDRO_LINES': RSLayer('Hydrologic Output Lines', 'HYDRO_LINES', 'Vector', 'vwReaches'),
        'HYDRO_GEOM_DGOS': RSLayer('Hydrologic Output DGOs Polygons', 'HYDRO_GEOM_DGOS', 'Vector', 'DGOGeometry'),
        'HYDRO_DGOS': RSLayer('Hydrologic Output DGOs', 'HYDRO_DGOS', 'Vector', 'vwDgos')
    }),
    'REPORT': RSLayer('Hydrologic Context Report', 'REPORT', 'HTMLFile', 'outputs/hydro.html')
}


def hydro_context(huc: int, dem: Path, hillshade: Path, igo: Path, dgo: Path, flowlines: Path,
                  output_folder: Path, meta: Dict[str, str]):

    log = Logger('Hydrologic Context')
    log.info(f'Starting Hydrologic Context v.{cfg.version}')
    log.info(f'HUC: {huc}')

    augment_layermeta('hydro', LYR_DESCRIPTIONS_JSON, LayerTypes)

    start_time = time.time()

    project_name = f'Hydrologic Context for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'hydro_context', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/hydro', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Outputs'])

    log.info('Adding input rasters to project')
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    dem_node, dem_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], dem)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # copy original vectors to inputs geopackage
    src_layers = {
        'IGO': igo,
        'DGO': dgo,
        'FLOWLINES': flowlines
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_POINTS'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'centerline_length': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_DGOS'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=DGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTReal,
            'segment_area': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_LINES'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, options=['FID=ReachID'], fields={
            'FCode': ogr.OFTInteger,
            'TotDASqKM': ogr.OFTReal,
            'DivDASqKM': ogr.OFTReal,
            'GNIS_Name': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal,
            'level_path': ogr.OFTReal,
            'ownership': ogr.OFTString,
            'divergence': ogr.OFTInteger,
            'stream_order': ogr.OFTInteger,
            'us_state': ogr.OFTString,
            'ecoregion_iii': ogr.OFTString,
            'ecoregion_iv': ogr.OFTString,
            'WatershedID': ogr.OFTString
        })

    db_metadata = {
        'Hydro DateTime': datetime.datetime.now().isoformat()
    }
    create_database(str(huc), outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'hydro_schema.sql'))

    # add a table entry that corresponds to the huc being run if doesn't exist.
    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute(f"SELECT * FROM Watersheds WHERE WatershedID = '{huc}'")
        if not database.curs.fetchone():
            database.curs.execute(f"SELECT * FROM Watersheds WHERE WatershedID = '{str(huc)[:8]}'")
            row = database.curs.fetchone()
            database.curs.execute(f"""INSERT INTO Watersheds (WatershedID, Name, States, QLow, Q2, MaxDrainage, EcoregionID)
                                  VALUES ('{huc}', '{row['Name']}', '{row['States']}', '{row['QLow']}', '{row['Q2']}', {row['MaxDrainage']}, {row['EcoregionID']})""")
            database.conn.commit()

    igo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_POINTS'].rel_path)
    line_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_LINES'].rel_path)
    dgo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['HYDRO_GEOM_DGOS'].rel_path)
    copy_features_fields(input_layers['IGO'], igo_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['FLOWLINES'], line_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['DGO'], dgo_geom_path, epsg=cfg.OUTPUT_EPSG)

    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute(f"""CREATE VIEW vwHydroParams AS SELECT W.WatershedID, W.Name AS Watershed, W.States, W.Metadata,
                                  E.EcoregionID, E.Name AS Ecoregion, HP.ParamID, HP.Name AS Parameter, HP.Aliases, HP.DataUnits, HP.EquationUnits,
                                  WHP.Value, HP.Conversion, WHP.Value * HP.Conversion AS ConvertedValue
                                  FROM Watersheds W INNER JOIN Ecoregions E ON W.EcoregionID = E.EcoregionID INNER JOIN
                                  WatershedHydroParams WHP ON W.WatershedID = WHP.WatershedID INNER JOIN HydroParams HP ON WHP.ParamID = HP.ParamID
                                  WHERE W.WatershedID LIKE '{str(huc)[:8]}%' ORDER BY LENGTH(W.WatershedID) DESC""")

        database.curs.execute("""INSERT INTO ReachAttributes (ReachID, FCode, NHDPlusID, WatershedID, StreamName, level_path, ownership, divergence, stream_order, us_state, ecoregion_iii, ecoregion_iv, DrainArea)
                              SELECT ReachID, FCode, NHDPlusID, WatershedID, GNIS_Name, level_path, ownership, divergence, stream_order, us_state, ecoregion_iii, ecoregion_iv, DivDASqKM FROM ReachGeometry""")
        database.curs.execute("""INSERT INTO DGOAttributes (DGOID, FCode, level_path, seg_distance, centerline_length, segment_area)
                              SELECT DGOID, FCode, level_path, seg_distance, centerline_length, segment_area FROM DGOGeometry""")
        database.curs.execute("""INSERT INTO IGOAttributes (IGOID, FCode, level_path, seg_distance, stream_size)
                              SELECT IGOID, FCode, level_path, seg_distance, stream_size FROM IGOGeometry""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwReaches', data_type, 'Reaches', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'ReachGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwReaches', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'ReachGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwIgos', data_type, 'igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'IGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwIgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'IGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'vwDgos', data_type, 'dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'DGOGeometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'vwDgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'DGOGeometry'""")

        database.conn.execute('CREATE INDEX ix_igogeometry_fcode on IGOGeometry(FCode)')
        database.conn.execute('CREATE INDEX ix_igogeometry_levelpath on IGOGeometry(level_path, seg_distance)')
        database.conn.execute('CREATE INDEX ix_igogeometry_size on IGOGeometry(stream_size)')

        database.conn.execute('CREATE INDEX ix_dgogeometry_fcode on DGOGeometry(FCode)')
        database.conn.execute('CREATE INDEX ix_dgogeometry_levelpath on DGOGeometry(level_path, seg_distance)')

        database.conn.commit()

        # database.curs.execute(f'UPDATE ReachAttributes SET WatershedID = {huc}')
        database.curs.execute(f'UPDATE DGOAttributes SET WatershedID = {huc}')
        database.curs.execute(f'UPDATE IGOAttributes SET WatershedID = {huc}')

        database.conn.commit()

    # Associate DGOs with corresponding IGOs
    dgo_igo = {}
    with GeopackageLayer(outputs_gpkg_path, 'DGOGeometry') as dgo_lyr, \
            GeopackageLayer(outputs_gpkg_path, 'IGOGeometry') as igo_lyr:
        for dgo_feat, *_ in dgo_lyr.iterate_features():
            dgo_id = dgo_feat.GetFID()
            lp = dgo_feat.GetField('level_path')
            seg_dist = dgo_feat.GetField('seg_distance')
            if lp is None or seg_dist is None:
                continue
            for igo_feat, *_ in igo_lyr.iterate_features(clip_shape=dgo_feat.GetGeometryRef()):
                if igo_feat:
                    igo_id = igo_feat.GetFID()
                    dgo_igo[dgo_id] = igo_id
                    break

    # Calculate slope, length, drainage area for reaches and DGOs
    reach_geometry(outputs_gpkg_path, dem_path, 100)
    dgo_geometry(outputs_gpkg_path, dem_path)

    # get rid of zero and null drainage areas
    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute('SELECT DrainArea FROM ReachAttributes WHERE DrainArea != 0 AND DrainArea IS NOT NULL')
        das = [row['DrainArea'] for row in database.curs.fetchall()]
        minval = min(das)
        database.curs.execute(f'UPDATE ReachAttributes SET DrainArea = {minval} WHERE DrainArea = 0 OR DrainArea IS NULL')
        database.conn.commit()

    # Calculate discharge and stream power values
    for suf in ['Low', '2']:
        hydrology(outputs_gpkg_path, suf, str(huc))

    # copy values from DGOs to IGOs
    with SQLiteCon(outputs_gpkg_path) as database:
        for dgo_id, igo_id in dgo_igo.items():
            database.curs.execute(f'UPDATE IGOAttributes SET ElevMax = (SELECT ElevMax FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET ElevMin = (SELECT ElevMin FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET Length_m = (SELECT Length_m FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET Slope = (SELECT Slope FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET DrainArea = (SELECT DrainArea FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET QLow = (SELECT QLow FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET Q2 = (SELECT Q2 FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET SPLow = (SELECT SPLow FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
            database.curs.execute(f'UPDATE IGOAttributes SET SP2 = (SELECT SP2 FROM DGOAttributes WHERE DGOID = {dgo_id}) WHERE IGOID = {igo_id}')
        database.conn.commit()

    ellapsed_time = time.time() - start_time

    project.add_metadata([
        RSMeta('ProcTimeS', f'{ellapsed_time:.2f}', RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Processing Time', pretty_duration(ellapsed_time), locked=True)]
    )

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

    report_path = os.path.join(output_folder, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    report = HydroReport(report_path, project)
    report.write()

    log.info('Hydrologic Context completed successfully')


def main():

    parser = argparse.ArgumentParser(description='Build a Hydrologic Context project')
    parser.add_argument('huc', type=int, help='Hydrologic Unit Code')
    parser.add_argument('dem', type=str, help='Path to DEM raster')
    parser.add_argument('hillshade', type=str, help='Path to hillshade raster')
    parser.add_argument('igo', type=str, help='Path to IGO feature class')
    parser.add_argument('dgo', type=str, help='Path to DGO feature class')
    parser.add_argument('flowlines', type=str, help='Path to flowlines feature class')
    parser.add_argument('output_folder', type=str, help='Output folder')
    parser.add_argument('--meta', type=str, help='Metadata in JSON format', default='{}')
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) run in debug mode', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    log = Logger('Hydrologic Context')
    log.setup(logPath=os.path.join(args.output_folder, 'hydro.log'), verbose=args.verbose)
    log.title(f'Hydrologic Context for HUC: {args.huc}')

    meta = parse_metadata(args.meta)

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_folder, 'hydro_memusage.log')
            retcode, max_obj = ThreadRun(hydro_context, memfile,
                                         args.huc, args.dem, args.hillshade, args.igo, args.dgo,
                                         args.flowlines, args.output_folder, meta)
            log.debug(f'Return code: {retcode}, Max memory usage: {max_obj}')
        else:
            hydro_context(args.huc, args.dem, args.hillshade, args.igo, args.dgo, args.flowlines, args.output_folder, meta)

    except Exception as ex:
        log.error(f'Error: {ex}')
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
