""" Riparian Condition Assessment Tool

Jordan Gilbert Jan 2023
"""

import argparse
import os
import time
import datetime
import sys
import traceback
import rasterio

from typing import Dict
from osgeo import ogr

from rscommons import initGDALOGRErrors, ModelConfig, RSLayer, RSProject, get_shp_or_gpkg
from rscommons import Logger, GeopackageLayer
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons import dotenv
from rscommons.vector_ops import copy_feature_class, get_geometry_unary_union
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields
from rscommons.util import parse_metadata, pretty_duration
from rscommons.moving_window import moving_window_dgo_ids
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta

from rcat.lib.veg_rasters import rcat_rasters
from rcat.lib.dgo_vegetation import dgo_vegetation
from rcat.lib.reach_vegetation import vegetation_summary
from rcat.lib.rcat_attributes import igo_attributes, reach_attributes
from rcat.lib.floodplain_accessibility import flooplain_access
from rcat.lib.reach_dgos import reach_dgos
from rcat.lib.rcat_fis import rcat_fis
from rcat.rcat_report import RcatReport
from rcat.__version__ import __version__

Path = str


initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'HILLSHADE': RSLayer('Hillshade', 'HILLSHADE', 'Raster', 'inputs/hillshade.tif'),
    'EXVEG': RSLayer('Existing Vegetation', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HISTVEG': RSLayer('Historic Vegetation', 'HISTVEG', 'Raster', 'inputs/historic_veg.tif'),
    'EXRIPARIAN': RSLayer('Existing Riparian', 'EXRIPARIAN', 'Raster', 'intermediates/ex_riparian.tif'),
    'HISTRIPARIAN': RSLayer('Historic Riparian', 'HISTRIPARIAN', 'Raster', 'intermediates/hist_riparian.tif'),
    'EXVEGETATED': RSLayer('Existing Vegetated', 'EXVEGETATED', 'Raster', 'intermediates/ex_vegetated.tif'),
    'HISTVEGETATED': RSLayer('Historic Vegetated', 'HISTVEGETATED', 'Raster', 'intermediates/hist_vegetated.tif'),
    'CONVERSION': RSLayer('Conversion Raster', 'CONVERSION', 'Raster', 'intermediates/conversion.tif'),
    'PITFILL': RSLayer('Pitfilled DEM', 'PITFILL', 'Raster', 'inputs/pitfill.tif'),
    'D8FLOWDIR': RSLayer('D8 Flow Direction', 'D8FLOWDIR', 'Raster', 'intermediates/d8_flow_dir.tif'),
    # rasterized infrastructure...
    'FPACCESS': RSLayer('Floodplain Accessibility', 'FPACCESS', 'Raster', 'intermediates/fp_access.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'ANTHROIGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'ANTHRODGO': RSLayer('Discrete Geographic Objects', 'DGO', 'Vector', 'dgo'),
        'ANTHROREACHES': RSLayer('Segmented Flowlines', 'REACHES', 'Vector', 'reaches'),
        'CANALS': RSLayer('Canals', 'CANAL', 'Vector', 'canals'),
        'ROADS': RSLayer('Roads', 'ROADS', 'Vector', 'roads'),
        'RAILS': RSLayer('Railroads', 'RAIL', 'Vector', 'rails'),
        'VALLEYBOTTOM': RSLayer('Valley Bottom', 'VALLEY', 'Vector', 'valley_bottom'),
        'FLOW_AREA': RSLayer('NHD Flow Area', 'FLOW_AREA', 'Vector', 'flowareas'),
        'WATERBODIES': RSLayer('NHD Waterbody', 'WATERBODIES', 'Vector', 'waterbodies')
    }),
    # 'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/intermediates.gpkg', {}),
    'OUTPUTS': RSLayer('RCAT Outputs', 'OUTPUTS', 'Geopackage', 'outputs/rcat.gpkg', {
        'GEOM_POINTS': RSLayer('RCAT IGO Point Geometry', 'RCAT_GEOM_POINTS', 'Vector', 'IGOGeometry'),
        'IGO': RSLayer('RCAT Output Points', 'RCAT_POINTS', 'Vector', 'vwIgos'),
        'GEOM_LINES': RSLayer('RCAT Reach Geometry', 'RCAT_GEOM_LINES', 'Vector', 'ReachGeometry'),
        'REACHES': RSLayer('RCAT Output Lines', 'RCAT_LINES', 'Vector', 'vwReaches'),
        'GEOM_DGOS': RSLayer('RCAT Output DGOs Polygons', 'RCAT_GEOM_DGOS', 'Vector', 'DGOGeometry'),
        'DGOS': RSLayer('RCAT Output DGOs', 'RCAT_DGOS', 'Vector', 'vwDgos')
    }),
    'REPORT': RSLayer('RCAT Report', 'REPORT', 'HTMLFile', 'outputs/rcat.html')
}


def rcat(huc: int, existing_veg: Path, historic_veg: Path, hillshade: Path, pitfilled: Path, igo: Path, dgo: Path,
         reaches: Path, roads: Path, rails: Path, canals: Path, valley: Path, output_folder: Path,
         flow_areas: Path, waterbodies: Path, meta: Dict[str, str]):

    log = Logger('RCAT')
    log.info(f'HUC: {huc}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')

    augment_layermeta('rcat', LYR_DESCRIPTIONS_JSON, LayerTypes)

    start_time = time.time()

    project_name = f'RCAT for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'RCAT', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/rcat', RSMetaTypes.URL, locked=True),
        RSMeta(f'HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True),
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Intermediates', 'Outputs'])

    log.info('Adding input rasters to project')
    _prj_existing_path_node, prj_existing_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)
    _prj_historic_path_node, prj_historic_path = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HISTVEG'], historic_veg)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['PITFILL'], pitfilled)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_folder, LayerTypes['INPUTS'].rel_path)
    outputs_gpkg_path = os.path.join(output_folder, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # copy original vectors to inputs geopackage
    src_layers = {
        'ANTHROIGO': igo,
        'ANTHRODGO': dgo,
        'ANTHROREACHES': reaches,
        'VALLEYBOTTOM': valley,
        'CANALS': canals,
        'ROADS': roads,
        'RAILS': rails,
        'FLOW_AREA': flow_areas,
        'WATERBODIES': waterbodies
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GEOM_POINTS'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'LevelPathI': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'LUI': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GEOM_DGOS'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=DGOID'], fields={
            'LevelPathI': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTInteger,
            'segment_area': ogr.OFTReal,
            'LUI': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GEOM_LINES'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiLineString, epsg=cfg.OUTPUT_EPSG, options=['FID=ReachID'], fields={
            'WatershedID': ogr.OFTString,
            'ReachCode': ogr.OFTInteger,
            'StreamName': ogr.OFTString,
            'NHDPlusID': ogr.OFTReal,
            'iPC_LU': ogr.OFTReal
        })

    db_metadata = {
        'RCATDateTime': datetime.datetime.now().isoformat(),
    }

    create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'rcat_schema.sql'))

    igo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['GEOM_POINTS'].rel_path)
    reach_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['GEOM_LINES'].rel_path)
    dgo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['GEOM_DGOS'].rel_path)
    copy_features_fields(input_layers['ANTHROIGO'], igo_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['ANTHROREACHES'], reach_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['ANTHRODGO'], dgo_geom_path, epsg=cfg.OUTPUT_EPSG)

    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute('INSERT INTO ReachAttributes (ReachID, ReachCode, WatershedID, StreamName, NHDPlusID, iPC_LU) SELECT ReachID, ReachCode, WatershedID, StreamName, NHDPlusID, iPC_LU FROM ReachGeometry')
        database.curs.execute('INSERT INTO IGOAttributes (IGOID, LevelPathI, seg_distance, stream_size, LUI) SELECT IGOID, LevelPathI, seg_distance, stream_size, LUI FROM IGOGeometry')
        database.curs.execute('INSERT INTO DGOAttributes (DGOID, LevelPathI, seg_distance, centerline_length, segment_area, LUI) SELECT DGOID, LevelPathI, seg_distance, centerline_length, segment_area, LUI FROM DGOGeometry')

        # Register vwReaches as a feature layer as well as its geometry column
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

        database.conn.execute('CREATE INDEX ix_igo_levelpath on IGOGeometry(LevelPathI)')
        database.conn.execute('CREATE INDEX ix_igo_segdist on IGOGeometry(seg_distance)')
        database.conn.execute('CREATE INDEX ix_igo_size on IGOGeometry(stream_size)')
        database.conn.execute('CREATE INDEX ix_dgo_levelpath on DGOGeometry(LevelPathI)')
        database.conn.execute('CREATE INDEX ix_dgo_segdist on DGOGeometry(seg_distance)')

        database.conn.commit()

        database.curs.execute('SELECT DISTINCT LevelPathI FROM IGOGeometry')
        levelps = database.curs.fetchall()
        levelpathsin = [lp['LevelPathI'] for lp in levelps]

    with SQLiteCon(inputs_gpkg_path) as db:
        db.conn.execute('CREATE INDEX ix_dgo_levelpath on dgo(LevelPathI)')
        db.conn.execute('CREATE INDEX ix_dgo_segdist on dgo(seg_distance)')
        db.conn.commit()

    distance_in = {
        '0': 200,
        '1': 400,
        '2': 1200,
        '3': 2000,
        '4': 8000
    }

    project.add_metadata(
        [RSMeta('Small Search Window', str(distance_in['0']), RSMetaTypes.INT, locked=True),
         RSMeta('Medium Search Window', str(distance_in['1']), RSMetaTypes.INT, locked=True),
         RSMeta('Large Search Window', str(distance_in['2']), RSMetaTypes.INT, locked=True),
         RSMeta('Very Large Search Window', str(distance_in['3']), RSMetaTypes.INT, locked=True),
         RSMeta('Huge Search Window', str(distance_in['4']), RSMetaTypes.INT, locked=True)])

    log.info('removing large rivers from DGO polygons')
    with rasterio.open(prj_existing_path) as veg_raster:
        gt = veg_raster.transform
        x_res = gt[0]

    if flow_areas:
        with get_shp_or_gpkg(flow_areas) as flow_areas_lyr:
            flow_area_ftr_ct = flow_areas_lyr.ogr_layer.GetFeatureCount()
        if flow_area_ftr_ct > 0:
            geom_flow_areas = get_geometry_unary_union(flow_areas)
            if geom_flow_areas.type == 'MultiPolygon':
                for g in geom_flow_areas.geoms:
                    if not g.is_valid:
                        g.make_valid()
            elif geom_flow_areas.type == 'Polygon':
                if not geom_flow_areas.is_valid:
                    geom_flow_areas.make_valid()
        else:
            geom_flow_areas = None
    else:
        geom_flow_areas = None

    if waterbodies:
        with get_shp_or_gpkg(waterbodies) as waterbodies_lyr:
            waterbody_ftr_ct = waterbodies_lyr.ogr_layer.GetFeatureCount()
        if waterbody_ftr_ct > 0:
            geom_waterbodies = get_geometry_unary_union(waterbodies)
            if geom_waterbodies.type == 'MultiPolygon':
                for g in geom_waterbodies.geoms:
                    if not g.is_valid:
                        g.make_valid()
            elif geom_waterbodies.type == 'Polygon':
                if not geom_waterbodies.is_valid:
                    geom_waterbodies.make_valid()
        else:
            geom_waterbodies = None
    else:
        geom_waterbodies = None

    updated_dgo_geoms = {}
    with GeopackageLayer(outputs_gpkg_path, 'DGOGeometry') as dgo_lyr:
        for dgo_ftr, *_ in dgo_lyr.iterate_features('Clipping large rivers from DGO polygons'):
            dgoid = dgo_ftr.GetFID()
            dgo_ogr = dgo_ftr.GetGeometryRef()
            dgo_geom = GeopackageLayer.ogr2shapely(dgo_ogr)
            geom = dgo_geom.buffer(x_res / 2)  # buffer by landfire resolution to make sure cells are captured in small streams
            if geom_flow_areas is not None:
                if geom.intersects(geom_flow_areas):
                    geom = geom.difference(geom_flow_areas)
            if geom_waterbodies is not None:
                if geom.intersects(geom_waterbodies):
                    geom = geom.difference(geom_waterbodies)

            updated_dgo_geoms[dgoid] = geom

    # get IDs of DGOs within moving windows of IGOs
    windows = moving_window_dgo_ids(igo_geom_path, input_layers['ANTHRODGO'], levelpathsin, distance_in)

    # store dgos associated with reaches with large rivers removed
    rdgos = reach_dgos(os.path.join(outputs_gpkg_path, 'ReachGeometry'), input_layers['ANTHRODGO'],
                       os.path.join(output_folder, LayerTypes['EXVEG'].rel_path), geom_flow_areas, geom_waterbodies, x_res)

    # generate vegetation derivative rasters
    intermediates = os.path.join(output_folder, 'intermediates')
    if not os.path.isdir(intermediates):
        os.mkdir(intermediates)
    rcat_rasters(prj_existing_path, prj_historic_path, outputs_gpkg_path, intermediates)

    # floodplain accessibility raster
    fp_access = os.path.join(output_folder, LayerTypes['FPACCESS'].rel_path)
    flooplain_access(pitfilled, input_layers['VALLEYBOTTOM'], input_layers['ANTHROREACHES'], intermediates, fp_access,
                     input_layers['ROADS'], input_layers['RAILS'], input_layers['CANALS'])

    # Add intermediate rasters to xml
    exrip_node, exrip_ras = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['EXRIPARIAN'])
    histrip_node, histrip_ras = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['HISTRIPARIAN'])
    exveg_node, exveg_ras = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['EXVEGETATED'])
    histveg_node, histveg_ras = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['HISTVEGETATED'])
    conv_node, conv_ras = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['CONVERSION'])
    flowdir_node, flowdir_ras = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['D8FLOWDIR'])
    fpaccess_node, fpacess_ras = project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['FPACCESS'])
    proj_rasters = [[exrip_node, exrip_ras], [histrip_node, histrip_ras], [exveg_node, exveg_ras], [histveg_node, histveg_ras],
                    [conv_node, conv_ras], [flowdir_node, flowdir_ras], [fpaccess_node, fpacess_ras]]

    # sample accessibility and vegetation and derivative rasters onto igos and reaches using moving windows/dgos
    int_rasters = ['fp_access.tif', 'ex_riparian.tif', 'hist_riparian.tif', 'ex_vegetated.tif', 'hist_vegetated.tif', 'conversion.tif']
    int_raster_paths = [os.path.join(intermediates, i) for i in int_rasters]
    int_raster_paths.append(prj_existing_path)
    int_raster_paths.append(prj_historic_path)
    for rast in int_raster_paths:
        dgo_vegetation(rast, updated_dgo_geoms, outputs_gpkg_path)
        vegetation_summary(outputs_gpkg_path, rdgos, rast)
    igo_attributes(outputs_gpkg_path, windows)
    reach_attributes(outputs_gpkg_path)

    # Calculate FIS for IGOs
    rcat_fis(outputs_gpkg_path, igos=True)
    # Calculate FIS for reaches
    rcat_fis(outputs_gpkg_path, igos=False)

    ellapsed = time.time() - start_time

    project.add_metadata([
        RSMeta("ProcTimeS", "{:.2f}".format(ellapsed), RSMetaTypes.HIDDEN, locked=True),
        RSMeta("ProcessingTime", pretty_duration(ellapsed), locked=True)
    ])

    # add raster resolution metadata
    for ras in proj_rasters:
        raster_resolution_meta(project, ras[1], ras[0])

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)
    report = RcatReport(report_path, project)
    report.write()

    log.info('RCAT completed successfully')


def main():
    parser = argparse.ArgumentParser(
        description='RCAT'
    )

    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('existing_veg', help='National existing vegetation raster', type=str)
    parser.add_argument('historic_veg', help='National historic vegetation raster', type=str)
    parser.add_argument('hillshade', help='Hillshade raster', type=str)
    parser.add_argument('pitfilled', help='Pit filled DEM raster', type=str)
    parser.add_argument('igo', help='Integrated geographic object with anthro attributes', type=str)
    parser.add_argument('dgo', help='Discrete geographic objects', type=str)
    parser.add_argument('reaches', help='Stream reaches with anthro attributes', type=str)
    parser.add_argument('roads', help='Roads layer', type=str)
    parser.add_argument('rails', help='Railroad layer', type=str)
    parser.add_argument('canals', help='Canals layer', type=str)
    parser.add_argument('valley', help='Valley bottom layer', type=str)
    parser.add_argument('output_folder', help='Output folder', type=str)
    parser.add_argument('--flow_areas', help='(optional) path to the flow area polygon feature class containing artificial paths', type=str)
    parser.add_argument('--waterbodies', help='(optional) waterbodies input', type=str)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help="(optional) save intermediate outputs for debugging", action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    meta = parse_metadata(args.meta)

    # Initiate the log file
    log = Logger('RCAT')
    log.setup(logPath=os.path.join(args.output_folder, "rcat.log"), verbose=args.verbose)
    log.title(f'RCAT for HUC: {args.huc}')

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'rcat_mem.log')
            retcode, max_obj = ThreadRun(rcat, memfile, args.huc,
                                         args.existing_veg, args.historic_veg, args.hillshade, args.pitfilled, args.igo,
                                         args.dgo, args.reaches, args.roads, args.rails, args.canals,
                                         args.valley, args.output_folder, args.flow_areas, args.waterbodies,
                                         meta=meta)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')

        else:
            rcat(args.huc, args.existing_veg, args.historic_veg, args.hillshade, args.pitfilled, args.igo, args.dgo,
                 args.reaches, args.roads, args.rails, args.canals, args.valley, args.output_folder, args.flow_areas,
                 args.waterbodies, meta=meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
