import argparse
import os
import sys
import time
import datetime
from typing import Dict, List

from osgeo import gdal, ogr

from rscommons import GeopackageLayer
from rscommons.util import parse_metadata, pretty_duration
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.vector_ops import copy_feature_class
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv
from rscommons.database import create_database, SQLiteCon
from rscommons.copy_features import copy_features_fields
from rscommons.moving_window import moving_window_dgo_ids
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions, raster_resolution_meta
from vbet.vbet_raster_ops import proximity_raster

from grazing.__version__ import __version__
from grazing.utils.water_raster import combine_water_features, create_water_raster
from grazing.utils.veg_suitability import vegetation_suitability


Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'lyr_descriptions.json')

LayerTypes = {
    'EXVEG': RSLayer('Existing Land Cover', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'SLOPE': RSLayer('DEM Slope', 'SLOPE', 'Raster', 'inputs/dem_slope.tif'),
    'INPUTS': RSLayer('Grazing Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'IGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'DGO': RSLayer('Discrete Geographic Object', 'DGO', 'Vector', 'dgo'),
        'CHANNEL': RSLayer('Channel Area', 'CHANNELS', 'Vector', 'channel'),
        'WATERBODIES': RSLayer('Waterbodies', 'WATERBODIES', 'Vector', 'waterbodies'),
    }),
    'VEGSUIT': RSLayer('Grazing Vegetation Suitability', 'VEGSUIT', 'Raster', 'intermediates/veg_suitability.tif'),
    'INTERMEDIATES': RSLayer('Grazing Intermediates', 'INTERMEDIATES', 'Geopackage', 'intermediates/intermediates.gpkg', {
        'WATER': RSLayer('Merged Channel and Waterbodies', 'WATER', 'Vector', 'water')
    }),
    'LIKELIHOOD': RSLayer('Grazing Likelihood', 'LIKELIHOOD', 'Raster', 'outputs/likelihood.tif'),
    'OUTPUTS': RSLayer('Grazing Outputs', 'OUTPUTS', 'Geopackage', 'outputs/outputs.gpkg', {
        'GRAZING_DGO_GEOM': RSLayer('Grazing DGO Polygon Geometry', 'GRAZING_DGO_GEOM', 'Vector', 'dgo_geometry'),
        'GRAZING_DGOS': RSLayer('Grazing Likelihood (DGOs)', 'GRAZING_LIKELIHOOD_DGO', 'Vector', 'grazing_dgos'),
        'GRAZING_IGO_GEOM': RSLayer('Grazing IGO Point Geometry', 'GRAZING_IGO_GEOM', 'Vector', 'igo_geometry'),
        'GRAZING_IGOS': RSLayer('Grazing Likelihood (IGOs)', 'GRAZING_LIKELIHOOD_IGO', 'Vector', 'grazing_igos')
    }),
}


def grazing_likelihood(huc: int, existing_veg: Path, slope: Path, hillshade: Path, igo: Path, dgo: Path,
                       waterbodies: Path, channel: Path, output_dir: Path, meta: Dict[str, str] = None) -> None:
    """
    Main function to run the Grazing Likelihood model.

    :param huc: HUC number for the project
    :param existing_veg: Path to existing vegetation raster
    :param slope: Path to slope raster
    :param hillshade: Path to hillshade raster
    :param inputs: Path to inputs geopackage
    :param waterbodies: Path to waterbodies geopackage
    :param channel: Path to channel geopackage
    :param output_dir: Directory to save outputs
    """
    # Initialize logger and project
    log = Logger('GrazingLikelihood')
    log.info(f'Starting Grazing Likelihood model v.{cfg.version}')
    log.info(f'HUC: {huc}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')

    # augment_layermeta('grazing', LYR_DESCRIPTIONS_JSON, LayerTypes)

    project_name = f'Grazing Likelihood for HUC {huc}'
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'Grazing', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/grazing', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Intermediates', 'Outputs'])

    log.info('Adding input rasters to project')
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['EXVEG'], existing_veg)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['OUTPUTS'])

    inputs_gpkg_path = os.path.join(output_dir, LayerTypes['INPUTS'].rel_path)
    intermediates_gpkg_path = os.path.join(output_dir, LayerTypes['INTERMEDIATES'].rel_path)
    outputs_gpkg_path = os.path.join(output_dir, LayerTypes['OUTPUTS'].rel_path)

    # Make sure we're starting with empty/fresh geopackages
    GeopackageLayer.delete(inputs_gpkg_path)
    GeopackageLayer.delete(intermediates_gpkg_path)
    GeopackageLayer.delete(outputs_gpkg_path)

    # copy original vectors to inputs geopackage
    src_layers = {
        'IGO': igo,
        'DGO': dgo,
        'CHANNEL': channel,
        'WATERBODIES': waterbodies
    }

    input_layers = {}
    for input_key, rslayer in LayerTypes['INPUTS'].sub_layers.items():
        input_layers[input_key] = os.path.join(inputs_gpkg_path, rslayer.rel_path)
        copy_feature_class(src_layers[input_key], input_layers[input_key], cfg.OUTPUT_EPSG)

    # Create the output feature class fields. Only those listed here will get copied from the source.
    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GRAZING_IGO_GEOM'].rel_path, delete_dataset=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, options=['FID=IGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'stream_size': ogr.OFTInteger,
            'centerline_length': ogr.OFTReal
        })

    with GeopackageLayer(outputs_gpkg_path, layer_name=LayerTypes['OUTPUTS'].sub_layers['GRAZING_DGO_GEOM'].rel_path, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbMultiPolygon, epsg=cfg.OUTPUT_EPSG, options=['FID=DGOID'], fields={
            'FCode': ogr.OFTInteger,
            'level_path': ogr.OFTReal,
            'seg_distance': ogr.OFTReal,
            'centerline_length': ogr.OFTReal,
            'segment_area': ogr.OFTReal
        })

    db_metadata = {'Grazing DateTime': datetime.datetime.now().isoformat()}

    # Execute the SQL to create the lookup tables in the output geopackage
    create_database(huc, outputs_gpkg_path, db_metadata, cfg.OUTPUT_EPSG, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database', 'grazing_schema.sql'))

    igo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['GRAZING_IGO_GEOM'].rel_path)
    dgo_geom_path = os.path.join(outputs_gpkg_path, LayerTypes['OUTPUTS'].sub_layers['GRAZING_DGO_GEOM'].rel_path)
    copy_features_fields(input_layers['IGO'], igo_geom_path, epsg=cfg.OUTPUT_EPSG)
    copy_features_fields(input_layers['DGO'], dgo_geom_path, epsg=cfg.OUTPUT_EPSG)

    with SQLiteCon(outputs_gpkg_path) as database:
        database.curs.execute("""INSERT INTO IGOAttributes (IGOID, FCode, level_path, seg_distance, stream_size)
                              SELECT IGOID, FCode, level_path, seg_distance, stream_size FROM igo_geometry""")
        database.curs.execute("""INSERT INTO DGOAttributes (DGOID, FCode, level_path, seg_distance, segment_area, centerline_length)
                              SELECT DGOID, FCode, level_path, seg_distance, segment_area, centerline_length FROM dgo_geometry""")
        # Register layers as feature layer as well as geometry column
        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'grazing_igos', data_type, 'igos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'igo_geometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'grazing_igos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'igo_geometry'""")

        database.curs.execute("""INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
            SELECT 'grazing_dgos', data_type, 'dgos', min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = 'dgo_geometry'""")

        database.curs.execute("""INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT 'grazing_dgos', column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'dgo_geometry'""")

        database.conn.execute('CREATE INDEX ix_igo_levelpath on igo_eometry(level_path)')
        database.conn.execute('CREATE INDEX ix_igo_segdist on igo_geometry(seg_distance)')
        database.conn.execute('CREATE INDEX ix_igo_size on igo_geometry(stream_size)')
        database.conn.execute('CREATE INDEX ix_dgo_levelpath on dgo_geometry(level_path)')
        database.conn.execute('CREATE INDEX ix_dgo_segdist on dgo_geometry(seg_distance)')

        database.conn.commit()

        database.curs.execute('SELECT DISTINCT level_path FROM IGOGeometry')
        levelps = database.curs.fetchall()
        levelpathsin = [lp['level_path'] for lp in levelps]

    # set window distances for different stream sizes
    distancein = {
        '0': 200,
        '1': 400,
        '2': 1200,
        '3': 2000,
        '4': 8000
    }
    project.add_metadata(
        [RSMeta('Small Search Window', str(distancein['0']), RSMetaTypes.INT, locked=True),
            RSMeta('Medium Search Window', str(distancein['1']), RSMetaTypes.INT, locked=True),
            RSMeta('Large Search Window', str(distancein['2']), RSMetaTypes.INT, locked=True),
            RSMeta('Very Large Search Window', str(distancein['3']), RSMetaTypes.INT, locked=True),
            RSMeta('Huge Search Window', str(distancein['4']), RSMetaTypes.INT, locked=True)])

    # associate DGO IDs with IGO IDs for moving windows
    windows = moving_window_dgo_ids(igo_geom_path, dgo_geom_path, levelpathsin, distancein)

    # generate raster of water features
    combine_water_features(input_layers['CHANNEL'], input_layers['WATERBODIES'], os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['WATER'].rel_path), cfg.OUTPUT_EPSG)
    create_water_raster(os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['WATER'].rel_path), os.path.join(output_dir, 'intermediates/water.tif'), slope)
    # create raster of proximity to water features
    proximity_raster(os.path.join(output_dir, 'intermediates/water.tif'), os.path.join(output_dir, 'intermediates/proximity.tif'), "GEO")

    # resample landfire down to 10m to match slope and proximity rasters
    ds = gdal.Open(slope)
    if ds is None:
        raise FileNotFoundError(f"Could not open slope raster: {slope}")
    gt = ds.GetGeoTransform()
    xres = gt[1]
    yres = abs(gt[5])
    existing_veg_resampled = os.path.join(output_dir, 'intermediates/existing_veg_resampled.tif')
    gdal.Warp(existing_veg_resampled, existing_veg, format='GTiff', xRes=xres, yRes=yres, resampleAlg='nearest_neighbor', targetSRS=f'EPSG:{cfg.OUTPUT_EPSG}', outputType=gdal.GDT_Int16)
    ds = None

    # create vegetation suitability raster
    vegetation_suitability(outputs_gpkg_path, existing_veg_resampled, os.path.join(intermediates_gpkg_path, LayerTypes['VEGSUIT'].rel_path))
