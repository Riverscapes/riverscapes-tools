# Name:     TauDEM Project Tool (HAND and other products)
#
# Purpose:  Perform
#
# Author:   Kelly Whitehead
#
# Date:     July 19, 2021
#
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
import time
from typing import Dict, List

# LEave OSGEO import alone. It is necessary even if it looks unused
from osgeo import gdal, osr
from osgeo.ogr import Layer
from rscommons import hand
from rscommons.classes.vector_classes import get_shp_or_gpkg, VectorBase
from rscommons.classes.rs_project import RSMeta, RSMetaTypes
from rscommons.util import safe_makedirs, parse_metadata, pretty_duration
from rscommons import RSProject, RSLayer, ModelConfig, Logger, dotenv, initGDALOGRErrors
from rscommons import GeopackageLayer
from rscommons.vector_ops import copy_feature_class
from rscommons.hand import hand_rasterize, run_subprocess
from rscommons.raster_warp import raster_warp
from rscommons.geographic_raster import gdal_dem_geographic

from taudem.taudem_report import TauDEMReport
from taudem.__version__ import __version__

initGDALOGRErrors()

Path = str

cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/TauDEM.xsd', __version__)

NCORES = os.environ['TAUDEM_CORES'] if 'TAUDEM_CORES' in os.environ else '2'

LayerTypes = {
    'DEM': RSLayer('DEM', 'DEM', 'Raster', 'inputs/dem.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Inputs', 'INPUTS', 'Geopackage', 'inputs/hand_inputs.gpkg', {
        # 'CHANNEL_AREA': RSLayer('Channel Area Polygons', 'CHANNEL_AREA', 'Vector', 'channel_areas'),
        # 'CHANNEL_LINES': RSLayer('Channel Lines', 'CHANNEL_LINES', 'Vector', 'channel_lines'),
        # 'DEM_MASK_POLY': RSLayer('DEM Mask Polygon', 'DEM_MASK_POLY', 'Vector', 'dem_mask_poly'),
    }),

    # Intermediate Products
    'DEM_MASKED': RSLayer('DEM Masked', 'DEM_MASKED', 'Raster', 'intermediates/dem_masked.tif'),
    'PITFILL': RSLayer('TauDEM Pitfill', 'PITFILL', 'Raster', 'intermediates/pitfill.tif'),
    'DINFFLOWDIR_ANG': RSLayer('TauDEM D-Inf Flow Directions', 'DINFFLOWDIR_ANG', 'Raster', 'intermediates/dinfflowdir_ang.tif'),
    'D8FLOWDIR_P': RSLayer('TauDEM D8 Flow Directions', 'D8FLOWDIR_P', 'Raster', 'intermediates/d8flowdir_p.tif'),
    'D8FLOWDIR_SD8': RSLayer('TauDEM D8 Flow Direction Slope', 'D8FLOWDIR_SD8', 'Raster', 'intermediates/d8flowdir_sd8.tif'),
    'AREADINF_SCA': RSLayer('TauDEM D-Inf Contributing Area', 'AREADINF_SCA', 'Raster', 'intermediates/areadinf_sca.tif'),
    'RASTERIZED_CHANNEL': RSLayer('Rasterized Channel', 'RASTERIZED_CHANNEL', 'Raster', 'intermediates/rasterized_channel.tif'),
    # 'INTERMEDIATES': RSLayer('Intermediates', 'INTERMEIDATES', 'Geopackage', 'intermediates/hand_intermediates.gpkg', {
    # }),

    # Outputs:
    'DINFFLOWDIR_SLP': RSLayer('TauDEM D-Inf Flow Directions Slope', 'DINFFLOWDIR_SLP', 'Raster', 'outputs/dinfflowdir_slp.tif'),
    'SLOPEAVEDOWN_SLPD': RSLayer('TauDEM Slope Average Down', 'SLOPEAVEDOWN_SLPD', 'Raster', 'outputs/slopeavedown_slpd.tif'),
    'HAND_RASTER': RSLayer('Hand Raster', 'HAND_RASTER', 'Raster', 'outputs/HAND.tif'),
    'TWI_RASTER': RSLayer('TWI Raster', 'TWI_RASTER', 'Raster', 'outputs/twi.tif'),
    'GDAL_SLOPE': RSLayer('Slope raster (GDAL)', 'GDAL_SLOPE', 'Raster', 'outputs/gdal_slope.tif'),
    'REPORT': RSLayer('RSContext Report', 'REPORT', 'HTMLFile', 'outputs/taudem.html')
}


def taudem(huc: int, input_channel_vector: Path, orig_dem: Path, project_folder: Path, mask_lyr_path: Path = None, epsg: int = cfg.OUTPUT_EPSG, meta: Dict[str, str] = None):
    """Run TauDEM tools to generate a Riverscapes TauDEM project, including HAND, TWI, Dinf Slope and other intermediate raster products.

    Args:
        huc (int): Huc Watershed ID
        input_channel_vector (Path): line or polygon feature layer that delineates drainage channel
        orig_dem (Path): dem of watershed
        hillshade (Path): hillshade of dem to include in project (optional, set to None if not included)
        project_folder (Path): Output folder for TauDEM project
        mask_lyr_path (Path, optional): polygon layer to mask DEM. Defaults to None.
        meta (Dict[str, str], optional): metadata to include in project. Defaults to None.
    """

    log = Logger('TauDEM')
    log.info('Starting TauDEM v.{}'.format(cfg.version))
    start_time = time.time()
    project_name = 'TauDEM project for HUC {}'.format(huc)
    project = RSProject(cfg, project_folder)
    project.create(project_name, 'TauDEM', [
        RSMeta('HUC{}'.format(len(huc)), str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('TauDEMProjectVersion', cfg.version),
        RSMeta('TauDEMTimestamp', str(int(time.time())), RSMetaTypes.TIMESTAMP),
        RSMeta('TauDEM_SoftwareVersion', '5.3.7'),
        RSMeta('TauDEM_Credits', 'Copyright (C) 2010-2015 David Tarboton, Utah State University'),
        RSMeta('TauDEM_Licence', 'https://hydrology.usu.edu/taudem/taudem5/GPLv3license.txt', RSMetaTypes.URL),
        RSMeta('TauDEM_URL', 'https://hydrology.usu.edu/taudem/taudem5/index.html', RSMetaTypes.URL)
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=["Inputs", "Intermediates", "Outputs"], create_folders=True)

    # Copy the inp
    _proj_dem_node, proj_dem = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['DEM'], orig_dem)
    # if hillshade is not None:
    #    _hillshade_node, hillshade = project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'], hillshade)

    # Find EPSG from dem
    dem = os.path.join(project_folder, LayerTypes['DEM'].rel_path)
    d = gdal.Open(dem)
    proj = osr.SpatialReference(wkt=d.GetProjection())
    epsg = int(proj.GetAttrValue('AUTHORITY', 1))
    gt = d.GetGeoTransform()
    cell_resolution = gt[1]
    cell_meters = cell_resolution / GeopackageLayer.rough_convert_metres_to_raster_units(dem, 1) if epsg == 4326 else cell_resolution

    # Copy input shapes to a geopackage
    inputs_gpkg_path = os.path.join(project_folder, LayerTypes['INPUTS'].rel_path)
    GeopackageLayer.delete(inputs_gpkg_path)

    with get_shp_or_gpkg(input_channel_vector) as in_layer:
        channel_vector_type = in_layer.ogr_geom_type
    if channel_vector_type in VectorBase.LINE_TYPES:
        LayerTypes['INPUTS'].add_sub_layer('CHANNEL_LINES', RSLayer('Channel Lines', 'CHANNEL_LINES', 'Vector', 'channel_lines'))
        channel_vector = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['CHANNEL_LINES'].rel_path)
    else:
        LayerTypes['INPUTS'].add_sub_layer('CHANNEL_AREA', RSLayer('Channel Area Polygons', 'CHANNEL_AREA', 'Vector', 'channel_area'))
        channel_vector = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['CHANNEL_AREA'].rel_path)
    copy_feature_class(input_channel_vector, channel_vector, epsg=epsg)

    if mask_lyr_path is not None:
        LayerTypes['INPUTS'].add_sub_layer('DEM_MASK_POLY', RSLayer('DEM Mask Polygon', 'DEM_MASK_POLY', 'Vector', 'dem_mask_poly'))
        dem_mask_path = os.path.join(inputs_gpkg_path, LayerTypes['INPUTS'].sub_layers['DEM_MASK_POLY'].rel_path) if mask_lyr_path else None
        copy_feature_class(mask_lyr_path, dem_mask_path, epsg=epsg)

    project.add_project_geopackage(proj_nodes['Inputs'], LayerTypes['INPUTS'])
    ##########################################################################
    # The main event:ce
    ##########################################################################

    # intermeidates_gpkg_path = os.path.join(project_folder, LayerTypes['INTERMEDIATES'].rel_path)
    # GeopackageLayer.delete(intermeidates_gpkg_path)
    intermediates_path = os.path.join(project_folder, 'intermediates')

    # If there's no mask we use the original DEM as-is
    hand_dem = proj_dem

    # We might need to mask the incoming DEM
    if mask_lyr_path is not None:
        new_proj_dem = os.path.join(project_folder, LayerTypes['DEM_MASKED'].rel_path)
        raster_warp(proj_dem, new_proj_dem, epsg=epsg, clip=dem_mask_path, raster_compression=" -co COMPRESS=LZW -co PREDICTOR=3")
        hand_dem = new_proj_dem

    path_rasterized_drainage = os.path.join(project_folder, LayerTypes['RASTERIZED_CHANNEL'].rel_path)
    hand_rasterize(channel_vector, hand_dem, path_rasterized_drainage)
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['RASTERIZED_CHANNEL'])

    # GDAL Products
    # Hillshade
    hillshade = os.path.join(project_folder, LayerTypes['HILLSHADE'].rel_path)
    if epsg == 4326:
        gdal_dem_geographic(hand_dem, hillshade, 'hillshade')
    else:
        gdal.DEMProcessing(hillshade, hand_dem, 'hillshade')
    project.add_project_raster(proj_nodes['Inputs'], LayerTypes['HILLSHADE'])

    # Slope
    gdal_slope = os.path.join(project_folder, LayerTypes['GDAL_SLOPE'].rel_path)
    if epsg == 4326:
        gdal_dem_geographic(hand_dem, gdal_slope, 'slope')
    else:
        gdal.DEMProcessing(gdal_slope, hand_dem, 'slope')
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['GDAL_SLOPE'])

    start_time = time.time()
    log.info('Starting TauDEM processes')

    # TauDEM Products
    # PitRemove
    log.info("Filling DEM pits")
    path_pitfill = os.path.join(project_folder, LayerTypes['PITFILL'].rel_path)
    pitfill_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "pitremove", "-z", hand_dem, "-fel", path_pitfill])
    if pitfill_status != 0 or not os.path.isfile(path_pitfill):
        raise Exception('TauDEM: pitfill failed')
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['PITFILL'])

    # Flow Dir
    log.info("Finding dinf flow direction")
    path_ang = os.path.join(project_folder, LayerTypes['DINFFLOWDIR_ANG'].rel_path)
    path_slp = os.path.join(project_folder, LayerTypes['DINFFLOWDIR_SLP'].rel_path)
    dinfflowdir_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "dinfflowdir", "-fel", path_pitfill, "-ang", path_ang, "-slp", path_slp])
    if dinfflowdir_status != 0 or not os.path.isfile(path_ang):
        raise Exception('TauDEM: dinfflowdir failed')
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['DINFFLOWDIR_ANG'])
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['DINFFLOWDIR_SLP'])

    # generate hand
    log.info("Generating HAND")
    hand_raster = os.path.join(project_folder, LayerTypes['HAND_RASTER'].rel_path)
    dinfdistdown_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "dinfdistdown", "-ang", path_ang, "-fel", path_pitfill, "-src", path_rasterized_drainage, "-dd", hand_raster, "-m", "ave", "v"])
    if dinfdistdown_status != 0 or not os.path.isfile(hand_raster):
        raise Exception('TauDEM: dinfdistdown failed')
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['HAND_RASTER'])

    # Generate Flow area
    log.info("Finding flow area")
    path_sca = os.path.join(project_folder, LayerTypes['AREADINF_SCA'].rel_path)
    dinfflowarea_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "areadinf", "-ang", path_ang, "-sca", path_sca])
    if dinfflowarea_status != 0 or not os.path.isfile(path_sca):
        raise Exception('TauDEM: AreaDinf failed')
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['AREADINF_SCA'])

    # Generate TWI
    log.info("Generating Topographic Wetness Index (TWI)")
    twi_raster = os.path.join(project_folder, LayerTypes['TWI_RASTER'].rel_path)
    twi_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "twi", "-slp", path_slp, "-sca", path_sca, '-twi', twi_raster])
    if twi_status != 0 or not os.path.isfile(twi_raster):
        raise Exception('TauDEM: TWI failed')
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['TWI_RASTER'])

    # Generate SlopeAveDown
    log.info("Finding d8 flow direction")
    d8_raster = os.path.join(project_folder, LayerTypes['D8FLOWDIR_P'].rel_path)
    d8_slope_raster = os.path.join(project_folder, LayerTypes['D8FLOWDIR_SD8'].rel_path)
    d8_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "d8flowdir", "-p", d8_raster, '-fel', path_pitfill, '-sd8', d8_slope_raster])
    if d8_status != 0 or not os.path.isfile(d8_raster):
        raise Exception('TauDEM: D8Flowdir')
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['D8FLOWDIR_P'])
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['D8FLOWDIR_SD8'])

    # Average Slope Down Distance
    log.info("Generating SlopeAveDown")
    slpd_raster = os.path.join(project_folder, LayerTypes['SLOPEAVEDOWN_SLPD'].rel_path)
    dn = cell_meters
    slpd_status = run_subprocess(intermediates_path, ["mpiexec", "-n", NCORES, "slopeavedown", "-p", d8_raster, "-fel", path_pitfill, '-slpd', slpd_raster, "-dn", str(dn)])
    if slpd_status != 0 or not os.path.isfile(slpd_raster):
        raise Exception('TauDEM: SlopeAveDown')
    project.add_project_raster(proj_nodes['Outputs'], LayerTypes['SLOPEAVEDOWN_SLPD'])

    ellapsed_time = time.time() - start_time
    project.add_metadata([
        RSMeta("ProcTimeS", "{:.2f}".format(ellapsed_time), RSMetaTypes.INT),
        RSMeta("ProcTimeHuman", pretty_duration(ellapsed_time))
    ])
    log.info("TauDEM process complete in {}".format(ellapsed_time))

    report_path = os.path.join(project.project_dir, LayerTypes['REPORT'].rel_path)
    project.add_report(proj_nodes['Outputs'], LayerTypes['REPORT'], replace=True)

    report = TauDEMReport(report_path, project)
    report.write()

    log.info('TauDEM Completed Successfully')


def main():

    parser = argparse.ArgumentParser(
        description='Riverscapes TauDEM Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('huc', help='NHD flow line ShapeFile path', type=str)
    parser.add_argument('channel', help='Channel line or area gpkg layer or ShapeFile path', type=str)
    parser.add_argument('dem', help='DEM raster path', type=str)
    parser.add_argument('output_dir', help='Folder where output TauDEM project will be created', type=str)

    parser.add_argument('--hillshade', help='Hillshade raster path', type=str)
    parser.add_argument('--mask', help='Optional shapefile to mask by', type=str, default=None)
    parser.add_argument('--epsg', help='Optional output epsg', type=int, default=None)
    parser.add_argument('--meta', help='riverscapes project metadata as comma separated key=value pairs', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='Add debug tools for tracing things like memory usage at a performance cost.', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('TauDEM')
    log.setup(logPath=os.path.join(args.output_dir, 'taudem.log'), verbose=args.verbose)
    log.title('Riverscapes TauDEM project For HUC: {}'.format(args.huc))

    meta = parse_metadata(args.meta)

    epsg = args.epsg if args.epsg is not None else cfg.OUTPUT_EPSG

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'taudem_mem.log')
            retcode, max_obj = ThreadRun(taudem, memfile, args.huc, args.channel, args.dem, args.output_dir, args.mask, epsg, meta)
            log.debug('Return code: {}, [Max process usage] {}'.format(retcode, max_obj))

        else:
            taudem(args.huc, args.channel, args.dem, args.output_dir, args.mask, epsg, meta)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
