# Name:     Vbet Centerlines (raster)
#
# Purpose:  Find the vbet centerlines per level path via rasters
#
#
# Author:   Kelly Whitehead
#
# Date:     Apr 11, 2022
# -------------------------------------------------------------------------------

import os
import sys
import argparse
from typing import ParamSpec

import gdal
from osgeo import osr, ogr
import rasterio
import numpy as np
from scipy.ndimage import label, generate_binary_structure, binary_dilation, binary_erosion, binary_closing

from rscommons import VectorBase, ProgressBar, GeopackageLayer, dotenv
from rscommons.vector_ops import copy_feature_class, polygonize
from rscommons.util import safe_makedirs
from rscommons.hand import run_subprocess
from rscommons.vbet_network import copy_vaa_attributes, join_attributes, create_stream_size_zones
from vbet.vbet_database import build_vbet_database, load_configuration
from vbet.vbet_raster_ops import rasterize_attribute, raster_clean, raster2array, array2raster, new_raster
from .cost_path import least_cost_path
from .raster2line import raster2line

NCORES = "8"
scenario_code = "UPDATED_TESTING"


def vbet_centerlines(in_line_network, dem, slope, in_catchments, in_channel_area, vaa_table, out_folder, level_paths=[], pitfill_dem=None, dinfflowdir_ang=None, dinfflowdir_slp=None, twi_raster=None):

    vbet_gpkg = os.path.join(out_folder, 'vbet.gpkg')
    intermediate_gpkg = os.path.join(out_folder, 'intermediate.gpkg')
    GeopackageLayer.delete(vbet_gpkg)
    line_network_features = os.path.join(vbet_gpkg, 'line_network')
    copy_feature_class(in_line_network, line_network_features)
    catchment_features = os.path.join(vbet_gpkg, 'catchments')
    copy_feature_class(in_catchments, catchment_features)

    build_vbet_database(vbet_gpkg)
    vbet_run = load_configuration(scenario_code, vbet_gpkg)

    vaa_table_name = copy_vaa_attributes(line_network_features, vaa_table)
    line_network = join_attributes(vbet_gpkg, "flowlines_vaa", os.path.basename(line_network_features), vaa_table_name, 'NHDPlusID', ['LevelPathI'], 4326)
    catchments = join_attributes(vbet_gpkg, "catchments_vaa", os.path.basename(catchment_features), vaa_table_name, 'NHDPlusID', ['LevelPathI'], 4326, geom_type='POLYGON')

    catchments_path = os.path.join(intermediate_gpkg, 'transform_zones')
    vaa_table_path = os.path.join(vbet_gpkg, vaa_table_name)
    create_stream_size_zones(catchments, vaa_table_path, 'NHDPlusID', 'StreamOrde', vbet_run['Zones'], catchments_path)

    in_rasters = {}
    in_rasters['Slope'] = slope

    # generate top level taudem products if they do not exist
    if pitfill_dem is None:
        pitfill_dem = os.path.join(out_folder, 'pitfill.tif')
        pitfill_status = run_subprocess(out_folder, ["mpiexec", "-n", NCORES, "pitremove", "-z", dem, "-fel", pitfill_dem])
        if pitfill_status != 0 or not os.path.isfile(pitfill_dem):
            raise Exception('TauDEM: pitfill failed')

    if not all([dinfflowdir_ang, dinfflowdir_slp]):
        dinfflowdir_slp = os.path.join(out_folder, 'dinfflowdir_slp.tif')
        dinfflowdir_ang = os.path.join(out_folder, 'dinfflowdir_ang.tif')
        dinfflowdir_status = run_subprocess(out_folder, ["mpiexec", "-n", NCORES, "dinfflowdir", "-fel", pitfill_dem, "-ang", dinfflowdir_ang, "-slp", dinfflowdir_slp])
        if dinfflowdir_status != 0 or not os.path.isfile(dinfflowdir_ang):
            raise Exception('TauDEM: dinfflowdir failed')

    if not twi_raster:
        twi_raster = os.path.join(out_folder, f'local_twi')

        sca = os.path.join(out_folder, 'sca.tif')
        twi_status = run_subprocess(out_folder, ["mpiexec", "-n", NCORES, "twi", "-slp", dinfflowdir_slp, "-sca", sca, '-twi', twi_raster])
        if twi_status != 0 or not os.path.isfile(twi_raster):
            raise Exception('TauDEM: TWI failed')
    in_rasters['TWI'] = twi_raster

    for zone in vbet_run['Zones']:
        # log.info(f'Rasterizing stream transform zones for {zone}')
        raster_name = os.path.join(out_folder, f'{zone.lower()}_transform_zones.tif')
        rasterize_attribute(catchments_path, raster_name, dem, f'{zone}_Zone')
        in_rasters[f'TRANSFORM_ZONE_{zone}'] = raster_name

    # run for orphan waterbodies??

    # iterate for each level path
    for level_path in level_paths:

        # Select channel areas that intersect flow lines

        evidence_raster = os.path.join(out_folder, f'vbet_evidence_{level_path}.tif')
        rasterized_channel = os.path.join(out_folder, f'rasterized_channel_{level_path}.tif')
        rasterize_level_path(line_network, dem, level_path, rasterized_channel)
        in_rasters['Channel'] = rasterized_channel

        # log.info("Generating HAND")
        hand_raster = os.path.join(out_folder, f'local_hand_{level_path}.tif')
        dinfdistdown_status = run_subprocess(out_folder, ["mpiexec", "-n", NCORES, "dinfdistdown", "-ang", dinfflowdir_ang, "-fel", pitfill_dem, "-src", rasterized_channel, "-dd", hand_raster, "-m", "ave", "v"])
        if dinfdistdown_status != 0 or not os.path.isfile(hand_raster):
            raise Exception('TauDEM: dinfdistdown failed')
        in_rasters['HAND'] = hand_raster

        # Open evidence rasters concurrently. We're looping over windows so this shouldn't affect
        # memory consumption too much
        read_rasters = {name: rasterio.open(raster) for name, raster in in_rasters.items()}
        out_meta = read_rasters['Slope'].meta
        out_meta['driver'] = 'GTiff'
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'

        write_rasters = {}  # {name: rasterio.open(raster, 'w', **out_meta) for name, raster in out_rasters.items()}
        write_rasters['VBET_EVIDENCE'] = rasterio.open(evidence_raster, 'w', **out_meta)

        progbar = ProgressBar(len(list(read_rasters['Slope'].block_windows(1))), 50, "Calculating evidence layer")
        counter = 0
        # Again, these rasters should be orthogonal so their windows should also line up
        for _ji, window in read_rasters['Slope'].block_windows(1):
            progbar.update(counter)
            counter += 1
            block = {block_name: raster.read(1, window=window, masked=True) for block_name, raster in read_rasters.items()}

            normalized = {}
            for name in vbet_run['Inputs']:
                if name in vbet_run['Zones']:
                    transforms = [np.ma.MaskedArray(transform(block[name].data), mask=block[name].mask) for transform in vbet_run['Transforms'][name]]
                    normalized[name] = np.ma.MaskedArray(np.choose(block[f'TRANSFORM_ZONE_{name}'].data, transforms, mode='clip'), mask=block[name].mask)
                else:
                    normalized[name] = np.ma.MaskedArray(vbet_run['Transforms'][name][0](block[name].data), mask=block[name].mask)

            fvals_topo = np.ma.mean([normalized['Slope'], normalized['HAND'], normalized['TWI']], axis=0)
            fvals_channel = 0.995 * block['Channel']
            fvals_evidence = np.maximum(fvals_topo, fvals_channel)

            write_rasters['VBET_EVIDENCE'].write(np.ma.filled(np.float32(fvals_evidence), out_meta['nodata']), window=window, indexes=1)
        write_rasters['VBET_EVIDENCE'].close()

        cost_path_raster = os.path.join(out_folder, f'cost_path_{level_path}.tif')
        valley_bottom = os.path.join(out_folder, f'valley_bottom_{level_path}.tif')

        temp_folder = os.path.join(out_folder, f'temp_{level_path}')
        safe_makedirs(temp_folder)

        # Generate Centerline from Cost Path
        generate_centerline_surface(evidence_raster, rasterized_channel, hand_raster, valley_bottom, cost_path_raster, temp_folder)
        vbet_polygon = os.path.join(out_folder, f'valley_bottom_{level_path}.shp')
        polygonize(valley_bottom, 1, vbet_polygon, epsg=4326)
        centerline = os.path.join(out_folder, f'centerline_{level_path}.tif')
        coords = get_endpoints(line_network, 'LevelPathI', level_path)
        least_cost_path(cost_path_raster, centerline, coords[0], coords[1])
        centerline_shp = os.path.join(out_folder, f'centerline_{level_path}.shp')
        raster2line(centerline, centerline_shp, 1)


def rasterize_level_path(line_network, dem, level_path, out_raster):

    ds_path, lyr_path = VectorBase.path_sorter(line_network)

    g = gdal.Open(dem)
    geo_t = g.GetGeoTransform()
    width, height = g.RasterXSize, g.RasterYSize
    xmin = min(geo_t[0], geo_t[0] + width * geo_t[1])
    xmax = max(geo_t[0], geo_t[0] + width * geo_t[1])
    ymin = min(geo_t[3], geo_t[3] + geo_t[-1] * height)
    ymax = max(geo_t[3], geo_t[3] + geo_t[-1] * height)
    # Close our dataset
    g = None

    progbar = ProgressBar(100, 50, f"Rasterizing for Level path {level_path}")

    sql = f"LevelPathI = {level_path}"

    def poly_progress(progress, _msg, _data):
        progbar.update(int(progress * 100))

    # https://gdal.org/programs/gdal_rasterize.html
    # https://gdal.org/python/osgeo.gdal-module.html#RasterizeOptions
    gdal.Rasterize(
        out_raster,
        ds_path,
        layers=[lyr_path],
        height=height,
        width=width,
        where=sql,
        burnValues=1, outputType=gdal.GDT_Int16,
        creationOptions=['COMPRESS=LZW'],
        allTouched=True,
        # outputBounds --- assigned output bounds: [minx, miny, maxx, maxy]
        outputBounds=[xmin, ymin, xmax, ymax],
        callback=poly_progress
    )

    progbar.finish()


def generate_centerline_surface(vbet_evidence_raster, rasterized_channel, channel_hand, out_valley_bottom, out_cost_path, temp_folder):

    # Read initial rasters as arrays
    vbet = raster2array(vbet_evidence_raster)
    chan = raster2array(rasterized_channel)
    hand = raster2array(channel_hand)

    # Generate Valley Bottom
    valley_bottom = ((vbet + chan) >= 0.68) * ((hand + chan) > 0)  # ((A + B) < 0.68) * (C > 0)
    valley_bottom_raw = os.path.join(temp_folder, "valley_bottom_raw.tif")
    array2raster(valley_bottom_raw, vbet_evidence_raster, valley_bottom, data_type=gdal.GDT_Int32)

    ds_valley_bottom = gdal.Open(valley_bottom_raw, gdal.GA_Update)
    band_valley_bottom = ds_valley_bottom.GetRasterBand(1)

    # Sieve and Clean Raster
    gdal.SieveFilter(srcBand=band_valley_bottom, maskBand=None, dstBand=band_valley_bottom, threshold=10, connectedness=8, callback=gdal.TermProgress_nocb)
    band_valley_bottom.SetNoDataValue(0)
    band_valley_bottom.FlushCache()
    valley_bottom_sieved = band_valley_bottom.ReadAsArray()

    # Region Tool to find only connected areas
    s = generate_binary_structure(2, 2)
    regions, _num = label(valley_bottom_sieved, structure=s)
    selection = regions * chan
    values = np.unique(selection)
    valley_bottom_region = np.isin(regions, values.nonzero())
    array2raster(os.path.join(temp_folder, 'regions.tif'), vbet_evidence_raster, regions, data_type=gdal.GDT_Int32)
    array2raster(os.path.join(temp_folder, 'valley_bottom_region.tif'), vbet_evidence_raster, valley_bottom_region.astype(int), data_type=gdal.GDT_Int32)

    # Clean Raster Edges
    valley_bottom_clean = binary_closing(valley_bottom_region.astype(int), iterations=2)
    array2raster(out_valley_bottom, vbet_evidence_raster, valley_bottom_clean, data_type=gdal.GDT_Int32)

    # Generate Inverse Raster for Proximity
    valley_bottom_inverse = (valley_bottom_clean != 1)
    inverse_mask_raster = os.path.join(temp_folder, 'inverse_mask.tif')
    array2raster(inverse_mask_raster, vbet_evidence_raster, valley_bottom_inverse)

    # Proximity Raster
    ds_valley_bottom_inverse = gdal.Open(inverse_mask_raster)
    band_valley_bottom_inverse = ds_valley_bottom_inverse.GetRasterBand(1)
    proximity_raster = os.path.join(temp_folder, 'proximity.tif')
    _ds_proximity, band_proximity = new_raster(proximity_raster, vbet_evidence_raster, data_type=gdal.GDT_Int32)
    gdal.ComputeProximity(band_valley_bottom_inverse, band_proximity, ['VALUES=1', "DISTUNITS=PIXEL", "COMPRESS=DEFLATE"])
    band_proximity.SetNoDataValue(0)
    band_proximity.FlushCache()
    proximity = band_proximity.ReadAsArray()

    # Rescale Raster
    rescaled = np.interp(proximity, (proximity.min(), proximity.max()), (0.0, 10.0))
    rescaled_raster = os.path.join(temp_folder, 'rescaled.tif')
    array2raster(rescaled_raster, vbet_evidence_raster, rescaled, data_type=gdal.GDT_Float32)

    # Centerline Cost Path
    cost_path = 10**((rescaled * -1) + 10) + (rescaled <= 0) * 1000000000000  # 10** (((A) * -1) + 10) + (A <= 0) * 1000000000000
    array2raster(out_cost_path, vbet_evidence_raster, cost_path, data_type=gdal.GDT_Float32)


def get_endpoints(line_network, field, attribute):

    from collections import Counter
    with GeopackageLayer(line_network) as lyr:
        coords = []
        for feat, *_ in lyr.iterate_features(attribute_filter=f'{field} = {attribute}'):
            geom = feat.GetGeometryRef()
            geom.FlattenTo2D()
            for pt in [geom.GetPoint(0), geom.GetPoint(geom.GetPointCount() - 1)]:
                coords.append(pt)

        counts = Counter(coords)

        output = [pt for pt, count in counts.items() if count == 1]

        return output


def main():
    parser = argparse.ArgumentParser(
        description='Riverscapes VBET Centerline Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('flowline_network', help='full nhd line network', type=str)
    parser.add_argument('dem', help='dem', type=str)
    parser.add_argument('slope', help='slope', type=str)
    parser.add_argument('catchments', type=str)
    parser.add_argument('channel_area', type=str)
    parser.add_argument('vaa_table')
    parser.add_argument('output_dir', help='Folder where output VBET project will be created', type=str)
    parser.add_argument('level_paths', help='csv list of level paths', type=str)

    parser.add_argument('--pitfill', help='riverscapes project metadata as comma separated key=value pairs', default=None)
    parser.add_argument('--dinfflowdir_ang', help='(optional) a little extra logging ', default=None)
    parser.add_argument('--dinfflowdir_slp', help='Add debug tools for tracing things like memory usage at a performance cost.', default=None)
    parser.add_argument('--twi_raster', help='Add debug tools for tracing things like memory usage at a performance cost.', default=None)

    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    level_paths = args.level_paths.split(',')

    vbet_centerlines(args.flowline_network, args.dem, args.slope, args.catchments, args.channel_area, args.vaa_table, args.output_dir, level_paths, args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, args.twi_raster)

    sys.exit(0)


if __name__ == '__main__':
    main()
