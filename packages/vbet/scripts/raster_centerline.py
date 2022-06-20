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

import gdal
from osgeo import ogr
import rasterio
from rasterio import shutil
import numpy as np
from scipy.ndimage import label, generate_binary_structure, binary_closing

from rscommons import ProgressBar, GeopackageLayer, dotenv
from rscommons.vector_ops import copy_feature_class, polygonize
from rscommons.util import safe_makedirs
from rscommons.hand import run_subprocess
from rscommons.vbet_network import copy_vaa_attributes, join_attributes, create_stream_size_zones, get_channel_level_path
from vbet.vbet_database import build_vbet_database, load_configuration
from vbet.vbet_raster_ops import rasterize_attribute, raster2array, array2raster, new_raster, rasterize
from vbet.vbet_outputs import vbet_merge
from .cost_path import least_cost_path
from .raster2line import raster2line_geom

NCORES = "8"
scenario_code = "UPDATED_TESTING"


def vbet_centerlines(in_line_network, dem, slope, in_catchments, in_channel_area, vaa_table, out_folder, level_paths=[], pitfill_dem=None, dinfflowdir_ang=None, dinfflowdir_slp=None, twi_raster=None, debug=True):

    vbet_gpkg = os.path.join(out_folder, 'vbet.gpkg')
    intermediate_gpkg = os.path.join(out_folder, 'intermediate.gpkg')
    GeopackageLayer.delete(vbet_gpkg)
    line_network_features = os.path.join(vbet_gpkg, 'line_network')
    copy_feature_class(in_line_network, line_network_features)
    catchment_features = os.path.join(vbet_gpkg, 'catchments')
    copy_feature_class(in_catchments, catchment_features)
    channel_area = os.path.join(vbet_gpkg, 'channel_area')
    copy_feature_class(in_channel_area, channel_area)

    build_vbet_database(vbet_gpkg)
    vbet_run = load_configuration(scenario_code, vbet_gpkg)

    vaa_table_name = copy_vaa_attributes(line_network_features, vaa_table)
    line_network = join_attributes(vbet_gpkg, "flowlines_vaa", os.path.basename(line_network_features), vaa_table_name, 'NHDPlusID', ['LevelPathI', 'DnLevelPat', 'UpLevelPat', 'Divergence'], 4326)
    catchments = join_attributes(vbet_gpkg, "catchments_vaa", os.path.basename(catchment_features), vaa_table_name, 'NHDPlusID', ['LevelPathI', 'DnLevelPat', 'UpLevelPat', 'Divergence'], 4326, geom_type='POLYGON')

    get_channel_level_path(channel_area, line_network, vaa_table)

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

    # Initialize Outputs
    output_centerlines = os.path.join(vbet_gpkg, "vbet_centerlines")
    output_vbet = os.path.join(vbet_gpkg, "vbet_polygons")
    output_active_vbet = os.path.join(vbet_gpkg, "active_vbet_polygons")
    with GeopackageLayer(output_centerlines, write=True) as lyr_cl_init, \
        GeopackageLayer(output_vbet, write=True) as lyr_vbet_init, \
        GeopackageLayer(output_active_vbet, write=True) as lyr_active_vbet_init, \
            GeopackageLayer(line_network) as lyr_ref:
        fields = {'LevelPathI': ogr.OFTString}
        lyr_cl_init.create_layer(ogr.wkbLineString, spatial_ref=lyr_ref.spatial_ref, fields=fields)
        lyr_vbet_init.create_layer(ogr.wkbPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)
        lyr_active_vbet_init.create_layer(ogr.wkbPolygon, spatial_ref=lyr_ref.spatial_ref, fields=fields)

    out_hand = os.path.join(out_folder, "composite_hand.tif")
    out_vbet_evidence = os.path.join(out_folder, 'composite_vbet_evidence.tif')

    # Generate the list of level paths to run, sorted by ascending order and optional user filter
    level_paths_to_run = []
    with GeopackageLayer(line_network) as line_lyr:
        for feat, *_ in line_lyr.iterate_features():
            level_path = feat.GetField('LevelPathI')
            level_paths_to_run.append(str(int(level_path)))
    level_paths_to_run = list(set(level_paths_to_run))
    if level_paths:
        level_paths_to_run = [level_path for level_path in level_paths_to_run if level_path in level_paths]
    level_paths_to_run.sort(reverse=False)
    level_paths_to_run.append(None)

    # iterate for each level path
    for level_path in level_paths_to_run:

        temp_folder = os.path.join(out_folder, 'temp', f'levelpath_{level_path}')
        safe_makedirs(temp_folder)

        sql = f"LevelPathI = {level_path}" if level_path is not None else "LevelPathI is NULL"
        # Select channel areas that intersect flow lines
        level_path_polygons = os.path.join(out_folder, f'channel_polygons.gpkg', f'level_path_{level_path}')
        copy_feature_class(channel_area, level_path_polygons, attribute_filter=sql)

        # current_vbet = collect_feature_class(output_vbet)
        # if current_vbet is not None:
        #     channel_polygons = collect_feature_class(level_path_polygons)
        #     if current_vbet.Contains(channel_polygons):
        #         continue

        evidence_raster = os.path.join(temp_folder, f'vbet_evidence_{level_path}.tif')
        rasterized_channel = os.path.join(temp_folder, f'rasterized_channel_{level_path}.tif')
        rasterize(level_path_polygons, rasterized_channel, dem, all_touched=True)
        in_rasters['Channel'] = rasterized_channel

        # log.info("Generating HAND")
        hand_raster = os.path.join(temp_folder, f'local_hand_{level_path}.tif')
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
        for _ji, window in read_rasters['HAND'].block_windows(1):
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

        # Generate VBET Polygon
        valley_bottom_raster = os.path.join(temp_folder, f'valley_bottom_{level_path}.tif')
        generate_vbet_polygon(evidence_raster, rasterized_channel, hand_raster, valley_bottom_raster, temp_folder)
        vbet_polygon = os.path.join(temp_folder, f'valley_bottom_{level_path}.shp')
        polygonize(valley_bottom_raster, 1, vbet_polygon, epsg=4326)
        # Add to existing feature class
        polygon = vbet_merge(vbet_polygon, output_vbet, level_path=level_path)

        active_valley_bottom_raster = os.path.join(temp_folder, f'active_valley_bottom_{level_path}.tif')
        generate_vbet_polygon(evidence_raster, rasterized_channel, hand_raster, active_valley_bottom_raster, temp_folder, threshold=0.90)
        active_vbet_polygon = os.path.join(temp_folder, f'active_valley_bottom_{level_path}.shp')
        polygonize(active_valley_bottom_raster, 1, active_vbet_polygon, epsg=4326)
        # Add to existing feature class
        active_polygon = vbet_merge(active_vbet_polygon, output_active_vbet, level_path=level_path)

        # Generate centerline for level paths only
        if level_path is not None:
            # Generate Centerline from Cost Path
            cost_path_raster = os.path.join(temp_folder, f'cost_path_{level_path}.tif')
            generate_centerline_surface(valley_bottom_raster, cost_path_raster, temp_folder)
            centerline_raster = os.path.join(temp_folder, f'centerline_{level_path}.tif')
            coords = get_endpoints(line_network, 'LevelPathI', level_path)
            least_cost_path(cost_path_raster, centerline_raster, coords[0], coords[1])
            centerline_full = raster2line_geom(centerline_raster, 1)

            if polygon is not None:
                centerline_intersected = polygon.Intersection(centerline_full)
                if centerline_intersected.GetGeometryName() == 'GeometryCollection':
                    for line in centerline_intersected:
                        centerline = ogr.Geometry(ogr.wkbMultiLineString)
                        if line.GetGeometryName() == 'LineString':
                            centerline.AddGeometry(line)
                else:
                    centerline = centerline_intersected
            else:
                centerline = centerline_full

            with GeopackageLayer(output_centerlines, write=True) as lyr_cl:
                out_feature = ogr.Feature(lyr_cl.ogr_layer_def)
                out_feature.SetGeometry(centerline)
                out_feature.SetField('LevelPathI', str(level_path))
                lyr_cl.ogr_layer.CreateFeature(out_feature)
                out_feature = None

        # clean up rasters
        rio_vbet = rasterio.open(valley_bottom_raster)
        for out_raster, in_raster in {out_hand: hand_raster, out_vbet_evidence: evidence_raster}.items():
            if os.path.exists(out_raster):
                out_temp = os.path.join(temp_folder, 'temp_raster')
                rio_dest = rasterio.open(out_raster)
                out_meta = rio_dest.meta
                out_meta['driver'] = 'GTiff'
                out_meta['count'] = 1
                out_meta['compress'] = 'deflate'
                rio_temp = rasterio.open(out_temp, 'w', **out_meta)
                rio_source = rasterio.open(in_raster)
                for _ji, window in rio_source.block_windows(1):
                    # progbar.update(counter)
                    # counter += 1
                    array_vbet_mask = np.ma.MaskedArray(rio_vbet.read(1, window=window, masked=True).data)
                    array_source = np.ma.MaskedArray(rio_source.read(1, window=window, masked=True).data, mask=array_vbet_mask.mask)
                    array_dest = np.ma.MaskedArray(rio_dest.read(1, window=window, masked=True).data, mask=array_vbet_mask.mask)
                    array_out = np.choose(array_vbet_mask, [array_dest, array_source])

                    rio_temp.write(np.ma.filled(np.float32(array_out), out_meta['nodata']), window=window, indexes=1)
                rio_temp.close()
                rio_dest.close()
                rio_source.close()
                shutil.copyfiles(out_temp, out_raster)
            else:
                rio_source = rasterio.open(in_raster)
                out_meta = rio_source.meta
                out_meta['driver'] = 'GTiff'
                out_meta['count'] = 1
                out_meta['compress'] = 'deflate'
                rio_dest = rasterio.open(out_raster, 'w', **out_meta)
                for _ji, window in rio_source.block_windows(1):
                    array_vbet_mask = np.ma.MaskedArray(rio_vbet.read(1, window=window, masked=True).data)
                    array_source = np.ma.MaskedArray(rio_source.read(1, window=window, masked=True).data, mask=array_vbet_mask.mask)
                    rio_dest.write(np.ma.filled(np.float32(array_source), out_meta['nodata']), window=window, indexes=1)
                rio_dest.close()
                rio_source.close()

        if debug is False:
            os.rmdir(temp_folder)


def generate_vbet_polygon(vbet_evidence_raster, rasterized_channel, channel_hand, out_valley_bottom, temp_folder, threshold=0.68):

    # Read initial rasters as arrays
    vbet = raster2array(vbet_evidence_raster)
    chan = raster2array(rasterized_channel)
    hand = raster2array(channel_hand)

    # Generate Valley Bottom
    valley_bottom = ((vbet + chan) >= threshold) * ((hand + chan) > 0)  # ((A + B) < 0.68) * (C > 0)
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


def generate_centerline_surface(vbet_raster, out_cost_path, temp_folder):

    vbet = raster2array(vbet_raster)

    # Generate Inverse Raster for Proximity
    valley_bottom_inverse = (vbet != 1)
    inverse_mask_raster = os.path.join(temp_folder, 'inverse_mask.tif')
    array2raster(inverse_mask_raster, vbet_raster, valley_bottom_inverse)

    # Proximity Raster
    ds_valley_bottom_inverse = gdal.Open(inverse_mask_raster)
    band_valley_bottom_inverse = ds_valley_bottom_inverse.GetRasterBand(1)
    proximity_raster = os.path.join(temp_folder, 'proximity.tif')
    _ds_proximity, band_proximity = new_raster(proximity_raster, vbet_raster, data_type=gdal.GDT_Int32)
    gdal.ComputeProximity(band_valley_bottom_inverse, band_proximity, ['VALUES=1', "DISTUNITS=PIXEL", "COMPRESS=DEFLATE"])
    band_proximity.SetNoDataValue(0)
    band_proximity.FlushCache()
    proximity = band_proximity.ReadAsArray()

    # Rescale Raster
    rescaled = np.interp(proximity, (proximity.min(), proximity.max()), (0.0, 10.0))
    rescaled_raster = os.path.join(temp_folder, 'rescaled.tif')
    array2raster(rescaled_raster, vbet_raster, rescaled, data_type=gdal.GDT_Float32)

    # Centerline Cost Path
    cost_path = 10**((rescaled * -1) + 10) + (rescaled <= 0) * 1000000000000  # 10** (((A) * -1) + 10) + (A <= 0) * 1000000000000
    array2raster(out_cost_path, vbet_raster, cost_path, data_type=gdal.GDT_Float32)


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

    level_paths = level_paths if level_paths != ['.'] else None

    vbet_centerlines(args.flowline_network, args.dem, args.slope, args.catchments, args.channel_area, args.vaa_table, args.output_dir, level_paths, args.pitfill, args.dinfflowdir_ang, args.dinfflowdir_slp, args.twi_raster)

    sys.exit(0)


if __name__ == '__main__':
    main()
