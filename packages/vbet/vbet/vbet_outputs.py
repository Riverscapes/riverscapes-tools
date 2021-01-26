from operator import attrgetter
from osgeo import ogr
import rasterio
import numpy as np
from shapely.ops import unary_union
from rscommons import ProgressBar, Logger, GeopackageLayer, VectorBase
from rscommons.vector_ops import get_num_pts, get_num_rings, remove_holes
from vbet.__version__ import __version__


def threshold(evidence_raster_path: str, thr_val: float, thresh_raster_path: str):
    """Threshold a raster to greater than or equal to a threshold value

    Args:
        evidence_raster_path (str): [description]
        thr_val (float): [description]
        thresh_raster_path (str): [description]
    """
    log = Logger('threshold')
    with rasterio.open(evidence_raster_path) as fval_src:
        out_meta = fval_src.meta
        out_meta['count'] = 1
        out_meta['compress'] = 'deflate'
        out_meta['dtype'] = rasterio.uint8
        out_meta['nodata'] = 0

        log.info('Thresholding at {}'.format(thr_val))
        with rasterio.open(thresh_raster_path, "w", **out_meta) as dest:
            progbar = ProgressBar(len(list(fval_src.block_windows(1))), 50, "Thresholding at {}".format(thr_val))
            counter = 0
            for ji, window in fval_src.block_windows(1):
                progbar.update(counter)
                counter += 1
                fval_data = fval_src.read(1, window=window, masked=True)
                # Fill an array with "1" values to give us a nice mask for polygonize
                fvals_mask = np.full(fval_data.shape, np.uint8(1))

                # Create a raster with 1.0 as a value everywhere in the same shape as fvals
                new_fval_mask = np.ma.mask_or(fval_data.mask, fval_data < thr_val)
                masked_arr = np.ma.array(fvals_mask, mask=[new_fval_mask])  # & ch_data.mask])
                dest.write(np.ma.filled(masked_arr, out_meta['nodata']), window=window, indexes=1)
            progbar.finish()


def sanitize(in_path: str, out_path: str, min_hole_sq_deg: float, buff_dist: float):
    """
        It's important to make sure we have the right kinds of geometries. Here we:
            1. Buffer out then back in by the same amount. TODO: THIS IS SUPER SLOW.
            2. Simply: for some reason area isn't calculated on inner rings so we need to simplify them first
            3. Remove small holes: Do we have donuts? Filter anythign smaller than a certain area

    Args:
        in_path (str): [description]
        out_path (str): [description]
        min_hole_sq_deg (float): [description]
        buff_dist (float): [description]
    """
    log = Logger('VBET Simplify')

    with GeopackageLayer(out_path, write=True) as out_lyr, \
            GeopackageLayer(in_path) as in_lyr:

        out_lyr.create_layer(ogr.wkbPolygon, spatial_ref=in_lyr.spatial_ref)

        geoms = []
        pts = 0
        square_buff = buff_dist * buff_dist

        # NOTE: Order of operations really matters here.
        in_rings = 0
        out_rings = 0
        in_pts = 0
        out_pts = 0

        for in_feat, _counter, _progbar in in_lyr.iterate_features("Sanitizing"):
            geom = VectorBase.ogr2shapely(in_feat)

            # First check. Just make sure this is a valid shape we can work with
            if geom.is_empty or geom.area < square_buff:
                # debug_writer(geom, '{}_C_BROKEN.geojson'.format(counter))
                continue

            pts += len(geom.exterior.coords)
            f_geom = geom

            # 1. Buffer out then back in by the same amount. TODO: THIS IS SUPER SLOW.
            f_geom = geom.buffer(buff_dist, resolution=1).buffer(-buff_dist, resolution=1)
            # debug_writer(f_geom, '{}_B_AFTER_BUFFER.geojson'.format(counter))

            # 2. Simply: for some reason area isn't calculated on inner rings so we need to simplify them first
            f_geom = f_geom.simplify(buff_dist, preserve_topology=True)

            # 3. Remove small holes: Do we have donuts? Filter anythign smaller than a certain area
            f_geom = remove_holes(f_geom, min_hole_sq_deg)

            # Second check here for validity after simplification
            if not f_geom.is_empty and f_geom.is_valid and f_geom.area > 0:
                geoms.append(f_geom)
                in_rings += get_num_pts(geom)
                out_rings += get_num_pts(f_geom)
                in_pts += get_num_rings(geom)
                out_pts += get_num_rings(f_geom)
                # debug_writer(f_geom, '{}_Z_FINAL.geojson'.format(counter))
            else:
                log.warning('Invalid GEOM')
                # debug_writer(f_geom, '{}_Z_REJECTED.geojson'.format(counter))
            # print('loop')

        log.debug('simplified: pts: {} ==> {}, rings: {} ==> {}'.format(in_pts, out_pts, in_rings, out_rings))

        # 5. Now we can do unioning fairly cheaply
        log.info('Unioning {} geometries'.format(len(geoms)))
        new_geom = unary_union(geoms)

        # Find the biggest area and just use that object thereby discarding all the little polygons that don't
        # touch the main one
        if new_geom.type == 'MultiPolygon':
            new_geom = max(list(new_geom), key=attrgetter('area'))

        log.info('Writing to disk')
        out_feat = ogr.Feature(out_lyr.ogr_layer_def)

        out_feat.SetGeometry(VectorBase.shapely2ogr(new_geom))
        out_lyr.ogr_layer.CreateFeature(out_feat)
        out_feat = None
