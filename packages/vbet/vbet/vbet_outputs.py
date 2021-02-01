from operator import attrgetter
from osgeo import ogr
import rasterio
from uuid import uuid4
from tempfile import NamedTemporaryFile
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


def sanitize(name: str, in_path: str, out_path: str, buff_dist: float):
    """
        It's important to make sure we have the right kinds of geometries.

    Args:
        name (str): Mainly just for good logging
        in_path (str): [description]
        out_path (str): [description]
        buff_dist (float): [description]
    """
    log = Logger('VBET Simplify')

    with GeopackageLayer(out_path, write=True) as out_lyr, \
            NamedTemporaryFile(suffix='.gpkg', mode="w+", delete=True) as tempgpkg, \
            GeopackageLayer(in_path) as in_lyr:

        out_lyr.create_layer(ogr.wkbPolygon, spatial_ref=in_lyr.spatial_ref)

        pts = 0
        square_buff = buff_dist * buff_dist

        # NOTE: Order of operations really matters here.
        in_pts = 0
        out_pts = 0

        with GeopackageLayer(tempgpkg.name, str(uuid4()), write=True, delete_dataset=True) as tmp_lyr:
            tmp_lyr.create_layer_from_ref(in_lyr)

            def geom_validity_fix(geom_in):
                f_geom = geom_in
                # Only clean if there's a problem:
                if not f_geom.IsValid():
                    f_geom = f_geom.Buffer(0)
                    if not f_geom.IsValid():
                        f_geom = f_geom.Buffer(buff_dist)
                        f_geom = f_geom.Buffer(-buff_dist)
                return f_geom

            # First loop finds the biggest area. We can use this to easily rule out other shapes
            biggest_area = (0, None, None, None)
            for in_feat, _counter, _progbar in in_lyr.iterate_features("Get largest shape for {}".format(name)):
                fid = in_feat.GetFID()
                geom = in_feat.GetGeometryRef()
                area = geom.Area()
                if area > biggest_area[0]:
                    biggest_area = (area, fid, in_feat, geom)

            if biggest_area[1] is None:
                raise Exception('Biggest shape could not be identified for layer {}'.format(name))

            # Second loop is about filtering bad areas and simplifying
            for in_feat, _counter, _progbar in in_lyr.iterate_features("Filtering out non-relevant shapes for {}".format(name)):
                fid = in_feat.GetFID()
                geom = in_feat.GetGeometryRef()

                area = geom.Area()
                pts += geom.GetBoundary().GetPointCount()
                # First check. Just make sure this is a valid shape we can work with
                # Make sure the area is greater than the square of the cell width
                # Make sure we're not significantly disconnected from the main shape
                # Make sure we intersect the main shape
                if geom.IsEmpty() \
                        or area < square_buff \
                        or biggest_area[3].Distance(geom) > 2 * buff_dist:
                    continue

                f_geom = geom.SimplifyPreserveTopology(buff_dist)
                # # Only fix things that need fixing
                # f_geom = geom_validity_fix(f_geom)

                # Second check here for validity after simplification
                # Then write to a temporary geopackage layer
                if not f_geom.IsEmpty() and f_geom.Area() > 0:
                    out_feature = ogr.Feature(tmp_lyr.ogr_layer_def)
                    out_feature.SetGeometry(f_geom)
                    out_feature.SetFID(fid)
                    tmp_lyr.ogr_layer.CreateFeature(out_feature)

                    in_pts += pts
                    out_pts += f_geom.GetBoundary().GetPointCount()
                else:
                    log.warning('Invalid GEOM with fid: {} for layer {}'.format(fid, name))

            # Load our simplifed biggest shape
            new_biggest_feat = tmp_lyr.ogr_layer.GetFeature(biggest_area[1])
            new_big_geom = new_biggest_feat.GetGeometryRef()
            new_big_geom = geom_validity_fix(new_big_geom)

            if not new_big_geom.IsValid():
                raise Exception('Final shape cannot be invalid for layer {}'.format(name))
            out_feat = ogr.Feature(out_lyr.ogr_layer_def)
            out_feat.SetGeometry(new_big_geom)

            out_lyr.ogr_layer.CreateFeature(out_feat)
            out_feat = None

        log.info('Writing to disk for layer {}'.format(name))
