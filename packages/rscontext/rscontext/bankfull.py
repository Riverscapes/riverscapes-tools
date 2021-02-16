"""Tools for generating and working with Bankfull Polygons

    Kelly Whitehead
    Feb 16, 2021
    """

import os
from uuid import uuid4
from osgeo import ogr

from rscommons import ProgressBar, Logger, GeopackageLayer, VectorBase, TempRaster, TempGeopackage
from rscommons.vector_ops import get_geometry_unary_union, collect_feature_class, buffer_by_field, merge_feature_classes


def bankfull_buffer(in_flowlines, espg, bankfull_path):

    buffer_by_field(in_flowlines, bankfull_path, "BFwidth", espg, min_buffer=0.0, centered=True)


def bankfull_nhd_area(bankfull_path, nhd_path, clip_path, espg, output_path, out_name):

    clip_geom = collect_feature_class(clip_path)

    with TempGeopackage('sanitize_temp') as tempgpkg, \
            GeopackageLayer(output_path, out_name, write=True) as lyr_output:

        merged_path = os.path.join(tempgpkg.filepath, f"bankfull_nhd_merge_{str(uuid4())}")

        with GeopackageLayer(merged_path, write=True, delete_dataset=True) as tmp_lyr:
            tmp_lyr.create_layer(ogr.wkbPolygon, espg)

        # Get merged and unioned Geom
        merge_feature_classes([nhd_path, bankfull_path], clip_geom, merged_path)
        out_geom = get_geometry_unary_union(merged_path)

        # Write Output
        lyr_output.create_layer(ogr.wkbPolygon, espg)
        feat = ogr.Feature(lyr_output.ogr_layer_def)
        feat.SetGeometry(out_geom)
        lyr_output.create_feature(out_geom)
