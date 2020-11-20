# -------------------------------------------------------------------------------
# Name:     Valley Bottom
#
# Purpose:  Perform initial VBET analysis that can be used by the BRAT conservation
#           module
#
# Author:   Philip Bailey
#
# Date:     7 Oct 2019
#
# https://nhd.usgs.gov/userGuide/Robohelpfiles/NHD_User_Guide/Feature_Catalog/Hydrography_Dataset/Complete_FCode_List.htm
# -------------------------------------------------------------------------------
import os
import json
from osgeo import gdal
from rscommons import Logger, VectorLayer
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry
from rscommons.vector_ops import get_geometry_unary_union
from osgeo import ogr
import osgeo.osr as osr


def vbet_network(flow_lines_lyr: VectorLayer, flow_areas_lyr: VectorLayer, out_lyr: VectorLayer, epsg: int = None):

    log = Logger('VBET Network')
    log.info('Generating perennial network')

    # Add input Layer Fields to the output Layer if it is the one we want
    out_lyr.create_layer_from_ref(flow_lines_lyr, epsg=epsg)

    # Perennial features
    log.info('Incorporating perennial features')
    include_features(flow_lines_lyr, out_lyr, "FCode = '46006'")

    # Flow area features
    polygon = get_geometry_unary_union(flow_areas_lyr, epsg=epsg)
    if polygon is not None:
        log.info('Incorporating flow areas.')
        include_features(flow_lines_lyr, out_lyr, "FCode <> '46006'", polygon)

    fcount = flow_lines_lyr.ogr_layer.GetFeatureCount()

    log.info('VBET network generated with {} features'.format(fcount))


def include_features(source_layer: VectorLayer, out_layer: VectorLayer, attribute_filter: str = None, clip_shape: BaseGeometry = None):

    for feature, _counter, _progbar in source_layer.iterate_features('Including Features', write_layers=[out_layer], attribute_filter=attribute_filter, clip_shape=clip_shape):
        out_feature = ogr.Feature(out_layer.ogr_layer_def)

        # Add field values from input Layer
        for i in range(0, out_layer.ogr_layer_def.GetFieldCount()):
            out_feature.SetField(out_layer.ogr_layer_def.GetFieldDefn(i).GetNameRef(), feature.GetField(i))

        geom = feature.GetGeometryRef()
        out_feature.SetGeometry(geom.Clone())
        out_layer.ogr_layer.CreateFeature(out_feature)
