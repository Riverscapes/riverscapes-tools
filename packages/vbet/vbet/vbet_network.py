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
from rscommons import Logger
from rscommons.shapefile import get_geometry_union
from shapely.geometry import mapping
from osgeo import ogr
import osgeo.osr as osr


def vbet_network(flow_lines, flow_areas, epsg, outpath):

    log = Logger('VBET Network')
    log.info('Generating perennial network')

    # Get the input flow lines layer
    driver = ogr.GetDriverByName("ESRI Shapefile")
    inDataSource = driver.Open(flow_lines, 0)
    flowlines_layer = inDataSource.GetLayer()

    if os.path.isfile(outpath):
        driver.DeleteDataSource(outpath)

    # Create the output shapefile
    outDataSource = driver.CreateDataSource(outpath)
    outLayer = outDataSource.CreateLayer('network', flowlines_layer.GetSpatialRef(), geom_type=ogr.wkbMultiLineString)

    # Add input Layer Fields to the output Layer if it is the one we want
    inLayerDefn = flowlines_layer.GetLayerDefn()
    for i in range(0, inLayerDefn.GetFieldCount()):
        fieldDefn = inLayerDefn.GetFieldDefn(i)
        outLayer.CreateField(fieldDefn)

    # Perennial features
    log.info('Incorporating perennial features')
    flowlines_layer.SetAttributeFilter("FCode = '46006'")
    flowlines_layer.SetSpatialFilter(None)
    include_features(flowlines_layer, outLayer)

    # Flow area features
    polygon = get_geometry_union(flow_areas, epsg)
    if polygon is not None:
        log.info('Incorporating flow areas.')
        flowlines_layer.SetAttributeFilter("FCode <> '46006'")
        flowlines_layer.SetSpatialFilter(ogr.CreateGeometryFromJson(json.dumps(mapping(polygon))))
        include_features(flowlines_layer, outLayer)

    fcount = flowlines_layer.GetFeatureCount()

    log.info('VBET network generated with {} features'.format(fcount))
    outDataSource = None
    inDataSource = None


def include_features(source_layer, dest_layer):

    # Get the output layer's feature definition now all the fields are present
    outLayerDefn = dest_layer.GetLayerDefn()

    for in_feature in source_layer:

        # Create output Feature
        out_feature = ogr.Feature(outLayerDefn)

        # Add field values from input Layer
        for i in range(0, outLayerDefn.GetFieldCount()):
            out_feature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), in_feature.GetField(i))

        geom = in_feature.GetGeometryRef()
        out_feature.SetGeometry(geom.Clone())
        dest_layer.CreateFeature(out_feature)
