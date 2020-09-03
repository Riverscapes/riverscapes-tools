import os
import json
from osgeo import ogr
from osgeo import osr
from rscommons.shapefile import get_geometry_union
from rscommons.shapefile import merge_feature_classes
from rscommons.shapefile import get_transform_from_epsg
from rscommons.util import safe_makedirs
from rscommons import Logger

# The NHD FCodes that identify canals
canal_fcodes = [33600, 33601, 33603]


def clean_ntd_data(shapefiles, nhd_flowlines, boundary_path, output_folder, output_epsg):

    log = Logger('Clean NTD')
    log.info('Merging transportation data for {} states.'.format(len(shapefiles)))

    paths = {
        'Roads': os.path.join(output_folder, 'roads.shp'),
        'Rail': os.path.join(output_folder, 'railways.shp'),
        'Canals': os.path.join(output_folder, 'canals.shp')
    }

    # Get the HUC boundary as a single polygon
    boundary = get_geometry_union(boundary_path, output_epsg)

    # Extract the canals from the NHD data
    extract_canals(nhd_flowlines, output_epsg, boundary, paths['Canals'])

    road = []
    rail = []
    for path_dic in shapefiles.values():
        road.extend([path for name, path in path_dic.items() if 'Trans_RoadSegment'.lower() in name.lower()])
        rail.extend([path for name, path in path_dic.items() if 'Trans_Rail'.lower() in name.lower()])

    log.info('{} road shapefiles identified.'.format(len(road)))
    log.info('{} rail shapefiles identified.'.format(len(rail)))

    merge_feature_classes(road, output_epsg, boundary, paths['Roads'])
    merge_feature_classes(rail, output_epsg, boundary, paths['Rail'])

    log.info('Merging transportation data complete.')
    return paths


def extract_canals(flowlines, epsg, boundary, outpath):

    log = Logger('Canals')

    driver = ogr.GetDriverByName("ESRI Shapefile")
    inDataSource = driver.Open(flowlines, 0)
    inLayer = inDataSource.GetLayer()
    inSpatialRef = inLayer.GetSpatialRef()
    inLayer.SetAttributeFilter("FCode IN ({0})".format(','.join([str(fcode) for fcode in canal_fcodes])))

    log.info('{:,} canal features identified in NHD flow lines.'.format(inLayer.GetFeatureCount()))

    if os.path.isfile(outpath):
        log.info('Skipping extracting canals from NHD flow lines because file exists.')
        return outpath

    extract_path = os.path.dirname(outpath)
    safe_makedirs(extract_path)

    # Create the output shapefile
    outSpatialRef, transform = get_transform_from_epsg(inSpatialRef, epsg)

    outDataSource = driver.CreateDataSource(outpath)
    outLayer = outDataSource.CreateLayer('network', outSpatialRef, geom_type=ogr.wkbMultiLineString)
    outLayerDefn = outLayer.GetLayerDefn()

    for feature in inLayer:
        geom = feature.GetGeometryRef()
        geom.Transform(transform)

        outFeature = ogr.Feature(outLayerDefn)
        outFeature.SetGeometry(geom)
        outLayer.CreateFeature(outFeature)

        outFeature = None
        feature = None

    log.info('{:,} features written to canals shapefile.'.format(outLayer.GetFeatureCount()))
    inDataSource = None
    outDataSource = None
    return outpath
