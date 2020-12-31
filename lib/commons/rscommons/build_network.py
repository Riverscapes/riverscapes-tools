# Name:     Build Network
#
# Purpose:  Filters out features from a flow line ShapeFile and
#           reprojects the features to the specified spatial reference.
#
#           The features to be removed are identified by NHD FCode.
#           Artifical channels can be retained, removed or subset into
#           those that occur within large waterbodies or big rivers
#           represented as polygons.
#
# Author:   Philip Bailey
#
# Date:     15 May 2019
# -------------------------------------------------------------------------------
import os
from osgeo import ogr, osr
from rscommons import ProgressBar, Logger, get_shp_or_gpkg, VectorBase
from rscommons.shapefile import get_geometry_union, get_transform_from_epsg
from rscommons.vector_ops import get_geometry_unary_union as get_geometry_unary_union_NEW
from typing import List, Dict

# https://nhd.usgs.gov/userGuide/Robohelpfiles/NHD_User_Guide/Feature_Catalog/Hydrography_Dataset/Complete_FCode_List.htm
FCodeValues = {
    33400: "connector",
    33600: "canal",
    33601: "aqueduct",
    33603: "stormwater",
    46000: "general",
    46003: "intermittent",
    46006: "perennial",
    46007: "ephemeral",
    55800: "artificial"
}

artifical_reaches = '55800'


def build_network(flowlines, flowareas, waterbodies, outpath, epsg,
                  reach_codes, waterbody_max_size):
    """Copy a polyline feature class and filter out features that are
    not needed.

    Arguments:
        flowlines {str} -- Path to original NHD flow lines polyline ShapeFile
        flowareas {str} -- Path to polygon ShapeFile of large river channels
        waterbodies {str} -- Path to waterbodies polygon ShapeFile
        outpath {str} -- Path where the output polyline ShapeFile will be created
        epsg {int} -- Spatial reference of the output ShapeFile
        perennial {bool} -- True retains perennial channels. False discards them.
        intermittent {bool} -- True retians intermittent channels. False discards them.
        ephemeral {bool} -- True retains ephemeral channels. False discards them.
        waterbody_max_size {float} -- Maximum size of waterbodies that will have their
        flow lines retained.
    """

    log = Logger('Build Network')

    if os.path.isfile(outpath):
        log.info('Skipping building network because output exists {}'.format(outpath))
        return None

    log.info("Building network from flow lines {0}".format(flowlines))

    if reach_codes:
        [log.info('Retaining {} reaches with code {}'.format(FCodeValues[int(r)], r)) for r in reach_codes]
    else:
        log.info('Retaining all reaches. No reach filtering.')

    # Get the input flow lines layer
    driver = ogr.GetDriverByName("ESRI Shapefile")
    inDataSource = driver.Open(flowlines, 0)
    inLayer = inDataSource.GetLayer()
    inSpatialRef = inLayer.GetSpatialRef()

    # Get the transformation required to convert to the target spatial reference
    outSpatialRef, transform = get_transform_from_epsg(inSpatialRef, epsg)

    # Remove output shapefile if it already exists
    if os.path.exists(outpath):
        driver.DeleteDataSource(outpath)

    # Make sure the output folder exists
    resultsFolder = os.path.dirname(outpath)
    if not os.path.isdir(resultsFolder):
        os.mkdir(resultsFolder)

    # Create the output shapefile
    outDataSource = driver.CreateDataSource(outpath)
    outLayer = outDataSource.CreateLayer('network', outSpatialRef, geom_type=ogr.wkbMultiLineString)

    # Add input Layer Fields to the output Layer if it is the one we want
    inLayerDefn = inLayer.GetLayerDefn()
    for i in range(0, inLayerDefn.GetFieldCount()):
        fieldDefn = inLayerDefn.GetFieldDefn(i)
        outLayer.CreateField(fieldDefn)

    # Process all perennial/intermittment/ephemeral reaches first
    if reach_codes and len(reach_codes) > 0:
        [log.info("{0} {1} network features (FCode {2})".format('Retaining', FCodeValues[int(key)], key)) for key in reach_codes]
        inLayer.SetAttributeFilter("FCode IN ({0})".format(','.join([key for key in reach_codes])))
    inLayer.SetSpatialFilter(None)

    log.info('Processing all reaches')
    process_reaches(inLayer, outLayer, transform)

    # Process artifical paths through small waterbodies
    if waterbodies and waterbody_max_size:
        small_waterbodies = get_geometry_union(waterbodies, epsg, 'AreaSqKm <= ({0})'.format(waterbody_max_size))
        log.info('Retaining artificial features within waterbody features smaller than {0}km2'.format(waterbody_max_size))
        inLayer.SetAttributeFilter('FCode = {0}'.format(artifical_reaches))
        inLayer.SetSpatialFilter(VectorBase.shapely2ogr(small_waterbodies))
        process_reaches(inLayer, outLayer, transform)

    # Retain artifical paths through flow areas
    if flowareas:
        flow_polygons = get_geometry_union(flowareas, epsg)
        if flow_polygons:
            log.info('Retaining artificial features within flow area features')
            inLayer.SetAttributeFilter('FCode = {0}'.format(artifical_reaches))
            inLayer.SetSpatialFilter(VectorBase.shapely2ogr(flow_polygons))
            process_reaches(inLayer, outLayer, transform)
        else:
            log.info('Zero artifical paths to be retained.')

    log.info(('{:,} features written to {:}'.format(outLayer.GetFeatureCount(), outpath)))
    log.info('Process completed successfully.')

    # Save and close DataSources
    inDataSource = None
    outDataSource = None


def process_reaches(inLayer, outLayer, transform):
    log = Logger('Process Reaches')
    # Get the output Layer's Feature Definition
    outLayerDefn = outLayer.GetLayerDefn()

    # Add features to the ouput Layer
    progbar = ProgressBar(inLayer.GetFeatureCount(), 50, "Processing Reaches")
    counter = 0
    progbar.update(counter)
    for inFeature in inLayer:
        counter += 1
        progbar.update(counter)

        # get the input geometry and reproject the coordinates
        geom = inFeature.GetGeometryRef()
        geom.Transform(transform)

        # Create output Feature
        outFeature = ogr.Feature(outLayerDefn)

        # Add field values from input Layer
        for i in range(0, outLayerDefn.GetFieldCount()):
            outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), inFeature.GetField(i))

        # Add new feature to output Layer
        outFeature.SetGeometry(geom)
        outLayer.CreateFeature(outFeature)
        outFeature = None

    progbar.finish()

# TODO: replace the above with this when BRAT no longer needs it


def build_network_NEW(flowlines_path: str,
                      flowareas_path: str,
                      out_path: str,
                      epsg: int = None,
                      reach_codes: List[int] = None,
                      waterbodies_path: str = None,
                      waterbody_max_size=None,
                      create_layer: bool = True):

    log = Logger('Build Network')

    log.info("Building network from flow lines {0}".format(flowlines_path))

    if reach_codes:
        [log.info('Retaining {} reaches with code {}'.format(FCodeValues[int(r)], r)) for r in reach_codes]
    else:
        log.info('Retaining all reaches. No reach filtering.')

    # Get the transformation required to convert to the target spatial reference
    if (epsg is not None):
        with get_shp_or_gpkg(flowareas_path) as flowareas_lyr:
            out_spatial_ref, transform = flowareas_lyr.get_transform_from_epsg(epsg)

    # Process all perennial/intermittment/ephemeral reaches first
    attribute_filter = None
    if reach_codes and len(reach_codes) > 0:
        [log.info("{0} {1} network features (FCode {2})".format('Retaining', FCodeValues[int(key)], key)) for key in reach_codes]
        attribute_filter = "FCode IN ({0})".format(','.join([key for key in reach_codes]))

    if create_layer == True:
        with get_shp_or_gpkg(flowlines_path) as flowlines_lyr, get_shp_or_gpkg(out_path, write=True) as out_lyr:
            out_lyr.create_layer_from_ref(flowlines_lyr)

    log.info('Processing all reaches')
    process_reaches_NEW(flowlines_path, out_path, attribute_filter=attribute_filter)

    # Process artifical paths through small waterbodies
    if waterbodies_path is not None and waterbody_max_size is not None:
        small_waterbodies = get_geometry_unary_union_NEW(waterbodies_path, epsg, 'AreaSqKm <= ({0})'.format(waterbody_max_size))
        log.info('Retaining artificial features within waterbody features smaller than {0}km2'.format(waterbody_max_size))
        process_reaches_NEW(flowlines_path,
                            out_path,
                            transform=transform,
                            attribute_filter='FCode = {0}'.format(artifical_reaches),
                            clip_shape=small_waterbodies
                            )

    # Retain artifical paths through flow areas
    if flowareas_path:
        flow_polygons = get_geometry_unary_union_NEW(flowareas_path, epsg)
        if flow_polygons:
            log.info('Retaining artificial features within flow area features')
            process_reaches_NEW(flowlines_path,
                                out_path,
                                transform=transform,
                                attribute_filter='FCode = {0}'.format(artifical_reaches),
                                clip_shape=flow_polygons
                                )

        else:
            log.info('Zero artifical paths to be retained.')

    with get_shp_or_gpkg(out_path) as out_lyr:
        log.info(('{:,} features written to {:}'.format(out_lyr.ogr_layer.GetFeatureCount(), out_path)))

    log.info('Process completed successfully.')
    return out_spatial_ref


def process_reaches_NEW(in_path: str, out_path: str, attribute_filter=None, transform=None, clip_shape=None):
    with get_shp_or_gpkg(in_path) as in_lyr, get_shp_or_gpkg(out_path, write=True) as out_lyr:
        for feature, _counter, _progbar in in_lyr.iterate_features("Processing reaches", attribute_filter=attribute_filter, clip_shape=clip_shape):
            # get the input geometry and reproject the coordinates
            geom = feature.GetGeometryRef()
            if transform is not None:
                geom.Transform(transform)

            # Create output Feature
            out_feature = ogr.Feature(out_lyr.ogr_layer_def)

            # Add field values from input Layer
            for i in range(0, out_lyr.ogr_layer_def.GetFieldCount()):
                field_name = out_lyr.ogr_layer_def.GetFieldDefn(i).GetNameRef()
                output_field_index = feature.GetFieldIndex(field_name)
                if output_field_index >= 0:
                    out_feature.SetField(field_name, feature.GetField(output_field_index))

            # Add new feature to output Layer
            out_feature.SetGeometry(geom)
            out_lyr.ogr_layer.CreateFeature(out_feature)
