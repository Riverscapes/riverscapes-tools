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
import argparse
import os
import sys
import traceback
import json
from osgeo import ogr
from osgeo import osr
from shapely.geometry import mapping, shape
from rscommons import ProgressBar, Logger, dotenv
from rscommons.util import safe_makedirs
from rscommons.shapefile import get_geometry_union, get_transform_from_epsg

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
    if waterbodies:
        small_waterbodies = get_geometry_union(waterbodies, epsg, 'AreaSqKm <= ({0})'.format(waterbody_max_size))
        log.info('Retaining artificial features within waterbody features smaller than {0}km2'.format(waterbody_max_size))
        inLayer.SetAttributeFilter('FCode = {0}'.format(artifical_reaches))
        inLayer.SetSpatialFilter(ogr.CreateGeometryFromWkb(small_waterbodies.wkb))
        process_reaches(inLayer, outLayer, transform)

    # Retain artifical paths through flow areas
    if flowareas:
        flow_polygons = get_geometry_union(flowareas, epsg)
        if flow_polygons:
            log.info('Retaining artificial features within flow area features')
            inLayer.SetAttributeFilter('FCode = {0}'.format(artifical_reaches))
            inLayer.SetSpatialFilter(ogr.CreateGeometryFromWkb(flow_polygons.wkb))
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


# def main():
#     parser = argparse.ArgumentParser(
#         description='Build Networks:',
#         # epilog="This is an epilog"
#     )
#     parser.add_argument('flowline', help='Input flowline ShapeFile path', type=str)
#     parser.add_argument('area', help='Input river areas ShapeFile path', type=str)
#     parser.add_argument('waterbody', help='Input waterbody ShapeFile path', type=str)
#     parser.add_argument('network', help='Output network ShapeFile path', type=str)
#     parser.add_argument('--perennial', help='(optional) include perennial channels', action='store_true', default=True)
#     parser.add_argument('--intermittent', help='(optional) include intermittent channels', action='store_true', default=False)
#     parser.add_argument('--ephemeral', help='(optional) include ephemeral channels', action='store_true', default=False)
#     parser.add_argument('--waterbodysize', help='(optional) Water body size', type=float, default=0.01)
#     parser.add_argument('--epsg', help='Output spatial reference EPSG', default=4269, type=int)
#     parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)

#     args = dotenv.parse_args_env(parser)

#     # make sure the output folder exists
#     results_folder = os.path.dirname(args.network)
#     safe_makedirs(results_folder)

#     # Initiate the log file
#     logg = Logger("Build Network")
#     logfile = os.path.join(results_folder, "build_network.log")
#     logg.setup(logPath=logfile, verbose=args.verbose)

#     if os.path.isfile(args.network):
#         logg.info('Deleting existing output {}'.format(args.network))
#         driver = ogr.GetDriverByName("ESRI Shapefile")
#         driver.DeleteDataSource(args.network)

#     reach_codes = args['reach_codes'].split('')

#     try:
#         build_network(args.flowline, args.area, args.waterbody, args.network, args.epsg,
#                       args.perennial, args.intermittent, args.ephemeral, args.waterbodysize)

#     except Exception as e:
#         logg.error(e)
#         traceback.print_exc(file=sys.stdout)
#         sys.exit(1)

#     sys.exit(0)


# if __name__ == '__main__':
#     main()
