# Name:     Segment Network
#
# Purpose:  This script segments a polyline ShapeFile into reaches of
#           user defined length. The script attempts to segment the lines
#           at a desired length but does not split lines if it would
#           result ina line less than the specified minimum length.
#
#           Note that the interval and minimum are in the linear units
#           of the input ShpaeFile (i.e. best used with projected data).
#
# Author:   Philip Bailey
#
# Date:     15 May 2019
# -------------------------------------------------------------------------------
import argparse
import os
import sys
import traceback
from osgeo import ogr, osr
from shapely.geometry import LineString, Point
from rscommons import Logger, ProgressBar, initGDALOGRErrors, dotenv, get_shp_or_gpkg, VectorBase
from rscommons.classes.vector_base import get_utm_zone_epsg

initGDALOGRErrors()


class SegmentFeature:
    '''
    '''

    def __init__(self, feature, transform):
        self.name = feature.GetField('GNIS_NAME')
        georef = feature.GetGeometryRef()
        self.fid = feature.GetFID()
        geotype = georef.GetGeometryType()
        self.FCode = feature.GetField('FCode')
        self.TotDASqKm = feature.GetField('TotDASqKm')
        self.DivDASqKm = feature.GetField('DivDASqKm')
        self.NHDPlusID = feature.GetField('NHDPlusID')

        if geotype not in [ogr.wkbLineStringZM, ogr.wkbLineString, ogr.wkbLineString25D, ogr.wkbLineStringM]:
            raise Exception('Multipart geometry in the original ShapeFile')

        pts = []

        pts = georef.GetPoints()

        self.start = ogr.Geometry(ogr.wkbPoint)
        self.start.AddPoint(*pts[0])

        self.end = ogr.Geometry(ogr.wkbPoint)
        self.end.AddPoint(*pts[-1])

        georef.Transform(transform)
        self.length_m = georef.Length()


def segment_network(inpath: str, outpath: str, interval: float, minimum: float, watershed_id: str, create_layer=False):
    """
    Chop the lines in a polyline feature class at the specified interval unless
    this would create a line less than the minimum in which case the line is not segmented.
    :param inpath: Original network feature class
    :param outpath: Output segmented network feature class
    :param interval: Distance at which to segment each line feature (map units)
    :param minimum: Minimum length below which lines are not segmented (map units)
    :param watershed_id: Give this watershed an id (str)
    :param create_layer: This layer may be created earlier. We can choose to create it. Defaults to false (bool)
    :return: None
    """

    log = Logger('Segment Network')

    if interval <= 0:
        log.info('Skipping segmentation.')
    else:
        log.info('Segmenting network to {}m, with minimum feature length of {}m'.format(interval, minimum))
        log.info('Segmenting network from {0}'.format(inpath))

    # NOTE: Remember to always open the 'write' layer first in case it's the same geopackage
    with get_shp_or_gpkg(outpath, write=True) as out_lyr, get_shp_or_gpkg(inpath) as in_lyr:
        # Get the input NHD flow lines layer
        srs = in_lyr.spatial_ref
        feature_count = in_lyr.ogr_layer.GetFeatureCount()
        log.info('Input feature count {:,}'.format(feature_count))

        # Get the closest EPSG possible to calculate length
        extent_poly = ogr.Geometry(ogr.wkbPolygon)
        extent_centroid = extent_poly.Centroid()
        utm_epsg = get_utm_zone_epsg(extent_centroid.GetX())
        transform_ref, transform = VectorBase.get_transform_from_epsg(in_lyr.spatial_ref, utm_epsg)

        # IN order to get accurate lengths we are going to need to project into some coordinate system
        transform_back = osr.CoordinateTransformation(transform_ref, srs)

        # Create the output shapefile
        if create_layer is True:
            out_lyr.create_layer_from_ref(in_lyr)
            # We add two features to this
            out_lyr.create_fields({
                'ReachID': ogr.OFTInteger,
                'WatershedID': ogr.OFTString
            })

        # Retrieve all input features keeping track of which ones have GNIS names or not
        named_features = {}
        all_features = []
        junctions = []

        # Omit pipelines with FCode 428**
        attribute_filter = 'FCode < 42800 OR FCode > 42899'
        log.info('Filtering out pipelines ({})'.format(attribute_filter))

        for in_feature, _counter, _progbar in in_lyr.iterate_features("Loading Network", attribute_filter=attribute_filter):
            # Store relevant items as a tuple:
            # (name, FID, StartPt, EndPt, Length, FCode)
            s_feat = SegmentFeature(in_feature, transform)

            # Add the end points of all lines to a single list
            junctions.extend([s_feat.start, s_feat.end])

            if not s_feat.name or len(s_feat.name) < 1 or interval <= 0:
                # Add features without a GNIS name to list. Also add to list if not segmenting
                all_features.append(s_feat)
            else:
                # Build separate lists for each unique GNIS name
                if s_feat.name not in named_features:
                    named_features[s_feat.name] = [s_feat]
                else:
                    named_features[s_feat.name].append(s_feat)

        # Loop over all features with the same GNIS name.
        # Only merge them if they meet at a junction where no other lines meet.
        log.info('Merging simple features with the same GNIS name...')
        for name, features in named_features.items():
            log.debug('   {} x{}'.format(name.encode('utf-8'), len(features)))
            all_features.extend(features)

        log.info('{:,} features after merging. Starting segmentation...'.format(len(all_features)))

        # Segment the features at the desired interval
        # rid = 0
        log.info('Segmenting Network...')
        progbar = ProgressBar(len(all_features), 50, "Segmenting")
        counter = 0

        # max_fid = out_lyr.ogr_layer.GetFeatureCount()

        out_lyr.ogr_layer.StartTransaction()
        for orig_feat in all_features:
            counter += 1
            progbar.update(counter)

            old_feat = in_lyr.ogr_layer.GetFeature(orig_feat.fid)
            old_geom = old_feat.GetGeometryRef()
            #  Anything that produces reach shorter than the minimum just gets added. Also just add features if not segmenting
            if orig_feat.length_m < (interval + minimum) or interval <= 0:
                new_ogr_feat = ogr.Feature(out_lyr.ogr_layer_def)
                copy_fields(old_feat, new_ogr_feat, in_lyr.ogr_layer_def, out_lyr.ogr_layer_def)
                # Set the attributes using the values from the delimited text file
                new_ogr_feat.SetField("GNIS_NAME", orig_feat.name)
                new_ogr_feat.SetField("WatershedID", watershed_id)
                # new_ogr_feat.SetFID(max_fid)
                new_ogr_feat.SetGeometry(old_geom)
                out_lyr.ogr_layer.CreateFeature(new_ogr_feat)
                # max_fid += 1
            else:
                # From here on out we use shapely and project to UTM. We'll transform back before writing to disk.
                new_geom = old_geom.Clone()
                new_geom.Transform(transform)
                remaining = LineString(new_geom.GetPoints())
                while remaining and remaining.length >= (interval + minimum):
                    part1shply, part2shply = cut(remaining, interval)
                    remaining = part2shply

                    new_ogr_feat = ogr.Feature(out_lyr.ogr_layer_def)
                    copy_fields(old_feat, new_ogr_feat, in_lyr.ogr_layer_def, out_lyr.ogr_layer_def)

                    # Set the attributes using the values from the delimited text file
                    new_ogr_feat.SetField("GNIS_NAME", orig_feat.name)
                    new_ogr_feat.SetField("WatershedID", watershed_id)
                    # new_ogr_feat.SetFID(max_fid)

                    geo = ogr.CreateGeometryFromWkt(part1shply.wkt)
                    geo.Transform(transform_back)
                    new_ogr_feat.SetGeometry(geo)
                    out_lyr.ogr_layer.CreateFeature(new_ogr_feat)
                    # max_fid += 1

                # Add any remaining line to outGeometries
                if remaining:
                    new_ogr_feat = ogr.Feature(out_lyr.ogr_layer_def)
                    copy_fields(old_feat, new_ogr_feat, in_lyr.ogr_layer_def, out_lyr.ogr_layer_def)

                    # Set the attributes using the values from the delimited text file
                    new_ogr_feat.SetField("GNIS_NAME", orig_feat.name)
                    new_ogr_feat.SetField("WatershedID", watershed_id)
                    # new_ogr_feat.SetFID(max_fid)

                    geo = ogr.CreateGeometryFromWkt(remaining.wkt)
                    geo.Transform(transform_back)
                    new_ogr_feat.SetGeometry(geo)
                    out_lyr.ogr_layer.CreateFeature(new_ogr_feat)
                    # max_fid += 1
        out_lyr.ogr_layer.CommitTransaction()
        progbar.finish()

        log.info(('{:,} features written to {:}'.format(out_lyr.ogr_layer.GetFeatureCount(), outpath)))
        log.info('Process completed successfully.')


def cut(line, distance):
    """
    Cuts a line in two at a distance from its starting point
    :param line: line geometry
    :param distance: distance at which to cut the liner
    :return: List where the first item is the first part of the line
    and the second is the remaining part of the line (if there is any)
    """

    if distance <= 0.0 or distance >= line.length:
        return (line)

    for i, p in enumerate(line.coords):
        pd = line.project(Point(p))
        if pd == distance:
            return (
                LineString(line.coords[:i + 1]),
                LineString(line.coords[i:])
            )
        if pd > distance:
            cp = line.interpolate(distance)
            return (
                LineString(line.coords[:i] + [cp]),
                LineString([cp] + line.coords[i:])
            )
        if line.is_ring and i + 2 == len(line.coords):
            # Rare case when line is ring (pt[0] == pt[-1]) and
            # distance lies between last and second to last coords
            cut_distance = distance - pd
            cp = LineString(line.coords[:-1]).interpolate(cut_distance)
            return (
                LineString(line.coords[:-1] + [cp]),
                LineString([cp] + line.coords[-1:])
            )
    return None


def copy_fields(in_feature, out_feature, in_layer_def, out_layer_def, skip_fid=False):
    # Add field values from input Layer
    for i in range(0, in_layer_def.GetFieldCount()):
        # skip if fid field
        if skip_fid is True and in_layer_def.GetFieldDefn(i).GetNameRef().lower() == 'fid':
            continue
        out_feature.SetField(out_layer_def.GetFieldDefn(i).GetNameRef(), in_feature.GetField(i))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('network', help='Input stream network ShapeFile path', type=str)
    parser.add_argument('segmented', help='Output segmented network ShapeFile path', type=str)
    parser.add_argument('interval', help='Interval distance at which to segment the network', type=float)
    parser.add_argument('minimum', help='Minimum feature length in the segmented network', type=float)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logg = Logger("Segment Network")
    logfile = os.path.join(os.path.dirname(args.segmented), "segment_network.log")
    logg.setup(logPath=logfile, verbose=args.verbose)

    if os.path.isfile(args.segmented):
        logg.info('Deleting existing output {}'.format(args.segmented))
        shpDriver = ogr.GetDriverByName("ESRI Shapefile")
        shpDriver.DeleteDataSource(args.segmented)

    try:
        segment_network(args.network, args.segmented, args.interval, args.minimum, args.tolerance)

    except Exception as e:
        logg.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
