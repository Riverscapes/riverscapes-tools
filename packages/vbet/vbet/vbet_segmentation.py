""" VBET Segmentation

    Purpose:    Functions to generate vbet segmentation points, polygons, and summarize attributes
    Author:     Kelly Whitehead
    Date:       August 2022
"""

import os
import sys
import argparse

from osgeo import ogr, osr
from shapely.ops import linemerge, voronoi_diagram
from shapely.geometry import MultiLineString, MultiPoint
from shapely.topology import TopologicalError

from rscommons import GeopackageLayer, Logger, VectorBase, dotenv
from rscommons.util import parse_metadata
from rscommons.classes.vector_base import get_utm_zone_epsg

Path = str


def generate_segmentation_points(line_network: Path, out_points_layer: Path, stream_size_lookup: dict, distance: float = 200.0):
    """heavily modified from: https://glenbambrick.com/2017/09/15/osgp-create-points-along-line/
    """

    log = Logger('Generate Segmentation Points')

    init_distance = distance / 2

    with GeopackageLayer(out_points_layer, write=True) as out_lyr, \
            GeopackageLayer(line_network) as line_lyr:

        out_fields = {"LevelPathI": ogr.OFTString,
                      "seg_distance": ogr.OFTReal,
                      # "seg_length": ogr.OFTReal,
                      "stream_size": ogr.OFTInteger}
        out_lyr.create_layer(ogr.wkbPoint, spatial_ref=line_lyr.spatial_ref, fields=out_fields)

        extent_poly = ogr.Geometry(ogr.wkbPolygon)
        extent_centroid = extent_poly.Centroid()
        utm_epsg = get_utm_zone_epsg(extent_centroid.GetX())
        transform_ref, transform = VectorBase.get_transform_from_epsg(line_lyr.spatial_ref, utm_epsg)
        # In order to get accurate lengths we are going to need to project into some coordinate system
        transform_back = osr.CoordinateTransformation(transform_ref, line_lyr.spatial_ref)

        for feat, *_ in line_lyr.iterate_features(write_layers=[out_lyr]):
            level_path = feat.GetField('LevelPathI')
            if level_path not in stream_size_lookup:
                log.error(f'Stream Size not found for LevelPathI {level_path}. Skipping segmentation')
                continue
            stream_size = stream_size_lookup[level_path]
            geom_line = feat.GetGeometryRef()
            geom_line.FlattenTo2D()
            geom_line.Transform(transform)
            shapely_multiline = VectorBase.ogr2shapely(geom_line)
            if shapely_multiline.length == 0.0:
                continue
            cleaned_line = clean_linestring(shapely_multiline)

            for shapely_line in cleaned_line.geoms:
                # list to hold all the point coords
                if shapely_line.geom_type != "LineString":
                    continue
                list_points = []
                # set the current distance to place the point
                current_dist = init_distance
                # get the geometry of the line as wkt
                # ## get the total length of the line
                line_length = shapely_line.length
                # append the starting coordinate to the list
                # list_points.append(Point(list(shapely_line.coords)[0]))
                # https://nathanw.net/2012/08/05/generating-chainage-distance-nodes-in-qgis/
                # while the current cumulative distance is less than the total length of the line
                while current_dist < line_length:
                    # use interpolate and increase the current distance
                    list_points.append((shapely_line.interpolate(current_dist), current_dist))
                    current_dist += distance
                # append end coordinate to the list
                # list_points.append(Point(list(shapely_line.coords)[-1]))

                # add points to the layer
                # for each point in the list
                for (pnt, out_dist) in list_points:  # enumerate(list_points, 1):
                    # create a point object
                    geom_pnt = ogr.Geometry(ogr.wkbPoint)
                    geom_pnt.AddPoint_2D(pnt.x, pnt.y)
                    geom_pnt.Transform(transform_back)
                    # populate the distance values for each point.
                    # start point
                    # if num == 1:
                    #     out_dist = 0
                    # elif num < len(list_points):
                    #     out_dist = distance * (num - 1)
                    # # end point
                    # elif num == len(list_points):
                    #     out_dist = int(line_length)
                    # add the point feature to the output.
                    attributes = {'LevelPathI': str(int(level_path)),
                                  'seg_distance': out_dist,
                                  'stream_size': stream_size}
                    out_lyr.create_feature(geom_pnt, attributes=attributes)


def split_vbet_polygons(vbet_polygons, segmentation_points, out_split_polygons):
    """split vbet polygons into segments based on segmentation points

    Args:
        vbet_polygons (Path): geopackage feature class of vbet polygons to split
        segmentation_points (Path): geopackage feature class of segmentation points used for splitting
        out_split_polygons (Path): output geopackage feature class to create
    """
    log = Logger('Split Polygons using Voronoi')

    with GeopackageLayer(out_split_polygons, write=True) as out_lyr, \
            GeopackageLayer(vbet_polygons) as vbet_lyr, \
            GeopackageLayer(segmentation_points) as points_lyr:

        fields = {'LevelPathI': ogr.OFTString,
                  'seg_distance': ogr.OFTReal}
        out_lyr.create_layer(ogr.wkbMultiPolygon, spatial_ref=vbet_lyr.spatial_ref, fields=fields)

        for vbet_feat, *_ in vbet_lyr.iterate_features(write_layers=[out_lyr]):

            level_path = vbet_feat.GetField('LevelPathI')
            if level_path is None:
                continue

            vbet_geom = vbet_feat.GetGeometryRef()
            vbet_sgeom = VectorBase.ogr2shapely(vbet_geom)
            list_points = []

            for point_feat, *_ in points_lyr.iterate_features(attribute_filter=f'LevelPathI = {level_path}'):
                point_geom = point_feat.GetGeometryRef()
                point_sgeom = VectorBase.ogr2shapely(point_geom)
                list_points.append(point_sgeom)

            seed_points_sgeom_mpt = MultiPoint(list_points)
            voronoi = voronoi_diagram(seed_points_sgeom_mpt, envelope=vbet_sgeom)
            for poly in voronoi.geoms:
                try:
                    poly_intersect = vbet_sgeom.intersection(poly)
                except TopologicalError as err:
                    # The operation 'GEOSIntersection_r' could not be performed. Likely cause is invalidity of the geometry
                    log.error(err)
                    continue
                if poly_intersect.geom_type in ['GeometryCollection', 'LineString'] or poly_intersect.is_empty:
                    continue
                clean_geom = poly_intersect.buffer(0) if poly_intersect.is_valid is not True else poly_intersect
                geom_out = VectorBase.shapely2ogr(clean_geom)
                geom_out = ogr.ForceToMultiPolygon(geom_out)
                out_lyr.create_feature(geom_out, {'LevelPathI': str(int(level_path))})

        for segment_feat, *_ in out_lyr.iterate_features('Writing segment dist to polygons'):
            polygon = segment_feat.GetGeometryRef()
            for pt_feat, *_ in points_lyr.iterate_features(clip_shape=polygon):
                seg_distance = pt_feat.GetField('seg_distance')
                segment_feat.SetField('seg_distance', seg_distance)
                out_lyr.ogr_layer.SetFeature(segment_feat)

    log.info('VBET polygon successfully segmented')


def calculate_segmentation_metrics(vbet_segment_polygons: Path, vbet_centerline: Path, dict_layers, attrib_filter=None):
    """_summary_

    Args:
        vbet_segment_polygons (_type_): _description_
        vbet_centerline (_type_): _description_
        dict_layers (_type_): Dictionary[layer_name(str), feature_class(str)]
    """

    log = Logger('Segmentation Metrics')

    with GeopackageLayer(vbet_segment_polygons, write=True) as vbet_lyr, \
            GeopackageLayer(vbet_centerline) as centerline_lyr:

        # Check fields and create if they don't exist
        exist_fields = vbet_lyr.get_fields()
        metric_field_names = []
        for metric_layer_name in dict_layers.keys():
            metric_field_names.extend([f"{metric_layer_name}_{metric_type}" for metric_type in ['area', 'prop']])
        metric_field_names.extend(['centerline_length', 'segment_area'])
        fields = {field_name: ogr.OFTReal for field_name in metric_field_names if field_name not in exist_fields}
        if len(fields) > 0:
            vbet_lyr.create_fields(fields)

        for vbet_feat, *_ in vbet_lyr.iterate_features('Calculating metrics per vbet segment', attribute_filter=attrib_filter, write_layers=[vbet_lyr]):
            vbet_geom = vbet_feat.GetGeometryRef()
            centroid = vbet_geom.Centroid()
            utm_epsg = get_utm_zone_epsg(centroid.GetX())
            _transform_ref, transform = VectorBase.get_transform_from_epsg(vbet_lyr.spatial_ref, utm_epsg)
            vbet_geom_transform = vbet_geom.Clone()
            vbet_geom_transform.Transform(transform)
            vbet_geom_transform_clean = vbet_geom_transform.MakeValid()
            if not vbet_geom_transform_clean.IsValid():
                log.warning(f'Unable to generate metrics for vbet segment {vbet_feat.GetFID()}: Invalid VBET Segment Geometry')
                continue
            vbet_area = vbet_geom_transform_clean.GetArea()
            if vbet_area == 0.0:
                log.warning(f'Unable to generate metrics for vbet segment {vbet_feat.GetFID()}: VBET Segment has no area')
                continue

            length = 0.0
            for centerline_feat, *_ in centerline_lyr.iterate_features(clip_shape=vbet_geom):
                centerline_geom = centerline_feat.GetGeometryRef()
                _transform_ref, transform = VectorBase.get_transform_from_epsg(centerline_lyr.spatial_ref, utm_epsg)
                centerline_geom.Transform(transform)
                if not centerline_geom.IsValid():
                    log.warning(f'Invalid centerline geometry found for vbet segment {vbet_feat.GetFID()}')
                try:
                    intersect_geom = vbet_geom_transform_clean.Intersection(centerline_geom)
                except IOError:
                    log.error(str(IOError))
                    break
                length = length + intersect_geom.Length()

            vbet_feat.SetField('centerline_length', length)
            vbet_feat.SetField('segment_area', vbet_area)

            for metric_layer_name, metric_layer_path in dict_layers.items():
                with GeopackageLayer(metric_layer_path) as metric_lyr:
                    metric_area = 0.0
                    for metric_feat, *_ in metric_lyr.iterate_features(clip_shape=vbet_geom):
                        in_metric_geom = metric_feat.GetGeometryRef()
                        in_metric_geom.Transform(transform)
                        metric_geom = in_metric_geom.MakeValid()
                        if not metric_geom.IsValid():
                            log.warning(f'Unable to generate metric for {metric_layer_name} for vbet segment {vbet_feat.GetFID()}. Invalid metric Geometry')
                            continue
                        try:
                            delta_geom = vbet_geom_transform_clean.Intersection(metric_geom)
                        except IOError:
                            log.error(str(IOError))
                            delta_geom = None
                            continue
                        delta_geom.MakeValid()
                        if not delta_geom.IsValid() or delta_geom.GetGeometryType() not in VectorBase.POLY_TYPES + VectorBase.COLLECTION_TYPES:
                            continue
                        metric_area = metric_area + delta_geom.GetArea()
                    metric_prop = metric_area / vbet_area
                    vbet_feat.SetField(f'{metric_layer_name}_area', metric_area)
                    vbet_feat.SetField(f'{metric_layer_name}_prop', metric_prop)
            vbet_lyr.ogr_layer.SetFeature(vbet_feat)
            vbet_feat = None
            vbet_geom = None


def clean_linestring(in_geom: ogr.Geometry) -> MultiLineString:
    """return a merged multilinestring from a linestring, multilinestring or geometrycollection

    Args:
        in_geom (ogr.Geometry): linestring, multilinestring or geometrycollection

    Returns:
        MultiLineString: cleaned multilinestring
    """

    if in_geom.geom_type == 'GeometryCollection':
        geoms = []
        for geom in in_geom.geoms:
            if geom.geom_type == 'LineString':
                geoms.append(geom)
        in_geom = MultiLineString(geoms)
    merged_line = linemerge(in_geom) if in_geom.geom_type == 'MultiLineString' else in_geom
    if merged_line.geom_type == 'LineString':
        merged_line = MultiLineString([merged_line])

    return merged_line


def summerize_vbet_metrics(segment_points: Path, segmented_polygons: Path, level_paths: list, distance_lookup: dict, metric_names: list):
    """generate moving window summary of segmented vbet polygons

    Args:
        segment_points (Path): geopackage feature class of segmentation points to include attributes
        segmented_polygons (Path): geopackage feature class of segmented vbet polygons
        level_paths (list): list of NHD level paths
        distance_lookup (dict): dictionary of distances per stream size
        metric_names (list): list of metric names to generate summary attributes on
    """

    with GeopackageLayer(segment_points, write=True) as lyr_pts, \
            GeopackageLayer(segmented_polygons) as lyr_polygons:

        metric_fields = {}
        for metric in metric_names:
            metric_fields[f'{metric}_area'] = ogr.OFTReal
            metric_fields[f'{metric}_area_prop'] = ogr.OFTReal
            metric_fields[f'{metric}_area_cl_len'] = ogr.OFTReal
        metric_fields['integrated_width'] = ogr.OFTReal
        metric_fields['window_size'] = ogr.OFTReal
        metric_fields['window_area'] = ogr.OFTReal
        metric_fields['centerline_length'] = ogr.OFTReal
        lyr_pts.create_fields(metric_fields)

        for level_path in level_paths:
            if level_path is None or level_path not in distance_lookup.keys():
                continue
            window_distance = distance_lookup[level_path]
            for feat_seg_pt, *_ in lyr_pts.iterate_features(f'Summerizing vbet metrics for {level_path}', attribute_filter=f"LevelPathI = {level_path}"):
                dist = feat_seg_pt.GetField('seg_distance')
                min_dist = dist - 0.5 * window_distance
                max_dist = dist + 0.5 * window_distance
                sql_seg_poly = f"LevelPathI = {level_path} AND seg_distance >= {min_dist} AND seg_distance <= {max_dist}"
                window_metrics = dict.fromkeys(metric_names, 0.0)
                window_length = 0.0
                window_area = 0.0
                for feat_seg_poly, *_ in lyr_polygons.iterate_features(attribute_filter=sql_seg_poly):
                    window_length = window_length + feat_seg_poly.GetField('centerline_length')
                    window_area = window_area + feat_seg_poly.GetField('segment_area')
                    for metric in metric_names:
                        metric_value = feat_seg_poly.GetField(f'{metric}_area')
                        metric_value = metric_value if metric_value is not None else 0.0
                        window_metrics[metric] = window_metrics[metric] + metric_value
                for metric, value in window_metrics.items():
                    # value = value / window_distance
                    value_per_length = value / window_length if window_length != 0.0 else 0.0
                    value_porportion = value / window_area if window_area != 0.0 else 0.0
                    feat_seg_pt.SetField(f'{metric}_area', value)
                    feat_seg_pt.SetField(f'{metric}_area_prop', value_porportion)
                    feat_seg_pt.SetField(f'{metric}_area_cl_len', value_per_length)
                integrated_width = window_area / window_length if window_length != 0.0 else 0.0
                feat_seg_pt.SetField('integrated_width', integrated_width)
                feat_seg_pt.SetField('window_size', window_distance)
                feat_seg_pt.SetField('window_area', window_area)
                feat_seg_pt.SetField('centerline_length', window_length)

                lyr_pts.ogr_layer.SetFeature(feat_seg_pt)
                feat_seg_pt = None


def vbet_segmentation(in_centerlines: str, vbet_polygons: str, metric_layers: dict, out_gpkg: str, interval=200):
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

    log = Logger('Segment Vbet Network')

    out_points = os.path.join(out_gpkg, 'segmentation_points')
    split_polygons = os.path.join(out_gpkg, 'segmented_vbet_polygons')

    log.info('Generating Segment Points')
    generate_segmentation_points(in_centerlines, out_points, interval)

    log.info('Splitting vbet Polygons')
    split_vbet_polygons(vbet_polygons, out_points, split_polygons)

    log.info('Calcuating vbet metrics')
    calculate_segmentation_metrics(split_polygons, in_centerlines, metric_layers)


def main():
    """Test vbet segmentation
    """
    parser = argparse.ArgumentParser(
        description='Riverscapes VBET Centerline Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('centerlines', help='vbet_centerlines', type=str)
    parser.add_argument('vbet_polygons', help='vbet polygons', type=str)
    parser.add_argument('metric_polygons', help='key value metric polygons', type=str)
    parser.add_argument('--interval', default=200)
    parser.add_argument('out_gpkg')

    args = dotenv.parse_args_env(parser)

    metrics = parse_metadata(args.metric_polygons)

    vbet_segmentation(args.centerlines, args.vbet_polygons, metrics, args.out_gpkg, args.interval)

    sys.exit(0)


if __name__ == '__main__':
    main()
