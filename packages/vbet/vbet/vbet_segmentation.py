"""_summary_
"""
import os
import sys
import argparse

from osgeo import ogr, osr
from shapely.ops import linemerge, voronoi_diagram
from shapely.geometry import MultiLineString, Point, MultiPoint

from rscommons import GeopackageLayer, Logger, VectorBase, dotenv
from rscommons.util import parse_metadata
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.vector_ops import geom_validity_fix

Path = str


def generate_segmentation_points(line_network, out_points_layer, distance=200):
    """heavily modified from: https://glenbambrick.com/2017/09/15/osgp-create-points-along-line/
    """

    log = Logger('Generate Segmentation Points')

    init_distance = distance / 2

    with GeopackageLayer(out_points_layer, write=True) as out_lyr, \
            GeopackageLayer(line_network) as line_lyr:

        out_fields = {"LevelPathI": ogr.OFTReal,
                      "SegDistance": ogr.OFTReal,
                      "SegLength": ogr.OFTReal}
        out_lyr.create_layer(ogr.wkbPoint, spatial_ref=line_lyr.spatial_ref, fields=out_fields)

        extent_poly = ogr.Geometry(ogr.wkbPolygon)
        extent_centroid = extent_poly.Centroid()
        utm_epsg = get_utm_zone_epsg(extent_centroid.GetX())
        transform_ref, transform = VectorBase.get_transform_from_epsg(line_lyr.spatial_ref, utm_epsg)
        # In order to get accurate lengths we are going to need to project into some coordinate system
        transform_back = osr.CoordinateTransformation(transform_ref, line_lyr.spatial_ref)

        for feat, *_ in line_lyr.iterate_features():
            level_path = feat.GetField('LevelPathI')
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
                for (pt, out_dist) in list_points:  # enumerate(list_points, 1):
                    # create a point object
                    pnt = ogr.Geometry(ogr.wkbPoint)
                    pnt.AddPoint_2D(pt.x, pt.y)
                    pnt.Transform(transform_back)
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
                    out_lyr.create_feature(pnt, {"LevelPathI": level_path, "SegDistance": out_dist})


def split_vbet_polygons(vbet_polygons, segmentation_points, out_split_polygons):

    log = Logger('Split Polygons using Voronoi')

    with GeopackageLayer(out_split_polygons, write=True) as out_lyr, \
            GeopackageLayer(vbet_polygons) as vbet_lyr, \
            GeopackageLayer(segmentation_points) as points_lyr:

        out_lyr.create_layer(ogr.wkbPolygon, spatial_ref=vbet_lyr.spatial_ref, fields={"LevelPathI": ogr.OFTReal})

        for vbet_feat, *_ in vbet_lyr.iterate_features():

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
                poly_intersect = vbet_sgeom.intersection(poly)
                if poly_intersect.geom_type in ['GeometryCollection', 'LineString'] or poly_intersect.is_empty:
                    continue
                clean_geom = poly_intersect.buffer(0) if poly_intersect.is_valid is not True else poly_intersect
                out_lyr.create_feature(clean_geom, {'LevelPathI': level_path})

        out_lyr.create_field('SegDistance', ogr.OFTReal)
        for segment_feat, *_ in out_lyr.iterate_features('Writing segment dist to polygons'):
            polygon = segment_feat.GetGeometryRef()
            for pt_feat, *_ in points_lyr.iterate_features(clip_shape=polygon):
                seg_distance = pt_feat.GetField('SegDistance')
                segment_feat.SetField('SegDistance', seg_distance)
                out_lyr.ogr_layer.SetFeature(segment_feat)


def calculate_segmentation_metrics(vbet_segment_polygons, vbet_centerline, dict_layers):

    log = Logger('Segmentation Metrics')

    with GeopackageLayer(vbet_segment_polygons, write=True) as vbet_lyr, \
            GeopackageLayer(vbet_centerline) as centerline_lyr:

        for metric_layer_name in dict_layers.keys():
            fields = {f'{metric_layer_name}_area': ogr.OFTReal,
                      f'{metric_layer_name}_prop': ogr.OFTReal, }
            vbet_lyr.create_fields(fields)
        vbet_lyr.create_fields({'centerline_length': ogr.OFTReal, 'segment_area': ogr.OFTReal})

        for vbet_feat, *_ in vbet_lyr.iterate_features():
            vbet_geom = vbet_feat.GetGeometryRef()
            centroid = vbet_geom.Centroid()
            utm_epsg = get_utm_zone_epsg(centroid.GetX())
            _transform_ref, transform = VectorBase.get_transform_from_epsg(vbet_lyr.spatial_ref, utm_epsg)
            vbet_geom_transform = vbet_geom.Clone()
            vbet_geom_transform.Transform(transform)
            vbet_geom_transform_clean = geom_validity_fix(vbet_geom_transform)
            vbet_area = vbet_geom_transform_clean.GetArea()
            if vbet_area == 0.0:
                continue

            length = 0.0
            for centerline_feat, *_ in centerline_lyr.iterate_features(clip_shape=vbet_geom):
                centerline_geom = centerline_feat.GetGeometryRef()
                _transform_ref, transform = VectorBase.get_transform_from_epsg(centerline_lyr.spatial_ref, utm_epsg)
                centerline_geom.Transform(transform)
                intersect_geom = vbet_geom_transform_clean.Intersection(centerline_geom)
                length = length + intersect_geom.Length()

            vbet_feat.SetField('centerline_length', length)
            vbet_feat.SetField('segment_area', vbet_area)

            for metric_layer_name, metric_layer_path in dict_layers.items():
                with GeopackageLayer(metric_layer_path) as metric_lyr:
                    for metric_feat, *_ in metric_lyr.iterate_features(clip_shape=vbet_geom):
                        in_metric_geom = metric_feat.GetGeometryRef()
                        in_metric_geom.Transform(transform)
                        metric_geom = geom_validity_fix(in_metric_geom)
                        if not all([metric_geom.IsValid(), vbet_geom_transform_clean.IsValid()]):
                            continue
                        try:
                            delta_geom = vbet_geom_transform_clean.Difference(metric_geom)
                        except IOError:
                            log.error(str(IOError))
                            delta_geom = None
                        metric_area = 0.0 if delta_geom is None else delta_geom.GetArea()
                        metric_prop = metric_area / vbet_area
                        vbet_feat.SetField(f'{metric_layer_name}_area', metric_area)
                        vbet_feat.SetField(f'{metric_layer_name}_prop', metric_prop)
            vbet_lyr.ogr_layer.SetFeature(vbet_feat)


def clean_linestring(in_geom):

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


def summerize_vbet_metrics(segment_points: Path, segmented_polygons: Path, level_paths: list, distance_lookup: dict, metric_names):

    with GeopackageLayer(segment_points, write=True) as lyr_pts, \
            GeopackageLayer(segmented_polygons) as lyr_polygons:

        metric_fields = {}
        for metric in metric_names:
            metric_fields[f'{metric}_area'] = ogr.OFTReal
            metric_fields[f'{metric}_area_prop'] = ogr.OFTReal
            metric_fields[f'{metric}_area_length'] = ogr.OFTReal
        metric_fields['integrated_width'] = ogr.OFTReal
        lyr_pts.create_fields(metric_fields)

        for level_path in level_paths:
            if level_path is None or level_path not in distance_lookup.keys():
                continue
            window_distance = distance_lookup[level_path]
            for feat_seg_pt, *_ in lyr_pts.iterate_features(attribute_filter=f"LevelPathI = {level_path}"):
                dist = feat_seg_pt.GetField('SegDistance')
                min_dist = dist - 0.5 * window_distance
                max_dist = dist + 0.5 * window_distance
                sql_seg_poly = f"LevelPathI = {level_path} AND SegDistance >= {min_dist} AND SegDistance <= {max_dist}"
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
                    value = value / window_distance
                    value_per_length = value / window_length if window_length != 0.0 else 0.0
                    value_porportion = value / window_area if window_area != 0.0 else 0.0
                    feat_seg_pt.SetField(f'{metric}_area', value)
                    feat_seg_pt.SetField(f'{metric}_area_prop', value_porportion)
                    feat_seg_pt.SetField(f'{metric}_area_length', value_per_length)
                integrated_width = window_area / window_length if window_length != 0.0 else 0.0
                feat_seg_pt.SetField(f'integrated_width', integrated_width)

                lyr_pts.ogr_layer.SetFeature(feat_seg_pt)


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
