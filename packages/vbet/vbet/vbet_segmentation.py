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
from shapely.validation import make_valid

from rscommons import GeopackageLayer, Logger, VectorBase, dotenv
from rscommons.util import parse_metadata
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.geometry_ops import get_rectangle_as_geom

Path = str


def generate_igo_points(line_network: Path, out_points_layer: Path, unique_stream_field, stream_size_lookup: dict, distance: dict):
    """generate the vbet segmentation center points/igos

    Args:
        line_network (Path): path to geopackage layer
        out_points_layer (Path): output igo geopackage layer
        stream_size_lookup (dict): level path id:stream size
        distance (float, optional): distance between points. Defaults to 200.0.
    """

    # process modified from: https://glenbambrick.com/2017/09/15/osgp-create-points-along-line/
    log = Logger('Generate Segmentation Points')

    # init_distance = distance / 2

    with GeopackageLayer(out_points_layer, write=True) as out_lyr, \
            GeopackageLayer(line_network) as line_lyr:

        out_fields = {f"{unique_stream_field}": ogr.OFTString,
                      "seg_distance": ogr.OFTReal,
                      "stream_size": ogr.OFTInteger}
        out_lyr.create_layer(
            ogr.wkbPoint, spatial_ref=line_lyr.spatial_ref, fields=out_fields)

        extent_poly = get_rectangle_as_geom(out_lyr.ogr_layer.GetExtent())
        extent_centroid = extent_poly.Centroid()
        utm_epsg = get_utm_zone_epsg(extent_centroid.GetX())
        transform_ref, transform = VectorBase.get_transform_from_epsg(
            line_lyr.spatial_ref, utm_epsg)
        # In order to get accurate lengths we are going to need to project into some coordinate system
        transform_back = osr.CoordinateTransformation(
            transform_ref, line_lyr.spatial_ref)

        for feat, *_ in line_lyr.iterate_features(write_layers=[out_lyr]):
            level_path = feat.GetField(f'{unique_stream_field}')
            if level_path not in stream_size_lookup:
                log.error(
                    f'Stream Size not found for Level Path {level_path}. Skipping segmentation')
                continue
            stream_size = stream_size_lookup[level_path]
            init_distance = distance[stream_size] / 2
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
                    list_points.append(
                        (shapely_line.interpolate(current_dist), current_dist))
                    current_dist += distance[stream_size]

                # add points to the layer
                # for each point in the list
                # enumerate(list_points, 1):
                for (pnt, out_dist) in list_points:
                    # create a point object
                    geom_pnt = ogr.Geometry(ogr.wkbPoint)
                    geom_pnt.AddPoint_2D(pnt.x, pnt.y)
                    geom_pnt.Transform(transform_back)
                    # add the point feature to the output.
                    attributes = {f'{unique_stream_field}': str(int(level_path)),
                                  'seg_distance': out_dist,
                                  'stream_size': stream_size}
                    out_lyr.create_feature(geom_pnt, attributes=attributes)


def split_vbet_polygons(vbet_polygons: Path, segmentation_points: Path, out_split_polygons: Path, unique_stream_field):
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

        fields = {f'{unique_stream_field}': ogr.OFTString,
                  'seg_distance': ogr.OFTReal}
        out_lyr.create_layer(ogr.wkbMultiPolygon,
                             spatial_ref=vbet_lyr.spatial_ref, fields=fields)

        for vbet_feat, *_ in vbet_lyr.iterate_features(write_layers=[out_lyr]):

            level_path = vbet_feat.GetField(f'{unique_stream_field}')
            if level_path is None:
                continue

            vbet_geom = vbet_feat.GetGeometryRef()
            if not vbet_geom.IsValid():
                vbet_geom = vbet_geom.MakeValid()
            vbet_sgeom = VectorBase.ogr2shapely(vbet_geom)
            if vbet_sgeom is None or vbet_sgeom.is_empty:
                continue
            list_points = []

            for point_feat, *_ in points_lyr.iterate_features(attribute_filter=f'{unique_stream_field} = {level_path}'):
                point_geom = point_feat.GetGeometryRef()
                point_sgeom = VectorBase.ogr2shapely(point_geom)
                if not point_sgeom.is_valid:
                    make_valid(point_sgeom)
                if point_sgeom is None or point_sgeom.is_empty:
                    continue
                list_points.append(point_sgeom)

            seed_points_sgeom_mpt = MultiPoint(list_points)
            if not seed_points_sgeom_mpt.is_valid:
                make_valid(seed_points_sgeom_mpt)
            if seed_points_sgeom_mpt is None or seed_points_sgeom_mpt.is_empty:
                continue
            log.info(f'Generating Voronoi Diagram for Level Path {level_path}')
            log.info(f'pts: {[pt.wkt for pt in seed_points_sgeom_mpt.geoms]}')
            log.info(f'vbet: {vbet_sgeom.wkt}')
            voronoi = voronoi_diagram(
                seed_points_sgeom_mpt, envelope=vbet_sgeom)
            for poly in voronoi.geoms:
                try:
                    poly_intersect = vbet_sgeom.intersection(poly)
                except TopologicalError as err:
                    # The operation 'GEOSIntersection_r' could not be performed. Likely cause is invalidity of the geometry
                    log.error(err)
                    continue
                if poly_intersect.geom_type in ['GeometryCollection', 'LineString'] or poly_intersect.is_empty:
                    continue
                clean_geom = poly_intersect.buffer(
                    0) if poly_intersect.is_valid is not True else poly_intersect
                geom_out = VectorBase.shapely2ogr(clean_geom)
                geom_out = ogr.ForceToMultiPolygon(geom_out)
                out_lyr.create_feature(
                    geom_out, {f'{unique_stream_field}': str(int(level_path))})

        for segment_feat, *_ in out_lyr.iterate_features('Writing segment dist to polygons'):
            polygon = segment_feat.GetGeometryRef()
            for pt_feat, *_ in points_lyr.iterate_features(clip_shape=polygon):
                seg_distance = pt_feat.GetField('seg_distance')
                segment_feat.SetField('seg_distance', seg_distance)
                out_lyr.ogr_layer.SetFeature(segment_feat)

    log.info('VBET polygon successfully segmented')


def calculate_dgo_metrics(vbet_dgos: Path, vbet_centerline: Path, dict_layers: dict, attrib_filter: str = None):
    """calculate the basic metrics on the dgos, later used with moving window for igos

    Args:
        vbet_dgo (Path): vbet dgo layer
        vbet_centerline (Path): centerline layer
        dict_layers (Path): Dictionary[layer_name(str), feature_class(str)]
        attrib_filter (str, optional): sql filter for dgos. defaluts to None
    """

    log = Logger('Segmentation Metrics')

    with GeopackageLayer(vbet_dgos, write=True) as lyr_dgos, \
            GeopackageLayer(vbet_centerline) as centerline_lyr:

        # Check fields and create if they don't exist
        exist_fields = lyr_dgos.get_fields()
        metric_field_names = []
        for metric_layer_name in dict_layers.keys():
            metric_field_names.extend(
                [f"{metric_layer_name}_{metric_type}" for metric_type in ['area', 'prop']])
        metric_field_names.extend(
            ['centerline_length', 'segment_area', 'integrated_width'])
        fields = {
            field_name: ogr.OFTReal for field_name in metric_field_names if field_name not in exist_fields}
        if len(fields) > 0:
            lyr_dgos.create_fields(fields)

        for feat_dgo, *_ in lyr_dgos.iterate_features('Calculating dgo metrics', attribute_filter=attrib_filter, write_layers=[lyr_dgos]):
            if not lyr_dgos.spatial_ref.IsProjected() == 1:
                dgo_geom_unproj = feat_dgo.GetGeometryRef()
                centroid = dgo_geom_unproj.Centroid()
                utm_epsg = get_utm_zone_epsg(centroid.GetX())
                _transform_ref, transform = VectorBase.get_transform_from_epsg(
                    lyr_dgos.spatial_ref, utm_epsg)
                vbet_geom_transform = dgo_geom_unproj.Clone()
                vbet_geom_transform.Transform(transform)
                vbet_geom_transform_clean = vbet_geom_transform.MakeValid()
            else:
                vbet_geom = feat_dgo.GetGeometryRef()
                vbet_geom_transform_clean = vbet_geom.Clone()
                if not vbet_geom_transform_clean.IsValid():
                    vbet_geom_transform_clean.MakeValid()

            if not vbet_geom_transform_clean.IsValid():
                log.warning(
                    f'Unable to generate metrics for vbet segment {feat_dgo.GetFID()}: Invalid VBET Segment Geometry')
                continue
            vbet_area = vbet_geom_transform_clean.GetArea()
            if vbet_area == 0.0:
                log.warning(
                    f'Unable to generate metrics for vbet segment {feat_dgo.GetFID()}: VBET Segment has no area')
                continue

            length = 0.0
            for feat_cl, *_ in centerline_lyr.iterate_features(clip_shape=feat_dgo.GetGeometryRef()):
                if not centerline_lyr.spatial_ref.IsProjected() == 1:
                    geom_cl = feat_cl.GetGeometryRef()
                    # _transform_ref, transform = VectorBase.get_transform_from_epsg(centerline_lyr.spatial_ref, utm_epsg)
                    geom_cl.Transform(transform)
                else:
                    geom_cl = feat_cl.GetGeometryRef()
                if not geom_cl.IsValid():
                    log.warning(
                        f'Invalid centerline geometry found for vbet segment {feat_dgo.GetFID()}')
                try:
                    intersect_geom = vbet_geom_transform_clean.Intersection(
                        geom_cl)
                except IOError:
                    log.error(str(IOError))
                    break
                length += intersect_geom.Length()

            feat_dgo.SetField('centerline_length', length)
            feat_dgo.SetField('segment_area', vbet_area)
            if feat_dgo.GetField('seg_distance') is not None:
                feat_dgo.SetField('integrated_width', vbet_area /
                                  length if length != 0.0 else 0.0)

            for metric_layer_name, metric_layer_path in dict_layers.items():
                with GeopackageLayer(metric_layer_path) as metric_lyr:
                    metric_area = 0.0
                    for metric_feat, *_ in metric_lyr.iterate_features(clip_shape=feat_dgo.GetGeometryRef()):
                        if not metric_lyr.spatial_ref.IsProjected() == 1:
                            in_metric_geom = metric_feat.GetGeometryRef()
                            in_metric_geom.Transform(transform)
                            metric_geom = in_metric_geom.MakeValid()
                        else:
                            metric_geom = metric_feat.GetGeometryRef()
                            metric_geom.MakeValid()
                        if not metric_geom.IsValid():
                            log.warning(
                                f'Unable to generate metric for {metric_layer_name} for vbet segment {feat_dgo.GetFID()}. Invalid metric Geometry')
                            continue
                        try:
                            delta_geom = vbet_geom_transform_clean.Intersection(
                                metric_geom)
                        except IOError:
                            log.error(str(IOError))
                            delta_geom = None
                            continue
                        delta_geom.MakeValid()
                        if not delta_geom.IsValid() or delta_geom.GetGeometryType() not in VectorBase.POLY_TYPES + VectorBase.COLLECTION_TYPES:
                            continue
                        metric_area = metric_area + delta_geom.GetArea()
                    metric_prop = metric_area / vbet_area
                    feat_dgo.SetField(f'{metric_layer_name}_area', metric_area)
                    feat_dgo.SetField(f'{metric_layer_name}_prop', metric_prop)
            lyr_dgos.ogr_layer.SetFeature(feat_dgo)
            feat_dgo = None
            dgo_geom_unproj = None


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
    merged_line = linemerge(
        in_geom) if in_geom.geom_type == 'MultiLineString' else in_geom
    if merged_line.geom_type == 'LineString':
        merged_line = MultiLineString([merged_line])

    return merged_line


def calculate_vbet_window_metrics(vbet_igos: Path, vbet_dgos: Path, level_paths: list, unique_stream_field: str, distance_lookup: dict, metric_names: list):
    """generate moving window summary of segmented vbet polygons

    Args:
        segment_points (Path): geopackage feature class of segmentation points to include attributes
        segmented_polygons (Path): geopackage feature class of segmented vbet polygons
        level_paths (list): list of NHD level paths
        distance_lookup (dict): dictionary of distances per stream size
        metric_names (list): list of metric names to generate summary attributes on
    """

    with GeopackageLayer(vbet_igos, write=True) as lyr_igos, \
            GeopackageLayer(vbet_dgos) as lyr_dgos:

        # Initialize Metric Fields
        metric_fields = {}
        metric_fields['window_size'] = ogr.OFTReal
        metric_fields['centerline_length'] = ogr.OFTReal
        metric_fields['window_area'] = ogr.OFTReal
        metric_fields['integrated_width'] = ogr.OFTReal
        for metric in metric_names:
            metric_fields[f'{metric}_area'] = ogr.OFTReal
            metric_fields[f'{metric}_proportion'] = ogr.OFTReal
            metric_fields[f'{metric}_itgr_width'] = ogr.OFTReal
        metric_fields['vb_acreage_per_mile'] = ogr.OFTReal
        metric_fields['vb_hectares_per_km'] = ogr.OFTReal
        metric_fields['low_lying_acreage_per_mile'] = ogr.OFTReal
        metric_fields['low_lying_hectares_per_km'] = ogr.OFTReal
        metric_fields['elevated_acreage_per_mile'] = ogr.OFTReal
        metric_fields['elevated_hectares_per_km'] = ogr.OFTReal

        lyr_igos.create_fields(metric_fields)

        for level_path in level_paths:
            if level_path is None or level_path not in distance_lookup.keys():
                continue
            window_distance = distance_lookup[level_path]
            window_addon = {200: 100, 400: 200,
                            1200: 300, 2000: 500, 8000: 2000}
            for feat_igo, *_ in lyr_igos.iterate_features(f'Summerizing vbet metrics for {level_path}', attribute_filter=f"{unique_stream_field} = {level_path}"):
                # Construct the igo window selection logic
                igo_distance = feat_igo.GetField('seg_distance')
                min_dist = igo_distance - 0.5 * window_distance
                max_dist = igo_distance + 0.5 * window_distance
                sql_igo_window = f"{unique_stream_field} = {level_path} AND seg_distance >= {min_dist} AND seg_distance <= {max_dist}"

                # Gather Window Measurements from the dgos
                window_measurements = dict.fromkeys(metric_names, 0.0)
                window_cl_length_m = 0.0
                window_area_m2 = 0.0
                for feat_dgo, *_ in lyr_dgos.iterate_features(attribute_filter=sql_igo_window):
                    window_cl_length_m += feat_dgo.GetField(
                        'centerline_length')
                    window_area_m2 += feat_dgo.GetField('segment_area')
                    for metric in metric_names:
                        metric_area = feat_dgo.GetField(f'{metric}_area')
                        metric_area = metric_area if metric_area is not None else 0.0
                        window_measurements[metric] += metric_area

                # Calculate the floodplain metrics
                for metric, area in window_measurements.items():
                    area_per_length = area / window_cl_length_m if window_cl_length_m != 0.0 else 0.0
                    area_porportion = area / window_area_m2 if window_area_m2 != 0.0 else 0.0
                    feat_igo.SetField(f'{metric}_area', area)
                    feat_igo.SetField(f'{metric}_proportion', area_porportion)
                    feat_igo.SetField(f'{metric}_itgr_width', area_per_length)

                # Measurement Conversions
                window_cl_length_mi = window_cl_length_m / 1609.344
                window_cl_length_km = window_cl_length_m / 1000
                window_area_acres = window_area_m2 / 4046.86
                window_area_hectares = window_area_m2 / 10000
                active_acres = (
                    window_measurements['low_lying_floodplain'] + window_measurements['active_channel']) / 4046.86
                active_hectares = (
                    window_measurements['low_lying_floodplain'] + window_measurements['active_channel']) / 10000
                inactive_acres = window_measurements['elevated_floodplain'] / 4046.86
                inactive_hectares = window_measurements['elevated_floodplain'] / 10000

                # Metric Calculations
                vb_acreage_per_mile = window_area_acres / \
                    window_cl_length_mi if window_cl_length_m != 0.0 else 0.0
                vb_hectares_per_km = window_area_hectares / \
                    window_cl_length_km if window_cl_length_m != 0.0 else 0.0
                active_acreage_per_mile = active_acres / \
                    window_cl_length_mi if window_cl_length_m != 0.0 else 0.0
                active_hectares_per_km = active_hectares / \
                    window_cl_length_km if window_cl_length_m != 0.0 else 0.0
                inactive_acreage_per_mile = inactive_acres / \
                    window_cl_length_mi if window_cl_length_m != 0.0 else 0.0
                inactive_hectares_per_km = inactive_hectares / \
                    window_cl_length_km if window_cl_length_m != 0.0 else 0.0
                integrated_width = window_area_m2 / \
                    window_cl_length_m if window_cl_length_m != 0.0 else 0.0

                # Write to fields
                feat_igo.SetField('integrated_width', integrated_width)
                feat_igo.SetField('window_size', window_distance +
                                  window_addon[int(window_distance)])
                feat_igo.SetField('window_area', window_area_m2)
                feat_igo.SetField('centerline_length', window_cl_length_m)
                feat_igo.SetField('vb_acreage_per_mile', vb_acreage_per_mile)
                feat_igo.SetField('vb_hectares_per_km', vb_hectares_per_km)
                feat_igo.SetField('low_lying_acreage_per_mile',
                                  active_acreage_per_mile)
                feat_igo.SetField('low_lying_hectares_per_km',
                                  active_hectares_per_km)
                feat_igo.SetField('elevated_acreage_per_mile',
                                  inactive_acreage_per_mile)
                feat_igo.SetField('elevated_hectares_per_km',
                                  inactive_hectares_per_km)

                lyr_igos.ogr_layer.SetFeature(feat_igo)
                feat_igo = None


def add_fcodes(in_dgos, in_igos, in_flowlines):

    with GeopackageLayer(in_dgos, write=True) as dgo_lyr, \
            GeopackageLayer(in_igos, write=True) as igo_lyr, \
            GeopackageLayer(in_flowlines) as lines_lyr:
        dgo_lyr.create_field('FCode', ogr.OFTInteger)
        igo_lyr.create_field('FCode', ogr.OFTInteger)

        for dgo_feat, *_ in dgo_lyr.iterate_features("Getting FCodes For DGOs and IGOs"):
            feat_geom = dgo_feat.GetGeometryRef().Clone()
            levelpath = dgo_feat.GetField('level_path')
            segdistance = dgo_feat.GetField('seg_distance')
            if segdistance is None:
                continue

            attributes = {}
            for line_feat, *_ in lines_lyr.iterate_features(clip_shape=feat_geom):
                line_geom = line_feat.GetGeometryRef()
                if line_geom.Intersects(feat_geom):
                    geom_section = feat_geom.Intersection(line_geom)
                    length = geom_section.Length()
                    attributes[line_feat.GetField('FCode')] = attributes.get(
                        line_feat.GetField('FCode'), 0) + length
            lines_lyr.ogr_layer.SetSpatialFilter(None)

            if len(attributes) == 0:
                maj_fode = None
            else:
                maj_fode = max(attributes, key=attributes.get)
            dgo_feat.SetField('FCode', maj_fode)
            dgo_lyr.ogr_layer.SetFeature(dgo_feat)
            dgo_feat = None

            for igo_feat, *_ in igo_lyr.iterate_features(attribute_filter=f"level_path = {levelpath} AND seg_distance = {segdistance}"):
                igo_feat.SetField('FCode', maj_fode)
                igo_lyr.ogr_layer.SetFeature(igo_feat)
                igo_feat = None


def vbet_segmentation(in_centerlines: str, vbet_polygons: str, unique_stream_field: str, metric_layers: dict, out_gpkg: str, ss_lookup: dict):
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
    generate_igo_points(in_centerlines, out_points, unique_stream_field,
                        ss_lookup, distance={0: 100, 1: 200, 2: 300})

    log.info('Splitting vbet Polygons')
    split_vbet_polygons(vbet_polygons, out_points,
                        split_polygons, unique_stream_field)

    log.info('Calcuating vbet metrics')
    calculate_dgo_metrics(split_polygons, in_centerlines, metric_layers)


def main():
    """Test vbet segmentation
    """
    parser = argparse.ArgumentParser(
        description='Riverscapes VBET Centerline Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('centerlines', help='vbet_centerlines', type=str)
    parser.add_argument('vbet_polygons', help='vbet polygons', type=str)
    parser.add_argument('unique_stream_field',
                        help='unique stream field', type=str)
    parser.add_argument('metric_polygons',
                        help='key value metric polygons', type=str)
    parser.add_argument('--interval', default=200)
    parser.add_argument('out_gpkg')

    args = dotenv.parse_args_env(parser)

    metrics = parse_metadata(args.metric_polygons)

    vbet_segmentation(args.centerlines, args.vbet_polygons,
                      args.unique_stream_field, metrics, args.out_gpkg, args.interval)

    sys.exit(0)


if __name__ == '__main__':
    main()
