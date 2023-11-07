
from typing import List, Dict
import os
import sqlite3
from osgeo import ogr
from shapely.geometry import Point, MultiPoint, LineString
from shapely.ops import split
from rscommons import get_shp_or_gpkg, GeopackageLayer, Logger
from rscommons.segment_network import copy_fields
from rscommons.vector_ops import collect_feature_class

Path = str


def rs_segmentation(
    nhd_flowlines_path: str, dict_lines: dict, areas: list, out_gpkg: str):
    """Segment the network in a few different ways

    Args:
        nhd_flowlines_path (str): Path to shapefile or geopackage containing the original network
        roads_path (str): Roads linestring shapefile or geopackage
        railways_path (str): Rails lienstring shapefile or geopackage
        ownership_path (str): Ownership polygon shapefile or geopackage
        out_gpkg (str): Output geopackage for all the output layers
        interval (float): Preferred segmentation distance split
        minimum (float): Minimum possible segment size
        watershed_id (str): Watershed ID
    """

    log = Logger('rs_segmentation')

    # If a point needs to be split we store the split pieces here
    split_feats = {}

    # Intersection points are useful in other tools so we keep them
    intersect_pts = {}

    # Split by features in the lines dict
    for name, path in dict_lines.items():
        log.info(f'Finding {name} intersections')
        intersect_pts[name] = split_geoms(nhd_flowlines_path, path, split_feats)

    # Split by areas
    for area in areas:
        log.info(f'Finding {area["name"]} intersections')
        intersect_pts[area["name"]] = split_geoms(nhd_flowlines_path, area['path'], split_feats)

    # Let's write our crossings to layers for later use. This can be used in BRAT or our other tools
    with GeopackageLayer(out_gpkg, layer_name='network_crossings', write=True) as out_lyr, \
            GeopackageLayer(nhd_flowlines_path) as in_lyr:
        out_lyr.create_layer(ogr.wkbPoint, spatial_ref=in_lyr.spatial_ref, fields={'type': ogr.OFTString})
        out_lyr.ogr_layer.StartTransaction()
        for geom_type_name, ogr_geom in intersect_pts.items():
            for pt in list(ogr_geom):
                out_feature = ogr.Feature(out_lyr.ogr_layer_def)
                out_feature.SetGeometry(GeopackageLayer.shapely2ogr(pt))
                out_feature.SetField('type', geom_type_name)
                out_lyr.ogr_layer.CreateFeature(out_feature)

        out_lyr.ogr_layer.CommitTransaction()
    # We're done with the original. Let that memory go.
    intersect_pts = None

    # Now, finally, write all the shapes, substituting splits where necessary
    network_crossings_path = os.path.join(out_gpkg, 'network_intersected')
    with GeopackageLayer(network_crossings_path, write=True) as out_lyr, \
            GeopackageLayer(nhd_flowlines_path) as net_lyr:
        out_lyr.create_layer_from_ref(net_lyr)
        out_lyr.ogr_layer.StartTransaction()
        fcounter = 0
        for feat, _counter, _progbar in net_lyr.iterate_features('Writing split features'):

            fid = feat.GetFID()

            # If a split happened then write the split geometries to the file.
            if fid in split_feats:
                for split_geom in split_feats[fid]:
                    new_feat = ogr.Feature(out_lyr.ogr_layer_def)
                    copy_fields(feat, new_feat, net_lyr.ogr_layer_def, out_lyr.ogr_layer_def, skip_fid=True)
                    new_feat.SetGeometry(GeopackageLayer.shapely2ogr(split_geom))
                    out_lyr.ogr_layer.CreateFeature(new_feat)
                    fcounter += 1

            # If no split was found, write the feature as-is
            else:
                new_feat = ogr.Feature(out_lyr.ogr_layer_def)
                copy_fields(feat, new_feat, net_lyr.ogr_layer_def, out_lyr.ogr_layer_def, skip_fid=True)
                out_lyr.ogr_layer.CreateFeature(new_feat)
                fcounter += 1
        out_lyr.ogr_layer.CommitTransaction()

    # attribute the  network with the polygon layers
    attr_dict = {}
    with GeopackageLayer(network_crossings_path) as intersected_lyr:
        for area in areas:
            with get_shp_or_gpkg(area['path']) as poly_lyr:
                for poly_feat, *_ in poly_lyr.iterate_features(f"Identifying network attributes values for the {area['name']} layer "):
                    poly_geom: ogr.Geometry = poly_feat.GetGeometryRef()

                    for net_feat, *_ in intersected_lyr.iterate_features(clip_shape=poly_feat):
                        # test if the geometry of the network feature is within the polygon feature
                        net_geom: ogr.Geometry = net_feat.GetGeometryRef()
                        net_centroid: ogr.Geometry = net_geom.Centroid()
                        if net_centroid.Within(poly_geom):
                            for attribute in area['attributes']:
                                attribute_name = attribute['out_field']
                                attribute_value = poly_feat.GetField(attribute['in_field'])
                                fid = net_feat.GetFID()
                                if fid not in attr_dict:
                                    attr_dict[fid] = {}
                                attr_dict[fid][attribute_name] = attribute_value

    with GeopackageLayer(network_crossings_path, write=True) as output_lyr:
        log.info('Writing attributes to the intersected network')
        for area in areas:
            out_fields = {}
            for attribute in area['attributes']:
                out_fields[attribute['out_field']] = ogr.OFTString
            output_lyr.create_fields(out_fields)
        output_lyr.ogr_layer.StartTransaction()
        for fid, attr in attr_dict.items():
            feat: ogr.Feature = output_lyr.ogr_layer.GetFeature(fid)
            for field_name, field_value in attr.items():
                feat.SetField(field_name, field_value)
            output_lyr.ogr_layer.SetFeature(feat)
        output_lyr.ogr_layer.CommitTransaction()

    # Index fields on the segmented networks
    with sqlite3.connect(out_gpkg) as conn:
        curs = conn.cursor()
        curs.execute('CREATE INDEX ix_network_intersected_ReachCode on network_intersected(ReachCode)')
        curs.execute('CREATE INDEX ix_network_intersected_NHDPlusID on network_intersected(NHDPlusID)')
        curs.execute('CREATE INDEX ix_network_intersected_FCode on network_intersected(FCode)')
        conn.commit()

        curs.execute('VACUUM')
    
    log.info('Segmentation Complete')


def split_geoms(base_feature_path: str, intersect_feature_path: str, split_feats: Dict[int, List[LineString]]) -> List[Point]:
    """Loop over base_feature_path and split it everywhere we find it intersecting with intersect_feature_path
    This creates the splits to be used later

    Args:
        base_feature_path (str): [description]
        intersect_feature_path (str): [description]
        split_feats (Dict[List[LineString]]): [description]

    Returns:
        (List[Point]): Returns all the intersection points.
    """

    log = Logger('split_geoms')
    log.info('Finding intersections')

    # We collect the raw NHD to use as a filter only
    base_collection = collect_feature_class(base_feature_path)
    # Then we use the same collection method to get a collection of intersected features that are likely to touch
    # our base_collection. This seems a bit redundantly redundant but it does speed things up.
    intersect_collection = GeopackageLayer.ogr2shapely(collect_feature_class(intersect_feature_path, clip_shape=base_collection))

    # if intersect_collection is a polygon or multipolygon, we need to get the boundary
    if intersect_collection.type == 'Polygon':
        intersect_collection = intersect_collection.boundary
    elif intersect_collection.type == 'MultiPolygon':
        intersect_collection = intersect_collection.boundary

    intersection_pts = []
    # Now go through using a clip_shape filter and do the actual splits. These features are likely to intersect
    # but not guaranteed so we still need to check.
    with get_shp_or_gpkg(base_feature_path) as in_lyr:
        for feat, _counter, _progbar in in_lyr.iterate_features("Finding intersections", clip_shape=intersect_collection):
            fid = feat.GetFID()
            shply_geom = GeopackageLayer.ogr2shapely(feat)

            if fid in split_feats:
                # If a previous incarnation of split_geoms already split this feature we have to work on the splits.
                candidates = split_feats[fid]
            else:
                candidates = [shply_geom]

            new_splits = []
            for candidate in candidates:

                # This call is not really related to the segmentation but we write it back to a point layer
                # for use in other tools.
                intersection = candidate.intersection(intersect_collection)

                # Split this candidate geometry by the intersect collection
                geom_split = split(candidate, intersect_collection)
                new_splits += list(geom_split.geoms)

                # Now add the intersection points to the list
                # >1 length means there was an intersection
                if len(geom_split.geoms) > 1:
                    if isinstance(intersection, Point):
                        intersection_pts.append(intersection)
                    elif isinstance(intersection, MultiPoint):
                        intersection_pts += list(intersection.geoms)
                    else:
                        raise Exception('Unhandled type: {}'.format(intersection.type))

            split_feats[fid] = new_splits
    return intersection_pts


def polygon_to_polyline(lines_path, polygon_path, network_path):
    with GeopackageLayer(lines_path, write=True) as out_layer, get_shp_or_gpkg(polygon_path) as polygon_lyr:
        out_layer.create_layer(ogr.wkbLineString, spatial_ref=polygon_lyr.spatial_ref)
        network_owener_collect = collect_feature_class(network_path)
        out_layer.ogr_layer.StartTransaction()
        for feat, _counter, _progbar in polygon_lyr.iterate_features('Converting ownership polygons to polylines', clip_shape=network_owener_collect):
            geom: ogr.Geometry = feat.GetGeometryRef()

            # Check that this feature has valid geometry. Really important since ownership shape layers are
            # Usually pretty messy.
            if geom.IsValid() and not geom.IsEmpty():

                # Flatten to 2D first to speed up the potential transform
                if geom.IsMeasured() > 0 or geom.Is3D() > 0:
                    geom.FlattenTo2D()

                # Get the boundary linestring
                boundary = geom.GetBoundary()
                b_type = boundary.GetGeometryType()

                # If the boundary is a multilinestring that's fine
                if b_type == ogr.wkbMultiLineString:
                    pass
                # if it's just one linestring we make it a multilinestring of one.
                elif b_type == ogr.wkbLineString:
                    boundary = [boundary]
                else:
                    raise Exception('Unsupported type: {}'.format(ogr.GeometryTypeToName(b_type)))

                # Now write each individual linestring back to our output layer
                for b_line in boundary:
                    out_feature = ogr.Feature(out_layer.ogr_layer_def)
                    out_feature.SetGeometry(b_line)
                    out_layer.ogr_layer.CreateFeature(out_feature)
        out_layer.ogr_layer.CommitTransaction()


def create_spatial_view(nhd_gpkg_path: str, network_layer: str, join_table: str, out_view: str, network_fields: dict, join_fields: dict, join_id) -> Path:

    with sqlite3.connect(nhd_gpkg_path) as conn:
        curs = conn.cursor()
        curs.execute(f"DROP VIEW IF EXISTS {out_view}")
        # create the view with specified of the fields from the flowline table and add the fields from the join table. the fields from the join table should have an ailas of the value in the dict.
        curs.execute(f"CREATE VIEW {out_view} AS SELECT {', '.join([f'{network_layer}.{field} AS {alias}' for field, alias in network_fields.items()])}, {', '.join([f'{join_table}.{field} AS {alias}' for field, alias in join_fields.items()])} FROM {network_layer} LEFT JOIN {join_table} ON {network_layer}.{join_id} = {join_table}.{join_id}")

        # do an insert select from the network layer to the gpkg_contents table
        curs.execute(f"INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id) SELECT '{out_view}', 'features', '{out_view}', '{out_view}', datetime('now'), min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = '{network_layer}'")

        # add to gpkg geometry columns
        curs.execute(f"INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) VALUES ('{out_view}', 'geom', 'LINESTRING', 4326, 0, 0)")

        # Create index
        curs.execute(f"CREATE INDEX IF NOT EXISTS {join_table}_NHDPlusID ON {join_table}(NHDPlusID)")
        curs.execute(f"CREATE INDEX IF NOT EXISTS NHDFlowline_fid ON NHDFlowline(fid)")

        conn.commit()

        curs.execute('VACUUM')

    return os.path.join(nhd_gpkg_path, out_view)
