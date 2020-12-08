
from typing import List, Dict
import os
from osgeo import ogr
from shapely.geometry import Point, MultiPoint, LineString
from shapely.ops import split
from rscommons import get_shp_or_gpkg, GeopackageLayer, Logger
from rscommons.segment_network import segment_network_NEW
from rscommons.vector_ops import copy_feature_class
from rscommons.vector_ops import collect_feature_class


def rs_segmentation(
    nhd_flowlines_path: str, roads_path: str, railways_path: str, ownership_path: str,
    out_gpkg: str, interval: float, minimum: float, watershed_id: str
):
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

    # First make a copy of the network.
    # TODO: When we migrate to geopackages we may need to revisit this.
    log.info('Copying raw network')
    network_copy_path = os.path.join(out_gpkg, 'network')
    copy_feature_class(nhd_flowlines_path, network_copy_path)

    # Segment the raw network without doing any intersections
    log.info('Segmenting the raw network')
    segment_network_NEW(network_copy_path, os.path.join(out_gpkg, 'network_300m'), interval, minimum, watershed_id, create_layer=True)

    # If a point needs to be split we store the split pieces here
    split_feats = {}

    # Intersection points are useful in other tools so we keep them
    intersect_pts = {}

    log.info('Finding road intersections')
    intersect_pts['roads'] = split_geoms(network_copy_path, roads_path, split_feats)

    log.info('Finding rail intersections')
    intersect_pts['rail'] = split_geoms(network_copy_path, railways_path, split_feats)

    # With ownership we need to convert polygons to polylines (linestrings) to get the crossing points
    # We can't use intersect_geometry_with_feature_class for this so we need to do something a little more manual
    log.info('Finding ownership intersections')

    ownership_lines_path = os.path.join(out_gpkg, "ownership_lines")
    with GeopackageLayer(ownership_lines_path, write=True) as out_layer, get_shp_or_gpkg(ownership_path) as own_lyr:
        out_layer.create_layer(ogr.wkbLineString, spatial_ref=own_lyr.spatial_ref)
        network_owener_collect = collect_feature_class(network_copy_path)
        for feat, _counter, _progbar in own_lyr.iterate_features('Converting ownership polygons to polylines', clip_shape=network_owener_collect):
            geom = feat.GetGeometryRef()

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

    # Now, finally, we're ready to do the actual intersection and splitting
    intersect_pts['ownership'] = split_geoms(network_copy_path, ownership_lines_path, split_feats)

    # Let's write our crossings to layers for later use. This can be used in BRAT or our other tools
    with GeopackageLayer(out_gpkg, layer_name='network_crossings', write=True) as out_lyr, \
            GeopackageLayer(network_copy_path) as in_lyr:
        out_lyr.create_layer(ogr.wkbPoint, spatial_ref=in_lyr.spatial_ref, fields={'type': ogr.OFTString})
        for geom_type_name, ogr_geom in intersect_pts.items():
            for pt in list(ogr_geom):
                out_feature = ogr.Feature(out_lyr.ogr_layer_def)
                out_feature.SetGeometry(GeopackageLayer.shapely2ogr(pt))
                out_feature.SetField('type', geom_type_name)
                out_lyr.ogr_layer.CreateFeature(out_feature)

    # We're done with the original. Let that memory go.
    intersect_pts = None

    # Now, finally, write all the shapes, substituting splits where necessary
    network_crossings_path = os.path.join(out_gpkg, 'network_intersected')
    with GeopackageLayer(network_crossings_path, write=True) as out_lyr, \
            GeopackageLayer(network_copy_path) as net_lyr:
        out_lyr.create_layer_from_ref(net_lyr)
        fcounter = 0
        for feat, _counter, _progbar in net_lyr.iterate_features('Writing split features'):

            fid = feat.GetFID()

            # If a split happened then write the split geometries to the file.
            if fid in split_feats:
                for split_geom in split_feats[fid]:
                    new_feat = feat.Clone()
                    new_feat.SetFID(fcounter)
                    new_feat.SetGeometry(GeopackageLayer.shapely2ogr(split_geom))
                    out_lyr.ogr_layer.CreateFeature(new_feat)
                    fcounter += 1

            # If no split was found, write the feature as-is
            else:
                new_feat = feat.Clone()
                new_feat.SetFID(fcounter)
                out_lyr.ogr_layer.CreateFeature(new_feat)
                fcounter += 1

    # Finally, segment this new layer the same way we did the raw network above.
    log.info('Segmenting the intersected network')
    segment_network_NEW(network_crossings_path, os.path.join(out_gpkg, 'network_intersected_300m'), interval, minimum, watershed_id, create_layer=True)

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
                new_splits += list(geom_split)

                # Now add the intersection points to the list
                # >1 length means there was an intersection
                if len(geom_split) > 1:
                    if isinstance(intersection, Point):
                        intersection_pts.append(intersection)
                    elif isinstance(intersection, MultiPoint):
                        intersection_pts += list(intersection)
                    else:
                        raise Exception('Unhandled type: {}'.format(intersection.type))

            split_feats[fid] = new_splits
    return intersection_pts
