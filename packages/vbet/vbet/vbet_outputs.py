""" VBET Output Functions

    Purpose:  Tools to support VBET vbet outputs
    Author:   North Arrow Research
    Date:     August 2022
"""

from uuid import uuid4
from osgeo import ogr

from rscommons import Logger, GeopackageLayer, TempGeopackage, get_shp_or_gpkg, Timer
from vbet.__version__ import __version__

Path = str


def sanitize(name: str, in_path: str, out_path: str, buff_dist: float, select_features=None):
    """
        It's important to make sure we have the right kinds of geometries.

    Args:
        name (str): Mainly just for good logging
        in_path (str): [description]
        out_path (str): [description]
        buff_dist (float): [description]
    """
    log = Logger('VBET Simplify')
    _timer = Timer()
    with GeopackageLayer(out_path, write=True) as out_lyr, \
            TempGeopackage('sanitize_temp') as tempgpkg, \
            GeopackageLayer(in_path) as in_lyr:

        # out_lyr.create_layer(ogr.wkbPolygon, spatial_ref=in_lyr.spatial_ref)
        out_lyr.create_layer_from_ref(in_lyr)
        out_layer_defn = out_lyr.ogr_layer.GetLayerDefn()
        field_count = out_layer_defn.GetFieldCount()

        pts = 0
        square_buff = buff_dist * buff_dist

        # NOTE: Order of operations really matters here.

        in_pts = 0
        out_pts = 0

        with GeopackageLayer(tempgpkg.filepath, "sanitize_{}".format(str(uuid4())), write=True, delete_dataset=True) as tmp_lyr, \
                GeopackageLayer(select_features) as lyr_select_features:

            # tmp_lyr.create_layer_from_ref(in_lyr)

            def geom_validity_fix(geom_in):
                f_geom = geom_in
                # Only clean if there's a problem:
                if not f_geom.IsValid():
                    f_geom = f_geom.Buffer(0)
                    if not f_geom.IsValid():
                        f_geom = f_geom.Buffer(buff_dist)
                        f_geom = f_geom.Buffer(-buff_dist)
                return f_geom

            # Only keep features intersected with network
            tmp_lyr.create_layer_from_ref(in_lyr)

            for candidate_feat, _c2, _p1 in in_lyr.iterate_features("Finding interesected features", write_layers=[tmp_lyr]):
                candidate_geom = candidate_feat.GetGeometryRef()
                candidate_geom = geom_validity_fix(candidate_geom)

                reach_attributes = {}
                for n in range(field_count):
                    field = out_layer_defn.GetFieldDefn(n)
                    value = candidate_feat.GetField(field.name)
                    reach_attributes[field.name] = value

                for select_feat, _counter, _progbar in lyr_select_features.iterate_features():
                    select_geom = select_feat.GetGeometryRef()
                    select_geom = geom_validity_fix(select_geom)
                    if select_geom.Intersects(candidate_geom):
                        feat = ogr.Feature(tmp_lyr.ogr_layer_def)
                        feat.SetGeometry(candidate_geom)
                        for field, value in reach_attributes.items():
                            feat.SetField(field, value)
                        tmp_lyr.ogr_layer.CreateFeature(feat)
                        feat = None
                        break

            # Second loop is about filtering bad areas and simplifying
            for in_feat, _counter, _progbar in tmp_lyr.iterate_features("Filtering out non-relevant shapes for {}".format(name), write_layers=[out_lyr]):

                reach_attributes = {}
                for n in range(field_count):
                    field = out_layer_defn.GetFieldDefn(n)
                    value = in_feat.GetField(field.name)
                    reach_attributes[field.name] = value

                fid = in_feat.GetFID()
                geom = in_feat.GetGeometryRef()
                geom = geom_validity_fix(geom)

                area = geom.Area()
                pts += geom.GetBoundary().GetPointCount()
                # First check. Just make sure this is a valid shape we can work with
                # Make sure the area is greater than the square of the cell width
                # Make sure we're not significantly disconnected from the main shape
                # Make sure we intersect the main shape
                if geom.IsEmpty() \
                        or area < square_buff:
                    # or biggest_area[3].Distance(geom) > 2 * buff_dist:
                    continue

                f_geom = geom.SimplifyPreserveTopology(buff_dist)
                # # Only fix things that need fixing
                f_geom = geom_validity_fix(f_geom)

                # Second check here for validity after simplification
                # Then write to a temporary geopackage layer
                if not f_geom.IsEmpty() and f_geom.Area() > 0:
                    out_feature = ogr.Feature(out_lyr.ogr_layer_def)
                    out_feature.SetGeometry(f_geom)
                    out_feature.SetFID(fid)
                    for field, value in reach_attributes.items():
                        out_feature.SetField(field, value)

                    out_lyr.ogr_layer.CreateFeature(out_feature)

                    in_pts += pts
                    out_pts += f_geom.GetBoundary().GetPointCount()
                else:
                    log.warning('Invalid GEOM with fid: {} for layer {}'.format(fid, name))

        log.info('Writing to disk for layer {}'.format(name))

    log.debug(f'Timer: {_timer.toString()}')


def vbet_merge(in_layer: Path, out_layer: Path, level_path: str = None) -> ogr.Geometry:
    """ clip and merge new vbet layer with exisiting output vbet layer

        returns clipped geometry
    """
    log = Logger('VBET Merge')
    geom = None
    _timer = Timer()
    with get_shp_or_gpkg(in_layer) as lyr_polygon, \
            GeopackageLayer(out_layer, write=True) as lyr_vbet:

        geoms_out = ogr.Geometry(ogr.wkbMultiPolygon)
        # TODO: Not sure we can use write_layers=[lyr_vbet] here because the in
        for feat, *_ in lyr_polygon.iterate_features("VBET Merge (outer)", write_layers=[lyr_vbet]):
            geom_ref = feat.GetGeometryRef()
            geom = geom_ref.Clone()
            geom = geom.MakeValid()
            for clip_feat, *_ in lyr_vbet.iterate_features("VBET Merge (inner)", clip_shape=geom):
                clip_geom = clip_feat.GetGeometryRef()
                geom = geom.Difference(clip_geom)
                if geom is None:
                    break
            geom_type = geom.GetGeometryName()
            if geom_type == 'GeometryCollection':
                break
            geom = ogr.ForceToMultiPolygon(geom)

            out_feature = ogr.Feature(lyr_vbet.ogr_layer_def)
            out_feature.SetGeometry(geom)
            out_feature.SetField("LevelPathI", level_path)
            lyr_vbet.ogr_layer.CreateFeature(out_feature)

            for g in geom:
                geoms_out.AddGeometry(g)

        log.debug(f'Timer: {_timer.toString()}')
        return geoms_out


def clean_up_centerlines(in_centerlines, vbet_polygons, out_centerlines, clip_buffer_value, unique_stream_field):

    with GeopackageLayer(out_centerlines, write=True) as lyr_centerlines, \
        GeopackageLayer(in_centerlines) as lyr_in_centerlines, \
            GeopackageLayer(vbet_polygons) as lyr_vbet:

        lyr_centerlines.create_layer_from_ref(lyr_in_centerlines)

        for feat_vbet, *_ in lyr_vbet.iterate_features():
            level_path = feat_vbet.GetField(f'{unique_stream_field}')
            if level_path is None or level_path == 'None':
                continue
            geom_vbet = feat_vbet.GetGeometryRef()
            geom_clip = geom_vbet.Buffer(clip_buffer_value)
            for feat_centerline, *_ in lyr_in_centerlines.iterate_features(write_layers=[lyr_centerlines], attribute_filter=f"{unique_stream_field} = {level_path}", clip_shape=geom_vbet):
                geom_centerline = feat_centerline.GetGeometryRef().Clone()
                geom_centerline_clipped = geom_clip.Intersection(geom_centerline)
                cl_part_index = feat_centerline.GetField('CL_Part_Index')
                attributes = {f'{unique_stream_field}': level_path, 'CL_Part_Index': cl_part_index}
                lyr_centerlines.create_feature(geom_centerline_clipped, attributes)
