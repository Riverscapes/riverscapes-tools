"""Creates a dictionary of the form {igoid: [window_length, window shapely object]}
for each igo in the input dataset.
"""

from osgeo import ogr

from rscommons import GeopackageLayer, VectorBase


def get_moving_windows(igo: str, dgo: str, level_paths: list, distance: dict):

    windows = {}

    with GeopackageLayer(igo) as lyr_igo, GeopackageLayer(dgo) as lyr_dgo:
        for level_path in level_paths:
            for feat_seg_pt, *_, in lyr_igo.iterate_features(f'Finding windows on {level_path}', attribute_filter=f"LevelPathI = {level_path}"):
                window_distance = distance[str(feat_seg_pt.GetField('stream_size'))]
                dist = feat_seg_pt.GetField('seg_distance')
                min_dist = dist - 0.5 * window_distance
                max_dist = dist + 0.5 * window_distance
                # sql_seg_poly = f"LevelPathI = {level_path} and seg_distance >= {min_dist} and seg_distance <= {max_dist}"
                geom_window_sections = ogr.Geometry(ogr.wkbMultiPolygon)
                window_length = 0.0
                window_area = 0.0
                for feat_seg_poly, *_ in lyr_dgo.iterate_features(attribute_filter=f"LevelPathI = {int(level_path)} and seg_distance >= {int(min_dist)} and seg_distance <= {int(max_dist)}"):
                    window_length += feat_seg_poly.GetField('centerline_length')
                    window_area += feat_seg_poly.GetField('segment_area')
                    geom = feat_seg_poly.GetGeometryRef()
                    if geom.GetGeometryName() in ['MULTIPOLYGON', 'GEOMETRYCOLLECTION']:
                        for i in range(0, geom.GetGeometryCount()):
                            geo = geom.GetGeometryRef(i)
                            if geo.GetGeometryName() == 'POLYGON':
                                geom_window_sections.AddGeometry(geo)
                    else:
                        geom_window_sections.AddGeometry(geom)

                if not geom_window_sections.IsValid():
                    geom_window_sections = geom_window_sections.MakeValid()
                windows[feat_seg_pt.GetFID()] = [VectorBase.ogr2shapely(geom_window_sections), window_length, window_area]

    return windows


def moving_window_dgo_ids(igo: str, dgo: str, level_paths: list, distance: dict):

    windows = {}

    with GeopackageLayer(igo) as lyr_igo, GeopackageLayer(dgo) as lyr_dgo:
        for level_path in level_paths:
            for feat_seg_pt, *_, in lyr_igo.iterate_features(f'Finding windows on {level_path}', attribute_filter=f"level_path = {level_path}"):
                window_distance = distance[str(feat_seg_pt.GetField('stream_size'))]
                dist = feat_seg_pt.GetField('seg_distance')
                min_dist = dist - 0.5 * window_distance
                max_dist = dist + 0.5 * window_distance

                dgoids = []
                for feat_seg_poly, *_ in lyr_dgo.iterate_features(attribute_filter=f"level_path = {int(level_path)} and seg_distance >= {int(min_dist)} and seg_distance <= {int(max_dist)}"):
                    dgoids.append(feat_seg_poly.GetFID())
                windows[feat_seg_pt.GetFID()] = dgoids

    return windows
