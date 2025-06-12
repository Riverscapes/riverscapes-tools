"""Creates a dictionary of the form {igoid: [window_length, window shapely object]}
for each igo in the input dataset.
"""

import os
import sqlite3

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
    dists = {}

    with sqlite3.connect(os.path.dirname(dgo)) as conn:
        curs = conn.cursor()
        for level_path in level_paths:
            curs.execute(f'SELECT seg_distance FROM {os.path.basename(dgo)} WHERE level_path = {level_path}')
            sds = [row[0] for row in curs.fetchall() if row[0] is not None]
            sds.sort()
            if len(sds) >= 2:
                dists[level_path] = sds[1] - sds[0]
            else:
                dists[level_path] = None

    with GeopackageLayer(igo) as lyr_igo, GeopackageLayer(dgo) as lyr_dgo:
        for level_path in level_paths:
            for feat_seg_pt, *_, in lyr_igo.iterate_features(f'Finding windows on {level_path}', attribute_filter=f"level_path = {level_path}"):
                window_distance = distance[str(feat_seg_pt.GetField('stream_size'))]
                if dists[level_path] is not None:
                    if window_distance < 2 * dists[level_path]:
                        window_distance = 2 * dists[level_path]
                dist = feat_seg_pt.GetField('seg_distance')
                min_dist = dist - 0.5 * window_distance
                max_dist = dist + 0.5 * window_distance

                dgoids = []
                for feat_seg_poly, *_ in lyr_dgo.iterate_features(attribute_filter=f"level_path = {int(level_path)} and seg_distance >= {int(min_dist)} and seg_distance <= {int(max_dist)}"):
                    dgoids.append(feat_seg_poly.GetFID())
                windows[feat_seg_pt.GetFID()] = dgoids

    return windows


def moving_window_by_intersection(igo: str, dgo: str, level_paths: list):
    """This function creates 3 DGO moving windows based on intersection instead of distance values (merged VBET projects can have duplicat seg_distance for now)"""

    windows = {}
    associated_dgo = {}

    with GeopackageLayer(igo) as lyr_igo, GeopackageLayer(dgo) as lyr_dgo:
        for level_path in level_paths:
            for feat_seg_pt, *_, in lyr_igo.iterate_features('Finding IGOs associated DGOs', attribute_filter=f"level_path = {level_path}"):
                geom = feat_seg_pt.GetGeometryRef()
                seg_dist = feat_seg_pt.GetField('seg_distance')

                for feat_seg_poly, *_ in lyr_dgo.iterate_features(attribute_filter=f"level_path = {int(level_path)} and seg_distance = {int(seg_dist)}"):
                    if feat_seg_poly.GetGeometryRef().Contains(geom):
                        associated_dgo[feat_seg_pt.GetFID()] = feat_seg_poly

        for level_path in level_paths:
            for feat_seg_pt, *_, in lyr_igo.iterate_features(f'Finding windows on {level_path}', attribute_filter=f"level_path = {level_path}"):
                dgoids = []
                if feat_seg_pt.GetFID() in associated_dgo:
                    adgo = associated_dgo[feat_seg_pt.GetFID()]
                    dgoids.append(adgo.GetFID())
                    for feat_seg_poly, *_ in lyr_dgo.iterate_features(clip_shape=adgo.GetGeometryRef(), attribute_filter=f"level_path = {int(level_path)}"):
                        geom_poly = feat_seg_poly.GetGeometryRef()
                        if geom_poly.Intersects(adgo.GetGeometryRef()) and feat_seg_poly.GetFID() not in dgoids:
                            dgoids.append(feat_seg_poly.GetFID())

                windows[feat_seg_pt.GetFID()] = dgoids

    return windows
