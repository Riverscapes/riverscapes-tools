"""Functions to attribute IGO points with attributes related to the presence of infrastructure within the riverscape.

Jordan Gilbert

Dec 2022
"""
import os
import sqlite3
import json
import numpy as np
from rscommons import GeopackageLayer, get_shp_or_gpkg
from rscommons.classes.vector_base import VectorBase, get_utm_zone_epsg
from rscommons.vector_ops import get_geometry_unary_union
from osgeo import ogr


def infrastructure_attributes(igo: str, dgo: str, road: str, rail: str, canal: str, crossings: str, diversions: str,
                              distance: dict, levelpaths: list, out_gpkg_path: str):

    in_data = {
        road: 'Road',
        rail: 'Rail',
        canal: 'Canal',
        crossings: 'RoadX',
        diversions: 'DivPts'
    }

    windows = get_moving_windows(igo, dgo, levelpaths, distance)

    # get epsg_proj
    with get_shp_or_gpkg(igo) as inref:
        ftr = inref.ogr_layer.GetFeature(1)
        long = ftr.GetGeometryRef().GetEnvelope()[0]
        epsg_proj = get_utm_zone_epsg(long)

    conn = sqlite3.connect(out_gpkg_path)
    # curs = conn.cursor()

    with get_shp_or_gpkg(road) as reflyr:
        sref, transform = reflyr.get_transform_from_epsg(reflyr.spatial_ref, epsg_proj)

    for dataset, label in in_data.items():
        ds = get_geometry_unary_union(dataset)
        # sref, transform = ds.get_transform_from_epsg(ds.spatial_ref, epsg_proj)

        for igoid, window in windows.items():

            lyr_cl = window[1].intersection(ds)
            # project clipped layer to utm epsg
            ogrlyr = VectorBase.shapely2ogr(lyr_cl)
            lyr_clipped = VectorBase.ogr2shapely(ogrlyr, transform=transform)

            window_ogr = VectorBase.shapely2ogr(window[1])
            window_proj = VectorBase.ogr2shapely(window_ogr, transform)
            window_area = window_proj.area
            if ds.type in ['MultiLineString', 'LineString']:
                lb1 = label + '_len'
                lb2 = label + '_dens'
                conn.execute(f'UPDATE IGOAttributes SET {lb1} = {lyr_clipped.length} WHERE IGOID = {igoid}')
                conn.execute(f'UPDATE IGOAttributes SET {lb2} {lyr_clipped.length/window_area} WHERE IGOID = {igoid}')
                conn.commit()
            if ds.type in ['Point', 'MultiPoint']:
                lb1 = label + '_ct'
                lb2 = label + '_dens'
                conn.execute(f'UPDATE IGOAttributes SET {lb1} = {len(lyr_clipped.coords.xy[0])} WHERE IGOID = {igoid}')
                conn.execute(f'UPDATE IGOAttributes SET {lb2} {len(lyr_clipped.coords.xy[0])/window_area} WHERE IGOID = {igoid}')
                conn.commit()


def get_moving_windows(igo: str, dgo: str, level_paths: list, distance: dict):

    windows = {}

    with GeopackageLayer(igo) as lyr_igo, GeopackageLayer(dgo) as lyr_dgo:
        for level_path in level_paths:
            for feat_seg_pt, *_, in lyr_igo.iterate_features(f'Finding windows on {level_path}', attribute_filter=f"LevelPathI = {level_path}"):
                window_distance = distance[str(feat_seg_pt.GetField('stream_size'))]
                dist = feat_seg_pt.GetField('seg_distance')
                min_dist = dist - 0.5 * window_distance
                max_dist = dist + 0.5 * window_distance
                sql_seg_poly = f"LevelPathI = {level_path} AND seg_distance >= {min_dist} AND seg_distance <= {max_dist}"
                geom_window_sections = ogr.Geometry(ogr.wkbMultiPolygon)
                window_length = 0.0
                window_area = 0.0
                for feat_seg_poly, *_ in lyr_dgo.iterate_features(attribute_filter=sql_seg_poly):
                    window_length = window_length + feat_seg_poly.GetField('centerline_length')
                    window_area = window_area + feat_seg_poly.GetField('segment_area')
                    geom = feat_seg_poly.GetGeometryRef()
                    if geom.GetGeometryName() in ['MULTIPOLYGON', 'GEOMETRYCOLLECTION']:
                        for i in range(0, geom.GetGeometryCount()):
                            geo = geom.GetGeometryRef(i)
                            if geo.GetGeometryName() == 'POLYGON':
                                geom_window_sections.AddGeometry(geo)
                    else:
                        geom_window_sections.AddGeometry(geom)

                windows[feat_seg_pt.GetFID()] = [window_distance, VectorBase.ogr2shapely(geom_window_sections)]

    return windows


igoin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/outputs/outputs.gpkg/anthro_igo_geom'
dgoin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/inputs/inputs.gpkg/dgo'
roadin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/inputs/inputs.gpkg/roads'
railin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/inputs/inputs.gpkg/rails'
canalin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/inputs/inputs.gpkg/canals'
crossingsin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/intermediates/intermediates.gpkg/road_crossings'
diversionsin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/intermediates/intermediates.gpkg/diversions'
out_gpkg_pathin = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/outputs/outputs.gpkg'

# with get_shp_or_gpkg(igo) as igo_lyr:
#    level_ps = [igo_lyr.]
#    level_ps_unique = np.unique(level_ps)
# driver = ogr.GetDriverByName('GPKG')
# src = driver.Open(os.path.dirname(igoin))
# lyr = src.GetLayerByName('anthro_igo_geom')
# level_ps = [ftr.GetField('LevelPathI') for ftr in lyr]
# levelpathsin = list(np.unique(level_ps))

conn = sqlite3.connect(out_gpkg_pathin)
curs = conn.cursor()
curs.execute('SELECT DISTINCT LevelPathI FROM anthro_igo_geom')
levelps = curs.fetchall()
levelpathsin = [levelps[i][0] for i in range(len(levelps))]
# conn.execute('CREATE INDEX ix_igo_levelpath on anthro_igo_geom(LevelPathI)')
# conn.execute('CREATE INDEX ix_igo_segdist on anthro_igo_geom(seg_distance)')
# conn.execute('CREATE INDEX ix_igo_size on anthro_igo_geom(stream_size)')
# conn.commit()
# conn = None
# conn = sqlite3.connect(os.path.dirname(dgo))
# conn.execute('CREATE INDEX ix_dgo_levelpath on dgo(LevelPathI)')
# conn.execute('CREATE INDEX ix_dgo_segdist on dgo(seg_distance)')
# conn.commit()
conn = None

distancein = {
    '0': 100,
    '1': 250,
    '2': 1000
}

# windows = get_moving_windows(igo, dgo, levelpaths, distance)
infrastructure_attributes(igoin, dgoin, roadin, railin, canalin, crossingsin, diversionsin, distancein, levelpathsin, out_gpkg_pathin)

# print(windows)
