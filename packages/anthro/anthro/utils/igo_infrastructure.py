"""Functions to attribute IGO points with attributes related to the presence of infrastructure within the riverscape.
Jordan Gilbert
Dec 2022
"""
import os
import sqlite3
from osgeo import ogr
from rscommons import Logger, get_shp_or_gpkg, GeopackageLayer
from rscommons.classes.vector_base import VectorBase, get_utm_zone_epsg
from rscommons.vector_ops import get_geometry_unary_union


def infrastructure_attributes(windows: str, road: str, rail: str, canal: str, crossings: str, diversions: str,
                              out_gpkg_path: str):

    log = Logger('IGO Infrastructure Attributes')
    log.info('Adding attributes for infrastructure density to IGOs')

    in_data = {
        road: 'Road',
        rail: 'Rail',
        canal: 'Canal',
        crossings: 'RoadX',
        diversions: 'DivPts'
    }

    # get epsg_proj
    with get_shp_or_gpkg(road) as inref:
        ftr = inref.ogr_layer.GetFeature(1)
        if ftr is not None:
            long = ftr.GetGeometryRef().GetEnvelope()[0]
            epsg_proj = get_utm_zone_epsg(long)
            ref_layer = road
        else:
            epsg_proj = None

    if not epsg_proj:
        with get_shp_or_gpkg(rail) as inref:
            ftr = inref.ogr_layer.GetFeature(1)
            if ftr is not None:
                long = ftr.GetGeometryRef().GetEnvelope()[0]
                epsg_proj = get_utm_zone_epsg(long)
                ref_layer = rail
            else:
                epsg_proj = None

    if not epsg_proj:
        with get_shp_or_gpkg(canal) as inref:
            ftr = inref.ogr_layer.GetFeature(1)
            if ftr is not None:
                long = ftr.GetGeometryRef().GetEnvelope()[0]
                epsg_proj = get_utm_zone_epsg(long)
                ref_layer = canal
            else:
                epsg_proj = None

    if not epsg_proj:
        log.info('No infrastructure datasets, skipping attributes')

        return

    with get_shp_or_gpkg(ref_layer) as reflyr:
        sref, transform = reflyr.get_transform_from_epsg(reflyr.spatial_ref, epsg_proj)

    attribs = {}
    gpkg_driver = ogr.GetDriverByName('GPKG')
    with GeopackageLayer(out_gpkg_path, 'DGOGeometry') as dgo_lyr:
        for dgo_ftr, *_ in dgo_lyr.iterate_features('initializing Attributes'):
            fid = dgo_ftr.GetFID()
            attribs[fid] = {'Road_len': 0, 'Rail_len': 0, 'Canal_len': 0, 'RoadX_ct': 0, 'DivPts_ct': 0, 'Road_prim_len': 0, 'Road_sec_len': 0, 'Road_4wd_len': 0}
        dgo_ftr = None
        for dataset, label in in_data.items():
            log.info(f'Calculating metrics for dataset: {label}')
            dsrc = gpkg_driver.Open(os.path.dirname(dataset))
            if dsrc.GetLayer(os.path.basename(dataset)) is None:
                continue
            ds = get_geometry_unary_union(dataset)
            if ds is None:
                log.info(f'Skipping dataset {label} because it contains no features')
                continue

            for dgo_ftr, *_ in dgo_lyr.iterate_features(f'summarizing dataset {label} metrics on DGOs'):
                dgoid = dgo_ftr.GetFID()

                dgo_ogr = dgo_ftr.GetGeometryRef()
                dgo_geom = VectorBase.ogr2shapely(dgo_ogr)

                lyr_cl = dgo_geom.intersection(ds)

                if lyr_cl.type in ['MultiLineString', 'LineString']:
                    lb1 = label + '_len'
                    if lyr_cl.is_empty is True:
                        attribs[dgoid][lb1] = 0
                    else:
                        ogrlyr = VectorBase.shapely2ogr(lyr_cl)
                        lyr_clipped = VectorBase.ogr2shapely(ogrlyr, transform=transform)
                        attribs[dgoid][lb1] = lyr_clipped.length

                if lyr_cl.type in ['MultiPoint']:
                    lb1 = label + '_ct'
                    if lyr_cl.is_empty is True:
                        attribs[dgoid][lb1] = 0
                    else:
                        attribs[dgoid][lb1] = len(lyr_cl.geoms)

                if lyr_cl.type in ['Point']:
                    lb1 = label + '_ct'
                    if lyr_cl.is_empty is True:
                        attribs[dgoid][lb1] = 0
                    else:
                        attribs[dgoid][lb1] = 1

            dgo_ftr = None

    log.info('Calculating road metrics by road type')
    with GeopackageLayer(out_gpkg_path, 'DGOGeometry') as dgo_lyr, GeopackageLayer(road) as road_lyr:
        if road_lyr.ogr_layer.GetFeatureCount() == 0:
            log.info('No road features, skipping road length calculation')
        else:
            for dgo_ftr, *_ in dgo_lyr.iterate_features():
                dgoid = dgo_ftr.GetFID()
                dgo_geom = dgo_ftr.GetGeometryRef()
                for road_ftr, *_ in road_lyr.iterate_features(clip_shape=dgo_geom, attribute_filter='tnmfrc in (1, 2, 3, 5)'):
                    road_geom = road_ftr.GetGeometryRef()
                    if road_geom.Intersects(dgo_geom):
                        road_clip = road_geom.Intersection(dgo_geom)
                        road_shapely = VectorBase.ogr2shapely(road_clip, transform=transform)
                        attribs[dgoid]['Road_prim_len'] += road_shapely.length
                road_ftr = None
                for road_ftr, *_ in road_lyr.iterate_features(clip_shape=dgo_geom, attribute_filter='tnmfrc = 4'):
                    road_geom = road_ftr.GetGeometryRef()
                    if road_geom.Intersects(dgo_geom):
                        road_clip = road_geom.Intersection(dgo_geom)
                        road_shapely = VectorBase.ogr2shapely(road_clip, transform=transform)
                        attribs[dgoid]['Road_sec_len'] += road_shapely.length
                road_ftr = None
                for road_ftr, *_ in road_lyr.iterate_features(clip_shape=dgo_geom, attribute_filter='tnmfrc = 6'):
                    road_geom = road_ftr.GetGeometryRef()
                    if road_geom.Intersects(dgo_geom):
                        road_clip = road_geom.Intersection(dgo_geom)
                        road_shapely = VectorBase.ogr2shapely(road_clip, transform=transform)
                        attribs[dgoid]['Road_4wd_len'] += road_shapely.length

    conn = sqlite3.connect(out_gpkg_path)
    curs = conn.cursor()

    for dgoid, vals in attribs.items():
        curs.execute(f'UPDATE DGOAttributes SET Road_len = {vals["Road_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET Rail_len = {vals["Rail_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET Canal_len = {vals["Canal_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET RoadX_ct = {vals["RoadX_ct"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET DivPts_ct = {vals["DivPts_ct"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET Road_prim_len = {vals["Road_prim_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET Road_sec_len = {vals["Road_sec_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET Road_4wd_len = {vals["Road_4wd_len"]} WHERE DGOID = {dgoid}')

    # summarize metrics from DGOs to IGOs using moving windows
    for igoid, dgoids in windows.items():
        road_len = 0
        rail_len = 0
        canal_len = 0
        roadx_ct = 0
        divpts_ct = 0
        road_prim_len = 0
        road_sec_len = 0
        road_4wd_len = 0
        window_len = 0

        for dgoid in dgoids:
            road_len += curs.execute(f'SELECT Road_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            rail_len += curs.execute(f'SELECT Rail_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            canal_len += curs.execute(f'SELECT Canal_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            roadx_ct += curs.execute(f'SELECT RoadX_ct FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            divpts_ct += curs.execute(f'SELECT DivPts_ct FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            road_prim_len += curs.execute(f'SELECT Road_prim_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            road_sec_len += curs.execute(f'SELECT Road_sec_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            road_4wd_len += curs.execute(f'SELECT Road_4wd_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            window_len += curs.execute(f'SELECT centerline_length FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]

        if window_len == 0:
            road_dens = 0
            rail_dens = 0
            canal_dens = 0
            roadx_dens = 0
            divpts_dens = 0
            road_prim_dens = 0
            road_sec_dens = 0
            road_4wd_dens = 0
        else:
            road_dens = road_len / window_len
            rail_dens = rail_len / window_len
            canal_dens = canal_len / window_len
            roadx_dens = roadx_ct / window_len
            divpts_dens = divpts_ct / window_len
            road_prim_dens = road_prim_len / window_len
            road_sec_dens = road_sec_len / window_len
            road_4wd_dens = road_4wd_len / window_len

        curs.execute(f'UPDATE IGOAttributes SET Road_len = {road_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Rail_len = {rail_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Canal_len = {canal_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET RoadX_ct = {roadx_ct} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET DivPts_ct = {divpts_ct} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_prim_len = {road_prim_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_sec_len = {road_sec_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_4wd_len = {road_4wd_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_dens = {road_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Rail_dens = {rail_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Canal_dens = {canal_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET RoadX_dens = {roadx_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET DivPts_dens = {divpts_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_prim_dens = {road_prim_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_sec_dens = {road_sec_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_4wd_dens = {road_4wd_dens} WHERE IGOID = {igoid}')

    conn.commit()
    conn.close()
