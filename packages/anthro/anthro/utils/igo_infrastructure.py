"""Functions to attribute IGO points with attributes related to the presence of infrastructure within the riverscape.
Jordan Gilbert
Dec 2022
"""
import os
import sqlite3
import json
import numpy as np
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
        else:
            epsg_proj = None

    if not epsg_proj:
        with get_shp_or_gpkg(road) as inref:
            ftr = inref.ogr_layer.GetFeature(1)
            if ftr is not None:
                long = ftr.GetGeometryRef().GetEnvelope()[0]
                epsg_proj = get_utm_zone_epsg(long)
            else:
                epsg_proj = None

    if not epsg_proj:
        with get_shp_or_gpkg(canal) as inref:
            ftr = inref.ogr_layer.GetFeature(1)
            if ftr is not None:
                long = ftr.GetGeometryRef().GetEnvelope()[0]
                epsg_proj = get_utm_zone_epsg(long)
            else:
                epsg_proj = None

    if not epsg_proj:
        log.info('No infrastructure datasets, skipping attributes')

        return

    # conn = sqlite3.connect(out_gpkg_path)
    # curs = conn.cursor()

    with get_shp_or_gpkg(road) as reflyr:
        sref, transform = reflyr.get_transform_from_epsg(reflyr.spatial_ref, epsg_proj)

    attribs = {}
    gpkg_driver = ogr.GetDriverByName('GPKG')
    with GeopackageLayer(out_gpkg_path, 'DGOGeometry') as dgo_lyr:
        for dgo_ftr, *_ in dgo_lyr.iterate_features('initializing Attributes'):
            fid = dgo_ftr.GetFID()
            attribs[fid] = {'Road_len': 0, 'Rail_len': 0, 'Canal_len': 0, 'Roadx_ct': 0, 'DivPts_ct': 0}
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
                        # curs.execute(f'UPDATE DGOAttributes SET {lb1} = 0 WHERE DGOID = {dgoid}')
                    else:
                        ogrlyr = VectorBase.shapely2ogr(lyr_cl)
                        lyr_clipped = VectorBase.ogr2shapely(ogrlyr, transform=transform)
                        attribs[dgoid][lb1] = lyr_clipped.length
                        # curs.execute(f'UPDATE DGOAttributes SET {lb1} = {lyr_clipped.length} WHERE DGOID = {dgoid}')

                if lyr_cl.type in ['MultiPoint']:
                    lb1 = label + '_ct'
                    if lyr_cl.is_empty is True:
                        attribs[dgoid][lb1] = 0
                        # curs.execute(f'UPDATE DGOAttributes SET {lb1} = 0 WHERE DGOID = {dgoid}')
                    else:
                        attribs[dgoid][lb1] = len(lyr_cl.geoms)
                        # curs.execute(f'UPDATE DGOAttributes SET {lb1} = {len(lyr_cl.geoms)} WHERE DGOID = {dgoid}')

                if lyr_cl.type in ['Point']:
                    lb1 = label + '_ct'
                    if lyr_cl.is_empty is True:
                        attribs[dgoid][lb1] = 0
                        # curs.execute(f'UPDATE DGOAttributes SET {lb1} = 0 WHERE DGOID = {dgoid}')
                    else:
                        attribs[dgoid][lb1] = 1
                        # curs.execute(f'UPDATE DGOAttributes SET {lb1} = 1 WHERE DGOID = {dgoid}')

    conn = sqlite3.connect(out_gpkg_path)
    curs = conn.cursor()

    for dgoid, vals in attribs.items():
        curs.execute(f'UPDATE DGOAttributes SET Road_len = {vals["Road_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET Rail_len = {vals["Rail_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET Canal_len = {vals["Canal_len"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET RoadX_ct = {vals["Roadx_ct"]} WHERE DGOID = {dgoid}')
        curs.execute(f'UPDATE DGOAttributes SET DivPts_ct = {vals["DivPts_ct"]} WHERE DGOID = {dgoid}')

    # fields = ['Road_len', 'Rail_len', 'Canal_len', 'RoadX_ct', 'DivPts_ct']
    # for field in fields:
    #     curs.execute(f'UPDATE DGOAttributes SET {field} = 0 WHERE {field} IS NULL')

    # summarize metrics from DGOs to IGOs using moving windows
    for igoid, dgoids in windows.items():
        road_len = 0
        rail_len = 0
        canal_len = 0
        roadx_ct = 0
        divpts_ct = 0
        window_area = 0

        for dgoid in dgoids:
            road_len += curs.execute(f'SELECT Road_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            rail_len += curs.execute(f'SELECT Rail_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            canal_len += curs.execute(f'SELECT Canal_len FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            roadx_ct += curs.execute(f'SELECT RoadX_ct FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            divpts_ct += curs.execute(f'SELECT DivPts_ct FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]
            window_area += curs.execute(f'SELECT segment_area FROM DGOAttributes WHERE DGOID = {dgoid}').fetchone()[0]

        road_dens = road_len / window_area
        rail_dens = rail_len / window_area
        canal_dens = canal_len / window_area
        roadx_dens = roadx_ct / window_area
        divpts_dens = divpts_ct / window_area

        curs.execute(f'UPDATE IGOAttributes SET Road_len = {road_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Rail_len = {rail_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Canal_len = {canal_len} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Roadx_ct = {roadx_ct} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET DivPts_ct = {divpts_ct} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Road_dens = {road_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Rail_dens = {rail_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Canal_dens = {canal_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET Roadx_dens = {roadx_dens} WHERE IGOID = {igoid}')
        curs.execute(f'UPDATE IGOAttributes SET DivPts_dens = {divpts_dens} WHERE IGOID = {igoid}')

    conn.commit()
    conn.close()
