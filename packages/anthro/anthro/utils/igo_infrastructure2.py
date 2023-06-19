"""Functions to attribute IGO points with attributes related to the presence of infrastructure within the riverscape.
Jordan Gilbert
Dec 2022
"""
import os
import sqlite3
import json
import numpy as np
from osgeo import ogr
from rscommons import Logger, get_shp_or_gpkg
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

    conn = sqlite3.connect(out_gpkg_path)

    with get_shp_or_gpkg(road) as reflyr:
        sref, transform = reflyr.get_transform_from_epsg(reflyr.spatial_ref, epsg_proj)

    gpkg_driver = ogr.GetDriverByName('GPKG')
    for dataset, label in in_data.items():
        log.info(f'Calculating metrics for dataset: {label}')
        dsrc = gpkg_driver.Open(os.path.dirname(dataset))
        if dsrc.GetLayer(os.path.basename(dataset)) is None:
            continue
        ds = get_geometry_unary_union(dataset)
        if ds is None:
            log.info(f'Skipping dataset {label} because it contains no features')
            continue

        counter = 1
        for igoid, window in windows.items():
            # print(f'summarizing on igo {counter} of {len(windows)} for dataset {label}')

            # windowpoly = get_geometry_unary_union(window[0])
            lyr_cl = window[0].intersection(ds)
            # project clipped layer to utm epsg

            # leave null if layer is empty?
            if lyr_cl.is_empty is True:
                continue
            else:
                if lyr_cl.type in ['MultiLineString', 'LineString']:
                    ogrlyr = VectorBase.shapely2ogr(lyr_cl)
                    lyr_clipped = VectorBase.ogr2shapely(ogrlyr, transform=transform)
                    lb1 = label + '_len'
                    lb2 = label + '_dens'
                    conn.execute(f'UPDATE IGOAttributes SET {lb1} = {lyr_clipped.length} WHERE IGOID = {igoid}')
                    if window[2] == 0.0:
                        conn.commit()
                        continue
                    else:
                        conn.execute(f'UPDATE IGOAttributes SET {lb2} = {lyr_clipped.length / window[2]} WHERE IGOID = {igoid}')
                    conn.commit()
                if lyr_cl.type in ['MultiPoint']:
                    lb1 = label + '_ct'
                    lb2 = label + '_dens'
                    conn.execute(f'UPDATE IGOAttributes SET {lb1} = {len(lyr_cl.geoms)} WHERE IGOID = {igoid}')
                    if window[2] == 0.0:
                        conn.commit()
                        continue
                    else:
                        conn.execute(f'UPDATE IGOAttributes SET {lb2} = {len(lyr_cl.geoms) / window[2]} WHERE IGOID = {igoid}')
                    conn.commit()
                if lyr_cl.type in ['Point']:
                    lb1 = label + '_ct'
                    lb2 = label + '_dens'
                    if lyr_cl.is_empty is True:
                        conn.execute(f'UPDATE IGOAttributes SET {lb1} = 0 WHERE IGOID = {igoid}')
                        conn.execute(f'UPDATE IGOAttributes SET {lb2} = 0 WHERE IGOID = {igoid}')
                    else:
                        conn.execute(f'UPDATE IGOAttributes SET {lb1} = 1 WHERE IGOID = {igoid}')
                        if window[2] == 0.0:
                            conn.commit()
                            continue
                        else:
                            conn.execute(f'UPDATE IGOAttributes SET {lb2} = {1 / window[2]} WHERE IGOID = {igoid}')
            counter += 1

    fields = ['Road_len', 'Road_dens', 'Rail_len', 'Rail_dens', 'Canal_len', 'Canal_dens', 'RoadX_ct', 'RoadX_dens', 'DivPts_ct', 'DivPts_dens']
    for field in fields:
        conn.execute(f'UPDATE IGOAttributes SET {field} = 0 WHERE {field} IS NULL')
    conn.commit()
