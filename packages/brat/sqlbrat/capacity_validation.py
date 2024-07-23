import os

from shapely.ops import nearest_points, unary_union

from rscommons import GeopackageLayer, Logger
from rscommons.classes.vector_base import VectorBase
from rscommons.database import SQLiteCon


def validate_capacity(brat_gpkg_path: str, dams_gpkg_path: str):

    log = Logger('BRAT Capacity Validation')

    dam_cts = {}  # reachid: dam count

    with GeopackageLayer(os.path.join(brat_gpkg_path, 'vwReaches')) as brat_lyr, \
            GeopackageLayer(os.path.join(dams_gpkg_path, 'census')) as dams_lyr:

        buffer_distance = brat_lyr.rough_convert_metres_to_vector_units(10)

        # create a dissolved drainage network
        line_geoms = [ftr for ftr in brat_lyr.ogr_layer]
        line_geoms_shapely = [VectorBase.ogr2shapely(line_geom) for line_geom in line_geoms]
        merged_line = unary_union(line_geoms_shapely)

        # get the points on the line network that are closest to the dam points
        for dam_ftr, *_ in dams_lyr.iterate_features('Finding dam counts for reaches'):
            dam_geom = dam_ftr.GetGeometryRef()
            nearest_line = nearest_points(merged_line, VectorBase.ogr2shapely(dam_geom))
            dam_buf = nearest_line[0].buffer(buffer_distance)

            for line_ftr, *_ in brat_lyr.iterate_features(clip_shape=dam_buf):
                reachid = line_ftr.GetFID()
                line_geom = line_ftr.GetGeometryRef()
                if line_geom is not None:
                    if reachid not in dam_cts.keys():
                        dam_cts[reachid] = 1
                    else:
                        dam_cts[reachid] += 1

    print((max(dam_cts, key=dam_cts.get)))
    print(dam_cts[max(dam_cts, key=dam_cts.get)])

    log.info('Adding dam count table to geopackage')
    with SQLiteCon(brat_gpkg_path) as db:
        db.curs.execute('SELECT ReachID FROM vwReaches')
        reachids = [row['ReachID'] for row in db.curs.fetchall()]
        db.curs.execute('DROP TABLE IF EXISTS dam_counts')
        db.curs.execute('CREATE TABLE dam_counts (reachid INTEGER PRIMARY KEY, dam_count INTEGER)')
        for reachid in reachids:
            if reachid in dam_cts.keys():
                db.curs.execute('INSERT INTO dam_counts (reachid, dam_count) VALUES (?, ?)', (reachid, dam_cts[reachid]))
            else:
                db.curs.execute('INSERT INTO dam_counts (reachid, dam_count) VALUES (?, ?)', (reachid, 0))
        db.conn.commit()
        db.curs.execute('INSERT INTO gpkg_contents (table_name, data_type) VALUES (?, ?)', ('dam_counts', 'attributes'))
        db.conn.commit()
    log.info('Done')


bgp = '/workspaces/data/brat/1701020501/outputs/brat.gpkg'
dgp = '/workspaces/data/beaver_activity/1701020501/census.gpkg'

validate_capacity(bgp, dgp)
