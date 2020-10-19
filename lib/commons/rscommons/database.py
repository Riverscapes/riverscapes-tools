import os
import csv
import json
import sqlite3
import argparse
from osgeo import ogr, osr
from rscommons import ProgressBar, Logger, ModelConfig, dotenv
from rscommons.shapefile import create_field
from rscommons.shapefile import get_transform_from_epsg
from rscommons.build_network import FCodeValues
from shapely.wkb import loads as wkbload
from shapely.geometry import shape

perennial_reach_code = 46006


def create_database(huc, db_path, metadata, epsg, schema_path):

    # We need to create a projection for this DB
    db_srs = osr.SpatialReference()
    db_srs.ImportFromEPSG(int(epsg))
    metadata['gdal_srs_proj4'] = db_srs.ExportToProj4()
    metadata['gdal_srs_axis_mapping_strategy'] = osr.OAMS_TRADITIONAL_GIS_ORDER

    if not os.path.isfile(schema_path):
        raise Exception('Unable to find database schema file at {}'.format(schema_path))

    log = Logger('Database')
    if os.path.isfile(db_path):
        log.info('Removing existing SQLite database at {0}'.format(db_path))
        os.remove(db_path)

    log.info('Creating SQLite database at {0}'.format(db_path))
    qry = open(schema_path, 'r').read()
    sqlite3.complete_statement(qry)
    conn = sqlite3.connect(db_path)
    curs = conn.cursor()
    curs.executescript(qry)

    # Load lookup table data into the database
    for dirName, dirs, files in os.walk(os.path.join(os.path.dirname(schema_path), 'data')):
        for file in files:
            with open(os.path.join(dirName, file), mode='r') as csvfile:
                d = csv.DictReader(csvfile)
                sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(os.path.splitext(file)[0], ','.join(d.fieldnames), ','.join('?' * len(d.fieldnames)))

                to_db = [[i[col] for col in d.fieldnames] for i in d]
                curs.executemany(sql, to_db)
                log.info('{:,} records loaded into {} lookup data table'.format(curs.rowcount, os.path.splitext(file)[0]))

    # Keep only the designated watershed
    curs.execute('DELETE FROM Watersheds WHERE WatershedID <> ?', [huc])

    # Retrieve the name of the watershed so it can be stored in riverscapes project
    curs.execute('SELECT Name FROM Watersheds WHERE WatershedID = ?', [huc])
    watershed_name = curs.fetchone()[0]

    conn.commit()
    conn.execute("VACUUM")

    # Write the metadata to the database
    if metadata:
        [store_metadata(db_path, key, value) for key, value in metadata.items()]

    return watershed_name


def update_database(db_path, csv_path):
    """ Update the lookup tables from CSV files in a path

    Arguments:
        db_path {[type]} -- [description]
        csv_path {[type]} -- [description]

    Raises:
        Exception: [description]

    Returns:
        [type] -- [description]
    """

    log = Logger('DatabaseUpdate')

    csv_path = csv_path if csv_path else os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'database', 'data')

    if not os.path.isfile(db_path):
        raise Exception('No existing db found at path: {}'.format(db_path))

    log.info('Updating SQLite database at {0}'.format(db_path))

    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    curs = conn.cursor()

    try:
        huc = conn.execute('SELECT WatershedID FROM Reaches GROUP BY WatershedID').fetchall()[0]['WatershedID']
    except Exception as e:
        log.error('Error retrieving HUC from DB')
        raise e

    # Load lookup table data into the database
    for dirName, dirs, files in os.walk(csv_path, '../database/data'):
        for file in files:
            filename = os.path.splitext(file)[0]
            with open(os.path.join(dirName, file), mode='r') as csvfile:
                d = csv.DictReader(csvfile)
                sql = 'INSERT OR REPLACE INTO {0} ({1}) VALUES ({2})'.format(os.path.splitext(file)[0], ','.join(d.fieldnames), ','.join('?' * len(d.fieldnames)))

                to_db = [[i[col] for col in d.fieldnames] for i in d]
                curs.executemany(sql, to_db)
                log.info('{:,} records updated or loaded into {} lookup data table'.format(curs.rowcount, os.path.splitext(file)[0]))

    # Keep only the designated watershed
    curs.execute('DELETE FROM Watersheds WHERE WatershedID <> ?', [huc])

    conn.commit()
    conn.execute("VACUUM")

    return db_path


def get_db_srs(database):
    meta = get_metadata(database)
    dbRef = osr.SpatialReference()
    dbRef.ImportFromProj4(meta['gdal_srs_proj4'])
    dbRef.SetAxisMappingStrategy(int(meta['gdal_srs_axis_mapping_strategy']))
    return dbRef


def populate_database(database, network, huc):

    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataset = driver.Open(network, 1)
    layer = dataset.GetLayer()
    in_spatial_ref = layer.GetSpatialRef()
    db_spatial_ref = get_db_srs(database)
    transform = osr.CoordinateTransformation(in_spatial_ref, db_spatial_ref)

    log = Logger('Database')
    log.info('Populating SQLite database with {0:,} features'.format(layer.GetFeatureCount()))

    # Determine the transformation if user provides an EPSG
    create_field(layer, 'ReachID', ogr.OFTInteger)

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    progbar = ProgressBar(layer.GetFeatureCount(), 50, "Populating features")
    counter = 0
    progbar.update(counter)
    for feature in layer:
        counter += 1
        progbar.update(counter)

        geom = feature.GetGeometryRef()
        if transform:
            geom.Transform(transform)
        geojson = geom.ExportToJson()

        reach_code = feature.GetField('FCode')
        if not reach_code:
            raise Exception('Missing reach code')

        # perennial = 1 if reach_code == '46006' else 0
        name = feature.GetField('GNIS_Name')

        drainage_area = feature.GetField('TotDASqKm')

        # Store the feature in the SQLite database
        curs.execute('INSERT INTO Reaches (WatershedID, Geometry, ReachCode, StreamName, iGeo_DA, Orig_DA) VALUES (?, ?, ?, ?, ?, ?)', [huc, geojson, reach_code, name, drainage_area, drainage_area])

        # Update the feature in the ShapeFile with the SQLite database ReachID
        feature.SetField('ReachID', curs.lastrowid)
        layer.SetFeature(feature)

        # Commit geometry every hundred rows so the memory buffer doesn't fill
        if counter % 10000 == 0:
            conn.commit()

    # Store whether each reach is Perennial or not
    curs.execute('UPDATE Reaches SET IsPeren = 1 WHERE (ReachCode = ?)', [perennial_reach_code])
    conn.commit()

    # Update reaches with NULL zero drainage area to have zero drainage area
    curs.execute('UPDATE Reaches SET iGeo_DA = 0 WHERE iGeo_DA IS NULL')
    conn.commit()

    progbar.finish()

    curs.execute('SELECT Count(*) FROM Reaches')
    reach_count = curs.fetchone()[0]
    log.info('{:,} reaches inserted into database'.format(reach_count))
    if reach_count < 1:
        raise Exception('Zero reaches in watershed. Unable to continue. Aborting.')

    for name, fcode in FCodeValues.items():
        curs.execute('SELECT Count(*) FROM Reaches WHERE ReachCode = ?', [str(fcode)])
        log.info('{:,} {} reaches (FCode {}) inserted into database'.format(curs.fetchone()[0], name, fcode))

    log.info('Database creation complete')


def load_geometries(database, target_srs=None, where_clause=None):

    transform = None
    if target_srs:
        db_srs = get_db_srs(database)
        # https://github.com/OSGeo/gdal/issues/1546
        target_srs.SetAxisMappingStrategy(db_srs.GetAxisMappingStrategy())
        transform = osr.CoordinateTransformation(db_srs, target_srs)

    conn = sqlite3.connect(database)
    curs = conn.execute('SELECT ReachID, Geometry FROM Reaches {}'.format('WHERE {}'.format(where_clause) if where_clause else ''))
    reaches = {}

    for row in curs.fetchall():
        geom = ogr.CreateGeometryFromJson(row[1])
        if transform:
            geom.Transform(transform)
        reaches[row[0]] = wkbload(geom.ExportToWkb())
    return reaches


def load_attributes(database, fields, where_clause=None):

    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory
    curs = conn.execute('SELECT ReachID, {} FROM vwReaches {}'.format(','.join(fields), 'WHERE {}'.format(where_clause) if where_clause else ''))
    reaches = {}
    for row in curs.fetchall():
        reaches[row['ReachID']] = {}
        for field in fields:
            reaches[row['ReachID']][field] = row[field]

    return reaches


def write_attributes(database, reaches, fields, set_null_first=True, summarize=True):

    if len(reaches) < 1:
        return

    conn = sqlite3.connect(database)
    conn.execute('pragma foreign_keys=ON')
    curs = conn.cursor()

    # Optionally clear all the values in the fields first
    if set_null_first is True:
        [curs.execute('UPDATE Reaches SET {} = NULL'.format(field)) for field in fields]

    results = []
    for reachid, values in reaches.items():
        results.append([values[field] if field in values else None for field in fields])
        results[-1].append(reachid)

    sql = 'UPDATE Reaches SET {} WHERE ReachID = ?'.format(','.join(['{}=?'.format(field) for field in fields]))
    curs.executemany(sql, results)
    conn.commit()

    if summarize is True:
        [summarize_reaches(database, field) for field in fields]


def summarize_reaches(database, field):

    log = Logger('Database')
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT Max({0}), Min({0}), Avg({0}), Count({0}) FROM Reaches WHERE ({0} IS NOT NULL)'.format(field))
    row = curs.fetchone()
    if row and row[3] > 0:
        msg = '{}, max: {:.2f}, min: {:.2f}, avg: {:.2f}'.format(field, row[0], row[1], row[2])
    else:
        msg = "0 non null values"

    curs.execute('SELECT Count(*) FROM Reaches WHERE {0} IS NULL'.format(field))
    row = curs.fetchone()
    msg += ', nulls: {:,}'.format(row[0])

    log.info(msg)


def set_reach_fields_null(database, fields):

    log = Logger('Database')
    log.info('Setting {} reach fields to NULL'.format(len(fields)))
    conn = sqlite3.connect(database)
    conn.execute('UPDATE Reaches SET {}'.format(','.join(['{} = NULL'.format(field) for field in fields])))
    conn.commit()
    conn.close()


def execute_query(database, sql, message='Executing database SQL query'):

    log = Logger('Database')
    log.info(message)

    conn = sqlite3.connect(database)
    conn.execute('pragma foreign_keys=ON')
    curs = conn.cursor()
    curs.execute(sql)
    conn.commit()
    log.info('{:,} records affected.'.format(curs.rowcount))


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_metadata(database):
    log = Logger('Database')
    log.debug('Retrieving metadata')

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('SELECT KeyInfo, ValueInfo FROM MetaData')
    meta = {}
    for row in curs.fetchall():
        meta[row[0]] = row[1]
    return meta


def store_metadata(database, key, value):

    log = Logger('Database')
    log.info('Storing metadata {} = {}'.format(key, value))

    formatted_value = value
    if isinstance(value, list):
        formatted_value = ', '.join(value)

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('INSERT OR REPLACE INTO MetaData (KeyInfo, ValueInfo) VALUES (?, ?)', [key, formatted_value])
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('network', help='Input ShapeFile network', type=str)
    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('database', help='Path to the output SQLite database ', type=str)
    parser.add_argument('--epsg', help='EPSG for storing geometries in the database', default=4326, type=float)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('', '0.0.1')
    create_database(args.huc, args.database, {'MyMeta': 2000}, cfg.OUTPUT_EPSG)
    populate_database(args.database, args.network, args.huc)


if __name__ == '__main__':
    main()
