"""
Script to create a SQLite database from CHaMP stream temperature CSV files.
This was written to process the CSV data downloaded from streamnet.

Philip Bailey
22 Oct 2025
"""

import os
import re
import json
import sqlite3
import argparse
from datetime import datetime
from rsxml import Logger
from osgeo import gdal, ogr, osr

FILE_NAME_REGEX = r'^(.*)-([0-9]{2}[0-9]{2}[0-9]{4})-.*\.csv\Z'

skip_watersheds = ['Basinwide', 'Walla Walla']


def stream_temp_to_db(input_dir: str, output_db: str, site_locations: str) -> None:

    log = Logger('CHaMP Stream Temp to GeoPPackage')
    log.info(f'Starting to process stream temperature CSV files from {input_dir} into database {output_db}.')

    # Load site locations JSON file (This was produced by exporting SQL query from Google Postgres DB)
    with open(site_locations, 'r', encoding='utf-8') as f:
        site_raw_json = json.load(f)
    site_data = {site['site_name']: site for site in site_raw_json}

    # Create a new GeoPackage
    driver: gdal.Driver = gdal.GetDriverByName("GPKG")
    dataset: gdal.Dataset = driver.Create(output_db, 0, 0, 0, gdal.GDT_Unknown)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    layer: ogr.Layer = dataset.CreateLayer("sites", srs, ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("site_name", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("longitude", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("latitude", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("watershed_id", ogr.OFTInteger))

    # Insert all the sites into the GeoPackage
    for site_name, site in site_data.items():

        # Skip non-CHaMP watersheds
        if site['watershed_name'] in skip_watersheds:
            continue

        if site['latitude'] is None or site['longitude'] is None:
            log.warning(f"Site {site_name} is missing latitude or longitude. Skipping.")
            continue

        feature: ogr.Feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField("site_name", site_name)
        feature.SetField("watershed_id", site['watershed_id'])
        feature.SetField("longitude", site['longitude'])
        feature.SetField("latitude", site['latitude'])

        # Create a point geometry
        point = ogr.Geometry(ogr.wkbPoint)
        point.SetPoint(0, site['longitude'], site['latitude'])
        feature.SetGeometry(point)
        layer.CreateFeature(feature)
        site['fid'] = feature.GetFID()
        feature.Destroy()

    layer = None
    dataset = None

    # # Connect to SQLite database (or create it)
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()

    # Create table for stream temperature data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stream_temp (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER REFERENCES sites(fid),
            event_date datetime NOT NULL ,
            temperature REAL NOT NULL,
            notes TEXT,
            data_update_notes TEXT,
            severity_level_recommended TEXT,
            severity_level_actual TEXT
        )
    ''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS watersheds (
        watershed_id INTEGER PRIMARY KEY AUTOINCREMENT,
        watershed_name TEXT UNIQUE NOT NULL
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stream_temp_site_id ON stream_temp(site_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTTS idx_sites_watershed_id ON sites(watershed_id)')

    # Spatial Views
    create_spatial_view(cursor, 'vw_avg_stream_temp', '''
        SELECT s.fid, s.geom, s.site_name, avg(t.temperature) avg_temp
        FROM sites s inner join stream_temp t on s.fid = t.site_id
        GROUP BY s.fid, s.geom, s.site_name''')

    # Insert all the unique watersheds
    watersheds = set((site['watershed_id'], site['watershed_name']) for site in site_data.values())
    cursor.executemany('INSERT OR IGNORE INTO watersheds (watershed_id, watershed_name) VALUES (?, ?)', watersheds)

    # Iterate over CSV files in the input directory
    file_name_pattern = re.compile(FILE_NAME_REGEX, re.IGNORECASE)
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                file_name = os.path.basename(file_path)
                match = file_name_pattern.match(file_name)
                if match:
                    site_name = match.group(1)
                    file_date_str = match.group(2)
                    file_date = datetime.strptime(file_date_str, '%m%d%Y')
                else:
                    log.error(f"Filename {file_name} does not match expected pattern. Skipping.")
                    continue

                if site_name not in site_data:
                    log.error(f"Site {site_name} not found in site locations JSON. Skipping file {file_name}.")
                    continue

                if site_data[site_name]['watershed_name'] in skip_watersheds:
                    log.info(f"Skipping site {site_name} in watershed {site_data[site_name]['watershed_name']}.")
                    continue

                site_id = site_data[site_name]['fid']

                with open(file_path, 'r') as f:
                    # Skip header
                    next(f)
                    for line in f:
                        event_date, temperature, notes, data_update_notes, severity_level_recommended, severity_level_actual = line.strip().split(',')

                        event_date = datetime.fromisoformat(event_date)
                        severity_level_actual = severity_level_actual if severity_level_actual and len(severity_level_actual) > 0 else None
                        severity_level_recommended = severity_level_recommended if severity_level_recommended and len(severity_level_recommended) > 0 else None
                        notes = notes if notes and len(notes) > 0 else None
                        data_update_notes = data_update_notes if data_update_notes and len(data_update_notes) > 0 else None

                        cursor.execute('''
                            INSERT INTO stream_temp (event_date, site_id, temperature, notes, data_update_notes, severity_level_recommended, severity_level_actual)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (event_date, site_id, float(temperature), notes, data_update_notes, severity_level_recommended, severity_level_actual))

    # Commit changes and close connection
    conn.commit()
    conn.close()


def create_spatial_view(curs: sqlite3.Cursor, view_name: str, select_sql: str) -> None:
    """
    Create a spatial view in the GeoPackage
    :param curs:
    :param view_name:
    :param select_sql:
    :return:
    """
    curs.execute(f"DROP VIEW IF EXISTS {view_name}")
    curs.execute(f"CREATE VIEW {view_name} AS {select_sql}")

    curs.execute(f"""
        INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id)
        SELECT '{view_name}', data_type, '{view_name}', min_x, min_y, max_x, max_y, srs_id
        FROM gpkg_contents
        WHERE table_name = 'sites'
    """)

    curs.execute(f"""
        INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
        SELECT '{view_name}', column_name, geometry_type_name, srs_id, z, m
        FROM gpkg_geometry_columns where table_name = 'sites'
    """)


def main():
    parser = argparse.ArgumentParser(description="Stream Temp")
    parser.add_argument("input_dir", type=str, help="Top level folder containing stream temp CSV files")
    parser.add_argument("output_db", type=str, help="Output SQLite database file")
    parser.add_argument('site_locations', type=str, help='JSON file containing CHaMP site locations')
    args = parser.parse_args()

    log = Logger('CHaMP Stream Temp')
    log.setup(logPath=os.path.join(os.path.dirname(args.output_db), "champ_stream_temp.log"), verbose=True)

    stream_temp_to_db(args.input_dir, args.output_db, args.site_locations)


if __name__ == "__main__":
    main()
