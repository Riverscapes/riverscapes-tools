"""Functions for cleaning the NHDPlusFlowlineVAA table in an NHDPlus geopackage.
"""

import os
import sqlite3

Path = str


def create_spatial_view(nhd_gpkg_path: str, network_layer: str, join_table: str, out_view: str, network_fields: dict, join_fields: dict, join_id, geom_type='LINESTRING') -> Path:
    """_summary_

    Args:
        nhd_gpkg_path (str): _description_
        network_layer (str): _description_
        join_table (str): _description_
        out_view (str): _description_
        network_fields (dict): _description_
        join_fields (dict): _description_
        join_id (_type_): _description_

    Returns:
        Path: _description_
    """

    with sqlite3.connect(nhd_gpkg_path) as conn:
        curs = conn.cursor()
        curs.execute(f"DROP VIEW IF EXISTS {out_view}")
        # Clean up GeoPackage metadata from any previous view with this name
        curs.execute(f"DELETE FROM gpkg_contents WHERE table_name = '{out_view}'")
        curs.execute(f"DELETE FROM gpkg_geometry_columns WHERE table_name = '{out_view}'")
        # create the view with specified of the fields from the flowline table and add the fields from the join table. the fields from the join table should have an ailas of the value in the dict.
        curs.execute(f"CREATE VIEW {out_view} AS SELECT {', '.join([f'{network_layer}.{field} AS {alias}' for field, alias in network_fields.items()])}, {', '.join(
            [f'{join_table}.{field} AS {alias}' for field, alias in join_fields.items()])} FROM {network_layer} LEFT JOIN {join_table} ON {network_layer}.{join_id} = {join_table}.{join_id}")

        # do an insert select from the network layer to the gpkg_contents table
        curs.execute(
            f"INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id) SELECT '{out_view}', 'features', '{out_view}', '{out_view}', datetime('now'), min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents WHERE table_name = '{network_layer}'")

        # add to gpkg geometry columns
        curs.execute(f"INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) VALUES ('{out_view}', 'geom', '{geom_type}', 4326, 0, 0)")

        # Create index
        curs.execute(f"CREATE INDEX IF NOT EXISTS ix_{join_table}_NHDPlusID ON {join_table}(NHDPlusID)")
        curs.execute(f"CREATE INDEX IF NOT EXISTS ix_{join_table}_fid ON {join_table}(fid)")

        conn.commit()

        curs.execute('VACUUM')

    return os.path.join(nhd_gpkg_path, out_view)


def clean_nhdplus_vaa_table(nhd_gpkg_path):
    """Removes rows from the NHDPlusFlowlineVAA table that do not have a corresponding row in the NHDFlowline table.
    """

    with sqlite3.connect(nhd_gpkg_path) as conn:
        curs = conn.cursor()
        join_data = curs.execute('SELECT NHDPlusFlowlineVAA.NHDPlusID FROM NHDPlusFlowlineVAA LEFT JOIN NHDFlowline ON NHDPlusFlowlineVAA.NHDPlusID = NHDFlowline.NHDPlusID WHERE NHDFlowline.NHDPlusID IS NULL').fetchall()
        del_ids = [i[0] for i in join_data]

        for id in del_ids:
            curs.execute('DELETE FROM NHDPlusFlowlineVAA WHERE NHDPlusFlowlineVAA.NHDPlusID = ?', [id])
        conn.commit()

        curs.execute('VACUUM')

    return
