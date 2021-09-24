import os
import sqlite3

import ogr

from rscommons.classes.vector_classes import GeopackageLayer


def build_vbet_metric_tables(database):

    database_folder = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'database')
    with sqlite3.connect(database) as conn:
        cursor = conn.cursor()
        with open(os.path.join(database_folder, 'vbet_metrics.sql')) as sqlfile:
            sql_commands = sqlfile.read()
            cursor.executescript(sql_commands)
            conn.commit()


def vbet_area_metrics(layer, db_path, summary_field):
    rows = {}
    with GeopackageLayer(layer) as vbet_lyr, \
            sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        for feat, *_ in vbet_lyr.iterate_features(f"Reading metrics for {layer}"):
            level_path = feat.GetField(summary_field)
            sql = f"""SELECT * FROM vbet_full_metrics WHERE {summary_field} = (?)"""
            cursor.execute(sql, (level_path,))
            values = cursor.fetchone()
            rows[level_path] = {key: value for key, value in zip(values.keys(), values)}

    with GeopackageLayer(layer, write=True) as vbet_lyr:
        fields = {'valley_bottom_ha': ogr.OFTReal,
                  'active_valley_bottom_ha': ogr.OFTReal,
                  'active_channel_ha': ogr.OFTReal,
                  'active_floodplain_ha': ogr.OFTReal,
                  'inactive_floodplain_ha': ogr.OFTReal,
                  'active_valley_bottom_pc': ogr.OFTReal,
                  'active_channel_pc': ogr.OFTReal,
                  'active_floodplain_pc': ogr.OFTReal,
                  'inactive_floodplain_pc': ogr.OFTReal,
                  'cnt_active_fp_units': ogr.OFTInteger,
                  'cnt_inactive_fp_units': ogr.OFTInteger}
        vbet_lyr.create_fields(fields)
        vbet_lyr.ogr_layer.StartTransaction()
        for feat, *_ in vbet_lyr.iterate_features(f"Writing metrics for {layer}"):
            level_path = feat.GetField(summary_field)
            values = rows[level_path]
            for key in fields:
                feat.SetField(key, values[key])
            vbet_lyr.ogr_layer.SetFeature(feat)
        vbet_lyr.ogr_layer.CommitTransaction()
    return


def floodplain_metrics(layer_name, db_path):

    rows = {}
    with GeopackageLayer(os.path.join(db_path, layer_name)) as vbet_lyr, \
            sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        for feat, *_ in vbet_lyr.iterate_features(f"Reading metrics for {os.path.join(db_path, layer_name)}"):
            fid = feat.GetFID()
            sql = f"""SELECT * FROM {layer_name}_metrics WHERE fid = (?)"""
            cursor.execute(sql, (fid,))
            values = cursor.fetchone()
            rows[fid] = {key: value for key, value in zip(values.keys(), values)}

    with GeopackageLayer(os.path.join(db_path, layer_name), write=True) as vbet_lyr:
        fields = {'prop_valley_bottom_pc': ogr.OFTReal,
                  f'prop_{layer_name.replace("_floodplain","").replace("_area","").replace("vbet_","")}_pc': ogr.OFTReal}
        vbet_lyr.create_fields(fields)
        vbet_lyr.ogr_layer.StartTransaction()
        for feat, *_ in vbet_lyr.iterate_features(f"Writing metrics for {os.path.join(db_path, layer_name)}"):
            fid = feat.GetFID()
            values = rows[fid]
            for key in fields:
                feat.SetField(key, values[key])
            vbet_lyr.ogr_layer.SetFeature(feat)
        vbet_lyr.ogr_layer.CommitTransaction()
