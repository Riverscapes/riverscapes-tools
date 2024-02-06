import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv


def vbet_metrics(anthro_proj_path, vbet_proj_path):

    log = Logger('Anthro Metrics')
    log.info('Calculating Anthro Metrics')

    if not os.path.exists(os.path.join(vbet_proj_path, 'vbet_metrics.json')):
        raise FileNotFoundError(f'vbet_metrics.json not found in {vbet_proj_path}')

    with open(os.path.join(vbet_proj_path, 'vbet_metrics.json')) as json_file:
        metrics = json.load(json_file)

    with sqlite3.connect(os.path.join(anthro_proj_path, 'outputs', 'anthro.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("""SELECT SUM(frac) FROM (SELECT LUI * (segment_area / tot_area) frac FROM 
                     (SELECT LUI, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_lui = curs.fetchone()[0]
        curs.execute("SELECT SUM(Road_len) FROM DGOAttributes")
        vb_road_len = curs.fetchone()[0] / 1000
        curs.execute("""SELECT SUM(frac) FROM (SELECT Road_len * (segment_area / tot_area) frac FROM 
                     (SELECT LUI, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
