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
        log.warning(f'vbet_metrics.json not found in {vbet_proj_path}; creating new metrics file')
        metrics = {}
    else:
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
        curs.execute("""SELECT SUM(frac) FROM (SELECT (Road_len / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT Road_len, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_road_dens = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (Road_prim_len / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT Road_prim_len, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_prim_road_dens = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (Road_sec_len / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT Road_sec_len, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_sec_road_dens = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (Road_4wd_len / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT Road_4wd_len, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_4wd_road_dens = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (Rail_len / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT Rail_len, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_rail_dens = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (Canal_len / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT Canal_len, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_canal_dens = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (RoadX_ct / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT RoadX_ct, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_road_cross = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (DivPts_ct / centerline_length) * (segment_area / tot_area) frac FROM 
                     (SELECT DivPts_ct, centerline_length, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_div_pts = curs.fetchone()[0]

    metrics['aveLUI'] = str(av_lui)
    metrics['totVBRoadLength'] = str(vb_road_len)
    metrics['aveRoadDensity'] = str(av_road_dens)
    metrics['avePrimaryRoadDensity'] = str(av_prim_road_dens)
    metrics['aveSecondaryRoadDensity'] = str(av_sec_road_dens)
    metrics['ave4wdRoadDensity'] = str(av_4wd_road_dens)
    metrics['aveRailDensity'] = str(av_rail_dens)
    metrics['aveCanalDensity'] = str(av_canal_dens)
    metrics['aveRoadCrossingDensity'] = str(av_road_cross)
    metrics['aveDiversionDensity'] = str(av_div_pts)

    with open(os.path.join(anthro_proj_path, 'anthro_metrics.json'), 'w') as json_file:
        json.dump(metrics, json_file, indent=2)

    log.info('Anthro Metrics Calculated')


def main():
    parser = argparse.ArgumentParser(description='Calculate Anthro Metrics')
    parser.add_argument('anthro_proj_path', help='Path to Anthro project')
    parser.add_argument('vbet_proj_path', help='Path to VBET project')
    args = dotenv.parse_args_env(parser)

    try:
        vbet_metrics(args.anthro_proj_path, args.vbet_proj_path)
    except Exception as e:
        Logger('Anthro Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
