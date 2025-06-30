import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv, RSProject, RSLayer


def anthro_metrics(anthro_proj_path, vbet_proj_path):

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
        curs.execute("SELECT SUM(Rail_len) FROM DGOAttributes")
        vb_rail_len = curs.fetchone()[0] / 1000
        curs.execute("SELECT SUM(Canal_len) FROM DGOAttributes")
        vb_canal_len = curs.fetchone()[0] / 1000
        curs.execute("SELECT SUM(RoadX_ct) FROM DGOAttributes")
        vb_road_cross = curs.fetchone()[0]
        curs.execute("SELECT SUM(DivPts_ct) FROM DGOAttributes")
        vb_div_pts = curs.fetchone()[0]
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

    metrics['aveLUI'] = av_lui
    metrics['totVBRoadLength'] = vb_road_len
    metrics['totVBRailLength'] = vb_rail_len
    metrics['totVBCanalLength'] = vb_canal_len
    metrics['totRoadCrossings'] = vb_road_cross
    metrics['totDiversionPoints'] = vb_div_pts
    metrics['aveRoadDensity'] = av_road_dens
    metrics['avePrimaryRoadDensity'] = av_prim_road_dens
    metrics['aveSecondaryRoadDensity'] = av_sec_road_dens
    metrics['ave4wdRoadDensity'] = av_4wd_road_dens
    metrics['aveRailDensity'] = av_rail_dens
    metrics['aveCanalDensity'] = av_canal_dens
    metrics['aveRoadCrossingDensity'] = av_road_cross
    metrics['aveDiversionDensity'] = av_div_pts

    with open(os.path.join(anthro_proj_path, 'anthro_metrics.json'), 'w') as json_file:
        json.dump(metrics, json_file, indent=2)

    proj = RSProject(None, os.path.join(anthro_proj_path, 'project.rs.xml'))
    realization_node = proj.XMLBuilder.find('Realizations').find('Realization')
    datasets_node = proj.XMLBuilder.add_sub_element(realization_node, 'Datasets')
    proj.add_dataset(datasets_node, os.path.join(anthro_proj_path, 'anthro_metrics.json'), RSLayer('Metrics', 'Metrics', 'File', 'anthro_metrics.json'), 'File')
    proj.XMLBuilder.write()

    log.info('Anthro Metrics Calculated')


def main():
    parser = argparse.ArgumentParser(description='Calculate Anthro Metrics')
    parser.add_argument('anthro_proj_path', help='Path to Anthro project')
    parser.add_argument('vbet_proj_path', help='Path to VBET project')
    args = dotenv.parse_args_env(parser)

    try:
        anthro_metrics(args.anthro_proj_path, args.vbet_proj_path)
    except Exception as e:
        Logger('Anthro Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
