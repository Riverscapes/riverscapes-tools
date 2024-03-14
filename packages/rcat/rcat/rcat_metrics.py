import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv, RSProject, RSLayer


def rcat_metrics(rcat_proj_path, anthro_proj_path):

    log = Logger('RCAT Metrics')
    log.info('Calculating RCAT Metrics')

    if not os.path.exists(os.path.join(anthro_proj_path, 'anthro_metrics.json')):
        log.warning(f'anthro_metrics.json not found in {anthro_proj_path}; creating new metrics file')
        metrics = {}
    else:
        with open(os.path.join(anthro_proj_path, 'anthro_metrics.json')) as json_file:
            metrics = json.load(json_file)

    with sqlite3.connect(os.path.join(rcat_proj_path, 'outputs', 'rcat.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("""SELECT SUM(FloodplainAccess * segment_area) / 1000000 FROM DGOAttributes WHERE seg_distance is not NULL""")
        accessible_floodplain_km2 = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT FloodplainAccess * (segment_area / tot_area) frac FROM 
                     (SELECT FloodplainAccess, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_floodplain_access = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT ExistingRiparianMean * (segment_area / tot_area) frac FROM 
                     (SELECT ExistingRiparianMean, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_riparian_mean = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT (1 - MIN(RiparianDeparture, 1)) * (segment_area / tot_area) frac FROM 
                     (SELECT RiparianDeparture, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_riparian_departure = curs.fetchone()[0]
        curs.execute("""SELECT SUM(Agriculture * segment_area) / 1000000 FROM DGOAttributes WHERE seg_distance is not NULL""")
        ag_km2 = curs.fetchone()[0]
        curs.execute("""SELECT SUM(Development * segment_area) / 1000000 FROM DGOAttributes WHERE seg_distance is not NULL""")
        dev_km2 = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT Condition * (segment_area / tot_area) frac FROM 
                     (SELECT Condition, segment_area FROM DGOAttributes WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM DGOAttributes WHERE seg_distance is not NULL))""")
        av_condition = curs.fetchone()[0]

    if os.path.exists(os.path.join(anthro_proj_path, 'anthro_metrics.json')):
        metrics['inaccessibleFloodplain'] = str(float(metrics['riverscapeArea']) - accessible_floodplain_km2)
        metrics['propInaccessibleFloodplain'] = str(float(metrics['inaccessibleFloodplain']) / float(metrics['riverscapeArea']))
        metrics['propAgriculture'] = str(ag_km2 / float(metrics['riverscapeArea']))
        metrics['propDeveloped'] = str(dev_km2 / float(metrics['riverscapeArea']))

    metrics['avePropAccessibleFloodplain'] = str(av_floodplain_access)
    metrics['avePropRiparian'] = str(av_riparian_mean)
    metrics['aveRiparianDeparture'] = str(av_riparian_departure)
    metrics['aveRiparianCondition'] = str(av_condition)

    with open(os.path.join(rcat_proj_path, 'rcat_metrics.json'), 'w') as json_out:
        json.dump(metrics, json_out, indent=2)

    proj = RSProject(None, os.path.join(rcat_proj_path, 'project.rs.xml'))
    realization_node = proj.XMLBuilder.find('Realizations').find('Realization')
    datasets_node = proj.XMLBuilder.add_sub_element(realization_node, 'Datasets')
    proj.add_dataset(datasets_node, os.path.join(rcat_proj_path, 'rcat_metrics.json'), RSLayer('Metrics', 'Metrics', 'File', 'rcat_metrics.json'), 'File')
    proj.XMLBuilder.write()

    log.info('RCAT Metrics calculated successfully')


def main():
    parser = argparse.ArgumentParser(description='Calculate RCAT Metrics')
    parser.add_argument('rcat_proj_path', type=str, help='Path to RCAT project')
    parser.add_argument('anthro_proj_path', type=str, help='Path to Anthro project')
    args = dotenv.parse_args_env(parser)

    try:
        rcat_metrics(args.rcat_proj_path, args.anthro_proj_path)
    except Exception as e:
        Logger('RCAT Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
