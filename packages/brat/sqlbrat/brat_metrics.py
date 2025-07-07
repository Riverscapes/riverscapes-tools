import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv, RSProject, RSLayer


def brat_metrics(brat_proj_path, hydro_proj_path, anthro_proj_path):
    """Append BRAT metrics to the metrics"""

    log = Logger('BRAT Context Metrics')
    log.info('Calculating BRAT Metrics')

    brat_metrics = {}

    hydro_metrics = None
    anthro_metrics = None

    try:
        with open(os.path.join(hydro_proj_path, 'hydro_metrics.json')) as f:
            hydro_metrics = json.load(f)
    except FileNotFoundError as e:
        log.warning(f'hydro_metrics.json not found in {hydro_proj_path}; {e}')

    try:
        with open(os.path.join(anthro_proj_path, 'anthro_metrics.json')) as f:
            anthro_metrics = json.load(f)
    except FileNotFoundError as e:
        log.warning(f'anthro_metrics.json not found in {anthro_proj_path}; {e}')

    if anthro_metrics and hydro_metrics:
        metrics = hydro_metrics.copy()
        metrics['anthro'] = anthro_metrics
    elif hydro_metrics and not anthro_metrics:
        metrics = hydro_metrics.copy()
    elif not hydro_metrics and anthro_metrics:
        metrics = anthro_metrics.copy()
    else:
        metrics = {}

    with sqlite3.connect(os.path.join(brat_proj_path, 'outputs', 'brat.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("SELECT SUM(mCC_EX_CT) FROM vwDgos WHERE mCC_EX_CT IS NOT NULL")
        brat_metrics['totalExistingDamCapacity'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(mCC_HPE_CT) FROM vwDgos WHERE mCC_HPE_CT IS NOT NULL")
        brat_metrics['totalHistoricDamCapacity'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT oCC_EX * (segment_area / tot_area) frac FROM (SELECT oCC_EX, segment_area
                     FROM vwDgos WHERE seg_distance IS NOT NULL), (SELECT SUM(segment_area) AS tot_area FROM vwDgos WHERE seg_distance IS NOT NULL))""")
        brat_metrics['avgExistingDamCapacity'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT oCC_HPE * (segment_area / tot_area) frac FROM (SELECT oCC_HPE, segment_area
                     FROM vwDgos WHERE seg_distance IS NOT NULL), (SELECT SUM(segment_area) AS tot_area FROM vwDgos WHERE seg_distance IS NOT NULL))""")
        brat_metrics['avgHistoricDamCapacity'] = curs.fetchone()[0]

    metrics['brat'] = brat_metrics

    with open(os.path.join(brat_proj_path, 'brat_metrics.json'), 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)

    proj = RSProject(None, os.path.join(brat_proj_path, 'project.rs.xml'))
    realization_node = proj.XMLBuilder.find('Realizations').find('Realization')
    datasets_node = proj.XMLBuilder.add_sub_element(realization_node, 'Datasets')
    proj.add_dataset(datasets_node, os.path.join(brat_proj_path, 'brat_metrics.json'),
                     RSLayer('Metrics', 'Metrics', 'File', 'brat_metrics.json'), 'File')
    proj.XMLBuilder.write()

    log.info('BRAT Metrics calculated successfully')


def main():

    parser = argparse.ArgumentParser(description='BRAT Metrics')
    parser.add_argument('brat_proj_path', help='Path to the BRAT project')
    parser.add_argument('hydro_proj_path', help='Path to the Hydro project')
    parser.add_argument('anthro_proj_path', help='Path to the Anthro project')
    args = dotenv.parse_args_env(parser)

    try:
        brat_metrics(args.brat_proj_path, args.hydro_proj_path, args.anthro_proj_path)
    except Exception as e:
        Logger('BRAT Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
