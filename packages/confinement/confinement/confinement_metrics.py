import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv, RSProject, RSLayer


def confinement_metrics(confinement_proj_path, vbet_proj_path):
    """Append Confinement metrics to the VBET metrics"""

    log = Logger('Confinement Context Metrics')
    log.info('Calculating Confinement Metrics')

    confinement_metrics = {}

    try:
        with open(os.path.join(vbet_proj_path, 'vbet_metrics.json')) as f:
            metrics = json.load(f)
    except FileNotFoundError as e:
        log.warning(f'vbet_metrics.json not found in {vbet_proj_path}; creating new metrics file. {e}')
        metrics = {}

    with sqlite3.connect(os.path.join(confinement_proj_path, 'outputs', 'confinement.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("""SELECT SUM(frac) FROM (SELECT confinement_ratio * (segment_area / tot_area) frac FROM (SELECT confinement_ratio, segment_area
                     FROM confinement_dgos WHERE seg_distance IS NOT NULL), (SELECT SUM(segment_area) AS tot_area FROM confinement_dgos WHERE seg_distance IS NOT NULL))""")
        confinement_metrics['avgConfinementRatio'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT constriction_ratio * (segment_area / tot_area) frac FROM (SELECT constriction_ratio, segment_area
                     FROM confinement_dgos WHERE seg_distance IS NOT NULL), (SELECT SUM(segment_area) AS tot_area FROM confinement_dgos WHERE seg_distance IS NOT NULL))""")
        confinement_metrics['avgConstrictionRatio'] = curs.fetchone()[0]

    metrics['confinement'] = confinement_metrics

    with open(os.path.join(confinement_proj_path, 'confinement_metrics.json'), 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)

    proj = RSProject(None, os.path.join(confinement_proj_path, 'project.rs.xml'))
    realization_node = proj.XMLBuilder.find('Realizations').find('Realization')
    datasets_node = proj.XMLBuilder.add_sub_element(realization_node, 'Datasets')
    proj.add_dataset(datasets_node, os.path.join(confinement_proj_path, 'confinement_metrics.json'),
                     RSLayer('Metrics', 'Metrics', 'File', 'confinement_metrics.json'), 'File')
    proj.XMLBuilder.write()

    log.info('Confinement Metrics calculated successfully')


def main():

    parser = argparse.ArgumentParser(description='Confinement Metrics')
    parser.add_argument('confinement_proj_path', help='Path to the Confinement project')
    parser.add_argument('vbet_proj_path', help='Path to the VBET project')
    args = dotenv.parse_args_env(parser)
    try:
        confinement_metrics(args.confinement_proj_path, args.vbet_proj_path)
    except Exception as e:
        Logger('Confinement Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
