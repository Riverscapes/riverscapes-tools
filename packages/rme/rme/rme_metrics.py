import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv, RSProject, RSLayer


def rme_metrics(rme_proj_path, brat_proj_path, rcat_proj_path, confinement_proj_path):
    """Append RME metrics to the metrics"""

    log = Logger('RME Context Metrics')
    log.info('Calculating RME Metrics')

    rme_metrics = {}

    brat_metrics = None
    rcat_metrics = None
    confinement_metrics = None

    try:
        with open(os.path.join(brat_proj_path, 'brat_metrics.json')) as f:
            brat_metrics = json.load(f)
    except FileNotFoundError as e:
        log.warning(f'brat_metrics.json not found in {brat_proj_path}; {e}')

    try:
        with open(os.path.join(rcat_proj_path, 'rcat_metrics.json')) as f:
            rcat_metrics = json.load(f)
    except FileNotFoundError as e:
        log.warning(f'rcat_metrics.json not found in {rcat_proj_path}; {e}')

    try:
        with open(os.path.join(confinement_proj_path, 'confinement_metrics.json')) as f:
            confinement_metrics = json.load(f)
    except FileNotFoundError as e:
        log.warning(f'confinement_metrics.json not found in {confinement_proj_path}; {e}')

    if confinement_metrics:
        try:
            rme_metrics['rs_context'] = confinement_metrics['rs_context']
        except KeyError as e:
            log.warning(f'Key "rs_context" not found in confinement_metrics.json; {e}')
            rme_metrics['rs_context'] = confinement_metrics.get('rs_context', {})
        try:
            rme_metrics['vbet'] = confinement_metrics['vbet']
        except KeyError as e:
            log.warning(f'Key "vbet" not found in confinement_metrics.json; {e}')
            rme_metrics['vbet'] = confinement_metrics.get('vbet', {})
        try:
            rme_metrics['confinement'] = confinement_metrics['confinement']
        except KeyError as e:
            log.warning(f'Key "confinement" not found in confinement_metrics.json; {e}')
            rme_metrics['confinement'] = confinement_metrics.get('confinement', {})
    if brat_metrics:
        try:
            rme_metrics['hydro_context'] = brat_metrics['hydro_context']
        except KeyError as e:
            log.warning(f'Key "hydro_context" not found in brat_metrics.json; {e}')
            rme_metrics['hydro_context'] = brat_metrics.get('hydro_context', {})
        try:
            rme_metrics['anthro'] = brat_metrics['anthro']
        except KeyError as e:
            log.warning(f'Key "anthro" not found in brat_metrics.json; {e}')
            rme_metrics['anthro'] = brat_metrics.get('anthro', {})
        try:
            rme_metrics['brat'] = brat_metrics['brat']
        except KeyError as e:
            log.warning(f'Key "brat" not found in brat_metrics.json; {e}')
            rme_metrics['brat'] = brat_metrics.get('brat', {})
    if rcat_metrics:
        try:
            rme_metrics['rcat'] = rcat_metrics['rcat']
        except KeyError as e:
            log.warning(f'Key "rcat" not found in rcat_metrics.json; {e}')
            rme_metrics['rcat'] = rcat_metrics.get('rcat', {})

    rme = {}

    with sqlite3.connect(os.path.join(rme_proj_path, 'outputs', 'riverscapes_metrics.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("""SELECT SUM(frac) FROM (SELECT valleybottom_gradient * (segment_area / tot_area) frac FROM
                     (SELECT valleybottom_gradient, segment_area FROM vw_dgo_metrics WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vw_dgo_metrics WHERE seg_distance is not NULL))""")
        rme['avgValleyBottomGradient'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT rel_flow_length * (segment_area / tot_area) frac FROM
                     (SELECT rel_flow_length, segment_area FROM vw_dgo_metrics WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vw_dgo_metrics WHERE seg_distance is not NULL))""")
        rme['avgRelFlowLength'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(frac) FROM (SELECT planform_sinuosity * (segment_area / tot_area) frac FROM
                     (SELECT planform_sinuosity, segment_area FROM vw_dgo_metrics WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vw_dgo_metrics WHERE seg_distance is not NULL))""")
        rme['avgPlanformSinuosity'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(confluences) FROM vw_dgo_metrics WHERE seg_distance is not NULL""")
        rme['totConfluences'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(diffluences) FROM vw_dgo_metrics WHERE seg_distance is not NULL""")
        rme['totDiffluences'] = curs.fetchone()[0]
        curs.execute("""SELECT SUM(tributaries) FROM vw_dgo_metrics WHERE seg_distance is not NULL""")
        rme['totTributaries'] = curs.fetchone()[0]

        rme_metrics['rme'] = rme

    with open(os.path.join(rme_proj_path, 'rme_metrics.json'), 'w', encoding='utf8') as f:
        json.dump(rme_metrics, f, indent=2)

    proj = RSProject(None, os.path.join(rme_proj_path, 'project.rs.xml'))
    realization_node = proj.XMLBuilder.find('Realizations').find('Realization')
    datasets_node = proj.XMLBuilder.add_sub_element(realization_node, 'Datasets')
    proj.add_dataset(datasets_node, os.path.join(rme_proj_path, 'rme_metrics.json'),
                     RSLayer('Metrics', 'Metrics', 'File', 'rme_metrics.json'), 'File')
    proj.XMLBuilder.write()

    log.info('RME Metrics calculated successfully')


def main():

    parser = argparse.ArgumentParser(description='RME Metrics')
    parser.add_argument('rme_proj_path', help='Path to the RME project')
    parser.add_argument('brat_proj_path', help='Path to the BRAT project')
    parser.add_argument('rcat_proj_path', help='Path to the RCAT project')
    parser.add_argument('confinement_proj_path', help='Path to the Confinement project')

    args = dotenv.parse_args_env(parser)

    try:
        rme_metrics(args.rme_proj_path, args.brat_proj_path, args.rcat_proj_path, args.confinement_proj_path)
    except Exception as e:
        Logger('RME Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
