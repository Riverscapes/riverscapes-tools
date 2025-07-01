import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv, RSProject, RSLayer


def hydro_metrics(hydro_proj_path, vbet_proj_path):
    """Append Hydro metrics to the VBET metrics"""

    log = Logger('Hydrologic Context Metrics')
    log.info('Calculating Hydro Metrics')

    hydro_metrics = {}

    try:
        with open(os.path.join(vbet_proj_path, 'vbet_metrics.json')) as f:
            metrics = json.load(f)
    except FileNotFoundError as e:
        log.warning(f'vbet_metrics.json not found in {vbet_proj_path}; creating new metrics file. {e}')
        metrics = {}

    except Exception as e:
        log.error(f'Error reading vbet_metrics.json: {e}')
        raise

    with sqlite3.connect(os.path.join(hydro_proj_path, 'outputs', 'hydro.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("SELECT MAX(drainage_area) FROM vwDgos WHERE drainage_area IS NOT NULL")
        hydro_metrics['DrainageArea'] = curs.fetchone()[0]
        curs.execute("SELECT MAX(QLow) FROM vwDgos WHERE QLow IS NOT NULL")
        hydro_metrics['QLow'] = curs.fetchone()[0]
        curs.execute("SELECT MAX(Q2) FROM vwDgos WHERE Q2 IS NOT NULL")
        hydro_metrics['Q2'] = curs.fetchone()[0]
        curs.execute("SELECT MAX(SPLow) FROM vwDgos WHERE SPLow IS NOT NULL")
        hydro_metrics['StreamPowerLow'] = curs.fetchone()[0]
        curs.execute("SELECT MAX(SP2) FROM vwDgos WHERE SP2 IS NOT NULL")
        hydro_metrics['StreamPower2'] = curs.fetchone()[0]
        curs.execute("SELECT MIN(Slope) FROM vwDgos WHERE Slope IS NOT NULL")
        hydro_metrics['ReachSlopeMin'] = curs.fetchone()[0]
        curs.execute("SELECT MAX(Slope) FROM vwDgos WHERE Slope IS NOT NULL")
        hydro_metrics['ReachSlopeMax'] = curs.fetchone()[0]
        curs.execute("SELECT AVG(Slope) FROM vwDgos WHERE Slope IS NOT NULL")
        hydro_metrics['ReachSlopeAvg'] = curs.fetchone()[0]

    metrics['hydro_context'] = hydro_metrics

    with open(os.path.join(hydro_proj_path, 'hydro_metrics.json'), 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)

    proj = RSProject(None, os.path.join(hydro_proj_path, 'project.rs.xml'))
    realization_node = proj.XMLBuilder.find('Realizations').find('Realization')
    datasets_node = proj.XMLBuilder.add_sub_element(realization_node, 'Datasets')
    proj.add_dataset(datasets_node, os.path.join(hydro_proj_path, 'hydro_metrics.json'), RSLayer('Metrics', 'Metrics', 'File', 'hydro_metrics.json'), 'File')
    proj.XMLBuilder.write()

    log.info('Hydro Metrics calculated successfully')


def main():

    parser = argparse.ArgumentParser(description='Hydro Metrics')
    parser.add_argument('hydro_proj_path', help='Path to the Hydro project')
    parser.add_argument('vbet_proj_path', help='Path to the VBET project')
    args = dotenv.parse_args_env(parser)
    try:
        hydro_metrics(args.hydro_proj_path, args.vbet_proj_path)
    except Exception as e:
        Logger('Hydro Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
