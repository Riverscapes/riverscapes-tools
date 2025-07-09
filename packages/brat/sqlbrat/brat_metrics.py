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
        metrics['anthro'] = anthro_metrics['anthro']
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

        ex_capacity_length = {}
        hist_capacity_length = {}
        risk_length = {}
        limited_length = {}
        opportunity_length = {}

        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_EX == 0")
        ex_capacity_length['none'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_EX > 0 AND oCC_EX <= 1")
        ex_capacity_length['rare'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_EX > 1 AND oCC_EX <= 5")
        ex_capacity_length['occasional'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_EX > 5 AND oCC_EX <= 15")
        ex_capacity_length['frequent'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_EX > 15")
        ex_capacity_length['pervasive'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_HPE == 0")
        hist_capacity_length['none'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_HPE > 0 AND oCC_HPE <= 1")
        hist_capacity_length['rare'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_HPE > 1 AND oCC_HPE <= 5")
        hist_capacity_length['occasional'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_HPE > 5 AND oCC_HPE <= 15")
        hist_capacity_length['frequent'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE oCC_HPE > 15")
        hist_capacity_length['pervasive'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Risk = 'Negligible Risk'")
        risk_length['negligible'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Risk = 'Minor Risk'")
        risk_length['minor'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Risk = 'Some Risk'")
        risk_length['some'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Risk = 'Considerable Risk'")
        risk_length['considerable'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Limitation = 'Anthropogenically Limited'")
        limited_length['anthropogenic'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Limitation = 'Stream Power Limited'")
        limited_length['stream_power'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Limitation = 'Slope Limited'")
        limited_length['slope'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Limitation = 'Potential Reservoir or Land Use Change'")
        limited_length['reservoir_or_land_use'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Limitation = 'Naturally Vegetation Limited'")
        limited_length['naturally_vegetation'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Limitation = 'Stream Size Limited'")
        limited_length['stream_size'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Limitation = 'Dam Building Possible'")
        limited_length['dam_building_possible'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Opportunity = 'Conservation/Appropriate for Translocation'")
        opportunity_length['conservation'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Opportunity = 'Encourage Beaver Expansion/Colonization'")
        opportunity_length['beaver_expansion'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Opportunity = 'Beaver Mimicry'")
        opportunity_length['beaver_mimicry'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Opportunity = 'Land Management Chanage'")
        opportunity_length['land_management'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Opportunity = 'Conflict Management'")
        opportunity_length['conflict_management'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Opportunity = 'Potential Floodplain/Side Channel Opportunities'")
        opportunity_length['potential_floodplain'] = curs.fetchone()[0]
        curs.execute("SELECT SUM(iGeo_Len) FROM vwReaches WHERE Opportunity = 'Natural or Anthropogenic Limitations'")
        opportunity_length['natural_or_anthropogenic'] = curs.fetchone()[0]

        brat_metrics['ex_capacity_length'] = ex_capacity_length
        brat_metrics['hist_capacity_length'] = hist_capacity_length
        brat_metrics['risk_length'] = risk_length
        brat_metrics['limited_length'] = limited_length
        brat_metrics['opportunity_length'] = opportunity_length

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
