import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv, RSProject, RSLayer


def vbet_metrics(vbet_proj_path, rsc_proj_path):
    """Append VBET metrics to the Riverscapes Context metrics"""

    log = Logger('VBET Metrics')
    log.info('Calculating VBET Metrics')

    vbet_metrics = {}

    if not os.path.exists(os.path.join(rsc_proj_path, 'rscontext_metrics.json')):
        raise FileNotFoundError(f'rscontext_metrics.json not found in {rsc_proj_path}')

    with open(os.path.join(rsc_proj_path, 'rscontext_metrics.json'), encoding='utf8') as json_file:
        metrics = json.load(json_file)

    with sqlite3.connect(os.path.join(vbet_proj_path, 'intermediates', 'vbet_intermediates.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("SELECT SUM(centerline_length) FROM vbet_dgos")
        riverscape_length = curs.fetchone()[0] / 1000
        curs.execute("SELECT SUM(segment_area) FROM vbet_dgos")
        riverscape_area = curs.fetchone()[0] / 1000000
        curs.execute("""SELECT SUM(frac) FROM (SELECT ((segment_area / 10000) / (centerline_length / 1000)) * (segment_area / tot_area) frac FROM 
                     (SELECT segment_area, centerline_length FROM vbet_dgos WHERE seg_distance is not NULL), (SELECT SUM(segment_area) AS tot_area FROM vbet_dgos WHERE seg_distance is not NULL))""")
        avg_ha_per_km = curs.fetchone()[0]
        curs.execute("""SELECT SUM(width_frac) FROM (SELECT (active_channel_area / centerline_length) * (segment_area / tot_area) width_frac FROM 
                     (SELECT active_channel_area, segment_area, centerline_length FROM vbet_dgos WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vbet_dgos WHERE seg_distance is not NULL))""")
        channel_width = curs.fetchone()[0]
        curs.execute("""SELECT SUM(width_frac) FROM (SELECT (low_lying_floodplain_area / centerline_length) * (segment_area / tot_area) width_frac FROM 
                     (SELECT low_lying_floodplain_area, segment_area, centerline_length FROM vbet_dgos WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vbet_dgos WHERE seg_distance is not NULL))""")
        lowlying_width = curs.fetchone()[0]
        curs.execute("""SELECT SUM(width_frac) FROM (SELECT (elevated_floodplain_area / centerline_length) * (segment_area / tot_area) width_frac FROM 
                     (SELECT elevated_floodplain_area, segment_area, centerline_length FROM vbet_dgos WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vbet_dgos WHERE seg_distance is not NULL))""")
        elevated_width = curs.fetchone()[0]
        curs.execute("""SELECT SUM(prop_frac) FROM (SELECT low_lying_floodplain_prop * (segment_area / tot_area) prop_frac FROM 
                     (SELECT low_lying_floodplain_prop, segment_area, centerline_length FROM vbet_dgos WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vbet_dgos WHERE seg_distance is not NULL))""")
        lowlying_prop = curs.fetchone()[0]
        curs.execute("""SELECT SUM(prop_frac) FROM (SELECT elevated_floodplain_prop * (segment_area / tot_area) prop_frac FROM 
                     (SELECT elevated_floodplain_prop, segment_area, centerline_length FROM vbet_dgos WHERE seg_distance is not NULL),
                     (SELECT SUM(segment_area) AS tot_area FROM vbet_dgos WHERE seg_distance is not NULL))""")
        elevated_prop = curs.fetchone()[0]

        curs.execute('SELECT Min(segment_area) , Max(segment_area), avg(segment_area), count(*) FROM vbet_dgos WHERE segment_area IS NOT NULL;')
        min_area, max_area, avg_area, count = curs.fetchone()
        vbet_metrics['vbetDgoCount'] = count
        vbet_metrics['vbetDgoMinArea'] = min_area
        vbet_metrics['vbetDgoMaxArea'] = max_area
        vbet_metrics['vbetDgoAvgArea'] = avg_area

        curs.execute('SELECT COUNT(*) FROM (SELECT DISTINCT level_path FROM vbet_dgos)')
        vbet_metrics['vbetLevelPathCount'] = curs.fetchone()[0]

    with sqlite3.connect(os.path.join(vbet_proj_path, 'outputs', 'vbet.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute('SELECT sum(centerline_length), min(centerline_length), max(centerline_length), avg(centerline_length), count(*) FROM vbet_igos')
        total_length, min_length, max_length, avg_length, count = curs.fetchone()
        vbet_metrics['vbetIgoCount'] = count
        vbet_metrics['vbetIgoTotalCenterlineLength'] = total_length
        vbet_metrics['vbetIgoMinCenterlineLength'] = min_length
        vbet_metrics['vbetIgoMaxCenterlineLength'] = max_length
        vbet_metrics['vbetIgoAvgCenterlineLength'] = avg_length

        curs = conn.cursor()
        curs.execute("""SELECT SUM(width_frac) FROM (SELECT integrated_width * (window_area / tot_area) width_frac FROM (SELECT window_area, integrated_width FROM vbet_igos),
                     (SELECT SUM(window_area) AS tot_area FROM vbet_igos))""")
        integrated_width = curs.fetchone()[0]

    riverscape_network_density = riverscape_length / float(metrics['rs_context']['catchmentArea'])
    tot_hec_per_km = riverscape_area * 100 / riverscape_length
    prop_riverscape = riverscape_area / float(metrics['rs_context']['catchmentArea'])

    vbet_metrics['riverscapeLength'] = riverscape_length
    vbet_metrics['riverscapeArea'] = riverscape_area
    vbet_metrics['riverscapeNetworkDensity'] = riverscape_network_density
    vbet_metrics['avgValleyBottomWidth'] = integrated_width
    vbet_metrics['avgChannelWidth'] = channel_width
    vbet_metrics['lowlyingWidth'] = lowlying_width
    vbet_metrics['elevatedWidth'] = elevated_width
    vbet_metrics['lowlyingProp'] = lowlying_prop
    vbet_metrics['elevatedProp'] = elevated_prop
    vbet_metrics['totalHectaresPerKm'] = tot_hec_per_km
    vbet_metrics['averageHectaresPerKm'] = avg_ha_per_km
    vbet_metrics['proportionRiverscape'] = prop_riverscape

    metrics['vbet'] = vbet_metrics

    with open(os.path.join(vbet_proj_path, 'vbet_metrics.json'), 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)

    proj = RSProject(None, os.path.join(vbet_proj_path, 'project.rs.xml'))
    realization_node = proj.XMLBuilder.find('Realizations').find('Realization')
    datasets_node = proj.XMLBuilder.add_sub_element(realization_node, 'Datasets')
    proj.add_dataset(datasets_node, os.path.join(vbet_proj_path, 'vbet_metrics.json'), RSLayer('Metrics', 'Metrics', 'File', 'vbet_metrics.json'), 'File')
    proj.XMLBuilder.write()

    log.info('VBET Metrics calculated successfully')


def main():

    parser = argparse.ArgumentParser(description='VBET Metrics')
    parser.add_argument('vbet_proj_path', help='Path to the VBET project')
    parser.add_argument('rsc_proj_path', help='Path to the RSContext project')
    args = dotenv.parse_args_env(parser)
    try:
        vbet_metrics(args.vbet_proj_path, args.rsc_proj_path)
    except Exception as e:
        Logger('VBET Metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
