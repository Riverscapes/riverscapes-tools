import argparse
import os
import traceback
import sys
import json
import sqlite3

from rscommons import Logger, dotenv


def vbet_metrics(vbet_proj_path, rsc_proj_path):

    log = Logger('VBET Metrics')
    log.info('Calculating VBET Metrics')

    if not os.path.exists(os.path.join(rsc_proj_path, 'rscontext_metrics.json')):
        raise FileNotFoundError(f'rscontext_metrics.json not found in {rsc_proj_path}')

    with open(os.path.join(rsc_proj_path, 'rscontext_metrics.json')) as json_file:
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

    with sqlite3.connect(os.path.join(vbet_proj_path, 'outputs', 'vbet.gpkg')) as conn:
        curs = conn.cursor()
        curs.execute("""SELECT SUM(width_frac) FROM (SELECT integrated_width * (window_area / tot_area) width_frac FROM (SELECT window_area, integrated_width FROM vbet_igos),
                     (SELECT SUM(window_area) AS tot_area FROM vbet_igos))""")
        integrated_width = curs.fetchone()[0]

    riverscape_network_density = riverscape_length / float(metrics['catchmentArea'])
    tot_hec_per_km = riverscape_area * 100 / riverscape_length
    prop_riverscape = riverscape_area / float(metrics['catchmentArea'])

    metrics['riverscapeLength'] = str(riverscape_length)
    metrics['riverscapeArea'] = str(riverscape_area)
    metrics['riverscapeNetworkDensity'] = str(riverscape_network_density)
    metrics['avgValleyBottomWidth'] = str(integrated_width)
    metrics['avgChannelWidth'] = str(channel_width)
    metrics['lowlyingWidth'] = str(lowlying_width)
    metrics['elevatedWidth'] = str(elevated_width)
    metrics['lowlyingProp'] = str(lowlying_prop)
    metrics['elevatedProp'] = str(elevated_prop)
    metrics['totalHectaresPerKm'] = str(tot_hec_per_km)
    metrics['averageHectaresPerKm'] = str(avg_ha_per_km)
    metrics['proportionRiverscape'] = str(prop_riverscape)

    with open(os.path.join(vbet_proj_path, 'vbet_metrics.json'), 'w') as f:
        json.dump(metrics, f, indent=2)


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
