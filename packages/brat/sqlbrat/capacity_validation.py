import os
import csv
import sys
import traceback
import argparse

import numpy as np
from shapely.ops import nearest_points, unary_union
import matplotlib.pyplot as plt
from scipy.stats import linregress

from rscommons import GeopackageLayer, Logger, dotenv
from rscommons.classes.vector_base import VectorBase
from rscommons.database import SQLiteCon


def validate_capacity(brat_gpkg_path: str, dams_gpkg_path: str):

    log = Logger('BRAT Capacity Validation')
    if not os.path.exists(os.path.join(os.path.dirname(brat_gpkg_path), 'validation')):
        os.mkdir(os.path.join(os.path.dirname(brat_gpkg_path), 'validation'))
    dam_count_table(brat_gpkg_path, dams_gpkg_path)
    electivity_index(brat_gpkg_path)
    validation_plots(brat_gpkg_path)

    log.info('Done')


def dam_count_table(brat_gpkg_path: str, dams_gpkg_path: str):

    dam_cts = {}  # reachid: dam count

    with GeopackageLayer(os.path.join(brat_gpkg_path, 'vwReaches')) as brat_lyr, \
            GeopackageLayer(os.path.join(dams_gpkg_path, 'dams')) as dams_lyr:

        buffer_distance = brat_lyr.rough_convert_metres_to_vector_units(0.1)

        # create a dissolved drainage network
        line_geoms = [ftr for ftr in brat_lyr.ogr_layer]
        line_geoms_shapely = [VectorBase.ogr2shapely(line_geom) for line_geom in line_geoms]
        merged_line = unary_union(line_geoms_shapely)

        # get the points on the line network that are closest to the dam points
        for dam_ftr, *_ in dams_lyr.iterate_features('Finding dam counts for reaches'):
            dam_id = dam_ftr.GetFID()
            dam_geom = dam_ftr.GetGeometryRef()
            nearest_line = nearest_points(merged_line, VectorBase.ogr2shapely(dam_geom))
            dam_buf = nearest_line[0].buffer(buffer_distance)

            ct = 0
            for line_ftr, *_ in brat_lyr.iterate_features(clip_shape=dam_buf):
                if ct == 0:
                    reachid = line_ftr.GetFID()
                    line_geom = line_ftr.GetGeometryRef()
                    if line_geom is not None:
                        if reachid not in dam_cts.keys():
                            dam_cts[reachid] = 1
                            ct += 1
                        else:
                            dam_cts[reachid] += 1
                            ct += 1

    with SQLiteCon(brat_gpkg_path) as db:
        db.curs.execute('SELECT fid FROM vwReaches')
        reachids = [row['fid'] for row in db.curs.fetchall()]
        db.curs.execute('DROP TABLE IF EXISTS dam_counts')
        db.curs.execute('CREATE TABLE dam_counts (ReachID INTEGER PRIMARY KEY, dam_count INTEGER, dam_density REAL, predicted_capacity REAL, length REAL)')
        db.curs.execute('INSERT INTO dam_counts (ReachID, predicted_capacity, length) SELECT fid, oCC_EX, iGeo_Len FROM vwReaches')
        for reachid in reachids:
            if reachid in dam_cts.keys():
                db.curs.execute('UPDATE dam_counts SET dam_count = ? WHERE reachid = ?', (dam_cts[reachid], reachid))
            else:
                db.curs.execute('UPDATE dam_counts SET dam_count = ? WHERE reachid = ?', (0, reachid))
        db.curs.execute('UPDATE dam_counts SET dam_density = dam_count / (length/1000)')
        db.conn.commit()


def electivity_index(gpkg_path: str):
    out_path = os.path.join(os.path.dirname(gpkg_path), 'validation/electivity_index.csv')
    if os.path.exists(out_path):
        os.remove(out_path)
    if os.path.exists(os.path.join(os.path.dirname(gpkg_path), 'validation/error_segments.csv')):
        os.remove(os.path.join(os.path.dirname(gpkg_path), 'validation/error_segments.csv'))
    if os.path.exists(os.path.join(os.path.dirname(gpkg_path), 'validation/none_segments.csv')):
        os.remove(os.path.join(os.path.dirname(gpkg_path), 'validation/none_segments.csv'))
    err_segs = {}
    none_segs = {}

    with SQLiteCon(gpkg_path) as db:
        db.curs.execute('SELECT SUM(dam_count) AS dams FROM dam_counts')
        total_dams = db.curs.fetchone()['dams']
        db.curs.execute('SELECT SUM(iGeo_Len) As len FROM vwReaches')
        total_length = db.curs.fetchone()['len']
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl, SUM(predicted_capacity * (length/1000)) AS cap FROM dam_counts WHERE predicted_capacity = 0')
        none_cap = db.curs.fetchone()
        none_len = none_cap['sl']
        none_ct = none_cap['dc']
        none_predcap = none_cap['cap']
        none_percap = round((none_ct / none_predcap)*100, 2) if none_predcap > 0 else 'NA'
        none_ei = (none_ct / total_dams) / (none_len / total_length)
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl, SUM(predicted_capacity * (length/1000)) AS cap FROM dam_counts WHERE predicted_capacity > 0 and predicted_capacity <= 1')
        rare_cap = db.curs.fetchone()
        rare_len = rare_cap['sl']
        rare_ct = rare_cap['dc']
        rare_predcap = rare_cap['cap']
        rare_percap = round((rare_ct / rare_predcap)*100, 2) if rare_predcap > 0 else 'NA'
        rare_ei = (rare_ct / total_dams) / (rare_len / total_length)
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl, SUM(predicted_capacity * (length/1000)) AS cap FROM dam_counts WHERE predicted_capacity > 1 and predicted_capacity <= 5')
        occ_cap = db.curs.fetchone()
        occ_len = occ_cap['sl']
        occ_ct = occ_cap['dc']
        occ_predcap = occ_cap['cap']
        occ_ei = (occ_ct / total_dams) / (occ_len / total_length)
        occ_percap = round((occ_ct / occ_predcap)*100, 2) if occ_predcap > 0 else 'NA'
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl, SUM(predicted_capacity * (length/1000)) AS cap FROM dam_counts WHERE predicted_capacity > 5 and predicted_capacity <= 15')
        freq_cap = db.curs.fetchone()
        freq_len = freq_cap['sl']
        freq_ct = freq_cap['dc']
        freq_predcap = freq_cap['cap']
        freq_percap = round((freq_ct / freq_predcap)*100, 2) if freq_predcap > 0 else 'NA'
        freq_ei = (freq_ct / total_dams) / (freq_len / total_length)
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl, SUM(predicted_capacity * (length/1000)) AS cap FROM dam_counts WHERE predicted_capacity > 15')
        perv_cap = db.curs.fetchone()
        perv_len = perv_cap['sl']
        perv_ct = perv_cap['dc']
        perv_predcap = perv_cap['cap']
        perv_percap = round((perv_ct / perv_predcap)*100, 2) if perv_predcap > 0 else 'NA'
        perv_ei = (perv_ct / total_dams) / (perv_len / total_length)

        db.curs.execute('SELECT ReachID, dam_count, predicted_capacity*(length/1000) AS pred FROM dam_counts WHERE dam_count > 0 and predicted_capacity NOT NULL')
        for row in db.curs.fetchall():
            if row['dam_count'] > row['pred'] + 1 or row['pred'] == 0:
                err_segs[row['ReachID']] = int(row['dam_count'] - row['pred'])
            if row['pred'] == 0:
                none_segs[row['ReachID']] = row['dam_count']

    if os.path.exists(out_path):
        os.remove(out_path)
    with open(out_path, 'w', newline='') as csvfile:
        fieldnames = ['Capacity', 'Stream Length (km)', 'Percent of Drainage Network', 'Surveyed Dams', 'BRAT Estimated Capacity',
                      'Average Surveyed Dam Density (dams/km)', 'Average Predicted Capacity (dams/km)', 'Percent of Modeled Capacity', 'Electivity Index']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'Capacity': 'None',
                         'Stream Length (km)': int(none_len/1000),
                         'Percent of Drainage Network': round((none_len/total_length)*100, 1),
                         'Surveyed Dams': none_ct,
                         'BRAT Estimated Capacity': int(none_predcap),
                         'Average Surveyed Dam Density (dams/km)': round(none_ct / (none_len/1000), 3),
                         'Average Predicted Capacity (dams/km)': round(none_predcap / (none_len/1000), 2),
                         'Percent of Modeled Capacity': none_percap,
                         'Electivity Index': round(none_ei, 2)})
        writer.writerow({'Capacity': 'Rare',
                         'Stream Length (km)': int(rare_len/1000),
                         'Percent of Drainage Network': round((rare_len/total_length)*100, 1),
                         'Surveyed Dams': rare_ct,
                         'BRAT Estimated Capacity': int(rare_predcap),
                         'Average Surveyed Dam Density (dams/km)': round(rare_ct / (rare_len/1000), 3),
                         'Average Predicted Capacity (dams/km)': round(rare_predcap / (rare_len/1000), 2),
                         'Percent of Modeled Capacity': rare_percap,
                         'Electivity Index': round(rare_ei, 2)})
        writer.writerow({'Capacity': 'Occasional',
                         'Stream Length (km)': int(occ_len/1000),
                         'Percent of Drainage Network': round((occ_len/total_length)*100, 1),
                         'Surveyed Dams': occ_ct,
                         'BRAT Estimated Capacity': int(occ_predcap),
                         'Average Surveyed Dam Density (dams/km)': round(occ_ct / (occ_len/1000), 3),
                         'Average Predicted Capacity (dams/km)': round(occ_predcap / (occ_len/1000), 2),
                         'Percent of Modeled Capacity': occ_percap,
                         'Electivity Index': round(occ_ei, 2)})
        writer.writerow({'Capacity': 'Frequent',
                         'Stream Length (km)': int(freq_len/1000),
                         'Percent of Drainage Network': round((freq_len/total_length)*100, 1),
                         'Surveyed Dams': freq_ct,
                         'BRAT Estimated Capacity': int(freq_predcap),
                         'Average Surveyed Dam Density (dams/km)': round(freq_ct / (freq_len/1000), 3),
                         'Average Predicted Capacity (dams/km)': round(freq_predcap / (freq_len/1000), 2),
                         'Percent of Modeled Capacity': freq_percap,
                         'Electivity Index': round(freq_ei, 2)})
        writer.writerow({'Capacity': 'Pervasive',
                         'Stream Length (km)': int(perv_len/1000),
                         'Percent of Drainage Network': round((perv_len/total_length)*100, 1),
                         'Surveyed Dams': perv_ct,
                         'BRAT Estimated Capacity': int(perv_predcap),
                         'Average Surveyed Dam Density (dams/km)': round(perv_ct / (perv_len/1000), 3),
                         'Average Predicted Capacity (dams/km)': round(perv_predcap / (perv_len/1000), 2),
                         'Percent of Modeled Capacity': perv_percap,
                         'Electivity Index': round(perv_ei, 2)})
        writer.writerow({'Capacity': 'Total',
                         'Stream Length (km)': int(total_length/1000),
                         'Percent of Drainage Network': 100,
                         'Surveyed Dams': total_dams,
                         'BRAT Estimated Capacity': int(none_predcap + rare_predcap + occ_predcap + freq_predcap + perv_predcap),
                         'Average Surveyed Dam Density (dams/km)': round((none_ct + rare_ct + occ_ct + freq_ct + perv_ct) / (total_length/1000), 3),
                         'Average Predicted Capacity (dams/km)': round((none_predcap + rare_predcap + occ_predcap + freq_predcap + perv_predcap) / (total_length/1000), 2),
                         'Percent of Modeled Capacity': round(((none_ct + rare_ct + occ_ct + freq_ct + perv_ct) / (none_predcap + rare_predcap + occ_predcap + freq_predcap + perv_predcap))*100, 2),
                         'Electivity Index': 'NA'})

    if len(err_segs) > 0:
        with open(os.path.join(os.path.dirname(gpkg_path), 'validation/error_segments.csv'), 'w', newline='') as csvfile:
            fieldnames = ['ReachID', 'Error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for k, v in err_segs.items():
                writer.writerow({'ReachID': k, 'Error': v})

    if len(none_segs) > 0:
        with open(os.path.join(os.path.dirname(gpkg_path), 'validation/none_segments.csv'), 'w', newline='') as csvfile:
            fieldnames = ['ReachID', 'Dams']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for k, v in none_segs.items():
                writer.writerow({'ReachID': k, 'Dams': v})


def validation_plots(brat_gpkg_path: str):
    pred_obs = {}
    with SQLiteCon(brat_gpkg_path) as db:
        db.curs.execute('SELECT AVG(dam_density) AS avg_dam_density, AVG(predicted_capacity) AS avg_pred_cap FROM dam_counts WHERE predicted_capacity = 0')
        none_res = db.curs.fetchone()
        pred_obs['none'] = [none_res['avg_dam_density'], none_res['avg_pred_cap']]
        db.curs.execute('SELECT AVG(dam_density) AS avg_dam_density, AVG(predicted_capacity) AS avg_pred_cap FROM dam_counts WHERE predicted_capacity > 0 and predicted_capacity <= 1')
        rare_res = db.curs.fetchone()
        pred_obs['rare'] = [rare_res['avg_dam_density'], rare_res['avg_pred_cap']]
        db.curs.execute('SELECT AVG(dam_density) AS avg_dam_density, AVG(predicted_capacity) AS avg_pred_cap FROM dam_counts WHERE predicted_capacity > 1 and predicted_capacity <= 5')
        occ_res = db.curs.fetchone()
        pred_obs['occ'] = [occ_res['avg_dam_density'], occ_res['avg_pred_cap']]
        db.curs.execute('SELECT AVG(dam_density) AS avg_dam_density, AVG(predicted_capacity) AS avg_pred_cap FROM dam_counts WHERE predicted_capacity > 5 and predicted_capacity <= 15')
        freq_res = db.curs.fetchone()
        pred_obs['freq'] = [freq_res['avg_dam_density'], freq_res['avg_pred_cap']]
        db.curs.execute('SELECT AVG(dam_density) AS avg_dam_density, AVG(predicted_capacity) AS avg_pred_cap FROM dam_counts WHERE predicted_capacity > 15')
        perv_res = db.curs.fetchone()
        pred_obs['perv'] = [perv_res['avg_dam_density'], perv_res['avg_pred_cap']]

    pred = np.asarray([v[1] for v in pred_obs.values()])
    obs = np.asarray([v[0] for v in pred_obs.values()])

    regr = linregress(pred, obs)

    fig, ax = plt.subplots()
    ax.scatter(pred, obs, c=['r', 'orange', 'yellow', 'g', 'b'])
    ax.plot(pred, regr.intercept + regr.slope * pred, 'k')
    ax.text(1, 0.5*max(obs), f'$R^2$: {regr.rvalue**2:.3f}')
    ax.set_title('Observed Vs. Predicted Dam Densities Averaged by Category')
    ax.set_xlabel(r'Predicted Maximum Capacity $\frac{dams}{km}$')
    ax.set_ylabel(r'Observed Dam Density $\frac{dams}{km}$')
    plt.tight_layout()
    plt.savefig(os.path.join(os.path.dirname(brat_gpkg_path), 'validation/obs_v_pred.png'))
    plt.show()

    with SQLiteCon(brat_gpkg_path) as db:
        db.curs.execute('SELECT dam_density, predicted_capacity FROM dam_counts WHERE dam_density > 0 or (dam_density = 0 and predicted_capacity = 0)')
        data = db.curs.fetchall()
        obs_pred = np.asarray(([d['dam_density'] for d in data if d['dam_density'] is not None and d['predicted_capacity'] is not None],
                               [d['predicted_capacity'] for d in data if d['dam_density'] is not None and d['predicted_capacity'] is not None]))

    quan_90 = np.quantile(obs_pred, 0.9, axis=0)
    quan_75 = np.quantile(obs_pred, 0.75, axis=0)

    res = linregress(obs_pred[1], obs_pred[0])
    res90 = linregress(obs_pred[1], quan_90)
    res75 = linregress(obs_pred[1], quan_75)

    xdata = np.sort(obs_pred[1])

    fig, ax = plt.subplots()
    ax.scatter(obs_pred[1], obs_pred[0], marker='x', c='darkblue')
    ax.plot([0, max(obs_pred[1])], [0, max(obs_pred[1])], 'r', label='1:1')
    ax.plot(xdata, res90.intercept + res90.slope * xdata, 'k', linestyle='-.', label='90th Percentile')
    ax.plot(xdata, res75.intercept + res75.slope * xdata, 'k', linestyle=':', label='75th Percentile')
    ax.plot(xdata, res.intercept + res.slope * xdata, 'k', linestyle='--', label='50th Percentile')
    ax.set_ylim(0, 45)
    ax.set_title('Observed Vs. Predicted Dam Densities')
    ax.set_xlabel(r'Predicted Maximum Capacity $\frac{dams}{km}$')
    ax.set_ylabel(r'Observed Dam Density $\frac{dams}{km}$')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(os.path.dirname(brat_gpkg_path), 'validation/regressions.png'))


def main():

    parser = argparse.ArgumentParser(description='Validate BRAT capacity estimates')
    parser.add_argument('huc', type=int, help='HUC code')
    parser.add_argument('brat_gpkg', type=str, help='Path to BRAT geopackage')
    parser.add_argument('dams_gpkg', type=str, help='Path to dams geopackage')
    parser.add_argument('--verbose', action='store_true', help='Print log messages to console', default=False)
    parser.add_argument('--debug', action='store_true', help='Run in debug mode', default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger('BRAT Capacity Validation')
    log.setup(logPath=os.path.join(os.path.dirname(args.brat_gpkg), 'validation/validation.log'), verbose=args.verbose)
    log.title(f'BRAT Capacity Validation for HUC {args.huc}')

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(os.path.dirname(args.brat_gpkg), 'validation/mem_usage.log')
            retcode, max_obj = ThreadRun(validate_capacity, memfile, args.brat_gpkg, args.dams_gpkg)
            log.debug(f'Return code: {retcode}, [Max process usage] {max_obj}')
        else:
            validate_capacity(args.brat_gpkg, args.dams_gpkg)
    except Exception as e:
        log.error(f'Error: {e}')
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
