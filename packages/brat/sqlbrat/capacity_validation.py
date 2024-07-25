import os
import csv

import numpy as np
from shapely.ops import nearest_points, unary_union
import matplotlib.pyplot as plt
from scipy.stats import linregress

from rscommons import GeopackageLayer, Logger
from rscommons.classes.vector_base import VectorBase
from rscommons.database import SQLiteCon


def validate_capacity(brat_gpkg_path: str, dams_gpkg_path: str):

    log = Logger('BRAT Capacity Validation')
    os.mkdir(os.path.join(os.path.dirname(brat_gpkg_path), 'validation'))
    dam_count_table(brat_gpkg_path, dams_gpkg_path)
    electivity_index(brat_gpkg_path)
    validation_plots(brat_gpkg_path)

    log.info('Done')


def dam_count_table(brat_gpkg_path: str, dams_gpkg_path: str):

    dam_cts = {}  # reachid: dam count

    with GeopackageLayer(os.path.join(brat_gpkg_path, 'vwReaches')) as brat_lyr, \
            GeopackageLayer(os.path.join(dams_gpkg_path, 'census')) as dams_lyr:

        buffer_distance = brat_lyr.rough_convert_metres_to_vector_units(10)

        # create a dissolved drainage network
        line_geoms = [ftr for ftr in brat_lyr.ogr_layer]
        line_geoms_shapely = [VectorBase.ogr2shapely(line_geom) for line_geom in line_geoms]
        merged_line = unary_union(line_geoms_shapely)

        # get the points on the line network that are closest to the dam points
        for dam_ftr, *_ in dams_lyr.iterate_features('Finding dam counts for reaches'):
            dam_geom = dam_ftr.GetGeometryRef()
            nearest_line = nearest_points(merged_line, VectorBase.ogr2shapely(dam_geom))
            dam_buf = nearest_line[0].buffer(buffer_distance)

            for line_ftr, *_ in brat_lyr.iterate_features(clip_shape=dam_buf):
                reachid = line_ftr.GetFID()
                line_geom = line_ftr.GetGeometryRef()
                if line_geom is not None:
                    if reachid not in dam_cts.keys():
                        dam_cts[reachid] = 1
                    else:
                        dam_cts[reachid] += 1

    with SQLiteCon(brat_gpkg_path) as db:
        db.curs.execute('SELECT ReachID FROM vwReaches')
        reachids = [row['ReachID'] for row in db.curs.fetchall()]
        db.curs.execute('DROP TABLE IF EXISTS dam_counts')
        db.curs.execute('CREATE TABLE dam_counts (ReachID INTEGER PRIMARY KEY, dam_count INTEGER, dam_density REAL, predicted_capacity REAL, length REAL)')
        db.curs.execute('INSERT INTO dam_counts (ReachID, predicted_capacity, length) SELECT ReachID, oCC_EX, iGeo_Len FROM vwReaches')
        for reachid in reachids:
            if reachid in dam_cts.keys():
                db.curs.execute('UPDATE dam_counts SET dam_count = ? WHERE reachid = ?', (dam_cts[reachid], reachid))
            else:
                db.curs.execute('UPDATE dam_counts SET dam_count = ? WHERE reachid = ?', (0, reachid))
        db.curs.execute('UPDATE dam_counts SET dam_density = dam_count / (length/1000)')
        db.conn.commit()


def electivity_index(gpkg_path: str):
    out_path = os.path.join(os.path.dirname(gpkg_path), 'validation/electivity_index.csv')

    with SQLiteCon(gpkg_path) as db:
        db.curs.execute('SELECT SUM(dam_count) AS dams FROM dam_counts')
        total_dams = db.curs.fetchone()['dams']
        db.curs.execute('SELECT SUM(iGeo_Len) As len FROM vwReaches')
        total_length = db.curs.fetchone()['len']
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl FROM dam_counts WHERE predicted_capacity = 0')
        none_cap = db.curs.fetchone()
        none_ei = (none_cap['dc'] / total_dams) / (none_cap['sl'] / total_length)
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl FROM dam_counts WHERE predicted_capacity > 0 and predicted_capacity <= 1')
        rare_cap = db.curs.fetchone()
        rare_ei = (rare_cap['dc'] / total_dams) / (rare_cap['sl'] / total_length)
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl FROM dam_counts WHERE predicted_capacity > 1 and predicted_capacity <= 5')
        occ_cap = db.curs.fetchone()
        occ_ei = (occ_cap['dc'] / total_dams) / (occ_cap['sl'] / total_length)
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl FROM dam_counts WHERE predicted_capacity > 5 and predicted_capacity <= 15')
        freq_cap = db.curs.fetchone()
        freq_ei = (freq_cap['dc'] / total_dams) / (freq_cap['sl'] / total_length)
        db.curs.execute('SELECT SUM(dam_count) AS dc, SUM(length) AS sl FROM dam_counts WHERE predicted_capacity > 15')
        perv_cap = db.curs.fetchone()
        perv__ei = (perv_cap['dc'] / total_dams) / (perv_cap['sl'] / total_length)

    with open(out_path, 'w', newline='') as csvfile:
        fieldnames = ['Capcacity', 'EI']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'Capcacity': 'None', 'EI': none_ei})
        writer.writerow({'Capcacity': 'Rare', 'EI': rare_ei})
        writer.writerow({'Capcacity': 'Occasional', 'EI': occ_ei})
        writer.writerow({'Capcacity': 'Frequent', 'EI': freq_ei})
        writer.writerow({'Capcacity': 'Pervasive', 'EI': perv__ei})


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
        obs_pred = np.asarray(([d['dam_density'] for d in data], [d['predicted_capacity'] for d in data]))

    quan_90 = np.quantile(obs_pred, 0.9, axis=0)
    quan_75 = np.quantile(obs_pred, 0.75, axis=0)

    res = linregress(obs_pred[1], obs_pred[0])
    res90 = linregress(obs_pred[1], quan_90)
    res75 = linregress(obs_pred[1], quan_75)

    xdata = np.sort(obs_pred[1])

    fig, ax = plt.subplots()
    ax.scatter(obs_pred[1], obs_pred[0], marker='x', c='darkblue')
    # ax.plot([0, max(obs_pred[1])], [0, max(obs_pred[1])], 'r', label='1:1')
    ax.plot(xdata, res90.intercept + res90.slope * xdata, 'k', linestyle='-.', label='90th Percentile')
    ax.plot(xdata, res75.intercept + res75.slope * xdata, 'k', linestyle=':', label='75th Percentile')
    ax.plot(xdata, res.intercept + res.slope * xdata, 'k', linestyle='--', label='50th Percentile')
    ax.set_title('Observed Vs. Predicted Dam Densities')
    ax.set_xlabel(r'Predicted Maximum Capacity $\frac{dams}{km}$')
    ax.set_ylabel(r'Observed Dam Density $\frac{dams}{km}$')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(os.path.dirname(brat_gpkg_path), 'validation/regressions.png'))


bgp = '/workspaces/data/brat/1701020501/outputs/brat.gpkg'
dgp = '/workspaces/data/beaver_activity/1701020501/census.gpkg'

validate_capacity(bgp, dgp)
