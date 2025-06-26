import os
import numpy as np
from math import ceil
from shapely.geometry import Polygon
from rscommons import GeopackageLayer, get_shp_or_gpkg


def sample_reaches(brat_gpgk_path: str, sample_size: int, stratification: dict = None, min_strat_sample: int = 1):

    split = ceil(sample_size ** 0.5)

    polys = []
    poly_reaches = {}
    reach_intersections = {}
    strat_vals = {}

    with GeopackageLayer(brat_gpgk_path, 'vwReaches') as reaches:
        x_min, x_max, y_min, y_max = reaches.ogr_layer.GetExtent()
        x_step = (x_max - x_min) / split
        y_step = (y_max - y_min) / split

    # generate a grid of rectangles that overlap the dataset
    x_curr = x_min
    while x_curr < (x_max + x_step):
        y_curr = y_min
        while y_curr < (y_max + y_step):
            poly = Polygon([(x_curr, y_curr), (x_curr + x_step, y_curr),
                            (x_curr + x_step, y_curr + y_step), (x_curr, y_curr + y_step)])
            polys.append(poly)
            y_curr += y_step
        x_curr += x_step

    # associate each reach with the rectangles
    for i, poly in enumerate(polys):
        with GeopackageLayer(brat_gpgk_path, 'vwReaches') as reaches:
            for ftr, *_ in reaches.iterate_features(clip_shape=poly):
                fid = ftr.GetFID()
                if i not in poly_reaches:
                    poly_reaches[i] = [fid]
                else:
                    poly_reaches[i].append(fid)
    to_del = []
    for poly_id, reach_ids in poly_reaches.items():
        if len(reach_ids) < 5:
            to_del.append(poly_id)
    for poly_id in to_del:
        del poly_reaches[poly_id]

    # get the field values of the stratification features for each reach
    if stratification is not None:
        for dataset, field in stratification.items():
            strat_vals[field] = []
            with GeopackageLayer(brat_gpgk_path, "vwReaches") as reaches, get_shp_or_gpkg(dataset) as strat:
                for ftr, *_ in reaches.iterate_features(f''):
                    fid = ftr.GetFID()
                    for strat_ftr, *_ in strat.iterate_features(clip_shape=ftr.GetGeometryRef()):
                        if strat_ftr.GetGeometryRef().Intersects(ftr.GetGeometryRef()):
                            fv = strat_ftr.GetField(field)
                            if fv not in strat_vals[field]:
                                strat_vals[field].append(fv)
                            if fid not in reach_intersections:
                                reach_intersections[fid] = [fv]
                            # else:
                            #     if fv not in reach_intersections[fid]:
                            #         reach_intersections[fid].append(fv)

        num_strats = len(strat_vals)
        if num_strats == 1:
            strat_1_vals = [v[0] for k, v in reach_intersections.items() if len(v) > 0]
            strat_1_unique = list(set(strat_1_vals))
        else:
            strat_1_vals = [v[0] for k, v in reach_intersections.items() if len(v) > 0]
            strat_2_vals = [v[1] for k, v in reach_intersections.items() if len(v) > 1]
            strat_1_unique = list(set(strat_1_vals))
            strat_2_unique = list(set(strat_2_vals))
    else:
        num_strats = 0

    # add the polygon id to the reach intersections
    for id, vals in reach_intersections.items():
        for polyid, reach_ids in poly_reaches.items():
            if id in reach_ids:
                if polyid not in reach_intersections[id] and len(reach_intersections[id]) < 2:
                    reach_intersections[id].append(polyid)

    to_del = []
    for id, vals in reach_intersections.items():
        if len(vals) < 2:
            to_del.append(id)
    for id in to_del:
        del reach_intersections[id]

    # now reach_intersections has all info needed? {reach_FID: [strat_val1, strat_val2, poly_id1]}
    out_reaches = []
    out_tot = 0

    if num_strats == 1:
        tot_strat_1 = min(sample_size // len(strat_1_unique), min_strat_sample)
        tot_poly = sample_size // len(poly_reaches)
        strat_1_cts = {v: 0 for v in strat_1_unique}
        poly_strat_cts = {v: 0 for v in poly_reaches}

        while any(val < tot_strat_1 for val in strat_1_cts.values()) or any(val < tot_poly for val in poly_strat_cts.values()):
            fid = None
            while fid is None or fid in out_reaches:
                fid = np.random.choice([fid for fid in reach_intersections.keys()])

            out_reaches.append(fid)
            strat_1_cts[reach_intersections[fid][0]] += 1
            poly_strat_cts[reach_intersections[fid][1]] += 1

            out_tot += 1
            print(strat_1_cts, poly_strat_cts, out_tot)

        if out_tot < sample_size:  # if you end up with less than you want, randomly select more
            while out_tot < sample_size:
                fid = np.random.choice([fid for fid in reach_intersections.keys()])
                if fid not in out_reaches:
                    out_reaches.append(fid)
                    strat_1_cts[reach_intersections[fid][0]] += 1
                    poly_strat_cts[reach_intersections[fid][1]] += 1
                    out_tot += 1

        else:  # if you end up with more than you want selectively delete
            while out_tot > sample_size:
                for fid in out_reaches:
                    if strat_1_cts[reach_intersections[fid][0]] > tot_strat_1 or poly_strat_cts[reach_intersections[fid][1]] > tot_poly:
                        out_reaches.remove(fid)
                        strat_1_cts[reach_intersections[fid][0]] -= 1
                        poly_strat_cts[reach_intersections[fid][1]] -= 1
                        out_tot -= 1
                        if out_tot <= sample_size:
                            break

    elif num_strats == 2:
        tot_strat_1 = sample_size // len(strat_1_unique)
        tot_strat_2 = sample_size // len(strat_2_unique)
        tot_poly = sample_size // len(poly_reaches)
        strat_1_cts = {v: 0 for v in strat_1_unique}
        strat_2_cts = {v: 0 for v in strat_2_unique}
        poly_strat_cts = {v: 0 for v in poly_reaches}

        while (any(strat_1_cts.values() < tot_strat_1) or
               any(strat_2_cts.values() < tot_strat_2) or
               any(poly_strat_cts.values() < tot_poly)):
            fid = np.random.choice(np.asarray(reach_intersections.keys()), size=1, replace=False)
            out_reaches.append(fid)
            strat_1_cts[reach_intersections[fid][0]] += 1
            strat_2_cts[reach_intersections[fid][1]] += 1
            poly_strat_cts[reach_intersections[fid][2]] += 1

            out_tot += 1

            if out_tot >= sample_size:
                break

    else:
        tot_poly = sample_size // len(poly_reaches)
        poly_strat_cts = {v: 0 for v in poly_reaches}

        while any(poly_strat_cts.values() < tot_poly):
            fid = np.random.choice(np.asarray(reach_intersections.keys()), size=1, replace=False)
            out_reaches.append(fid)
            poly_strat_cts[reach_intersections[fid][0]] += 1

            out_tot += 1

            if out_tot >= sample_size:
                break

    return out_reaches


in1 = '/workspaces/data/brat/1602010203/outputs/brat.gpkg'
in2 = 500
in3 = {'/workspaces/data/rs_context/1602010203/ecoregions/ecoregions.shp': 'US_L3NAME'}
sample_reaches(in1, in2, in3, 50)
