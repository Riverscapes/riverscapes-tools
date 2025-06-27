import argparse
import sys
import os
import traceback
import numpy as np
from math import ceil
from shapely.geometry import Polygon
from rscommons import GeopackageLayer, get_shp_or_gpkg, dotenv, Logger
from rscommons.vector_ops import copy_feature_class


def sample_reaches(brat_gpgk_path: str, sample_size: int, stratification: dict = None, min_strat_sample: int = 1):

    log = Logger('Sample Reaches')

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
    log.info(f'Generating {split}x{split} grid of rectangles to sample from.')
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
            tot_reaches = reaches.ogr_layer.GetFeatureCount()
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
    log.info('Getting stratification values for each reach.')
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
    log.info('Sampling reaches based on stratification and polygon coverage.')
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
                    if strat_1_cts[reach_intersections[fid][0]] > tot_strat_1 and poly_strat_cts[reach_intersections[fid][1]] > tot_poly:
                        out_reaches.remove(fid)
                        strat_1_cts[reach_intersections[fid][0]] -= 1
                        poly_strat_cts[reach_intersections[fid][1]] -= 1
                        out_tot -= 1
                        if out_tot <= sample_size:
                            break

    elif num_strats == 2:
        tot_strat_1 = min(sample_size // len(strat_1_unique), min_strat_sample)
        tot_strat_2 = min(sample_size // len(strat_2_unique), min_strat_sample)
        tot_poly = sample_size // len(poly_reaches)
        strat_1_cts = {v: 0 for v in strat_1_unique}
        strat_2_cts = {v: 0 for v in strat_2_unique}
        poly_strat_cts = {v: 0 for v in poly_reaches}

        while any(val < tot_strat_1 for val in strat_1_cts.values()) or any(val < tot_strat_2 for val in strat_2_cts.values()) or \
                any(val < tot_poly for val in poly_strat_cts.values()):
            fid = None
            while fid is None or fid in out_reaches:
                fid = np.random.choice([fid for fid in reach_intersections.keys()])

            out_reaches.append(fid)
            strat_1_cts[reach_intersections[fid][0]] += 1
            strat_2_cts[reach_intersections[fid][1]] += 1
            poly_strat_cts[reach_intersections[fid][2]] += 1

            out_tot += 1

            if out_tot < sample_size:  # if you end up with less than you want, randomly select more
                while out_tot < sample_size:
                    fid = np.random.choice([fid for fid in reach_intersections.keys()])
                    if fid not in out_reaches:
                        out_reaches.append(fid)
                        strat_1_cts[reach_intersections[fid][0]] += 1
                        strat_2_cts[reach_intersections[fid][1]] += 1
                        poly_strat_cts[reach_intersections[fid][2]] += 1
                        out_tot += 1

            else:  # if you end up with more than you want selectively delete
                while out_tot > sample_size:
                    for fid in out_reaches:
                        if strat_1_cts[reach_intersections[fid][0]] > tot_strat_1 and strat_2_cts[reach_intersections[fid][1]] > tot_strat_2 and poly_strat_cts[reach_intersections[fid][2]] > tot_poly:
                            out_reaches.remove(fid)
                            strat_1_cts[reach_intersections[fid][0]] -= 1
                            strat_2_cts[reach_intersections[fid][1]] -= 1
                            poly_strat_cts[reach_intersections[fid][2]] -= 1
                            out_tot -= 1
                            if out_tot <= sample_size:
                                break

    else:
        tot_poly = sample_size // len(poly_reaches)
        poly_strat_cts = {v: 0 for v in poly_reaches}

        while any(poly_strat_cts.values() < tot_poly):
            fid = None
            while fid is None or fid in out_reaches:
                fid = np.random.choice([fid for fid in reach_intersections.keys()])
            out_reaches.append(fid)
            poly_strat_cts[reach_intersections[fid][0]] += 1

            out_tot += 1

            if out_tot < sample_size:
                while out_tot < sample_size:
                    fid = np.random.choice([fid for fid in reach_intersections.keys()])
                    if fid not in out_reaches:
                        out_reaches.append(fid)
                        poly_strat_cts[reach_intersections[fid][0]] += 1
                        out_tot += 1

            else:
                while out_tot > sample_size:
                    for fid in out_reaches:
                        if poly_strat_cts[reach_intersections[fid][0]] > tot_poly:
                            out_reaches.remove(fid)
                            poly_strat_cts[reach_intersections[fid][0]] -= 1
                            out_tot -= 1
                            if out_tot <= sample_size:
                                break

    # out_reaches now has the FIDs of the reaches to sample
    log.info('Creating output feature class with sampled reaches.')
    in_fc = os.path.join(brat_gpgk_path, 'vwReaches')
    out_fc = os.path.join(os.path.dirname(brat_gpgk_path), 'sample_reaches.gpkg/samples')
    copy_feature_class(in_fc, out_fc, attribute_filter=f'ReachID IN ({",".join(map(str, out_reaches))})')

    return


def main():
    parser = argparse.ArgumentParser(description='Sample reaches from a BRAT geopackage.')
    parser.add_argument('huc', type=str, help='HUC or watershed.')
    parser.add_argument('brat_gpkg', type=str, help='Path to the BRAT geopackage file.')
    parser.add_argument('sample_size', type=int, help='Number of reaches to sample.')
    parser.add_argument('--stratification', type=str, help='Stratification fields in the format dataset:field.')
    parser.add_argument('--min_strat_sample', type=int, default=1, help='Minimum number of samples per stratification group.')

    args = dotenv.parse_args_env(parser)

    # Parse stratification argument if provided
    stratification = None
    if args.stratification:
        try:
            # Split on : to get dataset:field format
            dataset, field = args.stratification.split(':')
            stratification = {dataset: field}
        except ValueError:
            raise Exception(f'Stratification argument must be in format "dataset:field", got: {args.stratification}')

    log = Logger('GRTS Sampling')
    log.setup(logPath=os.path.join(os.path.dirname(args.brat_gpkg), 'sample_log'))
    log.title(f'GRTS Sample Reaches for {args.huc}')

    try:
        sample_reaches(args.brat_gpkg, args.sample_size, stratification, args.min_strat_sample)

    except Exception as e:
        log.error(f'Error occurred while sampling reaches: {e}')
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()


# in1 = '/workspaces/data/brat/1602010203/outputs/brat.gpkg'
# in2 = 500
# in3 = {'/workspaces/data/rs_context/1602010203/ecoregions/ecoregions.shp': 'US_L3NAME'}
# sample_reaches(in1, in2, in3, 50)
