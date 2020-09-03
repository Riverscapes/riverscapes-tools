# -------------------------------------------------------------------------------
# Name:     Hydrology Parameters
#
# Purpose:  Calculate the model parameters for hydrology regional equations.
#
# Author:   Philip Bailey
#
# Date:     29 Oct 2019
#
# -------------------------------------------------------------------------------
import argparse
import sqlite3
import os
import rasterio
from statistics import mean
import subprocess
import sqlite3
import json
import shutil
from rasterio.mask import mask
from shapely.geometry import Point
import numpy as np
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.shapefile import get_geometry_union

# These are the EVT_PHYS types that are considered forest.
# See the EVT_PHYS column in the BRAT VegetationTypes table.
forest_phys = ['Hardwood', 'Conifer', 'Conifer-Hardwood']


def hydro_parameters_batch(database, prism, huc_filter, download_folder):

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT WatershedID, Name, States FROM Watersheds WHERE ({}) AND (WatershedID = \'17020015\') ORDER BY WatershedID'.format(huc_filter))
    hucs = {row[0]: '{} ({})'.format(row[1], row[2]) for row in curs.fetchall()}
    print('{:,} HUCs loaded for processing using filter: {}'.format(len(hucs), huc_filter))

    curs.execute('SELECT VegetationID FROM VegetationTypes WHERE Physiognomy IN ({})'.format(','.join(["'{}'".format(name) for name in forest_phys])))
    forest_types = [row[0] for row in curs.fetchall()]

    curs.execute('SELECT ParamID, Name FROM HydroParams')
    params = {row[1]: row[0] for row in curs.fetchall()}

    # Reset all the hydro params to NULL for HUCs about to be processed
    # [curs.execute('UPDATE HUCs SET MeanPrecip = NULL, Relief = NULL, MeanElev = NULL, MinElev = NULL, MeanSlope = NULL,'
    #               'HighSlope = NULL, ForestCover = NULL, ForestCoverP1 = NULL WHERE HUC = ?', [huc]) for huc in hucs]
    # conn.commit()

    processing = 0
    for huc, name in hucs.items():
        processing += 1
        print('Processing HUC {} {}, {} of {}'.format(huc, name, processing, len(hucs)))

        data_folder = os.path.join(download_folder, huc)
        cmd = 'rscli download {} --type RSContext --meta huc8={} --verbose --file-filter "(dem.tif|slope.tif|existing_veg.tif|WBDHU8)" --no-input'.format(data_folder, huc)
        subprocess.run(cmd, shell=True)

        dem_raster = os.path.join(data_folder, 'topography', 'dem.tif')
        slp_raster = os.path.join(data_folder, 'topography', 'slope.tif')
        veg_raster = os.path.join(data_folder, 'vegetation', 'existing_veg.tif')
        huc_path = os.path.join(data_folder, 'hydrology', 'WBDHU8.shp')

        polygon = get_geometry_union(huc_path, 4326)

        mean_precip = precipitation(polygon, prism)
        dem_stats = raster_buffer_stats2({1: polygon}, dem_raster)
        slope_stats = raster_buffer_stats2({1: polygon}, slp_raster)
        high_slope_pc = high_slope(polygon, slp_raster)
        forest_cover_pc = forest_cover(polygon, forest_types, veg_raster)

        results = {
            'PRECIP': mean_precip,
            'RR': dem_stats[1]['Maximum'] - dem_stats[1]['Minimum'],
            'ELEV': dem_stats[1]['Mean'],
            'MINELEV': dem_stats[1]['Minimum'],
            'MEANSLOPE': slope_stats[1]['Mean'],
            'SLOP30_30M': high_slope_pc,
            'FOREST': forest_cover_pc
        }

        data = [(huc, params[param], value) for param, value in results.items()]
        curs.executemany('INSERT OR REPLACE INTO WatershedHydroParams (WatershedID, ParamID, Value) VALUES (?, ?, ?)', data)
        conn.commit()

        # Prevent hard drive from filling up. Comment out if you want to avoid repeated downloads and quick dev cycles
        os.remove(dem_raster)
        os.remove(slp_raster)
        os.remove(veg_raster)
        print(huc, 'complete')

    [print('{} = {:.2f}'.format(name, value)) for name, value in results.items()]
    return results


def high_slope(polygon, slope):

    with rasterio.open(slope) as src:
        raw_raster = mask(src, [polygon], crop=True)[0]
        mask_raster = np.ma.masked_values(raw_raster, src.nodata)
        total_area = mask_raster.count()
        high_area = mask_raster[mask_raster > 30].count()
        return (high_area / total_area) * 100


def forest_cover(polygon, forest_types, veg):

    with rasterio.open(veg) as src:
        raw_raster = mask(src, [polygon], crop=True)[0]
        mask_raster = np.ma.masked_values(raw_raster, src.nodata)
        total_area = mask_raster.count()

        forest_cells = 0
        for ftype in forest_types:
            cells = np.ma.array(mask_raster == ftype, dtype=bool).sum()
            forest_cells += cells

        # forest_area = (np.ma.array(mask_raster > 41, dtype=bool) & np.ma.array(mask_raster <= 43, dtype=bool)).sum()
        return (forest_cells / total_area) * 100


def precipitation(polygon, database):

    # Select PRISM locations with the watershed boundary
    coords = polygon.exterior.envelope.exterior.coords
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute('SELECT Longitude, Latitude, MeanPrecip FROM Precipitation'
                 ' WHERE (Latitude >= ?) AND (Latitude <= ?) AND (Longitude >= ?) AND (Longitude <= ?)',
                 [coords[0][1], coords[2][1], coords[0][0], coords[1][0]])

    # Filter to just those PRISM locations that are actually within the HUC boundary polygon
    monthly_precip = []
    for row in curs.fetchall():
        point = Point(row[0], row[1])
        if polygon.contains(point):
            monthly_precip.append(row[2])

    if len(monthly_precip) < 1:
        raise Exception('No monthly precipitation values found.')

    if sum(monthly_precip) < 1:
        raise Exception('Zero monthly precipitation.')

    return mean(monthly_precip)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))
    parser.add_argument('prism', help='Path to PRISM SQLite database', type=argparse.FileType('r'))
    parser.add_argument('huc_filter', help='SQL where clause for filtering HUCs', type=str)
    parser.add_argument('download_folder', help='Path to folder where downloads will be stored.', type=str)
    args = parser.parse_args()

    hydro_parameters_batch(args.database.name, args.prism.name, args.huc_filter, args.download_folder)


if __name__ == '__main__':
    main()
