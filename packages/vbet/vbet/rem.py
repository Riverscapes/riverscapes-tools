import numpy as np
import rasterio
from rscommons import ProgressBar


def relative_elevation(dem: str, channel: str, out_raster: str):

    with rasterio.open(channel) as src_chan, rasterio.open(dem) as src_dem:
        chan_arr = src_chan.read()[0, :, :]
        dem_arr = src_dem.read()[0, :, :]
        meta = src_dem.meta

    subtr_array = np.full((dem_arr.shape[0], dem_arr.shape[1]), src_dem.nodata)

    chan_vals = {}

    for row in range(chan_arr.shape[0]):
        for col in range(chan_arr.shape[1]):
            if chan_arr[row, col] == 1:
                chan_vals[str([row, col])] = [row, col, dem_arr[row, col]]

    rowvals = np.asarray([val[0] for val in chan_vals.values()])
    colvals = np.asarray([val[1] for val in chan_vals.values()])
    elevs = np.asarray([val[2] for val in chan_vals.values()])

    progbar = ProgressBar(dem_arr.shape[0])

    z_cts = 0
    for j in range(dem_arr.shape[0]):
        progbar.update(j)
        for i in range(dem_arr.shape[1]):
            if dem_arr[j, i] == src_dem.nodata:
                subtr_array[j, i] = None
                continue
            if chan_arr[j, i] == 1:
                subtr_array[j, i] = dem_arr[j, i]
                continue

            dist_arr = np.sqrt((j - rowvals)**2 + (i - colvals)**2)
            if 0 in dist_arr:
                z_cts += 1
                continue
            weight = 1 / dist_arr**2
            adj_weight = weight / np.sum(weight)
            elev_fracs = adj_weight * elevs
            # elev = sum(elev_fracs)

            subtr_array[j, i] = np.sum(elev_fracs)

    hand_array = dem_arr - subtr_array
    hand_array[np.where(hand_array < 0)] = 0.
    hand_array = np.nan_to_num(hand_array, nan=src_dem.nodata)

    with rasterio.open(out_raster, 'w', **meta) as dst:
        dst.write(hand_array, 1)
