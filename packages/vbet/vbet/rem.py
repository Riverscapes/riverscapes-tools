import numpy as np
import rasterio
from rsxml import ProgressBar


def relative_elevation(dem: str, channel: str, out_raster: str):

    with rasterio.open(channel) as src_chan, rasterio.open(dem) as src_dem:
        chan_arr = src_chan.read()[0, :, :]
        dem_arr = src_dem.read()[0, :, :]
        meta = src_dem.meta

    hand_array = np.full((dem_arr.shape[0], dem_arr.shape[1]), src_dem.nodata)

    chan_vals = {}

    for row in range(chan_arr.shape[0]):
        for col in range(chan_arr.shape[1]):
            if chan_arr[row, col] == 1:
                chan_vals[str([row, col])] = [row, col, dem_arr[row, col]]

    rowvals = np.asarray([val[0] for val in chan_vals.values()])
    colvals = np.asarray([val[1] for val in chan_vals.values()])
    elevs = np.asarray([val[2] for val in chan_vals.values()])

    progbar = ProgressBar(dem_arr.shape[0])

    for j in range(dem_arr.shape[0]):
        progbar.update(j)
        for i in range(dem_arr.shape[1]):
            if dem_arr[j, i] == src_dem.nodata:
                hand_array[j, i] = src_dem.nodata
                continue
            if chan_arr[j, i] == 1:
                hand_array[j, i] = 0.
                continue

            dist_arr = np.sqrt((j - rowvals)**2 + (i - colvals)**2)
            dist_sort = np.argpartition(dist_arr, int(0.05 * len(dist_arr)))
            dist_use = dist_arr[dist_sort[:int(0.05 * len(dist_arr))]]
            weight = 1 / dist_use**2
            adj_weight = weight / np.sum(weight)
            elev_fracs = adj_weight * elevs[dist_sort[:int(0.05 * len(dist_arr))]]
            # elev = sum(elev_fracs)

            hand_array[j, i] = dem_arr[j, i] - np.sum(elev_fracs)

    with rasterio.open(out_raster, 'w', **meta) as dst:
        dst.write(hand_array, 1)
