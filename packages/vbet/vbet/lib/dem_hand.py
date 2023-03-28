"""Custom HAND algorithm 

Jordan Gilbert

03/2023
"""

import argparse
from .hand import dem_hand
import rasterio
import numpy as np
from rscommons import dotenv


def hand(in_dem: str, in_channel: str, out_hand_path: str):

    with rasterio.open(in_dem) as dem_src, rasterio.open(in_channel) as chan_src:
        dem_array = dem_src.read()[0, :, :]
        dem_array = np.asarray(dem_array, dtype=float)
        dem_nd = float(dem_src.nodata)
        meta = dem_src.meta
        dtype = dem_src.dtypes[0]

        chan_array = chan_src.read()[0, :, :]
        chan_array = np.asarray(chan_array, dtype=float)

    hand_array = dem_hand.calc_hand(dem_array, chan_array, dem_nd)
    out_array = np.asarray(hand_array, dtype)

    with rasterio.open(out_hand_path, 'w', **meta) as dst:
        dst.write(out_array, 1)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('in_dem', help='', type=str)
    parser.add_argument('in_channel', help='', type=str)
    parser.add_argument('out_hand_path', help='', type=str)

    args = dotenv.parse_args_env(parser)

    hand(args.in_dem, args.in_channel, args.out_hand_path)


if __name__ == '__main__':
    main()
