# -------------------------------------------------------------------------------
# Name:     National Water Model
#
# Purpose:  Download NWM data and generate stream statistics.
#
# Author:   Philip Bailey
#
# Date:     20 Sep 2019
#
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import traceback
import uuid
import datetime
from osgeo import ogr
from sqlbrat.lib.flow_accumulation import flow_accumulation
from sqlbrat.lib.flow_accumulation import flow_accum_to_drainage_area


# https://unidata.github.io/netcdf4-python/netCDF4/index.html

from netCDF4 import Dataset
import numpy as np
from datetime import datetime


def nwm_read(nhd):

    driver = ogr.GetDriverByName('OpenFileGDB')
    dataset = driver.Open(nhd, 0)
    layer = dataset.GetLayer("NHDFlowline")

    for feature in layer:
        print('feature')
        break

    dataset = None

    # TODO: Paths need to be reset
    raise Exception('PATHS NEED TO BE RESET')

    rootgrp = Dataset('/SOMEPATH/GISData/NWM/200001011200.CHRTOUT_DOMAIN1.comp', 'r', format='NETCDF4')
    # print(rootgrp.data_model)

    # for attr in rootgrp.ncattrs():
    #     print(attr, '=', getattr(rootgrp, attr))

    # ---Check the data format
    # print (rootgrp.file_format)

    print('Dimension Keys:', rootgrp.dimensions.keys())
    print('Time', rootgrp.dimensions['time'])
    print('feature_id Dimension', rootgrp.dimensions['feature_id'])
    print('Variables', rootgrp.variables.keys())
    # print (rootgrp.variables['streamflow'])
    # print (rootgrp.variables['q_lateral'])
    # print (rootgrp.Conventions)
    # print (rootgrp.units)

    stream_flow = np.array(rootgrp.variables['streamflow'])
    reach_id = np.array(rootgrp.variables['feature_id'])
    time = np.array(rootgrp.variables['time'])
    ref_time = np.array(rootgrp.variables['reference_time'])

    print(datetime.fromtimestamp(time[0]))
    print(reach_id[0])
    print(stream_flow[0])

    index = np.where(reach_id == 55000300304574)
    q = stream_flow[index]

    print(q)
    print('Process complete')

    rootgrp.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('nhd', help='NHD Plus HR file geodatabase path', type=str)
    args = parser.parse_args()

    try:
        nwm_read(args.nhd)

    except Exception as e:
        # log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
