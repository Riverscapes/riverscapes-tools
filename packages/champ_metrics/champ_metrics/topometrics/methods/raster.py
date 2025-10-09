import numpy as np
import sys
from os import path
from champmetrics.lib.raster import Raster

def RasterMetrics(rpath):

    result = {'StDev': None}
    theRaster = Raster(rpath)
    result['StDev'] = np.std(theRaster.array)

    return result

if __name__ == "__main__":
    import logging
    import argparse
    logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
    dtfmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(filename='raster_metrics.log', level=logging.DEBUG, format=logfmt, datefmt=dtfmt)

    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('raster',
                        help='Path to the raster',
                        type=argparse.FileType('r'))
    args = parser.parse_args()

    if not args.raster:
        print("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    try:
        dMetrics = RasterMetrics(args.raster.name)

    except AssertionError as e:
        sys.exit(0)
    except Exception as e:
        raise
        sys.exit(0)
