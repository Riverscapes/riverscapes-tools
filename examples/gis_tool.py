# Name:     My awesome tool
#
# Purpose:  Do a thing and make things happen
#
# Author:   Matt
#
# Date:     9 Sep 2019
# -------------------------------------------------------------------------------
import argparse
import sys
import os
import traceback
import uuid
import datetime
from osgeo import ogr
from osgeo import gdal

from rscommons import Logger, initGDALOGRErrors, RSProject, RSLayer, ModelConfig, dotenv
from rscommons.util import safe_makedirs
# Import your own version. Don't just use RSCommons
from rscommons.__version__ import __version__
# Make sure we catch GDAL errors
initGDALOGRErrors()

# Setting up our model with an XSD will let us refer to it later
# The version should be your tool version. Usually this is stored in a __version__.py file
cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/MYTOOLHERE.xsd', __version__)


def run(my_arg1: str, my_arg2: int):

    log = Logger("My Awesome tool")
    log.info('Starting RSContext v.{}'.format(cfg.version))

    # do_work()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('arg1', help='Some Argument 1', type=str)
    parser.add_argument('arg2', help='Some Argument 2', type=int)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    # dotenv.parse_args_env will parse and replace any environment variables with the pattern {env:MYENVNAME}
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("My Awesome Tool")
    log.setup(logPath=os.path.join(args.output, "LOGFILE.log"), verbose=args.verbose)
    log.title("My Awesome Tool")

    # We catch the main call because it allows us to exit gracefully with an error message and stacktrace
    try:
        run(args.arg1, args.arg2)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
