# Philip Bailey
# 2023-07-07
# Demonstration script for stitching together multiple VBET geopackages
# This was built as a proof of concept for the BLM San Luis Valley 
# focal watershed project. It is not intended to be a production script.

import os
import sys
import traceback
import shutil
import tempfile
import zipfile
import subprocess
import argparse
from rscommons import (Logger, dotenv, initGDALOGRErrors)

# Literal list of VBET feature classes to be stitched together. 
# This should be enhanced to "self discover" the feature classes
feature_classes = {
    'POLYGON': [
        'active_floodplain',
        'active_valley_bottom',
        'floodplain',
        'inactive_floodplain',
        'vbet_full',
    ],
    'LINESTRING': [
        'vbet_centerlines',
    ],
    'POINT': [
        'vbet_igos'
    ]
}


def stitch_projects(directory: str, output_gpkg: str) -> None:
    """
    Stich together multiple VBET projects into a single geopackage
    directory: str - path to directory containing VBET zips downloaded from Data Exchange
    output_gpkg: str - path to output geopackage
    """
    
    # Get a list of zip files in the directory
    zip_files = [file for file in os.listdir(directory) if file.endswith('.zip')]

    for zip_file in zip_files:

        # Skip the Alamosa River zip file, because it was used as the seed for the output.
        # All subsequent Geopackages will be append to this one.
        if zip_file == 'VBET_Alamosa_River.zip':
            continue

        # Create a temporary directory to unzip the file
        temp_dir = tempfile.mkdtemp()

        try:
            # Extract the zip file into the temporary directory
            with zipfile.ZipFile(os.path.join(directory, zip_file), 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            for geometry_type, feature_class_list in feature_classes.items():
                for feature_class in feature_class_list:
                    input_gpkg = os.path.join(temp_dir, 'outputs', 'vbet.gpkg')
                    cmd = f'ogr2ogr -f GPKG -append -nlt {geometry_type} -nln {feature_class} {output_gpkg} {input_gpkg} {feature_class}'
                    print(cmd)

                    # Perform the shell command (replace 'your_command' with your desired command)
                    subprocess.run([cmd], shell=True, cwd=temp_dir)

        finally:
            # Delete the temporary directory and its contents
            shutil.rmtree(temp_dir)


def main():
    parser = argparse.ArgumentParser(
        description='Riverscapes Context Tool',
        # epilog="This is an epilog"
    )
    parser.add_argument('directory', help='Folder path containing VBET zip files downloaded from data exchange', type=str)
    parser.add_argument('output_gpkg', help='Path to existing GeoPackage that possesses the template feature classes for the output', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about things like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Sticher")
    log.setup(logPath=os.path.join(args.output, "sticher.log"), verbose=args.verbose)
    log.title('Stitcher')

    try:
        stitch_projects(args.directory, args.output_gpkg)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
