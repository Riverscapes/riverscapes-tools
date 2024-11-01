import os
import shutil
import subprocess
import argparse

from rscommons.util import safe_makedirs
from rscommons.clean_nhd_data import download_unzip

bucket = 'prd-tnm'
prefix = 'StagedProducts/Hydrography/NHDPlusHR/VPU/Current/GDB/'

def get_huc_conus(output_dir, huc_csv, temp_dir):
    # Output GeoPackage file
    output_gpkg_file = os.path.join(output_dir, 'huc10_production.gpkg')
    output_gpkg_dir = os.path.dirname(output_gpkg_file)

    safe_makedirs(output_gpkg_dir)
    safe_makedirs(temp_dir)
    safe_makedirs(output_dir)

    # Load the list of VPU keys from the CSV
    vpus = []
    try:
        with open(huc_csv, 'r') as f:
            for line in f:
                vpus.append(line.strip())
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    for vpu in vpus:
        try:
            print(f"Processing {vpu}")

            # NHDPLUS_H_0101_HU4_GDB.zip get the 4 digit number
            vpu_id = vpu.split('_')[2]

            # Get the VPU zip file URL
            nhd_url = f'https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/{vpu}'
            print(f"Downloading {nhd_url}")

            download_folder = os.path.join(temp_dir, f'download_huc_{vpu_id}')
            unzip_folder = os.path.join(temp_dir, f'unzip_huc_{vpu_id}')

            # Download and unzip the VPU zip file
            _out_zip_dir = download_unzip(nhd_url, download_folder, unzip_folder, False) 

            # Get the GDB file
            gdb_files = [f for f in os.listdir(unzip_folder) if f.endswith('.gdb')]
            if not gdb_files:
                print(f"No .gdb file found in {unzip_folder}")
                continue
            gdb_file = gdb_files[0]
            print(f"Found {gdb_file}")
            gdb_file_path = os.path.join(unzip_folder, gdb_file)

            # Convert to GeoPackage
            feature_class = 'WBDHU10'
            cmd = [
                'ogr2ogr', '-f', 'GPKG', '-makevalid', '-append', '-nln', feature_class,
                output_gpkg_file, gdb_file_path, feature_class
            ]
            subprocess.call(cmd, cwd=output_gpkg_dir)

        except Exception as e:
            print(f"Error processing {vpu}: {e}")
        finally:
            # Clean up
            if os.path.exists(unzip_folder):
                shutil.rmtree(unzip_folder)
            if os.path.exists(download_folder):
                shutil.rmtree(download_folder)
            print(f"Processed {vpu}")

    return output_gpkg_file

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_dir', help='Output directory')
    parser.add_argument('huc_csv', help='CSV file with list of VPU keys')
    parser.add_argument('temp_dir', help='Temporary directory')
    args = parser.parse_args()
    get_huc_conus(args.output_dir, args.huc_csv, args.temp_dir)
    get_huc_conus(out_dir, huc_csv, temp_dir)