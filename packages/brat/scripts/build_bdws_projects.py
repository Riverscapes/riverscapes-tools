"""
Temporary script to build Beaver Dam Water Storage projects
from data that Jordan Gilbert produced running Konrad's code.

Philip Bailey
Jan 2025.
"""

import os
import sys
from rsxml.util import safe_makedirs


top_level_dir = '/Users/philipbailey/GISData/riverscapes/bdws'
output_dir = '/Users/philipbailey/GISData/riverscapes/bdws/rs_projects'

huc8s = {
    "Tomales": {
        "folder": 'Tomales',
        "huc10s": [
            {
                "huc": "1805000504",
                "name": "Abbotts Lagoon",
                "dir": "Abbotts_Lagoon"
            },
            {
                "huc": "1805000505",
                "name": "Drakes Bay",
                "dir": "Brakes_Bay"
            },
            {
                "huc": "1805000501",
                "name": "Lagunitas Creek",
                "dir": "Lagunitas_Creek"
            },
            {
                "huc": "1805000503",
                "name": "Tomales Bay",
                "dir": "Tomales_Bay"
            }
        ]
    }
}

realizations = [10, 25, 50, 100]

copy_paths = {
    "root": [
        './project_bounds.geojson',
    ],
    "inputs": [
        './BDWS/dem.tif',
        './BDWS/inputs/dem_vb.tif',
        './BDWS/vb_buffered.*',
        './BDWS/d8slope.tif',
        './BDWS/pitfill.tif',
        './BDWS/inputs/fac.tif',
        './BDWS/inputs/flowdir.tif',
        './BDWS/flowacc.tif',
        './BDWS/inputs/brat.*',
        './BDWS/inputs/brat_perennial.*',
    ],
    'outputs': [
        'ModeledDamPoints.*',
        '*.tif',
    ]
}


def copy_file(src, dest_dir):
    """Copy source file to destination if it doesn't already exist"""

    # if not os.path.isfile(src):
    #     print(f'File not found: {src}')
    #     return

    safe_makedirs(dest_dir)

    # dest_file = os.path.join(dest_dir, os.path.basename(src))
    # if os.path.isfile(dest_file):
    #     print(f'File already exists: {dest_file}')
    #     return

    print(f'Copying {src} to {dest_dir}')
    os.system(f'cp {src} {dest_dir}')


for huc8_name, huc8_data in huc8s.items():
    print(f'Processing HUC8 {huc8_name}')
    huc8_path = os.path.join(top_level_dir, huc8_data['folder'])

    for huc10 in huc8_data['huc10s']:
        print(f'Processing HUC10 {huc10["huc"]}')
        huc10_path = os.path.join(huc8_path, huc10['dir'])

        output_huc10_folder = os.path.join(output_dir, f'{huc10['dir']}_{huc10['huc']}')

        # Copy root items for the HUC10 project
        for path in copy_paths['root']:
            copy_file(os.path.join(huc10_path, path), output_huc10_folder)

        # Copy input items for the HUC10 project
        for path in copy_paths['inputs']:
            copy_file(os.path.join(huc10_path, path), os.path.join(output_huc10_folder, 'inputs'))

        for realization in realizations:
            print(f'Processing realization {realization}')
            realization_name = f'realization_{realization}'
            realization_slug = f'{float(realization)}' if realization != 100 else f'{int(realization)}'
            input_realization_dir = os.path.join(huc10_path, f'BDWS/perennial/{realization}/outputs{realization_slug}')
            output_realization_dir = os.path.join(output_huc10_folder, 'outputs',  realization_name)

            # Copy output items for the realization
            for path in copy_paths['outputs']:
                copy_file(os.path.join(input_realization_dir, path), output_realization_dir)

        # Create project file
        project_file = os.path.join(output_huc10_folder, 'project.rs.xml')
