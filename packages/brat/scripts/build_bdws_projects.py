"""
Temporary script to build Beaver Dam Water Storage projects
from data that Jordan Gilbert produced running Konrad's code.

Philip Bailey
Jan 2025.
"""
import os
import subprocess
from datetime import datetime
import xml.etree.ElementTree as ET
from rsxml.util import safe_makedirs
from rsxml.project_xml import Project, Meta, MetaData, Realization, Dataset, ProjectBounds, Coords, BoundingBox

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
                "dir": "Drakes_Bay"
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

dataset_names = {
    'dem.tif': 'DEM',
    'dem_vb.tif': 'DEM within Valley Bottom',
    'vb_buffered': 'Valley Bottom Buffered',
    'd8slope.tif': 'D8 Slope',
    'pitfill.tif': 'Pit Filled DEM',
    'fac.tif': 'Flow Accumulation',
    'flowdir.tif': 'Flow Direction',
    'flowacc.tif': 'Flow Accumulation',
    'brat.': 'BRAT Output',
    'brat_perennial.': 'BRAT Perennial Output',
    'ModeledDamPoints.': 'Modeled Dam Points',
    'head_hi.tif': 'Head High',
    'WSESurf_lo.tif': 'Water Surface Elevation Low',
    'pondID.tif': 'Pond ID',
    'depLo.tif': 'Depths of modeded beaver ponds for low dam heights.',
    'damID.tif': 'Dam ID',
    'head_mid.tif': 'head_mid',
    'head_lo.tif': 'head_lo',
    'htAbove.tif': 'htAbove',
    'WSESurf_mid.tif': 'WSESurf_mid',
    'head_start.tif': '',
    'depHi.tif': 'Depths of modeded beaver ponds for high dam heights.',
    'depMid.tif': 'Depths of modeded beaver ponds for median dam heights.',
    'WSESurf_hi.tif': ''
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


def get_dataset_name(input_file_name: str) -> str:

    final_name = input_file_name
    for key, value in dataset_names.items():
        if value != '' and key in input_file_name:
            final_name = value
            break

    return final_name


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
                if path.endswith('.tif'):
                    for file_name in os.listdir(input_realization_dir):
                        if file_name.endswith('.tif'):
                            input_raster_path = os.path.join(input_realization_dir, file_name)
                            output_raster = os.path.join(output_realization_dir, os.path.basename(input_raster_path))
                            subprocess.run(["gdal_translate", "-co", "COMPRESS=LZW", input_raster_path, output_raster], check=True)
                else:
                    copy_file(os.path.join(input_realization_dir, path), output_realization_dir)

        # Open the corresponding BRAT project and read the bounds information from the XML
        brat_project_file = os.path.join(huc10_path, 'project.rs.xml')
        tree = ET.parse(brat_project_file)
        root = tree.getroot()
        centroid_lat = root.find("ProjectBounds/Centroid/Lat").text
        centroid_lon = root.find("ProjectBounds/Centroid/Lng").text

        min_lat = root.find("ProjectBounds/BoundingBox/MinLat").text
        min_lon = root.find("ProjectBounds/BoundingBox/MinLng").text
        max_lat = root.find("ProjectBounds/BoundingBox/MaxLat").text
        max_lon = root.find("ProjectBounds/BoundingBox/MaxLng").text

        # Create project file
        project_file = os.path.join(output_huc10_folder, 'project.rs.xml')

        project = Project(
            name=f'Beaver Dam Water Storage for {huc10["name"]} ',
            project_type='BDWS',
            proj_path=project_file,
            description='Beaver Dam Water Storage (BDWS) is a collection of Python classes for estimating surface water and groundwater stored by beaver dams. BDWS uses beaver dam capacity estimates from the Beaver Restoration Assesment Tool (BRAT) to place beaver dams along stream reaches, flow direction algebra to determine the area inundated by a dam, and MODFLOW-2005 to model potential changes to groundwater tables from beaver dam construction. BDWS is comprised of three classes. BDLoG (Beaver Dam Location Generator), which generates beaver dam locations along a stream network using BRAT outputs. BDSWEA (Beaver Dam Surface Water Estimation Algorithm), which estimates the amount of water a beaver dam of a given height at a given location could potentially store. BDflopy (Beaver Dam flopy), which uses the existing FloPy python module to automatically parameterize and run MODFLOW-2005 to estimate changes to groundwater storage resulting from beaver dam construction.',
            citation='Hafen, K. 2017. To what extent might beaver dam building buffer water storage losses associated with a declining snowpack? Master’s Thesis. Utah State University, Logan, Utah.',
            meta_data=MetaData(values=[
                Meta('HUC', huc10['huc']),
                Meta('ModelVersion', '1.0.0'),
                Meta('WebLink', 'https://konradhafen.github.io', type='url'),
                Meta('Contact', 'Konrad Hafen'),
                Meta('CitationUrl', 'https://digitalcommons.usu.edu/etd/6503', type='url'),
            ]),
            bounds=ProjectBounds(
                centroid=Coords(centroid_lon, centroid_lat),
                bounding_box=BoundingBox(min_lon, min_lat, max_lon, max_lat),
                filepath=os.path.relpath(os.path.join(output_huc10_folder, 'project_bounds.geojson'), os.path.dirname(project_file))
            )
        )

        for extensions in ['.tif', '.shp']:
            for file_name in os.listdir(os.path.join(output_huc10_folder, 'Inputs')):
                if file_name.endswith(extensions):
                    name = get_dataset_name(file_name)
                    project.common_datasets.append(Dataset(
                        name=name,
                        xml_id=f'{os.path.splitext(file_name)[0].upper()}',
                        path=os.path.relpath(os.path.join(output_huc10_folder, 'inputs', file_name), os.path.dirname(project_file)),
                        ds_type='Raster' if file_name.endswith('.tif') else 'Vector'
                    ))

        for realization_value in realizations:
            realization_name = f'{realization_value} percent BRAT Dam Realization'
            realization_slug = f'{float(realization_value)}' if realization_value != 100 else f'{int(realization_value)}'
            realization_dir = os.path.join(output_huc10_folder, 'outputs', f'realization_{realization_value}')

            datasets = []
            for extensions in ['.tif', '.shp']:
                for file_name in os.listdir(realization_dir):
                    if file_name.endswith(extensions):
                        name = get_dataset_name(file_name)
                        datasets.append(Dataset(
                            name=name,
                            xml_id=f'{os.path.splitext(file_name)[0].upper()}',
                            path=os.path.relpath(os.path.join(realization_dir, file_name), os.path.dirname(project_file)),
                            ds_type='Raster' if file_name.endswith('.tif') else 'Vector'
                        ))

            realization = Realization(
                name=realization_name,
                product_version='1.0.0',
                xml_id=f'REALIZATION_{realization_value}',
                description=f'{realization_value} percent BRAT Dam Realization',
                date_created=datetime.now(),
                outputs=datasets
            )

            project.realizations.append(realization)

            project.write()
            print(f'Project file written to {project_file}')
