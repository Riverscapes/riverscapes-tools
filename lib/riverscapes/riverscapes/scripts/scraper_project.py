"""Script to create a project XML file for the VBET synthesis GeoPackage."""
import os
import json
import argparse
import sqlite3
from datetime import datetime
from rsxml import dotenv
from rsxml.project_xml import Project, ProjectBounds, Coords, BoundingBox, Realization, MetaData, Geopackage, GeopackageLayer, GeoPackageDatasetTypes


def create_project_file(gpkg_path: str, author: str) -> None:
    """Creates a project XML file for the VBET synthesis GeoPackage.
    gpkg_path: Path to the VBET synthesis GeoPackage
    output_path: Output path to project XML
    """

    meta = MetaData()
    with sqlite3.connect(gpkg_path) as conn:
        curs = conn.cursor()
        meta.add_meta('Number of IGOs', get_db_statistic(curs, 'SELECT count(*) FROM vbet_igos'))
        meta.add_meta('Number of HUCs', get_db_statistic(curs, 'SELECT count(*) FROM hucs'))
        meta.add_meta('Number of Projects', get_db_statistic(curs, 'SELECT count(*) FROM projects'))
        meta.add_meta('Synthesis Performed by', get_db_statistic(curs, author))

        # Build a bounding box from the HUCs
        curs.execute('select min_x, min_y, max_x, max_y, srs_id from gpkg_contents WHERE table_name = ?', ['vbet_igos'])
        coords = curs.fetchone()
        centroid = Coords((coords[0] + coords[2]) / 2, (coords[1] + coords[3]) / 2)
        polygon = [
            [coords[0], coords[1]],
            [coords[0], coords[3]],
            [coords[2], coords[3]],
            [coords[2], coords[1]],
            [coords[0], coords[1]]
        ]
        bbox = BoundingBox(coords[0], coords[1], coords[2], coords[3])
        meta.add_meta('SRS ID', coords[4])

        geojson = {'type': 'FeatureCollection', 'features': [{
            'type': 'Feature',
            'properties': {},
            'geometry': {
                'type': 'Polygon',
                'coordinates': [polygon]
            }
        }]}

    # Create the GeoJSON file
    geojson_path = os.path.join(os.path.dirname(gpkg_path), 'project_bounds.geojson')
    with open(geojson_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(geojson))
    print(f'GeoJSON written to: {geojson_path}')

    project = Project(
        name='VBET Synthesis Version 3.1.0',
        proj_path='project.rs.xml',
        project_type='VBETSynthesis',
        description='Synthesises together results from the latest version of VBET for each 10 digit HUC from the Mississippi River Basin to the US west coast.'
        ' Most VBET projects in this synthesis are version 3.1.0, but some are lower versions. This synthesis only combines IGOs and their corresponding VBET metrics.'
        ' It does not include any other data from the source VBET projects.',
        bounds=ProjectBounds(
            centroid=centroid,
            bounding_box=bbox,
            filepath='project_bounds.json',
        ),
        meta_data=meta,
        realizations=[
            Realization(
                xml_id='vbet_synthesis_realization_01',
                name='Realization',
                product_version='0.0.1',
                date_created=datetime(2023, 9, 12), datasets=[
                    Geopackage(
                        xml_id='vbet_synth_geopackage',
                        name='Synthesis GeoPackage',
                        path=os.path.basename(gpkg_path),
                        description='The one and only GeoPackage produced by the synthesis. There are two features in this GeoPackage, one for the IGOs and one for the 10 digit HUCs.'
                        ' There are two additional non-spatial tables: projects that contains a list of all the VBET projects scraped. HUCs is another list of all the 10 digit HUCs in CONUS.'
                        ' These latter two tables are used to track processing and to ensure that all HUCs are accounted for. There is also a spatial view that joins the IGOs to the HUCs. This can be used for quick visualization of the data.',
                        layers=[
                            GeopackageLayer(lyr_name='vw_huc_summary_stats',
                                            name='HUC summary statistics',
                                            ds_type=GeoPackageDatasetTypes.VECTOR,
                                            description='Min, Max and Mean values for each VBET metric for each HUC',
                                            )
                        ]
                    )
                ]
            )
        ]
    )

    # Write it to disk
    output_path = os.path.join(os.path.dirname(gpkg_path), 'project.rs.xml')
    project.write(output_path)

    print(f'Process complete. Output written to: {output_path}')


def get_db_statistic(curs: sqlite3.Cursor, sql: str) -> int or float:
    """Retrieve the value from a SQL query."""
    curs.execute(sql)
    return curs.fetchone()[0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('gpkg_path', help='Path to the VBET synthesis GeoPackage', type=str)
    parser.add_argument('author', help='Name of the person who ran the script', type=str)
    args = dotenv.parse_args_env(parser)

    create_project_file(args.gpkg_path, args.author)
