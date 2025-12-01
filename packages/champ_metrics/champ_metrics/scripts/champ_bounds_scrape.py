"""
In Nov 2025 we needed to scrape the remaining CHaMP topo surveys into the data exchange.
These are 2018-2020 surveys collected by Boyd. They have the old, V1 project XML that 
does not have bounds information.

This script uses the survey extent for each project to populate the "" bounds database.
The RiverscapesXML migration script then writes this information to the projects as part
of the upgrade to V2 XML.


Philip Bailey
27 Nov 2025
"""
import os
import json
import sqlite3
import xml.etree.ElementTree as ET

top_level_dir = "/Users/philipbailey/GISData/champ/MonitoringDataUnzipped"
# workbench_db = "/Users/philipbailey/GISData/champ/workbench.db"
champ_bounds_db = "/Users/philipbailey/GISData/champ/champ_visit_bounds2.sqlite"

# This was a temporary DB that Philip built for Tyler Kunz to use to migrate
# Yankee Fork sites. It contains the Yankee Fork bounds polygon.
# In 2025 Philip wrote a temporary script in PyDex repo to generate a new SQLite
# db with the same schema with just the Yankee Fork bounds to migrate post-ChaMP
# sites.
champ_db = "/Users/philipbailey/GISData/champ/yankee_fork_bounds/yankee_fork_bounds.sqlite"


# Recursively find all project.rs.xml files in the Yankee Fork sites
project_files = []
for dirpath, __dirnames, filenames in os.walk(top_level_dir):
    for filename in filenames:
        if filename == "project.rs.xml":
            project_files.append(os.path.join(dirpath, filename))

# Remove any projects that don't contain "Yankee Fork" in their path
# because the folder might contain other post-champ surveys, surch as Humboldt County etc
print(f"Found {len(project_files)} total projects.")
yankee_fork_projects = [pf for pf in project_files if "YankeeFork" in pf]
print(f"Found {len(yankee_fork_projects)} Yankee Fork projects to migrate.")

with sqlite3.connect(champ_bounds_db) as conn:
    for project_file in yankee_fork_projects:
        print(f"Processing project file: {project_file}")
        tree = ET.parse(project_file)
        root = tree.getroot()

        site = root.find('MetaData/Meta[@name="Site"]').text
        visit = root.find('MetaData/Meta[@name="Visit"]').text
        watershed = root.find('MetaData/Meta[@name="Watershed"]').text
        year = root.find('MetaData/Meta[@name="Year"]').text

        if not all([site, visit, watershed, year]):
            raise ValueError(f"Missing metadata in {project_file}")
        
        print(f"Metadata - Site: {site}, Visit: {visit}, Watershed: {watershed}, Year: {year}")
        
        nod_survey_extent = root.find('Realizations/SurveyData[@projected="true"]/SurveyExtents/Vector/Path').text
        if not nod_survey_extent:
            raise ValueError(f"Missing SurveyExtent in {project_file}")
        
        survey_extent_path = os.path.join(os.path.dirname(project_file), nod_survey_extent)
        survey_extent_path = os.path.normpath(survey_extent_path)
        survey_extent_path = survey_extent_path.replace("\\", "/")  # for Windows paths
        if not os.path.exists(survey_extent_path):
            raise FileNotFoundError(f"Survey extent file not found: {survey_extent_path}")
        
        # Use GeoPandas to load the survey extent
        import geopandas as gpd
        gdf = gpd.read_file(survey_extent_path)
        if gdf.empty:
            raise ValueError(f"Survey extent file is empty: {survey_extent_path}")
        
        # Reproject the geometry to EPSG:4326 if needed
        if gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
        
        # Get the centroid of the first feature
        centroid = gdf.geometry.iloc[0].centroid

        # Get the bounding rectangle of the geometry
        bounds = gdf.total_bounds  # returns (minx, miny, maxx, maxy)
        minx, miny, maxx, maxy = bounds

        bounds_data = {
            'centroid': {'lat': centroid.y, 'lng': centroid.x},
            'boundingBox': {
                'MinLat': miny,
                'MinLng': minx,
                'MaxLat': maxy,
                'MaxLng': maxx
            }
        }

        # Get the the first feature as GeoJSON
        polygon_geojson = gdf.geometry.iloc[0].__geo_interface__

        curs = conn.cursor()
        curs.execute('INSERT INTO visits (visit, watershed, site, year, bounds, polygon) VALUES (?, ?, ?, ?, ?, ?)', [
            visit,
            watershed,
            site,
            year,
            json.dumps(bounds_data),
            json.dumps(polygon_geojson)
        ])
                        
    conn.commit()