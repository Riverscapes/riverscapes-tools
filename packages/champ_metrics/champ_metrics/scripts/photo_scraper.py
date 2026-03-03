"""
Script to build a riverscapes project from CHaMP photos.

Philip Bailey
26 Feb 2026

The input to this script is a folder structure of photos obtained from Streamnet, organized as follows.
The image files have inconsistent file names, and this script makes no attempt to fix the file names.
It uses them exactly as they come from Streamnet.T he image file names are not important to this script. 
Some of them contain contextual information about where they were taken in a sight, but this script 
ignores that information.

photos/
    watershed1/
        year1/
            site1/
                photo1.jpg
                photo2.jpg
            site2/
                photo3.jpg
        year2/
            site3/
                photo4.jpg
    watershed2/
        year3/
            site4/
                photo5.jpg


The script also takes as input a JSON file containing visit information for all CHaMP visits, which can be
obtained from the CHaMP Google Cloud Postgres database using the following query. This query is saved to
a JSON file and passed into the script as "visit info".

SELECT v.WatershedID,
       v.WatershedName,
       s.SiteName,
       s.SiteID,
       s.Latitude,
       s.Longitude,
       v.UTMZone,
       v.VisitID,
       v.SampleDate,
       v.VisitYear,
       v.Organization,
       v.CrewName,
       v.HitchName
FROM vwVisits v
         inner join CHaMP_Sites s on v.SiteID = s.SiteID;

The script first downloads image files from S3, then loops over them and copies them to a new directory
that is the output Riverscapes project. The site locations from the visit info JSON are used unless 
the image file contains EXIF location data, in which case the EXIF location data is used. 
The visit info JSON is also used to populate metadata fields in the project.

A list of all the photos is written to a temporary GeoJSON file, which is then converted to a GeoPackage 
using ogr2ogr and added to the Riverscapes project as a dataset.

Finally the project bounds are determined using a simple min/max approach to the photo locations. The
centroid is determined by averaging the latitudes and longitudes of the photo locations.

The project is uploaded to the Riverscapes Data Exchange using rscli command line operation.
"""
import re
import os
import json 
import argparse
import boto3
import sqlite3
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from datetime import datetime
from rsxml import Logger
from rsxml.project_xml import Project, Realization, Geopackage, GeopackageLayer, GeoPackageDatasetTypes, Meta, MetaData, ProjectBounds, Coords, BoundingBox

def scrape_photos(champ_visits: list, watershed: str, year: int, download_dir: str, project_photos_dir: str) -> list:
    """
    args:
    champ_visits: a list containing visit information for all CHaMP visits
    watershed: Name of the watershed being processed. Might contain spaces.
    year: 
    photos_dir: the top level directory under which photos occur, organized as described above
    """

    log = Logger("CHaMP Photo Scraper")

    # Make sure that the project photos directory is empty by deleting it if it exists and then recreating it
    delete_folder_contents(project_photos_dir)
    os.makedirs(project_photos_dir, exist_ok=True)

    # Recursively walk the photos directory and look for any JPG or PNG files
    # and copy them to a new directory named after the visitID in visit_info
    photo_data = []
    skipped_image_files = 0
    missing_visit_info = 0
    exif_success_count = 0
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            if not file.lower().endswith((".jpg", ".jpeg", ".png")):
                continue

            src_path = os.path.join(root, file)
            dst_path = os.path.join(project_photos_dir, os.path.relpath(src_path, download_dir))
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)

            # Copy, don't move the file
            with open(src_path, 'rb') as src_file:
                with open(dst_path, 'wb') as dst_file:
                    dst_file.write(src_file.read())
            
            # Match this regex (WatershedName/VisitYear/SiteName/Filename) to extract the watershed, year, and site from the file path
            pattern = r'[0-9]{4}\/photos\/(.*)\/(.*)$'
            match = re.search(pattern, dst_path)
            if not match:
                log.warning(f"File path does not match expected pattern: {dst_path}")
                skipped_image_files += 1
                continue

            site= match.group(1)
            filename = match.group(2)

            visit_data = get_visit_info(champ_visits, watershed, year, site)

            if not visit_data:
                missing_visit_info += 1
                log.warning(f"Missing visit info for file: {dst_path}") 
                continue

            try:
                lat, long = extract_coords(dst_path)
                visit_data['EXIF_Location'] = 0
                if lat is not None and long is not None:
                    visit_data["Latitude"] = lat
                    visit_data["Longitude"] = long
                    visit_data['EXIF_Location'] = 1
                    exif_success_count += 1
            except Exception as e:
                log.warning(f"Error extracting EXIF data from {dst_path}: {e}")

            # Make a copy of the vist_data dict
            file_data = visit_data.copy()
            file_data["FilePath"] = os.path.relpath(dst_path, os.path.dirname(project_photos_dir))
            photo_data.append(file_data)

    log.info(f"Total image files found: {len(photo_data) + skipped_image_files}")
    log.info(f"Total image files skipped due to filename pattern mismatch: {skipped_image_files}")
    log.info(f"Total image files skipped due to missing visit info: {missing_visit_info}")
    log.info(f"Total image files with successful EXIF data extraction: {exif_success_count}")

    return photo_data

def create_project_from_photos(watershed, year, photo_data, project_dir):


    log = Logger("Create Project")

    if len(photo_data) < 1:
        log.warning("No valid photo data found. Exiting.")
        return

    # Build the GeoJSON structure
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for entry in photo_data:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [entry["Longitude"], entry["Latitude"]]
            },
            "properties": entry
        }
        geojson["features"].append(feature)

    # Save the photo data to a temporary JSON file
    temp_json_file = os.path.join(os.path.dirname(project_dir), "photo_data.geojson")
    with open(temp_json_file, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=4)

    log.info(f"Photo data saved to {temp_json_file}")

    # GeoPackage generation
    gpkg_path = os.path.join(project_dir, "photos.gpkg")
    cmd = f'ogr2ogr -f GPKG "{gpkg_path}" "{temp_json_file}"'
    log.info(f"Generating GeoPackage with command: {cmd}")
    os.system(cmd)
    log.info(f"GeoPackage saved to {gpkg_path}")

    project_metadata = [
        Meta(name='ModelVersion', value='1.0.0'),
        Meta(name='Watershed', value=watershed),
        Meta(name='Year', value=year),
        Meta(name='Documentation', value='https://docs.riverscapes.net/initiatives/champ#photos', type='url'),
        Meta(name='Streamnet', value='https://www.streamnet.org/home/data-maps/champ/champ-files/', type='url')
    ]

    ############################################################################################################
    # Retrieve some metadata from the GeoPackage

    with sqlite3.connect(gpkg_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM photo_data")
        photo_count = cursor.fetchone()[0]
        project_metadata.append(Meta(name='Photo Count', value=photo_count))

        # Distinct site count
        cursor.execute("SELECT COUNT(DISTINCT SiteID) FROM photo_data")
        site_count = cursor.fetchone()[0]
        project_metadata.append(Meta(name='Site Count', value=site_count))

        # Distinct visit count
        cursor.execute("SELECT COUNT(DISTINCT VisitID) FROM photo_data")
        visit_count = cursor.fetchone()[0]
        project_metadata.append(Meta(name='Visit Count', value=visit_count))

        # Number of sites with EXIF location data
        cursor.execute("SELECT COUNT(DISTINCT SiteID) FROM photo_data WHERE EXIF_Location = 1")
        exif_site_count = cursor.fetchone()[0]
        project_metadata.append(Meta(name='Sites with EXIF Location', value=exif_site_count))

    for meta in project_metadata:
        log.info(f"{meta.name}: {meta.value}")

    ############################################################################################################
    # Create the Riverscapes Project

    # First write the bounding box to a GeoJSON file for use in the project bounds
    bbox_geojson_path = os.path.join(project_dir, "project_bounds.geojson")
    write_bbox_to_geojson(get_bounding_box(photo_data), bbox_geojson_path)
    log.info(f"Project bounding box saved to {bbox_geojson_path}")    

    centroid = get_centroid(photo_data)
    bounding_box = get_bounding_box(photo_data)

    project = Project(
        name=f'CHaMP Photo Project for {watershed} {year}',
        proj_path=os.path.join(project_dir, 'project.rs.xml'),
        project_type='CHaMPPhotos',
        description='Columbia Habitat Monitoring Program (CHaMP) photos scraped from Streamnet and organized into a Riverscapes Project.',
        meta_data=MetaData(project_metadata),
        bounds=ProjectBounds(
            centroid=Coords(*centroid),
            bounding_box=BoundingBox(*bounding_box),
            filepath=os.path.relpath(bbox_geojson_path, project_dir),
        ),
    )

    # Add a realization
    my_real = Realization(
                xml_id='PHOTOS',
                name=f'CHaMP {watershed} {year} Photos',
                product_version='1.0.0',
                date_created=datetime.now(),
    )

    # Add the GeoPackage dataset
    project.realizations.append(my_real)
    my_real.datasets.append(
        Geopackage(
            xml_id='PHOTO_GPKG',
            name='CHaMP Photo Data',
            path=os.path.basename(gpkg_path),
            description='GeoPackage containing CHaMP photo locations and metadata',
            layers=[
                GeopackageLayer(
                    lyr_name='photo_data',
                    name='Photo Points',
                    ds_type=GeoPackageDatasetTypes.VECTOR,
                    description='Layer containing CHaMP photo locations and metadata',
                )
            ]
        )
    )

    # Write it to disk
    project.write()
    log.info(f"Riverscapes project saved to {project.proj_path}")

    log.info("Photo scraping and project creation complete.")

def get_decimal_from_dms(dms, ref):
    degrees = dms[0]
    minutes = dms[1] / 60.0
    seconds = dms[2] / 3600.0
    
    if ref in ['S', 'W']:
        return -(degrees + minutes + seconds)
    return degrees + minutes + seconds

def extract_coords(image_path):
    try:
        img = Image.open(image_path)
        exif_data = img._getexif()
        
        if not exif_data:
            return None, None

        # GPSInfo tag ID is 34853
        if 34853 not in exif_data:
            return None, None

        gps_info = exif_data[34853]
        geotagging = {}
        for t, value in gps_info.items():
            decoded = GPSTAGS.get(t, t)
            geotagging[decoded] = value

        if 'GPSLatitude' in geotagging and 'GPSLongitude' in geotagging:
            lat_ref = geotagging.get('GPSLatitudeRef', 'N')
            lon_ref = geotagging.get('GPSLongitudeRef', 'E')

            lat = get_decimal_from_dms(geotagging['GPSLatitude'], lat_ref)
            lon = get_decimal_from_dms(geotagging['GPSLongitude'], lon_ref)
            return lat, lon

    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        
    return None, None

def get_centroid(photo_data):
    # calculate the centroid of the photo locations
    latitudes = [entry["Latitude"] for entry in photo_data]
    longitudes = [entry["Longitude"] for entry in photo_data]
    centroid_lat = sum(latitudes) / len(latitudes)
    centroid_lon = sum(longitudes) / len(longitudes)
    return (centroid_lon, centroid_lat)

def get_bounding_box(photo_data):
    # calculate the bounding box of the photo locations
    latitudes = [entry["Latitude"] for entry in photo_data]
    longitudes = [entry["Longitude"] for entry in photo_data]
    min_lat = min(latitudes)
    max_lat = max(latitudes)
    min_lon = min(longitudes)
    max_lon = max(longitudes)
    return (min_lon, min_lat, max_lon, max_lat)

def write_bbox_to_geojson(bbox, output_file):
    """
    Takes a bbox (min_lon, min_lat, max_lon, max_lat) 
    and writes a GeoJSON Polygon.
    """
    min_lon, min_lat, max_lon, max_lat = bbox

    # Define the 5 points of the rectangle (closing the loop)
    # Order: Bottom-Left, Bottom-Right, Top-Right, Top-Left, Bottom-Left
    coordinates = [[
        [min_lon, min_lat],
        [max_lon, min_lat],
        [max_lon, max_lat],
        [min_lon, max_lat],
        [min_lon, min_lat] 
    ]]

    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "name": "Project Bounding Box",
                    "min_lat": min_lat,
                    "max_lat": max_lat,
                    "min_lon": min_lon,
                    "max_lon": max_lon
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": coordinates
                }
            }
        ]
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, indent=2)

def get_visit_info(champ_visits, watershed, year, site):
    # read the visit info from the JSON file and return the visitID for the given watershed, year, and site
    for visit in champ_visits:
        if visit["WatershedName"].lower() == watershed.lower() \
            and visit["VisitYear"] == year \
            and visit["SiteName"].lower() == site.lower():
                return visit
    
    return None

def delete_folder_contents(folder_path):
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))

def download_files(watershed, year, download_dir, force_download=False):
    # Use boto3 to download the files from S3

    log = Logger("Download")

    # First ensure that the photo download folder is empty by deleting it if it exists and then recreating it
    if os.path.exists(download_dir):
        if force_download is True:
            delete_folder_contents(download_dir)
        else:
            log.info(f"Download directory already exists: {download_dir}. Skipping download.")
            return download_dir
    else:
        os.makedirs(download_dir)

    s3path = f'{watershed}/{year}/Photos'

    s3 = boto3.client('s3')
    download_count = 0
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket='champ-streamnet', Prefix=s3path):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue  # Skip directories

            local_path = os.path.join(download_dir, os.path.relpath(key, s3path))
            if os.path.exists(local_path):
                continue  # Skip if file already exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file('champ-streamnet', key, local_path)
            download_count += 1

    log.info(f"Downloaded {download_count} files from S3 path: {s3path} to local directory: {download_dir}")

    # log the total file count in the download directory
    total_files = sum(len(files) for _, _, files in os.walk(download_dir))
    log.info(f"Total files in download directory after download: {total_files}")

    return download_dir


def main():
    args = argparse.ArgumentParser(description="Scrape photos from a website")
    args.add_argument("processing_info", type=str, help="Path to JSON file with watershed and year processing status")
    args.add_argument("visit_info", type=str, help="Path to JSON file with CHaMP visit info")
    args.add_argument("working_dir", type=str, help="Top level directory under which photos will be downloaded and processed")
    args.add_argument("owner", type=str, help="Owner name to use in rscli upload command")
    args.add_argument("--force-download", action="store_true", help="Whether to force re-download of photos from S3 even if they already exist in the download directory")
    args.add_argument("--retain-project", action="store_true", default=False, help="Whether to retain the project directory after processing (by default it is deleted to save space)")
    args.add_argument("--skip-upload", action="store_true", default=False, help="Whether to skip the rscli upload step (for testing purposes)")
    args.add_argument("--retain-download", action="store_true", default=False, help="Whether to retain the downloaded photos in the download directory after processing (by default they are deleted to save space)")
    args = args.parse_args()

    if not os.path.isfile(args.processing_info):
        raise FileNotFoundError(f"Processing info JSON file not found: {args.processing_info}")

    if not os.path.isfile(args.visit_info):
        raise FileNotFoundError(f"Visit info JSON file not found: {args.visit_info}")
    
    if not os.path.isdir(args.working_dir):
        raise NotADirectoryError(f"Working directory not found: {args.working_dir}")
    
    log = Logger("CHaMP Photo Scraper")
    log.setup(log_path=os.path.join(args.working_dir, "champ_photo_scraper.log"), verbose=True)
    log.info(f"Starting photo scraping with visit info: {args.visit_info}")
    log.info(f"Working directory: {args.working_dir}")

    processing_info = json.load(open(args.processing_info))
    visit_info = json.load(open(args.visit_info))

    process_que = []
    for watershed in processing_info:
        watershed_name = watershed["watershedName"]
        for year_info in watershed["years"]:
            year = year_info["year"]
            status = year_info["status"]
            if status.lower() == "todo":
                process_que.append((watershed_name, year))

    log.info(f'{len(process_que)} watershed-year combinations marked as TODO in processing status file.')

    for watershed_name, year in process_que:
        log.info(f"Processing photos for {watershed_name} {year}")

        download_dir = os.path.join(args.working_dir, 'download', watershed_name, str(year))
        project_dir = os.path.join(args.working_dir, 'project', watershed_name, str(year))
        project_photos_dir = os.path.join(project_dir, 'photos')

        download_files(watershed_name, year, download_dir, force_download=args.force_download)    
        photos_info = scrape_photos(visit_info, watershed_name, year, download_dir, project_photos_dir)
        create_project_from_photos(watershed_name, year, photos_info, project_dir)

        # Set the processing status for this watershed-year to "complete" and write the JSON file back to disk
        for watershed in processing_info:
            if watershed["watershedName"] == watershed_name:
                for year_info in watershed["years"]:
                    if year_info["year"] == year:
                        year_info["status"] = "complete"
                        break
                break
        with open(args.processing_info, "w", encoding="utf-8") as f:
            json.dump(processing_info, f, indent=4)

        if args.skip_upload is True:
            log.info("Skipping rscli upload step as --skip-upload flag is set.")
        else:
            rscli_tags = f'CHAMP_Watershed_{watershed_name.replace(" ", "_")},CHAMP_Year_{year}'
            rscli_cmd = f'rscli upload --org {args.owner} --no-wait --no-input --tags {rscli_tags} "{project_dir}"'
            log.info(f"Uploading project to Riverscapes using command: {rscli_cmd}")
            os.system(rscli_cmd)

        if args.retain_project is False:
            log.info(f"Deleting project directory to save space: {project_dir}")
            delete_folder_contents(project_dir)
            os.rmdir(project_dir)
        else:
            log.info(f"Retaining project directory as --retain-project flag is set: {project_dir}")

        if args.retain_download is False:
            log.info(f"Deleting downloaded photos to save space: {download_dir}")
            delete_folder_contents(download_dir)
            os.rmdir(download_dir)
        else:
            log.info(f"Retaining downloaded photos as --retain-download flag is set: {download_dir}")

        log.info(f"Finished processing photos for {watershed_name} {year}")

if __name__ == "__main__":
    main()