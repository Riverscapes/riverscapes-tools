"""
Read the LINZ LiDAR Survey Index and build a GeoPackage index of LiDAR tiles.
This script was migrated from Lorin Gaertner's original in his personal github account repo.

Philip Bailey

25 Sep 2025
"""


import argparse
import geopandas as gpd
import pystac
import fsspec
import s3fs
import json
import posixpath
from shapely.geometry import box
from rscommons.dotenv import parse_args_env


def get_stac_collection_from_s3(s3_url: str) -> pystac.Collection | None:
    """
    Reads a STAC collection JSON from an S3 URL, attempting anonymous access.
    """
    try:
        # Use fsspec.open with protocol='s3' and anon=True for anonymous S3 access
        with fsspec.open(s3_url, 'r', protocol='s3', anon=True) as f:
            collection_dict = json.load(f)
            return pystac.Collection.from_dict(collection_dict, href=s3_url)
    except Exception as e:
        print(f"Error reading STAC collection from {s3_url}: {e}")
        print("Ensure the S3 bucket is publicly accessible or your AWS credentials are configured correctly if it's private.")
        return None


def load_survey_index_gdf_fromLINZ(api_key: str):
    """
    Loads the NZ LiDAR Survey Index as a GeoDataFrame.

    Args:
        survey_index_path (str): Path or URL to the survey index.
    Returns:
        GeoDataFrame or None
    """

    # # attempt 1 Using direct URL for the NZ LiDAR 1m DEM Survey Index GeoJSON feed
    nz_survey_index_url = f"https://data.linz.govt.nz/services;key={api_key}/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=data.linz.govt.nz:layer-121943&outputFormat=application/json&srsName=EPSG:4326"

    # Example if you download the GeoJSON manually:
    # nz_survey_index_local_path = "new_zealand_lidar_1m_dem_survey_index.geojson"
    # create_geopackage_index(nz_survey_index_local_path, "lidar_tiles_index.gpkg")

    # Example using geopackage layer
    # gdf = geopandas.read_file(
    # r"C:\nardata\datadownload\linz\lds-new-zealand-lidar-1m-dem-survey-index-GPKG\new-zealand-lidar-1m-dem-survey-index.gpkg",
    # layer="new_zealand_lidar_1m_dem_survey_index"
    # )

    # To use the direct URL, ensure you have your API key set up.
    if "YOUR_API_KEY" in nz_survey_index_url:
        print("WARNING: Please replace 'YOUR_API_KEY' in the script with your actual LINZ API key.")
        print("You can download the GeoJSON manually from the LINZ website and provide its local path as an alternative.")

    print(f"Loading NZ LiDAR Survey Index from: {nz_survey_index_url}")
    try:
        gdf = gpd.read_file(nz_survey_index_url)
        return gdf
    except Exception as e:
        print(f"Error loading survey index: {e}")
        return None


def open_all_items_from_collection(collection):
    s3_fs = s3fs.S3FileSystem(anon=True)
    items = []
    for link in collection.links:
        if link.rel == "item":
            item_url = link.get_absolute_href()
            with s3_fs.open(item_url, 'r') as f:
                item_dict = json.load(f)
                item = pystac.Item.from_dict(item_dict, href=item_url)
                items.append(item)
    return items


def process_survey_gdf_and_create_geopackage(survey_gdf, output_gpkg_path="lidar_tiles_index.gpkg"):
    """
    Processes the survey GeoDataFrame and creates a GeoPackage index of LiDAR tiles.

    Args:
        survey_gdf (GeoDataFrame): The loaded survey index.
        output_gpkg_path (str): Path for the output GeoPackage file.
    """
    if survey_gdf is None:
        print("No survey GeoDataFrame provided.")
        return

    all_tile_features = []

    for idx, row in survey_gdf.iterrows():
        source_url = row['source']
        if not source_url or not source_url.startswith('s3://'):
            print(f"Skipping row {idx} due to missing or invalid S3 source URL: {source_url}")
            continue

        print(f"Processing STAC collection from: {source_url}")
        collection = get_stac_collection_from_s3(source_url)

        if collection:
            items = open_all_items_from_collection(collection)
            for item in items:
                bbox = item.bbox
                if bbox is None:
                    print(f"Skipping item {item.id} due to missing bbox.")
                    continue
                geometry = box(bbox[0], bbox[1], bbox[2], bbox[3])

                properties = {
                    'id': item.id,
                    'collection_id': collection.id,
                    'geometry_type': 'Polygon',  # All items are assumed to have a polygon bbox

                    # Add any other relevant properties from the STAC item you'd like to include
                    # For example:
                    # 'asset_keys': list(item.assets.keys()),
                    # 'original_stac_url': source_url # Keep track of the original source
                }
                visual_asset = item.assets.get("visual")
                if visual_asset:
                    # Copy all keys from the visual asset node
                    visual_dict = visual_asset.to_dict()
                    # make href absolute if it's relative
                    href = visual_dict.get("href")
                    if href and href.startswith("."):
                        s3_dir = posixpath.dirname(item.get_self_href())
                        abs_href = s3_dir + "/" + href[2:]  # remove the ./
                        visual_dict["href"] = abs_href
                    properties.update(visual_dict)

                all_tile_features.append({'geometry': geometry, 'properties': properties})
        else:
            print(f"Could not retrieve STAC collection for {source_url}")
            raise RuntimeError(f"Could not retrieve STAC collection for {source_url}")

    if not all_tile_features:
        print("No tile features found to process.")
        return

    # Create a GeoDataFrame from the collected features
    # Ensure all properties have the same keys for consistent DataFrame creation
    # We'll normalize properties to ensure all dictionaries have the same keys
    normalized_features = []
    # Identify all unique keys present across all 'properties' dictionaries
    all_keys = set()
    for feature in all_tile_features:
        all_keys.update(feature['properties'].keys())

    # Create new dictionaries with all keys, filling missing ones with None
    for feature in all_tile_features:
        normalized_props = {key: feature['properties'].get(key) for key in all_keys}
        normalized_features.append({'geometry': feature['geometry'], 'properties': normalized_props})

    # Extract geometries and properties separately
    geometries = [f['geometry'] for f in normalized_features]
    properties_list = [f['properties'] for f in normalized_features]

    # Create a GeoDataFrame
    tiles_gdf = gpd.GeoDataFrame(properties_list, geometry=geometries, crs="EPSG:4326")  # STAC BBoxes are typically WGS84

    print(f"Writing {len(tiles_gdf)} tile features to {output_gpkg_path}")
    try:
        tiles_gdf.to_file(output_gpkg_path, driver="GPKG")
        print("GeoPackage created successfully!")
    except Exception as e:
        print(f"Error writing GeoPackage: {e}")


def main():
    """
    Main function to parse arguments and run the script.
    """
    parser = argparse.ArgumentParser(description="Build LiDAR tiles index from NZ LiDAR Survey Index")
    parser.add_argument("api_key", type=str, default="", help="LINZ API Key for accessing the survey index")
    parser.add_argument("output_gpkg", type=str, help="Output GeoPackage path")
    args = parse_args_env(parser)

    survey_gdf = load_survey_index_gdf_fromLINZ(args.api_key)
    process_survey_gdf_and_create_geopackage(survey_gdf, args.output_gpkg)

    print('Script finished.')

    return args


if __name__ == "__main__":
    main()
