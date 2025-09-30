import os
import argparse
# from rscontext_3dep.dem_builder import dem_builder


def build_lct_dems(geojson_folder: str, output_folder: str, resolution: float):
    """
    Build the 3DEM DEM projects for each LCT boundary GeoJSON file.
    """

    # Get all *.geojson files from geojson_folder
    geojson_files = [f for f in os.listdir(geojson_folder) if f.endswith('.geojson')]
    huc12s = [os.path.splitext(f)[0] for f in geojson_files]
    huc10s = list(set([huc[:10] for huc in huc12s]))

    print(f"Found {len(geojson_files)} HUC12 files in {geojson_folder}")
    print(f"Found {len(huc10s)} unique HUC10s")

    for huc10 in huc10s:
        print(f"Processing {huc10}")

        download_dir = os.path.join(output_folder, 'downloads')
        scratch_dir = os.path.join(output_folder, 'scratch')
        dem_projects_dir = os.path.join(output_folder, 'dem_projects')
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(scratch_dir, exist_ok=True)
        os.makedirs(dem_projects_dir, exist_ok=True)

        # Download veg, transportation and hydrography from HUC10 RSContext project

        for geojson_file in geojson_files:

            # Skip any HUC12s not in this HUC10
            if geojson_file[:10] != huc10:
                continue

            huc = os.path.basename(geojson_file)
            print(f"Processing HUC12 {huc}...")

            project_dir = os.path.join(dem_projects_dir, huc)
            os.makedirs(project_dir, exist_ok=True)
            
            # Build the 3DEP DEM for this HUC12
            bounds_path_file = os.path.join(geojson_folder, geojson_file)
            try:
                # dem_builder(geojson_file, resolution, download_dir, scratch_dir, project_dir, False)
                pass
            except Exception as e:
                print(f"Error processing {huc}: {e}")
                continue

            # Build a HUC12 project using the HUC10 RSContext data as a base and include the 3DEP DEM




    print("All processing complete.")


def main():
    parser = argparse.ArgumentParser(description="Build LCT DEMs from project files.")
    parser.add_argument('geojson_folder', type=str, help='Path to the folder containing LCT project files.')
    parser.add_argument('output_folder', type=str, help='Path to the folder where DEM files will be saved.')
    parser.add_argument('resolution', type=float,  default=1.0, help='Resolution for the DEM files.')
    args = parser.parse_args()

    build_lct_dems(args.geojson_folder, args.output_folder, args.resolution)


if __name__ == "__main__":
    main()
