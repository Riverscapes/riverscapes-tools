import os
import json
import argparse


def generate_lct_projects(geojson_folder: str, output_folder: str):
    """
    Generate LCT project files from GeoJSON boundary files.

    Parameters:
    - geojson_folder: Path to the folder containing GeoJSON boundary files.
    - output_folder: Path to the folder where project files will be saved.
    """
    import os
    import json

    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Iterate over GeoJSON files in the specified folder
    for filename in os.listdir(geojson_folder):
        if filename.endswith('.geojson'):
            filepath = os.path.join(geojson_folder, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                feature = json.load(f)

            # Extract properties and geometry
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})

            # Create a project dictionary (example structure)
            project = {
                "name": properties.get("huc12", "Unnamed Project"),
                "boundary": geometry,
                "metadata": properties
            }

            # Write project to a JSON file
            project_filename = f"{properties.get('huc12', 'Unnamed_Project')}_project.json"
            project_filepath = os.path.join(output_folder, project_filename)
            with open(project_filepath, 'w', encoding='utf-8') as pf:
                json.dump(project, pf, ensure_ascii=False, indent=2)

            print(f"Wrote project file: {project_filepath}")


def main():
    parser = argparse.ArgumentParser(description="Generate LCT project files from GeoJSON boundaries.")
    parser.add_argument('--geojson_folder', type=str, required=True, help='Path to the folder containing GeoJSON boundary files.')
    parser.add_argument('--output_folder', type=str, required=True, help='Path to the folder where project files will be saved.')
    args = parser.parse_args()

    generate_lct_projects(args.geojson_folder, args.output_folder)


if __name__ == "__main__":
    main()
