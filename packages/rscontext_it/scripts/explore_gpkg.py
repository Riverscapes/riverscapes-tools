import argparse
import sys
import sqlite3


def get_gpkg_version(gpkg_path: str) -> str:
    """
    Get the version of the GeoPackage file.
    :param gpkg_path: Path to the GeoPackage file.
    :return: Version string.
    """

    try:
        # Connect to the GeoPackage file
        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()

        # Query the application_id pragma
        cursor.execute("PRAGMA application_id;")
        application_id = cursor.fetchone()[0]

        # Query the user_version pragma
        cursor.execute("PRAGMA user_version;")
        user_version = cursor.fetchone()[0]

        # Interpret the application_id
        if application_id == 1196444487:
            app_version = "GPKG 1.2 or greater"
        elif application_id == 1196437808:
            app_version = "GPKG 1.0 or 1.1"
        else:
            app_version = "Unknown application_id"

        # Interpret the user_version
        major = user_version // 10000
        minor = (user_version % 10000) // 100
        patch = user_version % 100
        uversion = f"{major}.{minor}.{patch}"

        # Close the connection
        conn.close()

        return f"{app_version}, Version: {uversion}"
    except Exception as e:
        print(f"Error reading GeoPackage version: {e}")
        return None


def list_fields(gpkg_path: str, layer_name: str) -> list:
    """
    List all fields (columns) in a specific layer of the GeoPackage file.
    :param gpkg_path: Path to the GeoPackage file.
    :param layer_name: Name of the layer to inspect.
    :return: List of field names.
    """
    try:
        # Connect to the GeoPackage file
        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()

        # Query the table schema
        cursor.execute(f"PRAGMA table_info('{layer_name}');")
        fields = [row[1] for row in cursor.fetchall()]

        # Close the connection
        conn.close()

        return fields
    except Exception as e:
        print(f"Error reading fields from layer {layer_name}: {e}")
        return []


def list_gpkg_layers(gpkg_path: str):
    """
    List all layers (spatially enabled tables) in a GeoPackage file.
    :param gpkg_path: Path to the GeoPackage file.
    :return: List of layer names.
    """
    import sqlite3

    try:
        # Connect to the GeoPackage file
        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()

        # Query the gpkg_contents table for spatial tables
        query = "SELECT table_name FROM gpkg_contents WHERE data_type = 'features';"
        cursor.execute(query)
        layers = [row[0] for row in cursor.fetchall()]

        # Close the connection
        conn.close()

        return layers
    except Exception as e:
        print(f"Error reading GeoPackage layers: {e}")
        return []


def main():
    """
      Accept geopackage file path and describe it.
    """
    parser = argparse.ArgumentParser(description='Explore GeoPackage files.')
    print(sys.argv)
    parser.add_argument('input_gpkg', type=str, help='Path to the input GeoPackage file.')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output.')
    args = parser.parse_args()

    if args.verbose:
        print(f'Exploring GeoPackage: {args.input_gpkg}')

    version = get_gpkg_version(args.input_gpkg)
    print(f'GeoPackage Version: {version}')
    layers = list_gpkg_layers(args.input_gpkg)
    print('Layers in GeoPackage:')
    print(layers)
    print('-----------------------------------')
    for layer in layers:
        fields = list_fields(args.input_gpkg, layer)
        print(f'Fields in {layer}:')
        print(fields)

    # Add your logic here to process the GeoPackage file
    print('Processing complete.')


if __name__ == '__main__':
    main()
