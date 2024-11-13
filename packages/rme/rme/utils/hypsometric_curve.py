import os
import json
import argparse

import numpy as np
from osgeo import ogr
import rasterio
from rasterio.mask import mask
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from shapely.geometry import shape

from rscommons import dotenv
from rscommons.classes.vector_classes import GeopackageLayer


def calculate_hypsometric_curve(elevation_data: np.array, nodata_value, transform, rough_units, num_bins=50):
    print("Elevation data shape: ", elevation_data.shape)
    elevation_data = elevation_data.flatten()
    elevation_data = elevation_data[elevation_data != nodata_value]
    print("Elevation data shape after removing nodata values: ", elevation_data.shape)

    # Bin the elevation data
    min_elevation = np.min(elevation_data)
    max_elevation = np.max(elevation_data)
    bins = np.linspace(min_elevation, max_elevation, num_bins + 1)  # num_bins + 1 to get num_bins intervals
    binned_elevations = np.digitize(elevation_data, bins) - 1  # digitize returns indices starting from 1

    # Calculate the frequency distribution for each bin
    bin_counts = np.bincount(binned_elevations, minlength=num_bins + 1)

    # Ensure bin_counts has the same length as bin_midpoints
    bin_counts = bin_counts[:num_bins]

    # Calculate the midpoints of the bins for plotting
    bin_midpoints = (bins[:-1] + bins[1:]) / 2

    # Calculate the cell area in square miles using rough conversion
    cell_width = transform[0] * rough_units
    cell_height = abs(transform[4]) * rough_units  # Ensure the cell height is positive
    cell_area_m2 = cell_width * cell_height
    cell_area_mi2 = cell_area_m2 * 3.861e-7  # Convert square meters to square miles

    # Calculate the total area in square miles
    total_area_mi2 = len(elevation_data) * cell_area_mi2

    print(f"Cell width (m): {cell_width}")
    print(f"Cell height (m): {cell_height}")
    print(f"Cell area (m^2): {cell_area_m2}")
    print(f"Cell area (mi^2): {cell_area_mi2}")
    print(f"Total area (mi^2): {total_area_mi2}")

    print("Binned midpoints shape: ", bin_midpoints.shape)
    print("Bin counts shape: ", bin_counts.shape)
    return bin_midpoints, bin_counts, min_elevation, max_elevation, cell_area_mi2, total_area_mi2


def plot_hypsometric_curve(elevations, bin_counts, min_elevation, max_elevation, cell_area, total_area, output_path=None):
    print("Plotting hypsometric curve")
    fig, ax1 = plt.subplots(figsize=(10, 6))
    print("Elevations shape: ", elevations.shape)
    print("Bin counts shape: ", bin_counts.shape)
    heights = np.diff(elevations)
    heights = np.append(heights, heights[-1])  # Append the last height to match the length of elevations

    color_values = ["#ffebb0", "#267300", "#734d00", "#ffffff"]
    cmap = LinearSegmentedColormap.from_list("custom_cmap", color_values)

    # Normalize the elevations to the range [0, 1] for colormap
    norm_elevations = (elevations - min_elevation) / (max_elevation - min_elevation)
    colors = cmap(norm_elevations)

    # Calculate the area in square miles for each bin
    bin_areas = bin_counts * cell_area

    # Plot the original hypsometric curve
    ax1.barh(elevations, bin_areas, height=heights, align='center', label='Hypsometric Curve', color=colors)

    ax1.set_ylabel('Elevation')
    ax1.set_xlabel('Area (sq. mi)')
    ax1.set_title('Hypsometric Curve')
    ax1.grid(True)

    # Calculate the percentage that one square mile represents of the total area
    percentage_per_sq_mile = 100 / total_area

    # Create a secondary x-axis for the percentage of the area
    ax2 = ax1.twiny()
    ax2.set_xlim(ax1.get_xlim())
    ax2.set_xticks(ax1.get_xticks())

    # Ensure the number of tick labels matches the number of tick locations
    tick_labels = [x * percentage_per_sq_mile for x in ax1.get_xticks()]
    ax2.set_xticklabels([f'{x:.1f}%' for x in tick_labels])
    ax2.set_xlabel('Percentage of Area')

    plt.show()

    # save the plot if output_path is provided
    if output_path:
        plt.savefig(output_path)


def main():
    parser = argparse.ArgumentParser(description='Calculate hypsometric curve from a DEM within a polygon')
    parser.add_argument('geopackage_path', type=str, help='Path to the polygon geopackage')
    parser.add_argument('layer_name', type=str, help='Name of the layer in the geopackage')
    parser.add_argument('dem_path', type=str, help='Path to the DEM file')
    parser.add_argument('output_image', type=str, help='Path to save the hypsometric curve plot')
    args = dotenv.parse_args_env(parser)

    geopackage_path = args.geopackage_path
    layer_name = args.layer_name
    dem_path = args.dem_path
    output_image = args.output_image

    # create the output folder structure if it doesn't exist
    output_folder = os.path.dirname(output_image)
    os.makedirs(output_folder, exist_ok=True)

    # Read the polygon and DEM
    with GeopackageLayer(geopackage_path, layer_name) as polygon_layer:
        geoms = ogr.Geometry(ogr.wkbMultiPolygon)
        for feature, *_ in polygon_layer.iterate_features():
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            geoms.AddGeometry(geom)

        rough_units = 1 / polygon_layer.rough_convert_metres_to_vector_units(1.0)
        polygon_json = json.loads(geoms.ExportToJson())
    dem = rasterio.open(dem_path)

    # Mask the DEM with the polygon
    shapes = [shape(polygon_json)]
    masked_dem, _out_transform = mask(dem, shapes, crop=True)

    # Calculate the hypsometric curve with binning
    elevations, bin_counts, min_elevation, max_elevation, cell_area, total_area = calculate_hypsometric_curve(masked_dem, dem.nodata, dem.transform, rough_units)

    # Plot the hypsometric curve
    plot_hypsometric_curve(elevations, bin_counts, min_elevation, max_elevation, cell_area, total_area, output_image)


if __name__ == "__main__":
    main()
