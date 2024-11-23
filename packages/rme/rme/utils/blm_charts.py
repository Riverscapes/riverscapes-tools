
import os
import json
import argparse

import rasterio
from rasterio.mask import mask
from shapely.geometry import shape
from osgeo import ogr, osr

from rscommons import dotenv
from rscommons.plotting import horizontal_bar, vertical_bar
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.classes.vector_classes import ShapefileLayer, GeopackageLayer

from .blm_classes import existing_vegetation_types, rcat_conversion

land_owhership_colors = {
    'BIA': '#ffb554',
    'BLM': '#ffea8c',
    'USBR': '#1b007c',
    'FWS': '#5bb3a6',
    'USFS': '#9fddae',
    'DOE': '#d8a287',
    'DOD': '#fab1ff',
    'ST': '#73b2ff',
    'LG': '#bed2ff',
    'PVT': '#e1e1e1',
    'UND': '#cccccc'
}

land_ownership_labels = {
    'BIA': 'Bureau of Indian Affairs (BIA)',
    'BLM': 'Bureau of Land Management (BLM)',
    'USBR': 'Bureau of Reclamation (USBR)',
    'FWS': 'US Fish and Wildlife Service (FWS)',
    'USFS': 'US Forest Service (USFS)',
    'DOE': 'Department of Energy (DOE)',
    'DOD': 'Department of Defense (DOD)',
    'ST': 'State',
    'LG': 'Local',
    'PVT': 'Private',
    'UND': 'Unknown'
}

land_use_intensity_labels = {
    0: 'Very Low',
    33: 'Low',
    66: 'Moderate',
    100: 'High'
}

land_use_intensity_colors = {
    0: '#267300',
    33: '#a4c400',
    66: '#ffbb00',
    100: '#ff2600'
}


existing_vegetation_colors = {
    "Open tree canopy": "#b4ff94",
    "Closed tree canopy": "#003300",
    "Dwarf-shrubland": "#c7b081",
    "Shrubland": "#826548",
    "Sparse tree canopy": "#3792ad",
    "Herbaceous - shrub-steppe": "#c7b081",
    "Herbaceous - grassland": "#ffe282",
    "Non-vegetated": "#0000ff",
    "No Dominant Lifeform": "#ff7a8f",
    "Sparsely vegetated": "#646464",
}

rcat_conversion_colors = {
    "From Conifer to Riparian": "#f2d1e4",
    "From Devegetated to Riparian": "#f7d6a7",
    "From Grass/Shrubland to Riparian": "#d7ebf2",
    "Non-Riparian Conversion": "#ffffff",
    "Negligible to Minor Veg. Conversion": "#6dbe45",
    "Conv. To Grass/Shrubland": "#36c3f2",
    "Devegetation": "#a6742a",
    "Conifer Encroachment": "#d74699",
    "Conv. To Invasive": "#894c9e",
    "Development": "#ed2024",
    "Conv. to Agriculture": "#e5e515",
    "Multiple Conv. Types": "#4f4f4f",
    "Non-Riparian Conversions": "#f9720b"
}


def charts(rsc_project_folder, vbet_project_folder, rcat_project_folder, anthro_project_folder, rme_project_folder, out_path) -> dict:

    output_charts = {
        'Land Ownership by Area (Entire Watershed)': os.path.join(out_path, 'land_ownership_by_area.png'),
        'Land Ownership by Area (Riverscape)': os.path.join(out_path, 'land_ownership_by_area_riverscape.png'),
        'Land Ownership by Mileage': os.path.join(out_path, 'land_ownership_by_mileage.png'),
        'Land Use Intensity (BLM)': os.path.join(out_path, 'land_use_intensity_blm.png'),
        'Land Use Intensity (Non-BLM)': os.path.join(out_path, 'land_use_intensity_non_blm.png'),
        'Land Use Type (BLM)': os.path.join(out_path, 'land_use_type_blm.png'),
        'Land Use Type (Non-BLM)': os.path.join(out_path, 'land_use_type_non_blm.png'),
        'Land Use Conversion (BLM)': os.path.join(out_path, 'land_use_conversion_blm.png'),
        'Land Use Conversion (Non-BLM)': os.path.join(out_path, 'land_use_conversion_non_blm.png'),
    }

    # Get the HUC10 watershed boundary
    with GeopackageLayer(os.path.join(rsc_project_folder, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as polygon_layer:
        srs = polygon_layer.ogr_layer.GetSpatialRef()
        geom_huc10 = ogr.Geometry(ogr.wkbMultiPolygon)
        geom_huc10.AssignSpatialReference(srs)
        for feature, *_ in polygon_layer.iterate_features():
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            geom_huc10.AddGeometry(geom)
        centroid: ogr.Geometry = geom_huc10.Centroid()
        x, y, _ = centroid.GetPoint()
        utm = get_utm_zone_epsg(x)

        # Create the spatial reference and import the EPSG code
        srs_utm = osr.SpatialReference()
        srs_utm.ImportFromEPSG(utm)

        # geom_huc10.TransformTo(srs_utm)

    # Get the BLM and Non-BLM land ownership within the HUC10 boundary
    with ShapefileLayer(os.path.join(rsc_project_folder, 'ownership', 'ownership.shp')) as land_owner:
        geom_blm = ogr.Geometry(ogr.wkbMultiPolygon)
        geom_blm.AssignSpatialReference(srs)
        geom_non_blm = ogr.Geometry(ogr.wkbMultiPolygon)
        geom_non_blm.AssignSpatialReference(srs)
        geoms_non_blm = {}
        for feature, *_ in land_owner.iterate_features(clip_shape=geom_huc10):
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            geom_intersection: ogr.Geometry = geom.Intersection(geom_huc10)
            geom_intersection.MakeValid()
            if geom_intersection.IsEmpty() or geom_intersection.GetArea() == 0.0:
                continue
            if feature.GetField('ADMIN_AGEN') == 'BLM':
                if geom_intersection.GetGeometryType() == ogr.wkbMultiPolygon:
                    for g in geom_intersection:
                        geom_blm.AddGeometry(g)
                else:
                    geom_blm.AddGeometry(geom_intersection.Clone())
            else:
                geom_non_blm.AddGeometry(geom_intersection)
                if feature.GetField('ADMIN_AGEN') not in geoms_non_blm:
                    g = ogr.Geometry(ogr.wkbMultiPolygon)
                    g.AssignSpatialReference(srs)
                    geoms_non_blm[feature.GetField('ADMIN_AGEN')] = g
                if geom_intersection.GetGeometryType() == ogr.wkbMultiPolygon:
                    for g in geom_intersection:
                        geoms_non_blm[feature.GetField('ADMIN_AGEN')].AddGeometry(geom_intersection.Clone())
                else:
                    geoms_non_blm[feature.GetField('ADMIN_AGEN')].AddGeometry(geom_intersection.Clone())
            geom_intersection = None
        geom_blm.TransformTo(srs_utm)
        geom_non_blm.TransformTo(srs_utm)
        for geom in geoms_non_blm.values():
            geom.TransformTo(srs_utm)

    # Get the riverscape (vbet-dgo polygons) vs non-riverscape areas
    with GeopackageLayer(os.path.join(vbet_project_folder, 'outputs', 'vbet.gpkg'), 'vbet_full') as vbet_layer:
        geom_riverscape = ogr.Geometry(ogr.wkbMultiPolygon)
        geom_riverscape.AssignSpatialReference(srs)
        for feature, *_ in vbet_layer.iterate_features(clip_shape=geom_huc10):
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            geom_intersection: ogr.Geometry = geom.Intersection(geom_huc10)
            geom_intersection.MakeValid()
            if geom_intersection.IsEmpty() or geom_intersection.GetArea() == 0.0:
                continue
            geom_riverscape.AddGeometry(geom_intersection)
        geom_riverscape.TransformTo(srs_utm)

    # LAND OWNERSHIP
    # Horizontal Bar Chart of land ownesrship by area for watershed (non-riverscape)
    area_blm_land_ownership_acres = geom_blm.GetArea() * 0.000247105
    areas = {}
    for land_owner, geom in geoms_non_blm.items():
        areas[land_owner] = geom.GetArea() * 0.000247105
    areas['BLM'] = area_blm_land_ownership_acres

    values = list(areas.values())
    labels = [land_ownership_labels.get(l, l) for l in areas.keys()]
    colors = [land_owhership_colors.get(l, '#000000') for l in areas.keys()]
    horizontal_bar(values, labels, colors, 'Area (Acres)', 'Land Ownership by Area (Entire Watershed)', output_charts['Land Ownership by Area (Entire Watershed)'])
    # pie(values, labels, 'Land Ownership by Area', colors, os.path.join(out_path, 'land_ownership_by_area_pie.png'))

    # Horizontal Bar Chart of land ownesrship by area for riverscape
    geom_blm_riverscape = geom_riverscape.Intersection(geom_blm)
    area_blm_land_ownership_riverscape_acres = geom_blm_riverscape.GetArea() * 0.000247105
    riverscape_areas = {}
    for land_owner, geom in geoms_non_blm.items():
        riverscape_areas[land_owner] = geom.Intersection(geom_riverscape).GetArea() * 0.000247105
    riverscape_areas['BLM'] = area_blm_land_ownership_riverscape_acres

    values = list(riverscape_areas.values())
    labels = [land_ownership_labels.get(l, l) for l in riverscape_areas.keys()]
    colors = [land_owhership_colors.get(l, '#000000') for l in riverscape_areas.keys()]
    horizontal_bar(values, labels, colors, 'Area (Acres)', 'Land Ownership by Area (Riverscape)', output_charts['Land Ownership by Area (Riverscape)'])

    with GeopackageLayer(os.path.join(rsc_project_folder, 'hydrology', 'hydro_derivatives.gpkg'), 'network_intersected') as network_intersected_lyr:
        lengths = {}
        for feature, *_ in network_intersected_lyr.iterate_features(clip_shape=geom_huc10):
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            geom_intersection: ogr.Geometry = geom.Intersection(geom_huc10)
            geom_intersection.MakeValid()
            # transform to UTM
            geom_intersection.TransformTo(srs_utm)
            if geom_intersection.IsEmpty() or geom_intersection.Length() == 0.0:
                continue
            ownership = feature.GetField('ownership')
            if ownership not in lengths:
                lengths[ownership] = 0.0
            mileage = geom_intersection.Length() * 0.000621371
            lengths[ownership] += mileage
        total_length = sum(lengths.values())

    values = list(lengths.values())
    labels = [land_ownership_labels.get(l, l) for l in lengths.keys()]
    labels = [l if l is not None else '' for l in labels]
    colors = [land_owhership_colors.get(l, '#000000') for l in lengths.keys()]
    horizontal_bar(values, labels, colors, 'Length (mileage)', 'Land Ownership by Mileage', output_charts['Land Ownership by Mileage'])

    # get back to the original srs
    geom_blm.TransformTo(srs)
    geom_non_blm.TransformTo(srs)

    with GeopackageLayer(os.path.join(rsc_project_folder, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as polygon_layer:
        rough_units = 1 / polygon_layer.rough_convert_metres_to_vector_units(1.0)

    # RCAT
    # LU/LC Type - hori for non-riverscape (BLM vs. Non-BLM) (We can use RCAT land use types for this.)
    with rasterio.open(os.path.join(rcat_project_folder, 'inputs', 'existing_veg.tif')) as raster:
        no_data = int(raster.nodata)
        cell_width = raster.transform[0] * rough_units
        cell_height = abs(raster.transform[4]) * rough_units  # Ensure the cell height is positive
        cell_area_m2 = cell_width * cell_height
        cell_area_acres = cell_area_m2 * 0.000247105

        shapes_blm = [shape(json.loads(geom_blm.ExportToJson()))]
        masked, *_ = mask(raster, shapes_blm, crop=True)
        # get the count of each land use intensity value
        raster_counts_blm = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            name = existing_vegetation_types.get(value, 'Unknown')
            if name in raster_counts_blm:
                raster_counts_blm[name] += 1
            else:
                raster_counts_blm[name] = 1

        shapes_non_blm = [shape(json.loads(geom_non_blm.ExportToJson()))]
        masked, *_ = mask(raster, shapes_non_blm, crop=True)
        # get the count of each land use intensity value
        raster_counts_non_blm = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            name = existing_vegetation_types.get(value, 'Unknown')
            if name in raster_counts_non_blm:
                raster_counts_non_blm[name] += 1
            else:
                raster_counts_non_blm[name] = 1

    # Land Use Type BLM
    values = list(value * cell_area_acres for value in raster_counts_blm.values())
    labels = list(raster_counts_blm.keys())
    colors = [existing_vegetation_colors.get(l, '#000000') for l in raster_counts_blm.keys()]
    horizontal_bar(values, labels, colors, 'Acres', 'Land Use Type (BLM)', output_charts['Land Use Type (BLM)'])

    # Land Use Type Non-BLM
    values = list(value * cell_area_acres for value in raster_counts_non_blm.values())
    labels = list(raster_counts_non_blm.keys())
    colors = [existing_vegetation_colors.get(l, '#000000') for l in raster_counts_non_blm.keys()]
    horizontal_bar(values, labels, colors, 'Acres', 'Land Use Type (Non-BLM)', output_charts['Land Use Type (Non-BLM)'])

    # LU/LC Change - Bar or Pie chart of LU/LC change for non-riverscape (BLM vs. Non-BLM)
    with rasterio.open(os.path.join(rcat_project_folder, 'intermediates', 'conversion.tif')) as raster:
        no_data = int(raster.nodata)
        cell_width = raster.transform[0] * rough_units
        cell_height = abs(raster.transform[4]) * rough_units  # Ensure the cell height is positive
        cell_area_m2 = cell_width * cell_height
        cell_area_acres = cell_area_m2 * 0.000247105

        shapes_blm = [shape(json.loads(geom_blm.ExportToJson()))]
        masked, *_ = mask(raster, shapes_blm, crop=True)
        # get the count of each land use intensity value
        raster_counts_blm = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            name = rcat_conversion.get(value, 'Unknown')
            if name in raster_counts_blm:
                raster_counts_blm[name] += 1
            else:
                raster_counts_blm[name] = 1

        shapes_non_blm = [shape(json.loads(geom_non_blm.ExportToJson()))]
        masked, *_ = mask(raster, shapes_non_blm, crop=True)
        # get the count of each land use intensity value
        raster_counts_non_blm = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            name = rcat_conversion.get(value, 'Unknown')
            if name in raster_counts_non_blm:
                raster_counts_non_blm[name] += 1
            else:
                raster_counts_non_blm[name] = 1

    # Land Use Type BLM
    values = list(value * cell_area_acres for value in raster_counts_blm.values())
    labels = list(raster_counts_blm.keys())
    colors = [rcat_conversion_colors.get(l, '#000000') for l in raster_counts_blm.keys()]
    horizontal_bar(values, labels, colors, 'Acres', 'Land Use Change (BLM)', output_charts['Land Use Conversion (BLM)'])

    # Land Use Type Non-BLM
    values = list(value * cell_area_acres for value in raster_counts_non_blm.values())
    labels = list(raster_counts_non_blm.keys())
    colors = [rcat_conversion_colors.get(l, '#000000') for l in raster_counts_non_blm.keys()]
    horizontal_bar(values, labels, colors, 'Acres', 'Land Use Change (Non-BLM)', output_charts['Land Use Conversion (Non-BLM)'])

    # land use intensity - Horizontal bar chart of land use intensity for the non-riverscape (BLM vs. non-BLM) ( get this from anthro)
    with rasterio.open(os.path.join(anthro_project_folder, 'intermediates', 'lui.tif')) as raster_lui:
        no_data = int(raster_lui.nodata)
        cell_width = raster_lui.transform[0] * rough_units
        cell_height = abs(raster_lui.transform[4]) * rough_units  # Ensure the cell height is positive
        cell_area_m2 = cell_width * cell_height
        cell_area_acres = cell_area_m2 * 0.000247105

        shapes_blm = [shape(json.loads(geom_blm.ExportToJson()))]
        masked, *_ = mask(raster_lui, shapes_blm, crop=True)
        # get the count of each land use intensity value
        land_use_intensity_counts_blm = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            if value in land_use_intensity_counts_blm:
                land_use_intensity_counts_blm[value] += 1
            else:
                land_use_intensity_counts_blm[value] = 1

        shapes_non_blm = [shape(json.loads(geom_non_blm.ExportToJson()))]
        masked, *_ = mask(raster_lui, shapes_non_blm, crop=True)
        # get the count of each land use intensity value
        land_use_intensity_counts_non_blm = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            if value in land_use_intensity_counts_non_blm:
                land_use_intensity_counts_non_blm[value] += 1
            else:
                land_use_intensity_counts_non_blm[value] = 1

    # BLM land use intensity
    values = list(value * cell_area_acres for value in land_use_intensity_counts_blm.values())
    labels = [land_use_intensity_labels.get(l, l) for l in land_use_intensity_counts_blm.keys()]
    colors = [land_use_intensity_colors.get(l, '#000000') for l in land_use_intensity_counts_blm.keys()]
    horizontal_bar(values, labels, colors, 'Acres', 'Land Use Intensity (BLM)', output_charts['Land Use Intensity (BLM)'])

    # Non-BLM land use intensity
    values = list(value * cell_area_acres for value in land_use_intensity_counts_non_blm.values())
    labels = [land_use_intensity_labels.get(l, l) for l in land_use_intensity_counts_non_blm.keys()]
    colors = [land_use_intensity_colors.get(l, '#000000') for l in land_use_intensity_counts_non_blm.keys()]
    horizontal_bar(values, labels, colors, 'Acres', 'Land Use Intensity (Non-BLM)', output_charts['Land Use Intensity (Non-BLM)'])

    # land use intensity change - horizontal bar chart of land use intensity change for the non-riverscape (BLM vs. non-BLM) ( get this from anthro)

    return output_charts


def vegetation_charts(rcat_project_folder, out_path) -> dict:
    """
    Generates vegetation charts for a given RCAT project folder and saves them to the specified output path.
    Args:
        rcat_project_folder (str): The path to the RCAT project folder containing input data.
        out_path (str): The path where the output charts will be saved.
    Returns:
        dict: A dictionary containing the paths to the generated charts
    """

    output_charts = {
        'LANDFIRE Existing Vegetation Type (EVT)': os.path.join(out_path, 'evt_riverscapes.png'),
        'LANDFIRE Biophysical Setting (BPS)': os.path.join(out_path, 'bps_riverscapes.png')
    }

    # read the historic veg.clr file next to this file
    historic_vegetation_labels, historic_vegetation_colors = read_clr_file(os.path.join(os.path.dirname(__file__), 'historic_veg.clr'))

    # Get the riverscape (vbet-dgo polygons) vs non-riverscape areas
    with GeopackageLayer(os.path.join(rcat_project_folder, 'inputs', 'inputs.gpkg'), 'dgo') as dgo_layer:
        srs = dgo_layer.ogr_layer.GetSpatialRef()
        rough_units = 1 / dgo_layer.rough_convert_metres_to_vector_units(1.0)

        # geom_riverscape = ogr.Geometry(ogr.wkbMultiPolygon)
        # geom_riverscape.AssignSpatialReference(srs)
        shapes_riverscape = []
        for feature, *_ in dgo_layer.iterate_features():
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            if geom.IsEmpty() or geom.GetArea() == 0.0:
                continue
            if not geom.IsValid():
                geom.MakeValid()
            shapes_riverscape.append(shape(json.loads(geom.ExportToJson())))

            # if geom.GetGeometryType() == ogr.wkbMultiPolygon:
            #     for g in geom:
            #         geom_riverscape.AddGeometry(g.Clone())
            # else:
            #     geom_riverscape.AddGeometry(geom.Clone())
            # if not geom_riverscape.IsValid():
            #     geom_riverscape.MakeValid()

    # RCAT
    # LU/LC Type - hori for riverscape (We can use RCAT land use types for this.)
    with rasterio.open(os.path.join(rcat_project_folder, 'inputs', 'existing_veg.tif')) as raster:
        no_data = int(raster.nodata)
        cell_width = raster.transform[0] * rough_units
        cell_height = abs(raster.transform[4]) * rough_units  # Ensure the cell height is positive
        cell_area_m2 = cell_width * cell_height
        cell_area_acres = cell_area_m2 * 0.000247105

        masked, *_ = mask(raster, shapes_riverscape, crop=True)
        # get the count of each land use intensity value
        raster_counts = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            name = existing_vegetation_types.get(value, 'Unknown')
            if name in raster_counts:
                raster_counts[name] += 1
            else:
                raster_counts[name] = 1

    # Land Use Type Riverscape
    values = list(value * cell_area_acres for value in raster_counts.values())
    labels = list(str(key) for key in raster_counts)
    colors = [existing_vegetation_colors.get(l, '#000000') for l in raster_counts.keys()]
    # horizontal_bar(values, labels, colors, 'Acres', 'LANDFIRE Existing Vegetation Type (EVT)', output_charts['LANDFIRE Existing Vegetation Type (EVT)'])
    vertical_bar(values, labels, 'Acres of Riverscape', 'LANDFIRE Existing Vegetation Type (EVT)', output_charts['LANDFIRE Existing Vegetation Type (EVT)'], colors)

    # BPS Historic Land Use Type - hori for riverscape (We can use RCAT land use types for this.)
    with rasterio.open(os.path.join(rcat_project_folder, 'inputs', 'historic_veg.tif')) as raster:
        no_data = int(raster.nodata)
        cell_width = raster.transform[0] * rough_units
        cell_height = abs(raster.transform[4]) * rough_units  # Ensure the cell height is positive
        cell_area_m2 = cell_width * cell_height
        cell_area_acres = cell_area_m2 * 0.000247105

        masked, *_ = mask(raster, shapes_riverscape, crop=True)
        # get the count of each land use intensity value
        raster_counts = {}
        for value in masked.flatten():
            value = int(value)
            if value == no_data:
                continue
            name = value
            # name = rcat_conversion.get(value, 'Unknown')
            if name in raster_counts:
                raster_counts[name] += 1
            else:
                raster_counts[name] = 1

    # Historic Land Use Type Riverscape
    values = list(value * cell_area_acres for value in raster_counts.values())
    labels = list(historic_vegetation_labels.get(str(key), 'Unknown') for key in raster_counts)
    colors = [historic_vegetation_colors.get(str(l), '#000000') for l in raster_counts]
    # horizontal_bar(values, labels, colors, 'Acres', 'Historic Land Use Type (Riverscapes)', output_charts['Historic Land Use Type (Riverscapes)'])
    vertical_bar(values, labels, 'Acres of Riverscape', 'LANDFIRE Biophysical Setting (BPS)', output_charts['LANDFIRE Biophysical Setting (BPS)'], colors)

    return output_charts


def read_clr_file(file_path):

    # clr file is space-delimited text with the following format for each line: key, r, g, b, alpha and description (which can have spaces)
    # need to output two dicts, one with the key: description and the other with the key: #rrggbb

    with open(file_path, 'r') as f:
        lines = f.readlines()

    clr_dict = {}
    clr_hex_dict = {}
    for line in lines:
        parts = line.strip().split(' ')
        key = parts[0]
        r = int(parts[1])
        g = int(parts[2])
        b = int(parts[3])
        _a = int(parts[4])
        description = ' '.join(parts[5:])
        clr_dict[key] = description
        clr_hex_dict[key] = f'#{r:02x}{g:02x}{b:02x}'
    return clr_dict, clr_hex_dict


def riparian_charts(rme_project: str, riparian_gpkg: str) -> dict:

    rme_gpkg = os.path.join(rme_project, 'outputs', 'riverscapes_metrics.gpkg')

    results = {}

    # Loop over each VBET polygon
    with GeopackageLayer(rme_gpkg, 'rme_dgos') as rme_lyr:
        for rme_feature, *_ in rme_lyr.iterate_features():
            rme_feature: ogr.Feature
            rme_ownership = rme_feature.GetField('rme_dgo_ownership')
            rme_ownership = rme_ownership if rme_ownership == 'BLM' else 'Non-BLM'
            dgo_geom: ogr.Geometry = rme_feature.GetGeometryRef()

            # Get the riparian area within the VBET polygon
            with GeopackageLayer(riparian_gpkg, 'riparian') as riparian_lyr:

                # transform the VBET geometry to the riparian layer's srs
                riparian_lyr_srs = riparian_lyr.ogr_layer.GetSpatialRef()
                dgo_geom.TransformTo(riparian_lyr_srs)

                for riparian_feature, *_ in riparian_lyr.iterate_features(clip_shape=dgo_geom):
                    riparian_feature: ogr.Feature
                    riparian_class = riparian_feature.GetField('WETLAND_TY')
                    riparian_geom: ogr.Geometry = riparian_feature.GetGeometryRef()

                    # intersect the VBET geometry with the riparian geometry
                    riparian_geom: ogr.Geometry = riparian_geom.Intersection(dgo_geom)
                    riparian_geom.MakeValid()
                    if riparian_geom.IsEmpty() or riparian_geom.GetArea() == 0.0:
                        continue

                    if rme_ownership not in results:
                        results[rme_ownership] = {}

                    if riparian_class not in results[rme_ownership]:
                        results[rme_ownership][riparian_class] = 0.0

                    results[rme_ownership][riparian_class] += riparian_geom.GetArea()

    return results


def main():
    parser = argparse.ArgumentParser(description='Calculate hypsometric curve from a DEM within a polygon')
    parser.add_argument('riverscapes_context_project_folder', type=str, help='Path to the riverscapes context project folder')
    parser.add_argument('vbet_project_folder', type=str, help='Path to the vbet project folder')
    parser.add_argument('rcat_project_folder', type=str, help='Path to the rcat project folder')
    parser.add_argument('ahthro_project_folder', type=str, help='Path to the anthro project folder')
    parser.add_argument('rme_project_folder', type=str, help='Path to the rme project folder')
    parser.add_argument('riparian_gpkg', type=str, help='Path to the bespoke BLM riparian GeoPackage')
    parser.add_argument('output_path', type=str, help='Path to save the plots')
    args = dotenv.parse_args_env(parser)

    # out_charts = charts(args.riverscapes_context_project_folder, args.vbet_project_folder, args.rcat_project_folder, args.ahthro_project_folder, args.rme_project_folder, args.output_path)
    # veg_charts = vegetation_charts(args.rcat_project_folder, args.output_path)
    riparian_charts(args.rme_project_folder, args.riparian_gpkg)

    # add the vegetation charts to the output charts
    # out_charts.update(veg_charts)


if __name__ == '__main__':
    main()
