
import os
import json
import argparse

import rasterio
from rasterio.mask import mask
from shapely.geometry import shape
from osgeo import ogr, osr

from rscommons import dotenv
from rscommons.plotting import horizontal_bar, pie
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.classes.vector_classes import ShapefileLayer, GeopackageLayer

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


def charts(rsc_project_folder, vbet_project_folder, rcat_project_folder, anthro_project_folder, rme_project_folder, out_path):

    output_charts = {
        'Land Ownership by Area (Entire Watershed)': os.path.join(out_path, 'land_ownership_by_area.png'),
        'Land Ownership by Area (Riverscape)': os.path.join(out_path, 'land_ownership_by_area_riverscape.png'),
        'Land Ownership by Mileage': os.path.join(out_path, 'land_ownership_by_mileage.png'),
        'Land Use Intensity (BLM)': os.path.join(out_path, 'land_use_intensity_blm.png'),
        'Land Use Intensity (Non-BLM)': os.path.join(out_path, 'land_use_intensity_non_blm.png')
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
    colors = [land_owhership_colors.get(l, '#000000') for l in lengths.keys()]
    horizontal_bar(values, labels, colors, 'Length (mileage)', 'Land Ownership by Mileage', output_charts['Land Ownership by Mileage'])

    # RCAT
    # LU/LC Type - hori for non-riverscape (BLM vs. Non-BLM) (We can use RCAT land use types for this.)

    # load the lu raster
    # lu_path = os.path.join(rcat_gpkg, 'lu')
    # with rasterio.open(lu_path) as raster:
    #     shapes_blm = [shape(json.loads(geom_blm.ExportToJson()))]
    #     masked_raster_blm, _out_transform = mask(raster, shapes_blm, crop=True)

    #     shapes_non_blm = [shape(json.loads(geom_non_blm.ExportToJson()))]
    #     masked_raster_non_blm, _out_transform = mask(raster, shapes_non_blm, crop=True)

    # Bar or pie chart of LU/LC Type for riverscape (BLM vs. Non-BLM) (We can use RCAT land use types for this.)
    # LU/LC Change - Bar or Pie chart of LU/LC change for non-riverscape (BLM vs. Non-BLM)
    # LU/LC Change - Bar or Pie chart of LU/LC change for riverscape (BLM vs. Non-BLM)

    # Bar or pie chart of land use intensity for non-riverscape (BLM vs. non-BLM) ( get this from anthro)

    # Bar or pie chart of land use intensity for riverscape (BLM vs. non-BLM) ( get this from anthro)
    # LU/LC Type -horizontal bar chart for LULC Type for the non-riverscape (BLM vs. Non-BLM)
    # LU/LC Change - horizontal bar chart of LU/LC change for non-riverscape (BLM vs. Non-BLM)

    # land use intensity - Horizontal bar chart of land use intensity for the non-riverscape (BLM vs. non-BLM) ( get this from anthro)
    with rasterio.open(os.path.join(anthro_project_folder, 'intermediates', 'lui.tif')) as raster_lui:

        no_data = int(raster_lui.nodata)

        geom_blm.TransformTo(srs)  # get back to the original srs
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

        geom_non_blm.TransformTo(srs)  # get back to the original srs
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
    values = list(land_use_intensity_counts_blm.values())
    labels = [land_use_intensity_labels.get(l, l) for l in land_use_intensity_counts_blm.keys()]
    colors = [land_use_intensity_colors.get(l, '#000000') for l in land_use_intensity_counts_blm.keys()]
    horizontal_bar(values, labels, colors, 'Cell Count', 'Land Use Intensity (BLM)', output_charts['Land Use Intensity (BLM)'])

    # Non-BLM land use intensity
    values = list(land_use_intensity_counts_non_blm.values())
    labels = [land_use_intensity_labels.get(l, l) for l in land_use_intensity_counts_non_blm.keys()]
    colors = [land_use_intensity_colors.get(l, '#000000') for l in land_use_intensity_counts_non_blm.keys()]
    horizontal_bar(values, labels, colors, 'Cell Count', 'Land Use Intensity (Non-BLM)', output_charts['Land Use Intensity (Non-BLM)'])

    # land use intensity change - horizontal bar chart of land use intensity change for the non-riverscape (BLM vs. non-BLM) ( get this from anthro)

    return output_charts


def main():
    parser = argparse.ArgumentParser(description='Calculate hypsometric curve from a DEM within a polygon')
    parser.add_argument('riverscapes_context_project_folder', type=str, help='Path to the riverscapes context project folder')
    parser.add_argument('vbet_project_folder', type=str, help='Path to the vbet project folder')
    parser.add_argument('rcat_project_folder', type=str, help='Path to the rcat project folder')
    parser.add_argument('ahthro_project_folder', type=str, help='Path to the anthro project folder')
    parser.add_argument('rme_project_folder', type=str, help='Path to the rme project folder')
    parser.add_argument('output_path', type=str, help='Path to save the plots')
    args = dotenv.parse_args_env(parser)

    out_charts = charts(args.riverscapes_context_project_folder, args.vbet_project_folder, args.rcat_project_folder, args.ahthro_project_folder, args.rme_project_folder, args.output_path)


if __name__ == '__main__':
    main()
