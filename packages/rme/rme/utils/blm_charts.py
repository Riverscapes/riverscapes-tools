
import os
import argparse

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


def charts(land_owner_shp, nhdplushr_gkpg, vbet_gpkg, ahthro_gpkg, rcat_gpkg, out_path):

    # Get the HUC10 watershed boundary
    with GeopackageLayer(nhdplushr_gkpg, 'WBDHU10') as polygon_layer:
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
    with ShapefileLayer(land_owner_shp) as land_owner:
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
    with GeopackageLayer(vbet_gpkg, 'vbet_full') as vbet_layer:
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
    horizontal_bar(values, labels, colors, 'Area (Acres)', 'Land Ownership by Area (Entire Watershed)', os.path.join(out_path, 'land_ownership_by_area.png'))
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
    horizontal_bar(values, labels, colors, 'Area (Acres)', 'Land Ownership by Area (Riverscape)', os.path.join(out_path, 'land_ownership_by_area_riverscape.png'))

    # Get the RCAT

    # LU/LC Type - hori for non-riverscape (BLM vs. Non-BLM) (We can use RCAT land use types for this.)

    # Bar or pie chart of LU/LC Type for riverscape (BLM vs. Non-BLM) (We can use RCAT land use types for this.)
    # LU/LC Change - Bar or Pie chart of LU/LC change for non-riverscape (BLM vs. Non-BLM)
    # LU/LC Change - Bar or Pie chart of LU/LC change for riverscape (BLM vs. Non-BLM)

    # Bar or pie chart of land use intensity for non-riverscape (BLM vs. non-BLM) ( get this from anthro)

    # Bar or pie chart of land use intensity for riverscape (BLM vs. non-BLM) ( get this from anthro)
    # LU/LC Type -horizontal bar chart for LULC Type for the non-riverscape (BLM vs. Non-BLM)
    # LU/LC Change - horizontal bar chart of LU/LC change for non-riverscape (BLM vs. Non-BLM)
    # land use intensity - Horizontal bar chart of land use intensity for the non-riverscape (BLM vs. non-BLM) ( get this from anthro)
    # land use intensity change - horizontal bar chart of land use intensity change for the non-riverscape (BLM vs. non-BLM) ( get this from anthro)


def main():
    parser = argparse.ArgumentParser(description='Calculate hypsometric curve from a DEM within a polygon')
    parser.add_argument('land_ownership_shp', type=str, help='Path to the land ownership shapefile')
    parser.add_argument('nhdplushr_gpkg', type=str, help='Path to the NHDPlus HR geopackage')
    parser.add_argument('vbet_gpkg', type=str, help='')
    parser.add_argument('ahthro_gpkg', type=str, help='')
    parser.add_argument('rcat_gpkg', type=str, help='')
    parser.add_argument('output_path', type=str, help='Path to save the plots')
    args = dotenv.parse_args_env(parser)

    charts(args.land_ownership_shp, args.nhdplushr_gpkg, args.vbet_gpkg, args.ahthro_gpkg, args.rcat_gpkg, args.output_path)


if __name__ == '__main__':
    main()
