"""
Generate Metrics JSON for a single Riverscapes Context Project
"""
import sqlite3
import argparse
import os
import traceback
import sys
from math import pi
import json
from shapely.geometry import Point
from osgeo import ogr, osr
from rscommons import GeopackageLayer, dotenv, Logger, RSProject, RSLayer, ShapefileLayer
from rscommons.classes.vector_base import VectorBase, get_utm_zone_epsg
from rscommons.raster_buffer_stats import raster_buffer_stats2


def rscontext_metrics(project_path):
    """Calculate metrics for the context layers in a project."""

    log = Logger('rscontext_metrics')
    log.info('Calculating metrics for RSContext project')

    out_metrics = {}

    with GeopackageLayer(os.path.join(project_path, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as wbd_lyr:
        long = wbd_lyr.ogr_layer.GetExtent()[0]
        proj_epsg = get_utm_zone_epsg(long)
        _sref, transform = wbd_lyr.get_transform_from_epsg(wbd_lyr.spatial_ref, proj_epsg)

        ftr = wbd_lyr.ogr_layer.GetNextFeature()
        catchment_area_km2 = ftr.GetField('AreaSqKm')

        geom = VectorBase.ogr2shapely(ftr, transform)
        if not geom.is_valid:
            geom = geom.buffer(0)

        catchment_rect = geom.minimum_rotated_rectangle
        dists = [Point(catchment_rect.exterior.coords[i]).distance(Point(catchment_rect.exterior.coords[i+1])) for i in range(4)]
        rad_dists = [Point(geom.centroid.coords).distance(Point(catchment_rect.exterior.coords[i])) for i in range(4)]

        catchment_length_km = max(dists) / 1000
        bounding_circle_area = pi * (min(rad_dists) / 1000) ** 2
        catchment_perim_km = geom.length / 1000

    with sqlite3.connect(os.path.join(project_path, 'hydrology', 'nhdplushr.gpkg')) as conn:
        cursor = conn.cursor()

        cursor.execute('SELECT HUC10, Name, States, AreaSqKm, AreaAcres FROM WBDHU10 LIMIT 1')
        row = cursor.fetchone()
        out_metrics['huc'] = row[0]
        out_metrics['hucName'] = row[1]
        out_metrics['hucStates'] = row[2]
        out_metrics['hucAreaSqKm'] = row[3]
        out_metrics['hucAreaAcres'] = row[4]

        ####################
        # Flowline metrics

        cursor.execute('SELECT Coalesce(Sum(LengthKM), 0) FROM NHDFlowline WHERE FCode IN (46006, 55800)')
        out_metrics['flowlineLengthPerennialKm'] = cursor.fetchone()[0]

        cursor.execute('SELECT Coalesce(Sum(LengthKM), 0) FROM NHDFlowline WHERE FCode = 46003')
        out_metrics['flowlineLengthIntermittentKm'] = cursor.fetchone()[0]

        cursor.execute('SELECT Coalesce(Sum(LengthKM), 0) FROM NHDFlowline WHERE FCode = 46007')
        out_metrics['flowlineLengthEphemeralKm'] = cursor.fetchone()[0]

        cursor.execute('SELECT Coalesce(Sum(LengthKM), 0) FROM NHDFlowline WHERE FCode IN (33600, 33601, 33603)')
        out_metrics['flowlineLengthCanalsKm'] = cursor.fetchone()[0]

        cursor.execute('SELECT Coalesce(Sum(LengthKM), 0), Coalesce(count(*),0) FROM NHDFlowline')
        out_metrics['flowlineLengthAllKm'], out_metrics['flowlineFeatureCount'] = cursor.fetchone()

        ####################################################################################################
        # Waterbody metrics
        cursor.execute('select coalesce(sum(AreaSqKM), 0), coalesce(count(*), 0) FROM NHDWaterbody')
        out_metrics['waterbodyAreaSqKm'], out_metrics['waterbodyFeatureCount'] = cursor.fetchone()

        # Waterbody types. The first 3 digits of the 5 digit FCode identify the waterbody "SUBTYPE"
        # https://www.usgs.gov/ngp-standards-and-specifications/national-hydrography-dataset-nhd-data-dictionary-feature-classes
        for name, fcode in [('LakesPonds', 390),  ('Reservoir', 436),     ('Estuaries', 493),     ('Playa', 361), ('SwampMarsh', 466), ('IceSnow', 378)]:
            cursor.execute('SELECT coalesce(sum(AreaSqKm), 0), coalesce(count(*), 0) FROM NHDWaterbody WHERE CAST(FCode AS TEXT) LIKE ?', [f'{fcode}%'])
            out_metrics[f'waterbody{name}AreaSqKm'], out_metrics[f'waterbody{name}FeatureCount'] = cursor.fetchone()

    ####################################################################################################
    # Raster Statistics

    proj = RSProject(None, os.path.join(project_path, 'project.rs.xml'))
    datasets_node = proj.XMLBuilder.find('Realizations').find('Realization').find('Datasets')

    for raster_id in ['DEM', 'SLOPE', 'Precip']:
        raster_path = os.path.join(project_path, datasets_node.find(f'Raster[@id="{raster_id}"]/Path').text)

        with GeopackageLayer(os.path.join(project_path, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as watershed_lyr:
            _spatial_ref, transform = VectorBase.get_transform_from_raster(watershed_lyr.spatial_ref, raster_path)
            trans_polygons = {feat.GetFID():  VectorBase.ogr2shapely(feat.GetGeometryRef(), transform) for feat, *_, in watershed_lyr.iterate_features()}
            raster_stats = raster_buffer_stats2(trans_polygons, raster_path)

            raster_slug = raster_id.lower()
            out_metrics[f'{raster_slug}Maximum'] = max(raster_stats.values(), key=lambda x: x['Maximum'])['Maximum']
            out_metrics[f'{raster_slug}Minimum'] = min(raster_stats.values(), key=lambda x: x['Minimum'])['Minimum']
            out_metrics[f'{raster_slug}Sum'] = sum(entry['Sum'] for entry in raster_stats.values())
            out_metrics[f'{raster_slug}Count'] = sum(entry['Count'] for entry in raster_stats.values())
            out_metrics[f'{raster_slug}Mean'] = out_metrics[f'{raster_slug}Sum'] / out_metrics[f'{raster_slug}Count'] if out_metrics[f'{raster_slug}Count'] > 0 else None
            out_metrics[f'{raster_slug}Range'] = out_metrics[f'{raster_slug}Maximum'] - out_metrics[f'{raster_slug}Minimum']

    out_metrics["catchmentLength"] = catchment_length_km
    out_metrics["catchmentArea"] = catchment_area_km2
    out_metrics["catchmentPerimeter"] = catchment_perim_km
    out_metrics["circularityRatio"] = catchment_area_km2 / bounding_circle_area
    out_metrics["elongationRatio"] = catchment_area_km2**0.5 / catchment_length_km
    out_metrics["formFactor"] = catchment_area_km2 / catchment_length_km**2
    out_metrics["catchmentRelief"] = out_metrics['demRange']
    out_metrics["reliefRatio"] = (out_metrics['demRange'] / 1000) / catchment_length_km
    out_metrics["drainageDensityPerennial"] = out_metrics['flowlineLengthPerennialKm'] / catchment_area_km2
    out_metrics["drainageDensityIntermittent"] = out_metrics['flowlineLengthIntermittentKm'] / catchment_area_km2
    out_metrics["drainageDensityEphemeral"] = out_metrics['flowlineLengthEphemeralKm'] / catchment_area_km2
    out_metrics["drainageDensityAll"] = (out_metrics['flowlineLengthPerennialKm'] +
                                         out_metrics['flowlineLengthIntermittentKm'] +
                                         out_metrics['flowlineLengthEphemeralKm']) / catchment_area_km2

    out_metrics['ownership'] = land_ownership(project_path)
    out_metrics_f = {'rs_context': out_metrics}

    with open(os.path.join(project_path, 'rscontext_metrics.json'), 'w', encoding='utf8') as f:
        json.dump(out_metrics_f, f, indent=2)

    proj = RSProject(None, os.path.join(project_path, 'project.rs.xml'))
    datasets_node = proj.XMLBuilder.find('Realizations').find('Realization').find('Datasets')
    proj.add_dataset(datasets_node, os.path.join(project_path, 'rscontext_metrics.json'), RSLayer('Metrics', 'Metrics', 'File', 'rscontext_metrics.json'), 'File')
    proj.XMLBuilder.write()


def land_ownership(project_path: str, ) -> dict:
    """Calculate the area of each land ownership type within the project watershed."""

    # Load the watershed geometries and determine the UTM zone
    with GeopackageLayer(os.path.join(project_path, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as polygon_layer:
        huc_srs = polygon_layer.ogr_layer.GetSpatialRef()

        # Combine all the watershed geometries into a single multi-polygon
        geom_huc10 = ogr.Geometry(ogr.wkbMultiPolygon)
        geom_huc10.AssignSpatialReference(huc_srs)
        for feature, *_ in polygon_layer.iterate_features():
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            geom_huc10.AddGeometry(geom)

        # Get the UTM zone for the centroid of the watershed
        centroid: ogr.Geometry = geom_huc10.Centroid()
        x, _y, _ = centroid.GetPoint()
        utm = get_utm_zone_epsg(x)
        srs_utm = osr.SpatialReference()
        srs_utm.ImportFromEPSG(utm)

    # Loop over all ownership polygons and intersect with watershed
    ownership_areas = {}
    with ShapefileLayer(os.path.join(project_path, 'ownership', 'ownership.shp')) as land_owner:
        for feature, *_ in land_owner.iterate_features(clip_shape=geom_huc10):
            feature: ogr.Feature
            owner = feature.GetField('ADMIN_AGEN')
            if owner is None or owner == '':
                continue

            geom: ogr.Geometry = feature.GetGeometryRef()
            geom.TransformTo(huc_srs)
            intersection = geom.Intersection(geom_huc10)
            if intersection is None or intersection.IsEmpty():
                continue
            intersection.MakeValid()
            if intersection.IsEmpty() or intersection.GetArea() == 0.0:
                continue

            if owner not in ownership_areas:
                ownership_areas[owner] = 0.0

            # Store the intersection area in the UTM projection
            intersection.TransformTo(srs_utm)
            ownership_areas[owner] += intersection.GetArea()

    return ownership_areas


def main():
    """Run this method to generate metrics for the RSContext project."""

    parser = argparse.ArgumentParser()
    parser.add_argument('project_path', help='Path to project directory', type=str)
    args = dotenv.parse_args_env(parser)

    try:
        rscontext_metrics(args.project_path)
    except Exception as e:
        Logger('rscontext_metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
