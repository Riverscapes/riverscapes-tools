import argparse
import os
import traceback
import sys
from math import pi
import json
from rscommons import GeopackageLayer, Raster, dotenv, Logger, RSProject, RSLayer
from rscommons.classes.vector_base import VectorBase, get_utm_zone_epsg
from shapely.geometry import Point


def rscontext_metrics(project_path):
    """Calculate metrics for the context layers in a project."""

    log = Logger('rscontext_metrics')
    log.info('Calculating metrics for RSContext project')

    out_metrics = {}

    with GeopackageLayer(os.path.join(project_path, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as wbd_lyr:
        long = wbd_lyr.ogr_layer.GetExtent()[0]
        proj_epsg = get_utm_zone_epsg(long)
        sref, transform = wbd_lyr.get_transform_from_epsg(wbd_lyr.spatial_ref, proj_epsg)

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

    with GeopackageLayer(os.path.join(project_path, 'hydrology', 'nhdplushr.gpkg'), layer_name='NHDFlowline') as nhd_lyr:
        peren = 0
        interm = 0
        ephem = 0
        for ftr, *_ in nhd_lyr.iterate_features():
            if ftr.GetField('FCode') in (46006, 55800):
                peren += ftr.GetField('LengthKM')
            elif ftr.GetField('FCode') == 46003:
                interm += ftr.GetField('LengthKM')
            elif ftr.GetField('FCode') == 46007:
                ephem += ftr.GetField('LengthKM')

    with Raster(os.path.join(project_path, 'topography/dem.tif')) as dem_src:
        dem = dem_src.array
        dem = dem[dem != dem_src.nodata]
        relief = dem.max() - dem.min()

    out_metrics["catchmentLength"] = str(catchment_length_km)
    out_metrics["catchmentArea"] = str(catchment_area_km2)
    out_metrics["catchmentPerimeter"] = str(catchment_perim_km)
    out_metrics["circularityRatio"] = str(catchment_area_km2 / bounding_circle_area)
    out_metrics["elongationRatio"] = str(catchment_area_km2**0.5 / catchment_length_km)
    out_metrics["formFactor"] = str(catchment_area_km2 / catchment_length_km**2)
    out_metrics["catchmentRelief"] = str(relief)
    out_metrics["reliefRatio"] = str((relief / 1000) / catchment_length_km)
    out_metrics["drainageDensityPerennial"] = str(peren / catchment_area_km2)
    out_metrics["drainageDensityIntermittent"] = str(interm / catchment_area_km2)
    out_metrics["drainageDensityEphemeral"] = str(ephem / catchment_area_km2)
    out_metrics["drainageDensityAll"] = str((peren + interm + ephem) / catchment_area_km2)

    with open(os.path.join(project_path, 'rscontext_metrics.json'), 'w') as f:
        json.dump(out_metrics, f, indent=2)

    proj = RSProject(None, os.path.join(project_path, 'project.rs.xml'))
    datasets_node = proj.XMLBuilder.find('Realizations').find('Realization').find('Datasets')
    proj.add_dataset(datasets_node, os.path.join(project_path, 'rscontext_metrics.json'), RSLayer('Metrics', 'Metrics', 'File', 'rscontext_metrics.json'), 'File')
    proj.XMLBuilder.write()


def main():

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
