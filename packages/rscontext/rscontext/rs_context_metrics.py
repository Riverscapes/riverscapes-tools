import os
from math import pi
from rscommons import GeopackageLayer
from rscommons.classes.vector_base import VectorBase, get_utm_zone_epsg
from shapely import minimum_bounding_radius


def rscontext_metrics(project_path):
    """Calculate metrics for the context layers in a project."""

    out_metrics = {}

    with GeopackageLayer(os.path.join(project_path, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as wbd_lyr:
        long = wbd_lyr.ogr_layer.GetExtent()[0]
        proj_epsg = get_utm_zone_epsg(long)
        sref, transform = wbd_lyr.get_transform_from_epsg(wbd_lyr.spatial_ref, proj_epsg)

        ftr = wbd_lyr.ogr_layer.GetNextFeature()
        basin_area_km2 = ftr.GetField('AreaSqKm')

        geom = VectorBase.ogr2shapely(ftr, transform)
        if not geom.is_valid:
            geom = geom.buffer(0)
        
        basin_length_km = minimum_bounding_radius(geom) * 2 / 1000
        bounding_circle_area = pi * (minimum_bounding_radius(geom) / 1000) ** 2
        basin_perim_km = geom.length / 1000

        out_metrics['basinArea'] = basin_area_km2



proj_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/1601020204'

rscontext_metrics(proj_path)