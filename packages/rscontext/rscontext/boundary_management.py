
import ogr

from rscommons.vector_ops import get_geometry_unary_union
from shapely.ops import unary_union

from rscommons.classes.vector_classes import GeopackageLayer, get_shp_or_gpkg
from rscommons.classes.raster import get_data_polygon


def raster_area_intersection(rasters, bound_layer, out_layer):

    with get_shp_or_gpkg(bound_layer) as in_lyr:
        spatial_ref = in_lyr.spatial_ref

    r_polys = []
    for raster in rasters:
        poly = get_data_polygon(raster)
        for p in poly:
            r_polys.append(p)

    raster_bound = unary_union(r_polys)
    polygon_bound = get_geometry_unary_union(bound_layer)
    out_bound = raster_bound.intersection(polygon_bound)

    with GeopackageLayer(out_layer, write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPolygon, spatial_ref=spatial_ref)
        out_lyr.create_feature(out_bound)
