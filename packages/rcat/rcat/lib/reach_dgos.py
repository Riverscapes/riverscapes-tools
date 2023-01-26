import os
from shapely.ops import unary_union
from rscommons import VectorBase, GeopackageLayer, Logger
from rscommons.vector_ops import get_shp_or_gpkg


def reach_dgos(reaches: str, dgos: str, proj_raster: str, flowarea: str = None, waterbody: str = None):
    """
    reaches: path to polyline reaches (layer in geopackage)
    dgos: path to DGOs
    proj_raster: path to any project raster (just for finding buffer distance)
    flowarea: flow area polygons to remove from output polygons
    waterbody: waterbody polygons to remove from output polygons
    """
    log = Logger('Reach DGOs')

    log.info('Finding DGOs associated with each input reach')
    raster_buffer = VectorBase.rough_convert_metres_to_raster_units(proj_raster, 100)
    polygons = {}
    with GeopackageLayer(reaches) as lyr:
        for feature, _counter, _progbar in lyr.iterate_features():
            reach_id = feature.GetFID()
            geom = feature.GetGeometryRef()

            with get_shp_or_gpkg(dgos) as dgolyr:
                dgolyr.ogr_layer.SetSpatialFilter(feature.GetGeometryRef())
                if dgolyr.ogr_layer.GetFeatureCount() == 0:
                    log.info(f'feature {reach_id} has no associated DGOs, using 100m buffer')
                    polygon = VectorBase.ogr2shapely(geom).buffer(raster_buffer)
                elif dgolyr.ogr_layer.GetFeatureCount == 1:
                    ftrs = [ftr for ftr in dgolyr.ogr_layer]
                    polygon = VectorBase.ogr2shapely(ftrs[0].GetGeometryRef())
                else:
                    polys = [VectorBase.ogr2shapely(ftr.GetGeometryRef()) for ftr in dgolyr.ogr_layer]
                    polygon = unary_union(polys)

                if flowarea:
                    polygon = polygon.difference(flowarea)
                if waterbody:
                    polygon = polygon.difference(waterbody)

                polygons[reach_id] = polygon

    return polygons


rs = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/outputs/rcat.gpkg/ReachGeometry'
dgo = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/inputs.gpkg/dgo'
projras = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/inputs/existing_veg.tif'

reach_dgos(rs, dgo, projras)
