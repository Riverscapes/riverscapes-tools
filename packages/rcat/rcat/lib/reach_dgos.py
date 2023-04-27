import argparse
from shapely.ops import unary_union
from rscommons import VectorBase, GeopackageLayer, Logger, dotenv
from rscommons.vector_ops import get_shp_or_gpkg


def reach_dgos(reaches: str, dgos: str, proj_raster: str, flowarea: str = None, waterbody: str = None, window_buffer=None):
    """
    Finds the DGOs that intersect each reach and removes large rivers/waterbodies

    Arguments:
        reaches (str): path to polyline reaches (layer in geopackage)
        dgos (str): path to DGOs
        proj_raster (str): path to any project raster (just for finding buffer distance)
        flowarea (str): flow area polygons to remove from output polygons
        waterbody (str): waterbody polygons to remove from output polygons
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
                    if window_buffer:
                        polygon = polygon.buffer(window_buffer)
                else:
                    polys = [VectorBase.ogr2shapely(ftr.GetGeometryRef()) for ftr in dgolyr.ogr_layer]
                    if window_buffer:
                        polys = [poly.buffer(window_buffer) for poly in polys]
                    polygon = unary_union(polys)

                if flowarea:
                    polygon = polygon.difference(flowarea)
                if waterbody:
                    polygon = polygon.difference(waterbody)

                polygons[reach_id] = polygon

    return polygons


def main():
    """Find the DGOs that intersect each reach, removing large rivers
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('reaches', help='Path to stream reaches feature class', type=str)
    parser.add_argument('dgos', help='Path to DGO feature class', type=str)
    parser.add_argument('proj_raster', help='Path to a project raster to get distance conversion', type=str)
    parser.add_argument('--flowarea', help='Path to NHD flow area feature class representing larger rivers', type=str)
    parser.add_argument('--waterbody', help='Path to NHD waterbody feature class', type=str)

    args = dotenv.parse_args_env(parser)

    reach_dgos(args.reaches, args.dgos, args.proj_raster, args.flowarea, args.waterbody)


if __name__ == '_main__':
    main()
