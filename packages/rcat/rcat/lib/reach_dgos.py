import argparse
import rasterio
from shapely.ops import unary_union
from rsxml import Logger, dotenv
from rscommons import VectorBase, GeopackageLayer
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
    with rasterio.open(proj_raster) as raster:
        gt = raster.transform
        x_res = gt[0]
    polygons = {}

    # Load flowarea and waterbody geometries once if provided
    flowarea_geom = None
    waterbody_geom = None
    if flowarea:
        try:
            with get_shp_or_gpkg(flowarea) as flow_lyr:
                flow_features = [VectorBase.ogr2shapely(f.GetGeometryRef()) for f in flow_lyr.ogr_layer]
                if flow_features:
                    flowarea_geom = unary_union(flow_features)
        except Exception as e:
            log.warning(f'Could not load flowarea: {e}')

    if waterbody:
        try:
            with get_shp_or_gpkg(waterbody) as wb_lyr:
                wb_features = [VectorBase.ogr2shapely(f.GetGeometryRef()) for f in wb_lyr.ogr_layer]
                if wb_features:
                    waterbody_geom = unary_union(wb_features)
        except Exception as e:
            log.warning(f'Could not load waterbody: {e}')

    # Open DGO layer once outside the loop for better performance
    with get_shp_or_gpkg(dgos) as dgolyr, GeopackageLayer(reaches) as lyr:
        total_features = lyr.ogr_layer.GetFeatureCount()
        log.info(f'Processing {total_features} reach features')

        for feature, counter, progbar in lyr.iterate_features():
            try:
                reach_id = feature.GetFID()
                geom = feature.GetGeometryRef()

                # Clear any existing spatial filter and set new one
                dgolyr.ogr_layer.SetSpatialFilter(None)
                dgolyr.ogr_layer.SetSpatialFilter(geom)

                # Count intersecting features first to avoid loading all into memory
                intersecting_count = dgolyr.ogr_layer.GetFeatureCount()

                if intersecting_count == 0:
                    log.debug(f'Reach {reach_id} has no associated DGOs, using 100m buffer')
                    p = VectorBase.ogr2shapely(geom)
                    polygon = p.buffer(raster_buffer)
                    width = 100
                elif intersecting_count == 1:
                    # Get the single feature
                    dgolyr.ogr_layer.ResetReading()
                    ftr = next(iter(dgolyr.ogr_layer))
                    seg_area = ftr.GetField('segment_area')
                    cl_len = ftr.GetField('centerline_length')
                    polygon = VectorBase.ogr2shapely(ftr.GetGeometryRef())
                    if window_buffer:
                        polygon = polygon.buffer(window_buffer)
                    width = seg_area / cl_len if cl_len > 0 else 10
                else:
                    # Process multiple features
                    dgolyr.ogr_layer.ResetReading()
                    polys = []
                    widths = []
                    for ftr in dgolyr.ogr_layer:
                        try:
                            poly = VectorBase.ogr2shapely(ftr.GetGeometryRef())
                            if poly and poly.is_valid:
                                polys.append(poly)
                                cl_len = ftr.GetField('centerline_length')
                                seg_area = ftr.GetField('segment_area')
                                if cl_len and cl_len > 0 and seg_area:
                                    widths.append(seg_area / cl_len)
                        except Exception as e:
                            log.warning(f'Error processing DGO feature: {e}')
                            continue

                    if not polys:
                        log.warning(f'Reach {reach_id} has no valid DGO geometries, using 100m buffer')
                        p = VectorBase.ogr2shapely(geom)
                        polygon = p.buffer(raster_buffer)
                        width = 100
                    else:
                        if window_buffer:
                            polys = [poly.buffer(window_buffer) for poly in polys]
                        polygon = unary_union(polys)
                        width = min(widths) if widths else 10

                # Apply difference operations if geometries were loaded
                try:
                    if flowarea_geom and polygon.intersects(flowarea_geom):
                        polygon = polygon.difference(flowarea_geom)
                    if waterbody_geom and polygon.intersects(waterbody_geom):
                        polygon = polygon.difference(waterbody_geom)
                except Exception as e:
                    log.warning(f'Error applying flowarea/waterbody difference for reach {reach_id}: {e}')

                # buffer by raster resolution to ensure sampling of at least one pixel
                try:
                    polygon = polygon.buffer(x_res / 2)
                    polygons[reach_id] = [polygon, width]
                except Exception as e:
                    log.warning(f'Error buffering polygon for reach {reach_id}: {e}')
                    # Fallback to simple buffer
                    p = VectorBase.ogr2shapely(geom)
                    polygon = p.buffer(raster_buffer)
                    polygons[reach_id] = [polygon, 100]

                # Log progress every 1000 features
                if counter % 1000 == 0:
                    log.info(f'Processed {counter}/{total_features} reaches')

            except Exception as e:
                log.error(f'Error processing reach {reach_id}: {e}')
                # Add fallback polygon to keep processing
                try:
                    p = VectorBase.ogr2shapely(geom)
                    polygon = p.buffer(raster_buffer)
                    polygons[reach_id] = [polygon, 100]
                except Exception as ex:
                    log.error(f'Error creating fallback polygon for reach {reach_id}: {ex}')
                continue

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
