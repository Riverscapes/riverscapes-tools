import numpy as np
import rasterio
from rasterio.mask import mask
from osgeo import ogr
from shapely.geometry import Point
from shapely.ops import linemerge

from rscommons import GeopackageLayer, VectorBase
from rscommons.geometry_ops import reduce_precision, get_endpoints
from rscommons.database import SQLiteCon
from rscommons.classes.vector_base import get_utm_zone_epsg


def dgo_geometry(dgos, flowlines, dem, buffer_distance, out_gpkg_path):
    """Calculate DGO geometry BRAT attributes

    Args:
        dgos (str): Path to DGO feature class
        flowlines (str): Path to input flowlines feature class
        dem (str): Path to DEM raster
        buffer_distance (float): Buffer distance in meters
        transform (osr.CoordinateTransformation): Coordinate transformation object
        out_gpkg_path (str): Path to output geopackage"""

    buffer = VectorBase.rough_convert_metres_to_raster_units(dem, buffer_distance)

    out_da = {}
    out_slope = {}

    with GeopackageLayer(dgos) as dgos_lyr, GeopackageLayer(flowlines) as flowlines_lyr, rasterio.open(dem) as src_raster:
        nf = dgos_lyr.ogr_layer.GetNextFeature()
        gt = nf.GetGeometryRef().GetEnvelope()
        epsg = get_utm_zone_epsg(gt[1])
        transform = VectorBase.get_transform_from_epsg(dgos_lyr.spatial_ref, epsg)[1]

        for dgo_ftr, _counter, _progbar in dgos_lyr.iterate_features("Processing DGOs"):
            dgoid = dgo_ftr.GetFID()
            dgo_geom = dgo_ftr.GetGeometryRef()

            # first get the max drainage area for flowlines that intersect the DGO
            drn_area = {}
            for flowline_ftr, _counter, _progbar in flowlines_lyr.iterate_features(clip_shape=dgo_geom):

                da = flowline_ftr.GetField('DivDASqKm')
                flowline_geom = flowline_ftr.GetGeometryRef()
                line_clipped = flowline_geom.Intersection(dgo_geom)
                if da not in drn_area.keys():
                    drn_area[da] = line_clipped.Length()
                else:
                    drn_area[da] += line_clipped.Length()
            if len(drn_area) > 0:
                max_da = max(drn_area, key=drn_area.get)
            else:
                max_da = None
            out_da[dgoid] = max_da

            # then get slope
            ftrs = []
            for flowline_ftr, _counter, _progbar in flowlines_lyr.iterate_features(clip_shape=dgo_geom):
                if flowline_ftr.GetGeometryRef().GetGeometryName() == 'MULTILINESTRING':
                    for geom in flowline_ftr.GetGeometryRef():
                        ftrs.append(VectorBase.ogr2shapely(geom))
                else:
                    ftrs.append(VectorBase.ogr2shapely(flowline_ftr.GetGeometryRef()))
            if len(ftrs) > 0:
                fl_geom = linemerge(ftrs)
                fl_geom = VectorBase.shapely2ogr(fl_geom)
            else:
                out_slope[dgoid] = None
                continue
            geom_clipped = dgo_geom.Intersection(fl_geom)
            if geom_clipped.GetGeometryName() == 'MULTILINESTRING':
                geom_clipped = reduce_precision(geom_clipped, 6)
                geom_clipped = ogr.ForceToLineString(geom_clipped)
            endpoints = get_endpoints(geom_clipped)
            elevations = [None, None]
            if len(endpoints) == 2:
                elevations = []
                for pnt in endpoints:
                    point = Point(pnt)
                    polygon = point.buffer(buffer)
                    raw_raster, _out_transform = mask(src_raster, [polygon], crop=True)
                    mask_raster = np.ma.masked_values(raw_raster, src_raster.nodata)
                    value = float(mask_raster.min())
                    elevations.append(value)
                elevations.sort()
            else:
                elevations = []
                elevs = []
                for pnt in endpoints:
                    point = Point(pnt)
                    polygon = point.buffer(buffer)
                    raw_raster, _out_transform = mask(src_raster, [polygon], crop=True)
                    mask_raster = np.ma.masked_values(raw_raster, src_raster.nodata)
                    value = float(mask_raster.min())
                    elevs.append(value)
                elevations.append(min(elevs))
                elevations.append(max(elevs))
                elevations.sort()
            geom_clipped.Transform(transform)
            chan_length = geom_clipped.Length()

            out_slope[dgoid] = (elevations[1] - elevations[0]) / chan_length

    with SQLiteCon(out_gpkg_path) as db_conn:
        for dgoid, da in out_da.items():
            db_conn.curs.execute("""UPDATE DGOAttributes SET iGeo_DA = ? WHERE DGOID = ?""", (da, dgoid))
        for dgoid, slope in out_slope.items():
            db_conn.curs.execute("""UPDATE DGOAttributes SET iGeo_Slope = ? WHERE DGOID = ?""", (slope, dgoid))


# dgo_geometry('/workspaces/data/test_data/test.gpkg/vbet_dgos', '/workspaces/data/test_data/test.gpkg/network_intersected', '/workspaces/data/test_data/dem.tif', 100, '/workspaces/data/test_data/test.gpkg')
