import rasterio
import time
from osgeo import gdal
from rscommons import get_shp_or_gpkg
from rscommons.vector_ops import get_geometry_unary_union, VectorBase
from shapely.ops import unary_union, linemerge
from shapely.geometry import LineString, MultiLineString

# generate raster from hillshade (same size and extent) of nodata values
# for each cell
#     if cell point location intersects valley bottom
#         if line from cell point to nearest channel intersects infrastructure
#             cell value 0 - disconnected
#         else
#             cell value 1 - connected
#
# need

raster_src_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/16010202/inputs/dem_hillshade.tif'
vb_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/valley_bottom.shp'
flowlines_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/flowlines.shp'
raster_tmp = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/rastertmp2.tif'
road_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/roads.shp'
rail_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/rails.shp'
canal_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/canals.shp'
paths = {
    road_path: None, rail_path: None, canal_path: None}

# raster_ds = gdal.Open(raster_src_path)
# geot = raster_ds.GetGeoTransform()

# vec_ds = gdal.OpenEx(vb_path)
# lyr = vec_ds.GetLayer()

# drv_tiff = gdal.GetDriverByName('GTiff')

# vb_ras_ds = drv_tiff.Create(raster_tmp, raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Int16)
# vb_ras_ds.SetGeoTransform(geot)

# gdal.RasterizeLayer(vb_ras_ds, [1], lyr)
# vb_ras_ds.GetRasterBand(1).SetNoDataValue(0.0)
# vb_ras_ds = None

network = get_geometry_unary_union(flowlines_path, attribute_filter='FCode = 46006 OR FCode = 46003 OR FCode = 55800 OR FCode = 46007')
vb_union = get_geometry_unary_union(vb_path)

infr_geoms = []
for path, lines in paths.items():
    with get_shp_or_gpkg(path) as lyr:
        ftrs = [lyr.ogr_layer.GetFeature(i) for i, _ in enumerate(lyr.ogr_layer)]
        sh_ftrs = [VectorBase.ogr2shapely(ftr) for ftr in ftrs]
        final_ftrs = [f for f in sh_ftrs if f.is_valid and f.type == 'LineString']
        for f in final_ftrs:
            infr_geoms.append(f)

infra = MultiLineString(infr_geoms)
infrastructure = vb_union.intersection(infra)

info = {}

with rasterio.open(raster_tmp) as src:
    array = src.read()[0, :, :]
    print(array.shape)

    st1 = time.time()
    for y in range(array.shape[0]):
        for x in range(array.shape[1]):
            if array[y, x] == src.nodata:
                continue
            else:
                coordx = src.transform[2] + x * src.transform[0]
                coordy = src.transform[5] + y * src.transform[4]
                info[str([y, x])] = {'row': y, 'col': x, 'coords': (coordx, coordy)}
    end1 = time.time()
print(f'ellapsed for finding coordinates of raster cells: {end1-st1}')

st2 = time.time()
counter = 1
for loc, vals in info.items():
    print(f'{counter} of {len(info)}')
    stream_distance = 1000000
    # stream_coords = None
    inf_distance = 1000000
    # inf_coords = None
    for geom in network.geoms:
        for i, _ in enumerate(geom.coords.xy[0]):
            if ((vals['coords'][0] - geom.coords.xy[0][i])**2 + (vals['coords'][1] - geom.coords.xy[1][i])**2)**0.5 < stream_distance:
                # coords = (geom.coords.xy[0][i], geom.coords.xy[1][i])
                stream_distance = ((vals['coords'][0] - geom.coords.xy[0][i])**2 + (vals['coords'][1] - geom.coords.xy[1][i])**2)**0.5
    for inf_geom in infrastructure.geoms:
        for j, _ in enumerate(inf_geom.coords.xy[0]):
            if ((vals['coords'][0] - inf_geom.coords.xy[0][j])**2 + (vals['coords'][1] - inf_geom.coords.xy[1][j])**2)**0.5 < inf_distance:
                inf_distance = ((vals['coords'][0] - inf_geom.coords.xy[0][j])**2 + (vals['coords'][1] - inf_geom.coords.xy[1][j])**2)**0.5

    if inf_distance < stream_distance:
        info[loc].update({'new_val': 0.0})
        counter += 1
    else:
        info[loc].update({'new_val': 1.0})
        counter += 1
end2 = time.time()

print(f'ellapsed for calculating accessibility of raster cells: {end2-st2}')

# welp I like this algorithm but it'll take days to run...
