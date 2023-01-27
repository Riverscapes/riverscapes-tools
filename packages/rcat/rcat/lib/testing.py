from osgeo import gdal
from rscommons import VectorBase
from rscommons.vector_ops import get_shp_or_gpkg
import rasterio
from rasterio.mask import mask
import numpy as np


raster = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/intermediates/fp_access.tif'

dgo = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rcat/16010202/intermediates/single_dgo.shp'

with get_shp_or_gpkg(dgo) as lyr:
    ftr = lyr.ogr_layer.GetNextFeature()
    poly = VectorBase.ogr2shapely(ftr)

dataset = gdal.Open(raster)
geo_transform = dataset.GetGeoTransform()

conversion_factor = VectorBase.rough_convert_metres_to_raster_units(raster, 1.0)
cell_area = abs(geo_transform[1] * geo_transform[5]) / conversion_factor**2

count = 165
with rasterio.open(raster) as src:

    veg_counts = []

    raw_raster = mask(src, [poly], crop=True)[0]
    mask_raster = np.ma.masked_values(raw_raster, src.nodata)

    for oldvalue in np.unique(mask_raster):
        if oldvalue is not np.ma.masked:
            cell_count = np.count_nonzero(mask_raster == oldvalue)
            veg_counts.append([count, int(oldvalue), cell_count * cell_area, cell_count])

    print(veg_counts)
