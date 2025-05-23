import os
import numpy as np
import rasterio
from osgeo import ogr, gdal
from rscommons import Logger, GeopackageLayer, VectorBase


def combine_water_features(channel: str, waterbody: str, output: str, epsg: int) -> None:
    """
    Combine water features from two layers into a single layer.

    Args:
        channel (str): Path to the channel layer.
        waterbody (str): Path to the waterbody layer.
        output (str): Path to the output layer.
    """
    logger = Logger()
    logger.info("Combining water features...")

    # Create a GeopackageLayer object for the output layer
    with GeopackageLayer(output, layer_name='water', write=True) as out_layer, \
            GeopackageLayer(channel) as channel_layer, \
            GeopackageLayer(waterbody) as waterbody_layer:

        out_layer.create_layer(ogr.wkbMultiPolygon, epsg)

        chan_spatial_ref, chan_transform = VectorBase.get_transform_from_epsg(channel_layer.spatial_ref, epsg)
        water_spatial_ref, water_transform = VectorBase.get_transform_from_epsg(waterbody_layer.spatial_ref, epsg)

        out_layer.ogr_layer.StartTransaction()

        for feat, *_ in channel_layer.iterate_features(attribute_filter='FCode IN (46006, 55800, 33600)'):
            geom = feat.GetGeometryRef()
            if chan_spatial_ref is not None:
                geom.Transform(chan_transform)
            out_feature = ogr.Feature(out_layer.ogr_layer_def)
            out_feature.SetGeometry(geom)
            out_layer.ogr_layer.CreateFeature(out_feature)

        for feat, *_ in waterbody_layer.iterate_features():  # no attribute filter for now (maybe there's ephemeral classes to not include?)
            geom = feat.GetGeometryRef()
            if water_spatial_ref is not None:
                geom.Transform(water_transform)
            out_feature = ogr.Feature(out_layer.ogr_layer_def)
            out_feature.SetGeometry(geom)
            out_layer.ogr_layer.CreateFeature(out_feature)
        out_layer.ogr_layer.CommitTransaction()

    logger.info("Water features combined successfully.")

    return


def create_water_raster(water_layer: str, output: str, raster_like: str) -> None:

    with GeopackageLayer(water_layer) as water_layer, rasterio.open(raster_like) as raster:
        # Create a new raster with the same dimensions and CRS as the input raster
        tmp_raster = os.path.join(os.path.dirname(output), 'tmp_raster.tif')

        target_ds = gdal.GetDriverByName('GTiff').Create(tmp_raster, raster.width, raster.height, 1, gdal.GDT_Int16)
        target_ds.SetGeoTransform((raster.transform[2], raster.transform[0], raster.transform[1], raster.transform[5], raster.transform[3], raster.transform[4]))
        target_ds.SetProjection(raster.crs.to_wkt())
        target_ds.GetRasterBand(1).SetNoDataValue(-9999)
        target_ds.GetRasterBand(1).Fill(0)
        # Rasterize the water features
        gdal.RasterizeLayer(target_ds, [1], water_layer.ogr_layer, burn_values=[1], options=['ALL_TOUCHED=TRUE', 'COMPRESS=LZW'])
        target_ds.FlushCache()
        target_ds = None

    with rasterio.open(raster_like) as src_a, rasterio.open(tmp_raster) as src_b:
        band_a = src_a.read(1)
        band_b = src_b.read(1)

        nodata_a = src_a.nodata
        nodata_b = src_b.nodata
        if nodata_a is None or nodata_b is None:
            raise ValueError("NoData value not set for one of the rasters.")

        mask = band_a == nodata_a
        band_b[mask] = nodata_b

        meta = src_b.meta.copy()
        with rasterio.open(output, 'w', **meta) as dst:
            dst.write(band_b, 1)

    os.remove(tmp_raster)

    print('done')
