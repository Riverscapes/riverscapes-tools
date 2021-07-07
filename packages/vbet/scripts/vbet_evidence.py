import os
import sys
from math import floor

from osgeo import gdal, ogr

from rscommons import GeopackageLayer


attributes = {'HAND': 'HAND.tif',
              'ChannelDist': 'ChannelEuclideanDist.tif',
              'TWI': 'twi.tif',
              'Slope': 'slope.tif'}


def extract_vbet_evidence(observation_points, vbet_data_root, out_points):

    with GeopackageLayer(observation_points) as in_points, \
            GeopackageLayer(out_points, write=True) as out_layer:

        out_layer.create_layer_from_ref(in_points)
        for field_name in attributes:
            out_layer.create_field(field_name, ogr.OFTReal)
        out_layer_defn = out_layer.ogr_layer.GetLayerDefn()

        for feat, *_ in in_points.iterate_features():

            huc = feat.GetField('HUC8')
            geom = feat.GetGeometryRef()
            new_feat = ogr.Feature(out_layer_defn)

            for attribute, path in attributes.items():
                raster_path = os.path.join(vbet_data_root, huc, 'inputs' if attribute == 'slope' else 'intermediates', path)
                if os.path.exists(raster_path):
                    value = extract_raster_by_point(geom, raster_path)
                else:
                    value = None
                new_feat.SetField(attribute, value)

            new_feat.SetGeometry(geom)
            out_layer.ogr_layer.CreateFeature(new_feat)


def extract_raster_by_point(geom, raster_path):

    src_ds = gdal.Open(raster_path)
    gt = src_ds.GetGeoTransform()
    rb = src_ds.GetRasterBand(1)

    mx, my = geom.GetX(), geom.GetY()  # coord in map units

    # Convert from map to pixel coordinates.
    # Only works for geotransforms with no rotation.
    px = floor((mx - gt[0]) / gt[1])  # x pixel
    py = floor((my - gt[3]) / gt[5])  # y pixel

    intval = rb.ReadAsArray(px, py, 1, 1)

    return float(intval[0][0])


def main():

    extract_vbet_evidence(sys.argv[1], sys.argv[2], sys.argv[3])


if __name__ == '__main__':
    main()
