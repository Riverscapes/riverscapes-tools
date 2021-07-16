import os
import sys
from csv import DictWriter
from math import floor

from osgeo import gdal, ogr

from rscommons import GeopackageLayer


attributes = {'HAND': 'HAND.tif',
              'ChannelDist': 'ChannelEuclideanDist.tif',
              'TWI': 'twi.tif',
              'Slope': 'slope.tif'}

observation_fields = {'observationid': ogr.OFTInteger,
                      'HUC8': ogr.OFTInteger,
                      'categoryid': ogr.OFTInteger,
                      'confidence': ogr.OFTReal,
                      'userid': ogr.OFTInteger,
                      'notes': ogr.OFTString,
                      'created_date': ogr.OFTString,
                      'updated_date': ogr.OFTString}

category_lookup = {1: 'Estimated Active Channel',
                   2: 'Highly Likely Active Floodplain',
                   3: 'Likely Active Floodplain',
                   9: 'Likely Terrace (non-valley bottom)',
                   6: 'Plausible Valley Bottom Extent',
                   4: 'Possible Active Floodplain',
                   5: 'Possible Inactive Floodplain',
                   10: 'Upland (non-valley bottom)',
                   8: 'Very Unlikely Valley Bottom Extent'}


def extract_vbet_evidence(observation_points, vbet_data_root, out_points):

    with open(out_points, 'w', newline='') as csvfile:
        writer = DictWriter(csvfile, [n for n in observation_fields] + ['category_name'] + [n for n in attributes])
        with GeopackageLayer(observation_points) as in_points:
            # GeopackageLayer(out_points, write=True) as out_layer:

            # out_layer.create_layer_from_ref(in_points)
            # for field_name in attributes:
            #     out_layer.create_field(field_name, ogr.OFTReal)
            # out_layer_defn = out_layer.ogr_layer.GetLayerDefn()

            writer.writeheader()

            for feat, *_ in in_points.iterate_features():
                print(feat.GetFID())
                feat_attributes = {name: feat.GetField(name) for name in observation_fields}

                feat_attributes['category_name'] = category_lookup[feat_attributes['categoryid']]

                geom = feat.GetGeometryRef()
                # new_feat = ogr.Feature(out_layer_defn)

                for attribute, path in attributes.items():
                    raster_path = os.path.join(vbet_data_root, feat_attributes['HUC8'], 'inputs' if attribute == 'Slope' else 'intermediates', path)
                    if os.path.exists(raster_path):
                        value = extract_raster_by_point(geom, raster_path)
                    else:
                        value = None
                    feat_attributes[attribute] = value

                # for field, value in feat_attributes.items():
                #     new_feat.SetField(field, value)

                # for field in observation_fields:
                #     value = feat.GetField(field)
                #     new_feat.SetField(field, value)

                # new_feat.SetGeometry(geom)
                # out_layer.ogr_layer.CreateFeature(new_feat)

                writer.writerow(feat_attributes)


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
