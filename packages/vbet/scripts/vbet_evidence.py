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

    with open(out_points, 'w', newline='') as csvfile, \
            GeopackageLayer(observation_points) as in_points:

        writer = DictWriter(csvfile, [n for n in observation_fields] + ['category_name', 'StreamOrder', 'DrainageAreaSqkm', 'InputZone'] + [n for n in attributes])
        writer.writeheader()

        for feat, *_ in in_points.iterate_features():
            feat_attributes = {name: feat.GetField(name) for name in observation_fields}
            feat_attributes['category_name'] = category_lookup[feat_attributes['categoryid']]
            geom = feat.GetGeometryRef()

            for attribute, path in attributes.items():
                raster_path = os.path.join(vbet_data_root, feat_attributes['HUC8'], 'inputs' if attribute == 'Slope' else 'intermediates', path)
                if os.path.exists(raster_path):
                    value = extract_raster_by_point(geom, raster_path)
                else:
                    value = None
                feat_attributes[attribute] = value

            catchments_path = os.path.join(vbet_data_root, feat_attributes['HUC8'], 'inputs', 'vbet_inputs.gpkg', 'catchments')
            flowlines_path = os.path.join(vbet_data_root, feat_attributes['HUC8'], 'inputs', 'vbet_inputs.gpkg', 'Flowlines_VAA')

            with GeopackageLayer(catchments_path) as catchments, \
                    GeopackageLayer(flowlines_path) as flowlines:

                for catchment_feat, *_ in catchments.iterate_features(clip_shape=geom):
                    nhd_id = catchment_feat.GetField('NHDPlusID')

                    for flowline_feat, *_ in flowlines.iterate_features(attribute_filter=f"NHDPlusID = {nhd_id}"):
                        feat_attributes['StreamOrder'] = flowline_feat.GetField('StreamOrde')
                        feat_attributes['DrainageAreaSqkm'] = flowline_feat.GetField('TotDASqKm')

                        if feat_attributes['StreamOrder'] < 2:
                            feat_attributes['InputZone'] = 'Small'
                        elif feat_attributes['StreamOrder'] < 4:
                            feat_attributes['InputZone'] = "Medium"
                        else:
                            feat_attributes['InputZone'] = 'Large'

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
