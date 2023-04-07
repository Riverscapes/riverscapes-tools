""" Script to grab vbet evidience for observation points
"""

import os
import sqlite3
import sys
from csv import DictWriter
from math import floor

from osgeo import gdal, ogr

from rscommons import GeopackageLayer, get_shp_or_gpkg


attributes = {'HAND': 'intermediates/hand_composite.tif',
              'Slope': 'inputs/slope.tif'}

observation_fields = {'observationid': ogr.OFTInteger,
                      # 'HUC8': ogr.OFTInteger,
                      'categoryid': ogr.OFTInteger,
                      'confidence': ogr.OFTReal,
                      'userid': ogr.OFTInteger,
                      'notes': ogr.OFTString,
                      'created_date': ogr.OFTString,
                      'updated_date': ogr.OFTString}

oldcategory_lookup = {1: 'Estimated Active Channel',
                      2: 'Highly Likely Active Floodplain',
                      3: 'Likely Active Floodplain',
                      9: 'Likely Terrace (non-valley bottom)',
                      6: 'Plausible Valley Bottom Extent',
                      4: 'Possible Active Floodplain',
                      5: 'Possible Inactive Floodplain',
                      10: 'Upland (non-valley bottom)',
                      8: 'Very Unlikely Valley Bottom Extent'}

category_lookup = {11: 'Active Channel Area',
                   12: 'Active Floodplain',
                   13: 'Inactive Floodplain',
                   14: 'Terrace',
                   15: 'Probably not Valley Bottom',
                   16: 'Definately not Valley Bottom',
                   17: 'Fan - Active Channel',
                   18: 'Fan - Active Floodplain',
                   19: 'Fan - Inactive Floodplain'}


def extract_vbet_evidence(observation_points, vbet_data_root, out_points):

    csv_mode = 'w'
    # existing_ids = []
    # if os.path.exists(out_points):

    #     with open(out_points, 'r'):

    #     csv_mode = 'a'

    with open(out_points, csv_mode, newline='') as csvfile, \
            GeopackageLayer(observation_points) as in_points:

        writer = DictWriter(csvfile, [n for n in observation_fields] + ['category_name', 'StreamOrder', 'DrainageAreaSqkm', 'InputZone'] + [n for n in attributes])
        if csv_mode == 'w':
            writer.writeheader()

        for feat, *_ in in_points.iterate_features():
            feat_attributes = {name: feat.GetField(name) for name in observation_fields}
            feat_attributes['category_name'] = category_lookup[feat_attributes['categoryid']]
            geom = feat.GetGeometryRef()

            huc = str(feat_attributes['HUC8']).zfill(8)

            for attribute, path in attributes.items():
                raster_path = os.path.join(vbet_data_root, path['project'], huc, path['folder'], path['name'])
                if os.path.exists(raster_path):
                    value = extract_raster_by_point(geom, raster_path)
                else:
                    value = None
                feat_attributes[attribute] = value

            # catchments_path = os.path.join(vbet_data_root, feat_attributes['HUC8'], 'inputs', 'vbet_inputs.gpkg', 'catchments')
            # flowlines_path = os.path.join(vbet_data_root, feat_attributes['HUC8'], 'inputs', 'vbet_inputs.gpkg', 'Flowlines_VAA')
            catchments_path = os.path.join(vbet_data_root, 'rs_context', huc, 'hydrology', 'NHDPlusCatchment.shp')
            flowlines_path = os.path.join(vbet_data_root, 'rs_context', huc, 'hydrology', 'nhd_data.sqlite')

            with get_shp_or_gpkg(catchments_path) as catchments:

                for catchment_feat, *_ in catchments.iterate_features(clip_shape=geom):
                    nhd_id = catchment_feat.GetField('NHDPlusID')

                    with sqlite3.connect(flowlines_path) as conn:

                        curs = conn.cursor()
                        if len(curs.execute('select * from NHDPlusFlowlineVAA where NHDPlusID = ?', (nhd_id,)).fetchall()) > 0:
                            feat_attributes['StreamOrder'] = curs.execute('select StreamOrde from NHDPlusFlowlineVAA where NHDPlusID = ?', (nhd_id,)).fetchall()[0][0]
                            feat_attributes['DrainageAreaSqkm'] = curs.execute('select TotDASqKm from NHDPlusFlowlineVAA where NHDPlusID = ?', (nhd_id,)).fetchall()[0][0]

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
