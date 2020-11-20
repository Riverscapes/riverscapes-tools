from osgeo import ogr, osr, gdal
import os
import sys
import binascii
from datetime import datetime, date, time
import numpy as np
# https://gis.stackexchange.com/questions/277587/why-editing-a-geopackage-table-with-ogr-is-very-slow


def main():
    gdal.UseExceptions()  # Exceptions will get raised on anything >= gdal.CE_Failure
    ogr.UseExceptions()

    currdir = os.path.dirname(__file__)

    output_gpkg = os.path.join(currdir, '..', 'test', 'data', 'WBD-8-10-12.gpkg')

    if os.path.exists(output_gpkg):
        os.remove(output_gpkg)

    copy_layer(os.path.join(currdir, 'data', 'WBDHU8.shp'), None, output_gpkg, 'WBDHU8')
    copy_layer(os.path.join(currdir, 'data', 'WBDHU10.shp'), None, output_gpkg, 'WBDHU10')
    copy_layer(os.path.join(currdir, 'data', 'WBDHU12.shp'), None, output_gpkg, 'WBDHU12')
    copy_layer(os.path.join(currdir, 'data', 'NHDFlowline.shp'), None, output_gpkg, 'NHDFlowline')

    with open(os.path.join(currdir, 'data', 'citbeav.png'), 'rb') as f:
        content = f.read()
    binary_data = binascii.hexlify(content)

    add_weird_fields(output_gpkg, 'NHDFlowline', binary_data)

    print('done')


def add_weird_fields(src_path, lyr_name, binary_data):
    driver_gpkg = ogr.GetDriverByName("GPKG")
    src = driver_gpkg.Open(src_path, 1)

    lyr = src.GetLayer(lyr_name)
    lyr_def = lyr.GetLayerDefn()

    all_unicode = ''.join([chr(i) for i in range(32, 3000)])
    now = datetime.now()
    fields = {
        "NAME torture TeSt 0123914-_!": {
            "ogrType": ogr.OFTInteger,
            "limits": [1, 2, 3]
        },
        "TEST_BADValues": {
            "ogrType": ogr.OFTReal,
            "limits": [0, None, float("nan")]
        },
        "TEST_OFTReal": {
            "ogrType": ogr.OFTReal,
            "limits": [np.finfo('float64').max, np.finfo('float64').min]
        },
        "TEST_OFTString": {
            "ogrType": ogr.OFTString,
            "limits": ['', ' ', all_unicode, 'this is a string see?']
        },
        "TEST_OFSTInt16": {
            "ogrType": ogr.OFSTInt16,
            "limits": [float(np.iinfo('int16').max), float(np.iinfo('int16').min)]
        },
        "TEST_OFSTFloat32": {
            "ogrType": ogr.OFSTFloat32,
            "limits": [float(np.finfo('float32').max), float(np.finfo('float32').min)]
        },
        "TEST_OFSTBoolean": {
            "ogrType": ogr.OFSTBoolean,
            "limits": [True, False, None]
        },
        "TEST_OFTInteger": {
            "ogrType": ogr.OFTInteger,
            "limits": [np.iinfo('int32').max, np.iinfo('int32').min]},
        "TEST_OFTInteger64List": {
            "ogrType": ogr.OFTInteger64List,
            "limits": [[1, 2, 3, 4, 5], [np.iinfo('int64').max, np.iinfo('int64').min], [0], [8]]
        },
        "TEST_OFTInteger64": {
            "ogrType": ogr.OFTInteger64,
            "limits": [np.iinfo('int64').max, np.iinfo('int64').min]
        },
        "TEST_OFTTime": {
            "ogrType": ogr.OFTTime,
            "limits": [str(now.time())]
        },
        "TEST_OFTDate": {
            "ogrType": ogr.OFTDate,
            "limits": [str(now.date())]
        },
        "TEST_OFTDateTime": {
            "ogrType": ogr.OFTDateTime,
            "limits": [str(now)]
        },
        "TEST_OFTBinary": {
            "ogrType": ogr.OFTBinary,
            "limits": [1]
        }
    }

    for name, fld in fields.items():
        fdef = ogr.FieldDefn(name, fld['ogrType'])
        lyr.CreateField(fdef)

    lyr.StartTransaction()

    counter = 0
    for feat in lyr:
        counter += 1
        # Add field values from input Layer
        for fname, fld in fields.items():
            val = fld['limits'][counter % len(fld['limits'])]
            idx = lyr_def.GetFieldIndex(fname)
            if (fld['ogrType'] is ogr.OFTInteger64List):
                feat.SetFieldInteger64List(idx, val)
            elif (fld['ogrType'] is ogr.OFTBinary):
                feat.SetFieldBinaryFromHexString(idx, str(binary_data))
            else:
                feat.SetField(fname, val)
            lyr.SetFeature(feat)

    lyr.CommitTransaction()

    # Clean up
    src.Destroy()


def copy_layer(in_src_path, in_lyr_name, out_src_path, out_lyr_name):
    in_driver = ogr.GetDriverByName('ESRI Shapefile')
    driver_gpkg = ogr.GetDriverByName("GPKG")

    out_spatial_ref = osr.SpatialReference()
    out_spatial_ref.ImportFromEPSG(4326)

    out_src = driver_gpkg.Open(out_src_path, 1)
    if not out_src:
        out_src = driver_gpkg.CreateDataSource(out_src_path)

    in_src = in_driver.Open(in_src_path)
    in_lyr = in_src.GetLayer(in_lyr_name if in_lyr_name is not None else 0)

    out_lyr = out_src.CreateLayer(out_lyr_name, out_spatial_ref, geom_type=in_lyr.GetGeomType(), options=['FID=fid2'])
    in_lyr_def = in_lyr.GetLayerDefn()

    # Create manual FID
    # out_lyr.CreateField(ogr.FieldDefn('fid2', ogr.OFTInteger))
    for fieldidx in range(0, in_lyr_def.GetFieldCount()):
        name = in_lyr_def.GetFieldDefn(fieldidx).GetName()
        print(name)
        out_lyr.CreateField(in_lyr_def.GetFieldDefn(fieldidx))

    out_lyr_def = out_lyr.GetLayerDefn()
    out_lyr.StartTransaction()

    counter = 0
    for in_feature in in_lyr:
        counter += 20
        out_feature = ogr.Feature(out_lyr_def)

        # Set the geom
        geom = in_feature.GetGeometryRef()
        out_feature.SetGeometry(geom)

        # Add field values from input Layer
        # out_feature.SetField('fid2', counter)
        for i in range(0, in_lyr_def.GetFieldCount()):
            out_feature.SetField(in_lyr_def.GetFieldDefn(i).GetNameRef(), in_feature.GetField(i))

        out_lyr.CreateFeature(out_feature)
        out_feature = None

    out_lyr.CommitTransaction()

    # Clean up
    in_src.Destroy()
    out_src.Destroy()


if __name__ == '__main__':
    main()

    """


layer.StartTransaction()
layer.CommitTransaction()



    """
