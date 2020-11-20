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

    output_shp = os.path.join(currdir, 'data', 'WBD-8-10-12.shp')

    if os.path.exists(output_shp):
        os.remove(output_shp)

    copy_layer(os.path.join(currdir, '..', 'test', 'data', 'WBDHU8.shp'), None, output_shp, 'weird')

    print('done')


def copy_layer(in_src_path, in_lyr_name, out_src_path, out_lyr_name):
    in_driver = ogr.GetDriverByName('ESRI Shapefile')
    driver_gpkg = ogr.GetDriverByName("ESRI Shapefile")

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
    for fieldidx in range(0, in_lyr_def.GetFieldCount()):
        name = in_lyr_def.GetFieldDefn(fieldidx).GetName()
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
