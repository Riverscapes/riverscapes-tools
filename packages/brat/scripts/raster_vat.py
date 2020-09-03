import csv
import sqlite3
import json
import argparse
import os
from rscommons import dotenv
from rscommons.util import safe_makedirs
from osgeo import gdal, ogr
import dbf


# Don't forgbet to pip install dbfread. I didn't add it to requirements.txt

# This is for veg classification only
LOOKUP = {
    # CSV_COLNAME: (GFT, GFU)
    # 414,10010,10,1010010,Inter-Mountain Basins Sparsely Vegetated Systems,100,100,Sparsely Vegetated,255,Sparse,255,190,1.00000000000,1.00000000000,0.74509000000
    'VALUE': (gdal.GFT_Integer, gdal.GFU_Generic),
    'R': (gdal.GFT_Integer, gdal.GFU_Red),
    'G': (gdal.GFT_Integer, gdal.GFU_Green),
    'B': (gdal.GFT_Integer, gdal.GFU_Generic),
    'RED': (gdal.GFT_Real, gdal.GFU_Blue),
    'GREEN': (gdal.GFT_Real, gdal.GFU_Green),
    'BLUE': (gdal.GFT_Real, gdal.GFU_Blue),
    # BPS Columns
    'BPS_CODE': (gdal.GFT_Integer, gdal.GFU_Generic),
    'ZONE': (gdal.GFT_String, gdal.GFU_Generic),
    'BPS_MODEL': (gdal.GFT_String, gdal.GFU_Generic),
    'BPS_NAME': (gdal.GFT_String, gdal.GFU_Name),
    'GROUPID': (gdal.GFT_Integer, gdal.GFU_Generic),
    'GROUPMODEL': (gdal.GFT_String, gdal.GFU_Generic),
    'GROUPNAME': (gdal.GFT_String, gdal.GFU_Generic),
    'GROUPVEG': (gdal.GFT_String, gdal.GFU_Generic),
    # These are EVT Columns:
    'EVT_NAME': (gdal.GFT_String, gdal.GFU_Name),
    'LFRDB': (gdal.GFT_Integer, gdal.GFU_Generic),
    'EVT_FUEL': (gdal.GFT_Integer, gdal.GFU_Generic),
    'EVT_FUEL_N': (gdal.GFT_String, gdal.GFU_Generic),
    'EVT_LF': (gdal.GFT_String, gdal.GFU_Generic),
    'EVT_GP': (gdal.GFT_Integer, gdal.GFU_Generic),
    'EVT_PHYS': (gdal.GFT_String, gdal.GFU_Generic),
    'EVT_GP_N': (gdal.GFT_String, gdal.GFU_Generic),
    'SAF_SRM': (gdal.GFT_String, gdal.GFU_Generic),
    'EVT_ORDER': (gdal.GFT_String, gdal.GFU_Generic),
    'EVT_CLASS': (gdal.GFT_String, gdal.GFU_Generic),
    'EVT_SBCLS': (gdal.GFT_String, gdal.GFU_Generic),
    # 140EVT
    'OID_': (gdal.GFT_String, gdal.GFU_Generic),
    'CLASSNAME': (gdal.GFT_String, gdal.GFU_Name)
}


def csv_dict(csv_path):
    with open(csv_path, mode='r') as infile:
        reader = csv.DictReader(infile)
        print('csv_path: {}'.format(csv_path))
        return [r for r in reader]


def main(rin, data):
    rat = gdal.RasterAttributeTable()
    rat.GetRowCount()
    rat.GetColumnCount()

    # safe_makedirs(os.path.dirname(rout))
    data_ext = os.path.splitext(data)[1]

    if data_ext == '.dbf':
        dbf_process(rat, data)
    elif data_ext == '.csv':
        csv_process(rat, data)
    else:
        raise Exception('Datafile must be DBF or CSV')

    driver = gdal.GetDriverByName("GTiff")
    ds_in = gdal.Open(rin)
    # ds_out = driver.CreateCopy(rout, ds_in, options=["TILED=YES", "COMPRESS=LZW"])
    ds_in.GetRasterBand(1).SetDefaultRAT(rat)
    ds_in = None
    # ds_out = None
    print('done')


def csv_process(rat, csv_path):
    csv_data = csv_dict(csv_path)

    # First get all the fields
    for k in csv_data[0].keys():
        kup = k.upper()
        if kup not in LOOKUP:
            raise Exception('Could not find field in lookup: {}'.format(k))
        gdal_type = LOOKUP[kup][0]
        gdal_usage = LOOKUP[kup][1]
        rat.CreateColumn(k, gdal_type, gdal_usage)

    row = 0
    for row_data in csv_data:
        col = 0
        for k, v in row_data.items():
            kup = k.upper()
            gdal_type = LOOKUP[kup][0]
            if gdal_type == gdal.GFT_Integer:
                rat.SetValueAsInt(row, col, int(v))
            elif gdal_type == gdal.GFT_Real:
                rat.SetValueAsDouble(row, col, float(v))
            elif gdal_type == gdal.GFT_String:
                rat.SetValueAsString(row, col, str(v))
            else:
                raise Exception('Type not found')
            col += 1
        row += 1

    print('here')


def dbf_process(rat, dbf_path):
    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(dbf_path)
    inLayer = data_source.GetLayerByIndex(0)
    inLayerDef = inLayer.GetLayerDefn()

    # First get all the fields
    for i in range(inLayerDef.GetFieldCount()):
        inFieldDef = inLayerDef.GetFieldDefn(i)
        fname = inFieldDef.GetName()
        typename = inFieldDef.GetTypeName()
        gdal_type = None
        if 'int' in typename.lower():
            gdal_type = gdal.GFT_Integer
        elif 'real' in typename.lower():
            gdal_type = gdal.GFT_Real
        elif 'string' in typename.lower():
            gdal_type = gdal.GFT_String
        else:
            raise Exception('Type not found')

        usage = gdal.GFU_Generic
        rat.CreateColumn(fname, gdal_type, usage)

    row = 0
    for feature in inLayer:
        for i in range(inLayerDef.GetFieldCount()):
            inFieldDef = inLayerDef.GetFieldDefn(i)
            typename = inFieldDef.GetTypeName()
            if 'int' in typename.lower():
                rat.SetValueAsInt(row, i, feature.GetField(i))
            elif 'real' in typename.lower():
                rat.SetValueAsDouble(row, i, feature.GetField(i))
            elif 'string' in typename.lower():
                rat.SetValueAsString(row, i, feature.GetField(i))
            else:
                raise Exception('Type not found')
        row += 1
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('raster_in', help='Path to Raster to be augmented', type=str)
    # parser.add_argument('raster_out', help='Path to desired output raster', type=str)
    parser.add_argument('data', help='Path to csv/dbf file containing VAT attribs', type=str)

    args = dotenv.parse_args_env(parser)

    if not os.path.isfile(args.raster_in):
        raise Exception('Input file does not exist: {}'.format(args.raster_in))

    if not os.path.isfile(args.data):
        raise Exception('Data file does not exist: {}'.format(args.data))

    # if os.path.isfile(args.raster_out):
    #     os.remove(args.raster_out)

    main(args.raster_in, args.data)

    """
    TYPES:
    ===============
    GFT_Integer = _gdalconst.GFT_Integer
    GFT_Real = _gdalconst.GFT_Real
    GFT_String = _gdalconst.GFT_String

    USAGES:
    ===============
    GFU_Generic = _gdalconst.GFU_Generic
    GFU_PixelCount = _gdalconst.GFU_PixelCount
    GFU_Name = _gdalconst.GFU_Name
    GFU_Min = _gdalconst.GFU_Min
    GFU_Max = _gdalconst.GFU_Max
    GFU_MinMax = _gdalconst.GFU_MinMax
    GFU_Red = _gdalconst.GFU_Red
    GFU_Green = _gdalconst.GFU_Green
    GFU_Blue = _gdalconst.GFU_Blue
    GFU_Alpha = _gdalconst.GFU_Alpha
    GFU_RedMin = _gdalconst.GFU_RedMin
    GFU_GreenMin = _gdalconst.GFU_GreenMin
    GFU_BlueMin = _gdalconst.GFU_BlueMin
    GFU_AlphaMin = _gdalconst.GFU_AlphaMin
    GFU_RedMax = _gdalconst.GFU_RedMax
    GFU_GreenMax = _gdalconst.GFU_GreenMax
    GFU_BlueMax = _gdalconst.GFU_BlueMax
    GFU_AlphaMax = _gdalconst.GFU_AlphaMax
    GFU_MaxCount = _gdalconst.GFU_MaxCount

    """
