import argparse
from osgeo import ogr
from osgeo import osr


def nhd_to_geopackage(gdb_path, geopackage_path):

    gdb_driver = ogr.GetDriverByName("OpenFileGDB")
    gdb_data_source = gdb_driver.Open(gdb_path, 0)

    gdb_layer = gdb_data_source.GetLayer('NHDFlowline')
    gdb_spatial_ref = gdb_layer.GetSpatialRef()

    gpk_driver = ogr.GetDriverByName("GPKG")
    gpk_data_source = gpk_driver.CreateDataSource(geopackage_path)

    gpk_layer = gpk_data_source.CreateLayer("NHDFlowline", gdb_spatial_ref, geom_type=ogr.wkbMultiLineString)

    idField = ogr.FieldDefn("id", ogr.OFTInteger)
    gpk_layer.CreateField(idField)
    gpk_feature_def = gpk_layer.GetLayerDefn()

    for gdb_feature in gdb_layer:
        feature = ogr.Feature(gpk_feature_def)
        feature.SetGeometry(gdb_feature.GetGeometryRef())
        feature.SetField("id", 1)
        gpk_layer.CreateFeature(feature)
        feature = None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('gdb', help='NHD file geodatabase path', type=str)
    parser.add_argument('geopackage', help='Output geopackage path', type=str)
    args = parser.parse_args()

    nhd_to_geopackage(args.gdb, args.geopackage)


if __name__ == '__main__':
    main()
