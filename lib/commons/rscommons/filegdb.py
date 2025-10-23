import os
from osgeo import ogr
from osgeo import osr
import sqlite3
import argparse
from shapely.ops import transform
from functools import reduce
import json
from rscommons.util import safe_makedirs
from rscommons import ProgressBar, Logger
from rscommons.shapefile import create_field, get_transform_from_epsg
from rscommons.geometry_ops import shapely_to_ogr_geometry


def export_feature_class(filegdb, featureclass, output_dir, output_epsg, retained_fields, attribute_filter, spatial_filter):

    log = Logger('Export FC')
    log.info('Exporting geodatabase feature class {}'.format(featureclass))

    # Get the input layer
    in_driver = ogr.GetDriverByName("OpenFileGDB")
    in_datasource = in_driver.Open(filegdb, 0)
    in_layer = in_datasource.GetLayer(featureclass)
    inSpatialRef = in_layer.GetSpatialRef()

    if attribute_filter:
        log.info('Export attribute filter: {}'.format(attribute_filter))
        in_layer.SetAttributeFilter(attribute_filter)

    if spatial_filter:
        log.info('Export spatial filter area: {}'.format(spatial_filter.area))
        in_layer.SetSpatialFilter(shapely_to_ogr_geometry(spatial_filter))

    safe_makedirs(output_dir)

    # Create the output layer
    out_shapefile = os.path.join(output_dir, featureclass + '.shp')
    out_driver = ogr.GetDriverByName("ESRI Shapefile")
    outSpatialRef, transform = get_transform_from_epsg(inSpatialRef, output_epsg)

    # Remove output shapefile if it already exists
    if os.path.exists(out_shapefile):
        out_driver.DeleteDataSource(out_shapefile)

    # Create the output shapefile
    out_datasource = out_driver.CreateDataSource(out_shapefile)
    out_layer = out_datasource.CreateLayer(featureclass, outSpatialRef, geom_type=in_layer.GetGeomType())

    # Add input Layer Fields to the output Layer if it is the one we want
    in_layer_def = in_layer.GetLayerDefn()
    for i in range(0, in_layer_def.GetFieldCount()):
        field_def = in_layer_def.GetFieldDefn(i)
        field_name = field_def.GetName()
        if retained_fields and field_name not in retained_fields:
            continue

        fieldTypeCode = field_def.GetType()
        new_field_def = ogr.FieldDefn(field_name, fieldTypeCode)

        if field_name.lower() == 'nhdplusid' and fieldTypeCode == ogr.OFTReal:
            new_field_def.SetWidth(32)
            new_field_def.SetPrecision(0)
        out_layer.CreateField(new_field_def)

    # Get the output Layer's Feature Definition
    out_layer_def = out_layer.GetLayerDefn()

    # Add features to the ouput Layer
    progbar = ProgressBar(in_layer.GetFeatureCount(), 50, "Adding features to output layer")
    counter = 0
    for in_feature in in_layer:
        counter += 1
        progbar.update(counter)
        # Create output Feature
        out_feature = ogr.Feature(out_layer_def)
        geom = in_feature.GetGeometryRef()
        geom.Transform(transform)

        # Add field values from input Layer
        for i in range(0, out_layer_def.GetFieldCount()):
            field_def = out_layer_def.GetFieldDefn(i)
            field_name = field_def.GetName()
            if retained_fields and field_name not in retained_fields:
                continue

            out_feature.SetField(out_layer_def.GetFieldDefn(i).GetNameRef(), in_feature.GetField(i))

        # Set geometry as centroid
        out_feature.SetGeometry(geom)
        # Add new feature to output Layer
        out_layer.CreateFeature(out_feature)
        out_feature = None

    progbar.finish()
    # Save and close DataSources
    in_datasource = None
    out_datasource = None

    return out_shapefile


def export_table(filegdb, tablename, db_path, retained_fields, attribute_filter, indexes=None, drop=True):
    log = Logger('Export Table')
    log.info('Exporting geodatabase table {}'.format(tablename))
    in_driver = ogr.GetDriverByName("OpenFileGDB")
    in_datasource = in_driver.Open(filegdb, 0)
    in_layer = in_datasource.GetLayer(tablename)
    in_layer_def = in_layer.GetLayerDefn()

    if attribute_filter:
        log.info('Export attribute filter: {}'.format(attribute_filter))
        in_layer.SetAttributeFilter(attribute_filter)

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()

    # Draw up a dynamic schema based on the header information for this layer
    header = []
    for i in range(0, in_layer_def.GetFieldCount()):
        field_def = in_layer_def.GetFieldDefn(i)
        field_name = field_def.GetName()
        if retained_fields and field_name not in retained_fields:
            continue
        if field_def.GetType() not in OGRType2SQLITEType:
            log.warning("Can't handle Field of type: {}", field_def.GetType())
            continue
        header.append([field_name, OGRType2SQLITEType[field_def.GetType()], i])

    # Check if the table is there already and drop it if so
    curs.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{}'".format(tablename))
    table_count = curs.fetchone()[0]
    if drop and table_count == 1:
        print('Table {} exists. Dropping it'.format(tablename))
        curs.executescript('DROP TABLE {};'.format(tablename))
        table_count = 0

    # Dynamic table creation
    if table_count == 0:
        table_schema = 'CREATE TABLE {} (FID INTEGER PRIMARY KEY NOT NULL, {});'.format(tablename, ', '.join(['{} {}'.format(name, dtype) for name, dtype, idx in header]))
        curs.executescript(table_schema)

    # Now loop over the features and drop them into the DB
    progbar = ProgressBar(in_layer.GetFeatureCount(), 50, "Adding table line to db")
    counter = 0
    for in_feature in in_layer:
        counter += 1
        progbar.update(counter)
        obj = json.loads(in_feature.ExportToJson())
        sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(tablename, ','.join([name for name, dtype, idx in header]), ','.join(['?' for val in header]))
        curs.execute(sql, [obj['properties'][name] for name, dtype, idx in header])

    progbar.finish()

    if table_count == 0 and indexes and len(indexes) > 0:
        for idxfld in indexes:
            idx_name = 'IX_{}_{}'.format(tablename, idxfld)
            idx_schema = 'CREATE INDEX {} ON {} ({});'.format(idx_name, tablename, idxfld)
            curs.executescript(idx_schema)

    conn.commit()
    conn.execute("VACUUM")
    return 'hi there'


def copy_attributes(src_path, featureclass, dest_path, join_field, attributes, attribute_filter):

    # Get the input layer
    in_driver = ogr.GetDriverByName("OpenFileGDB")
    in_datasource = in_driver.Open(src_path, 0)
    in_layer = in_datasource.GetLayer(featureclass)

    # Get the output layer
    out_driver = ogr.GetDriverByName("ESRI Shapefile")
    out_datasource = out_driver.Open(dest_path, 1)
    out_layer = out_datasource.GetLayer()

    if attribute_filter:
        in_layer.SetAttributeFilter(attribute_filter)

    # Delete any existing field and re-add to the output feature class
    [create_field(out_layer, field) for field in attributes]

    values = {}

    progbarIn = ProgressBar(in_layer.GetFeatureCount(), 50, "Reading Features")
    counterIn = 0
    for feature in in_layer:
        counterIn += 1
        progbarIn.update(counterIn)

        key = feature.GetField(join_field)
        values[key] = {}
        for field in attributes:
            values[key][field] = feature.GetField(field)

    progbarIn.finish()
    in_datasource = None

    progbarOut = ProgressBar(out_layer.GetFeatureCount(), 50, "Writing Features")
    counterOut = 0
    for feature in out_layer:
        counterIn += 1
        progbarOut.update(counterOut)

        key = feature.GetField(join_field)
        if key in values:
            for field in attributes:
                if field in values[key]:
                    feature.SetField(field, values[key][field])
        out_layer.SetFeature(feature)

    progbarOut.finish()
    out_datasource = None


# NULL. The value is a NULL value.
# INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
# REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
# TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
# BLOB. The value is a blob of data, stored exactly as it was input.

OGRType2SQLITEType = {
    ogr.OFTBinary: 'INTEGER',
    ogr.OFTDate: 'TEXT',
    ogr.OFTDateTime: 'TEXT',
    ogr.OFTInteger: 'INTEGER',
    ogr.OFTInteger64: 'INTEGER',
    # ogr.OFTInteger64List: 'TEXT',
    ogr.OFTReal: 'REAL',
    # ogr.OFTRealList: 'TEXT',
    ogr.OFTString: 'TEXT',
    # ogr.OFTStringList: 'TEXT',
    ogr.OFTTime: 'TEXT',
    ogr.OFTWideString: 'TEXT',
    # ogr.OFTWideStringList: 'TEXT'
}
