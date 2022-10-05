import csv
import sqlite3
import json
import argparse
import os
import datetime
import uuid
from rscommons import Logger, ModelConfig, RSProject, RSLayer, dotenv
from rscommons.classes.rs_project import RSMeta
from rscommons.shapefile import create_field
from rscommons.util import safe_makedirs
from osgeo import gdal, ogr, osr
import csv
from sqlbrat.__version__ import __version__

cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/fmLTPBR.xsd', __version__)
# Don't forgbet to pip install dbfread. I didn't add it to requirements.txt

LayerTypes = {
    # key: (name, id, tag, relpath)
    'Photos': RSLayer('Photos', 'PHOTO', 'Vector', 'Photos.shp'),
    'Reaches': RSLayer('Reaches', 'REACH', 'Vector', 'Reaches.shp'),
    'Structures': RSLayer('Structures', 'STRUCT', 'Vector', 'Structures.shp'),
}


REACH_FIELDS = [
    # csv_name, shp_name, type, length
    ["pk", "pk", ogr.OFTString],
    ["fk_Project", "fkProj", ogr.OFTString],
    ["Name", "Name", ogr.OFTString],
    ["StreamName", "StreamName", ogr.OFTString],
    ["Type", "Type", ogr.OFTString],
    ["ObjectivePrimary", "ObjPrimary", ogr.OFTString],
    ["ObjectiveSecondary", "ObjSecond", ogr.OFTString],
    ["ActiveFloodplainCurrent", "AFPCurrent", ogr.OFTInteger],
    ["ActiveFloodplainTarget", "AFPTarget", ogr.OFTInteger],
    ["Description", "Desc", ogr.OFTString, 254],
    ["LengthM", "LengthM", ogr.OFTReal],
    ["calc_CountComplex", "CtComplex", ogr.OFTInteger],
    ["calc_CountFeature", "CtFeature", ogr.OFTInteger],
    ["calc_CountFieldSurvey", "CtFSurvey", ogr.OFTInteger],
    ["calc_CountPhotoPoint", "CtPhPt", ogr.OFTInteger],
    ["calc_CountRiverscapeSurvey", "CtRsSurvey", ogr.OFTInteger],
    ["calc_CountVisit", "CtVisit", ogr.OFTInteger],
    ["calc_FloodplainAreaCurrentKM", "FPACurrKM", ogr.OFTReal],
    ["calc_FloodplainAreaTargetKM", "FPAreaTgKM", ogr.OFTReal],
    ["calc_StructureSumBuilt", "StSmBuilt", ogr.OFTInteger],
    ["calc_StructureSumTotal", "StSmTotal", ogr.OFTInteger],
    ["calc_StructureSumUnbuilt", "StSmUnbt", ogr.OFTInteger],
    ["DateCreated", "DateCreate", ogr.OFTString],
    ["DateModified", "DateMod", ogr.OFTString],
    ["ExportDateTime", "ExpMod", ogr.OFTString]
]

STRUCTURE_FIELDS = [
    # csv_name, shp_name, type, length
    ["pk", "pk", ogr.OFTString],
    ["fk_Complex", "fkComplex", ogr.OFTString],
    ["ObsID", "ObsID", ogr.OFTInteger],
    ["Name", "Name", ogr.OFTString],
    ["Name2", "Name2", ogr.OFTString],
    ["Count", "Count", ogr.OFTInteger],
    ["BuildDate", "BuildDate", ogr.OFTString],
    ["Description", "Desc", ogr.OFTString, 254],
    ["DateCreated", "DateCreate", ogr.OFTString],
    ["DateModified", "DateMod", ogr.OFTString],
    ["ExportDateTime", "ExpMod", ogr.OFTString]
]

PHOTO_FIELDS = [
    # csv_name, shp_name, type, length
    ["pk", "pk", ogr.OFTString],
    ["fk_Reach", "fkReach", ogr.OFTString],
    ["Name", "Name", ogr.OFTString],
    ["Description", "Desc", ogr.OFTString, 254],
    ["CameraFacing", "CamFacing", ogr.OFTString],
    ["CameraStanding", "CamStand", ogr.OFTString],
    ["calc_CountPhotos", "CtPhotos", ogr.OFTInteger],
    ["calc_MaxDate", "MaxDate", ogr.OFTString],
    ["calc_MinDate", "MinDate", ogr.OFTString],
    ["DateCreated", "DateCreate", ogr.OFTString],
    ["DateModified", "DateMod", ogr.OFTString],
    ["ExportDateTime", "ExpMod", ogr.OFTString],
    # these two come from Photo.csv
    ["PhotoDate", "PhotoDate", ogr.OFTString],
    ["PhotoNotes", "PhotoNotes", ogr.OFTString],
    # Link field
    ["Path", "Path", ogr.OFTString]
]


def csv_dict(csv_path):
    with open(csv_path, mode='r') as infile:
        reader = csv.DictReader(infile)
        print('csv_path: {}'.format(csv_path))
        return [r for r in reader]


def main(folder_path):
    csv_paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]
    csvs = {os.path.basename(f).split('.')[0]: csv_dict(f) for f in csv_paths}

    project, realization = create_project(csvs['Project'][0], folder_path)

    make_reaches(folder_path, csvs['Reach'])
    make_structures(folder_path, csvs['Structure'])
    make_photopoints(folder_path, csvs['Photo'], csvs['PhotoPoint'])

    intermediate_node = project.XMLBuilder.add_sub_element(realization, 'Intermediates')
    output_node = project.XMLBuilder.add_sub_element(realization, 'Outputs')

    for csv in csv_paths:
        relpath = os.path.relpath(csv, folder_path)
        name = os.path.basename(relpath).split('.')[0]
        nodeid = 'CSV_' + name.upper()
        project.add_project_vector(intermediate_node, RSLayer(name, nodeid, 'CSV', relpath))

    project.add_project_vector(output_node, LayerTypes['Photos'])
    project.add_project_vector(output_node, LayerTypes['Reaches'])
    project.add_project_vector(output_node, LayerTypes['Structures'])

    print("done")


def make_photopoints(folder_path, photos, photo_pts):
    log = Logger('Shapefile')
    outpath = os.path.join(folder_path, LayerTypes['Photos'].rel_path)

    safe_makedirs(os.path.dirname(outpath))

    driver = ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(outpath):
        driver.DeleteDataSource(outpath)

    # Create the output shapefile
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(int(cfg.OUTPUT_EPSG))
    outDataSource = driver.CreateDataSource(outpath)
    outLayer = outDataSource.CreateLayer('photos', outSpatialRef, geom_type=ogr.wkbPoint)

    # Add the fields we're interested in
    for field in PHOTO_FIELDS:
        csv_name = field[0]
        shp_name = field[1]
        ogr_type = field[2]
        fld = ogr.FieldDefn(shp_name, ogr_type)
        if ogr_type == ogr.OFTString and len(field) > 3:
            fld.SetWidth(field[3])
        outLayer.CreateField(fld)

    for p in photos:
        photo_point = [phpt for phpt in photo_pts if phpt['pk'] == p['fk_PhotoPoint']][0].copy()

        # Pull some fields from the other record
        photo_point['pk'] = p['pk']
        photo_point['PhotoDate'] = p['PhotoDate']
        photo_point['PhotoNotes'] = p['PhotoNotes']

        photo_point['Path'] = 'Photo/{}.jpg'.format(photo_point['pk'])
        if not os.path.isfile(os.path.join(folder_path, photo_point['Path'])):
            raise Exception('file not found: {}'.format(photo_point['Path']))

        feature = ogr.Feature(outLayer.GetLayerDefn())

        for field in PHOTO_FIELDS:
            if photo_point[csv_name]:
                # Set the attributes using the values from the delimited text file
                if ogr_type == ogr.OFTInteger:
                    feature.SetField(shp_name, int(photo_point[csv_name]))
                elif ogr_type == ogr.OFTReal:
                    feature.SetField(shp_name, float(photo_point[csv_name]))
                elif ogr_type == ogr.OFTString:
                    feature.SetField(shp_name, photo_point[csv_name])

        # create the WKT for the feature using Python string formatting
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(float(photo_point['Longitude']), float(photo_point['Latitude']))

        # Set the feature geometry using the point
        feature.SetGeometry(pt)
        # Create the feature in the layer (shapefile)
        outLayer.CreateFeature(feature)
        # Dereference the feature
        feature = None


def make_structures(folder_path, structures):
    log = Logger('Shapefile')
    outpath = os.path.join(folder_path, LayerTypes['Structures'].rel_path)

    driver = ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(outpath):
        driver.DeleteDataSource(outpath)

    # Create the output shapefile
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(int(cfg.OUTPUT_EPSG))
    outDataSource = driver.CreateDataSource(outpath)
    outLayer = outDataSource.CreateLayer('structures', outSpatialRef, geom_type=ogr.wkbPoint)

    # Add the fields we're interested in
    for field in STRUCTURE_FIELDS:
        csv_name = field[0]
        shp_name = field[1]
        ogr_type = field[2]
        fld = ogr.FieldDefn(shp_name, ogr_type)
        if ogr_type == ogr.OFTString and len(field) > 3:
            fld.SetWidth(field[3])
        outLayer.CreateField(fld)

    for struc in structures:
        feature = ogr.Feature(outLayer.GetLayerDefn())

        for field in STRUCTURE_FIELDS:
            csv_name = field[0]
            shp_name = field[1]
            ogr_type = field[2]

            if struc[csv_name]:
                # Set the attributes using the values from the delimited text file
                if ogr_type == ogr.OFTInteger:
                    feature.SetField(shp_name, int(struc[csv_name]))
                elif ogr_type == ogr.OFTReal:
                    feature.SetField(shp_name, float(struc[csv_name]))
                elif ogr_type == ogr.OFTString:
                    feature.SetField(shp_name, struc[csv_name])

        # create the WKT for the feature using Python string formatting
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(float(struc['Longitude']), float(struc['Latitude']))

        # Set the feature geometry using the point
        feature.SetGeometry(point)
        # Create the feature in the layer (shapefile)
        outLayer.CreateFeature(feature)
        # Dereference the feature
        feature = None


def make_reaches(folder_path, reaches):
    log = Logger('Shapefile')
    outpath = os.path.join(folder_path, LayerTypes['Reaches'].rel_path)

    driver = ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(outpath):
        driver.DeleteDataSource(outpath)

    # Create the output shapefile
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(int(cfg.OUTPUT_EPSG))
    outDataSource = driver.CreateDataSource(outpath)
    outLayer = outDataSource.CreateLayer('reaches', outSpatialRef, geom_type=ogr.wkbLineString)

    # Add the fields we're interested in
    for field in REACH_FIELDS:
        csv_name = field[0]
        shp_name = field[1]
        ogr_type = field[2]
        fld = ogr.FieldDefn(shp_name, ogr_type)
        if ogr_type == ogr.OFTString and len(field) > 3:
            fld.SetWidth(field[3])
        outLayer.CreateField(fld)

    for reach in reaches:
        feature = ogr.Feature(outLayer.GetLayerDefn())

        for field in REACH_FIELDS:
            csv_name = field[0]
            shp_name = field[1]
            ogr_type = field[2]

            if reach[csv_name]:
                # Set the attributes using the values from the delimited text file
                if ogr_type == ogr.OFTInteger:
                    feature.SetField(shp_name, int(reach[csv_name]))
                elif ogr_type == ogr.OFTReal:
                    feature.SetField(shp_name, float(reach[csv_name]))
                elif ogr_type == ogr.OFTString:
                    feature.SetField(shp_name, reach[csv_name])

        # create the WKT for the feature using Python string formatting
        line = ogr.Geometry(ogr.wkbLineString)
        line.AddPoint(float(reach['StartLongitude']), float(reach['StartLatitude']))
        line.AddPoint(float(reach['StopLongitude']), float(reach['StopLatitude']))

        # Set the feature geometry using the point
        feature.SetGeometry(line)
        # Create the feature in the layer (shapefile)
        outLayer.CreateFeature(feature)
        # Dereference the feature
        feature = None


def create_project(project_csv, output_dir):

    project_name = project_csv['Name']
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'fmLTPBR')

    project.add_metadata([RSMeta(k, v) for k, v in project_csv.items()])

    realization = project.add_realization(project_name, 'fmLTPBR1', cfg.version)

    project.XMLBuilder.write()
    return project, realization


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Path to folder containing csv and images', type=str)

    args = dotenv.parse_args_env(parser)

    if not os.path.isdir(args.folder):
        raise Exception('folder must exist')

    main(args.folder)
