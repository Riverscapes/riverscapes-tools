from osgeo import ogr
import json
import os
from shapely.geometry import shape
from rscommons import Logger
from .exception import DataException


class Shapefile:

    def __init__(self, sFilename=None):
        self.filename = sFilename
        self.driver = ogr.GetDriverByName("ESRI Shapefile")
        self.datasource = None
        self.fields = {}
        self.features = {}
        self.log = Logger("Shapefile")
        self.loaded = False
        if sFilename and os.path.isfile(sFilename):
            self.load(sFilename)
            self.loaded = True

    def load(self, sFilename):
        dataSource = self.driver.Open(sFilename, 0)
        self.layer = dataSource.GetLayer()
        self.spatialRef = self.layer.GetSpatialRef()

        self.getFieldDef()
        self.getFeatures()

    def create(self, sFilename, spatialRef=None, geoType=ogr.wkbMultiLineString):
        if os.path.exists(sFilename):
            self.driver.DeleteDataSource(sFilename)
        self.driver = None
        self.driver = ogr.GetDriverByName("ESRI Shapefile")
        self.datasource = self.driver.CreateDataSource(sFilename)
        self.layer = self.datasource.CreateLayer(sFilename, spatialRef, geom_type=geoType)

    def createField(self, fieldName, ogrOFT):
        """
        Create a field on the layer
        :param fieldName:
        :param ogrOFT:
        :return:
        """
        aField = ogr.FieldDefn(fieldName, ogrOFT)
        self.layer.CreateField(aField)

    def getFieldDef(self):
        self.fields = {}
        lyrDefn = self.layer.GetLayerDefn()
        for i in range(lyrDefn.GetFieldCount()):
            fieldName = lyrDefn.GetFieldDefn(i).GetName()
            fieldTypeCode = lyrDefn.GetFieldDefn(i).GetType()
            fieldType = lyrDefn.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
            fieldWidth = lyrDefn.GetFieldDefn(i).GetWidth()
            GetPrecision = lyrDefn.GetFieldDefn(i).GetPrecision()

            self.fields[fieldName] = {
                'fieldName': fieldName,
                'fieldTypeCode': fieldTypeCode,
                'fieldType': fieldType,
                'fieldWidth': fieldWidth,
                'GetPrecision': GetPrecision
            }

    def attributesToList(self, desiredFields):
        if len(self.features) == 0:
            return []

        feats = []
        for feat in self.features:
            fields = {}
            for aField in desiredFields:
                fields[aField] = feat.GetField(aField)

            feats.append(fields)
        return feats

    def getFeatures(self):

        self.features = []
        for feat in self.layer:
            self.features.append(feat)

    def featuresToShapely(self):
        if len(self.features) == 0:
            return []

        feats = []
        for feat in self.features:
            # Very hacky, but some line features will not load json at all and unable to test if geometry type == unknown.
            try:
                featobj = json.loads(feat.ExportToJson())
            except:
                geom = ogr.CreateGeometryFromWkt(feat.geometry().ExportToWkt())
                featobj = {
                    "geometry": json.loads(geom.ExportToJson())}
            # TODO: ExportToJSON seems to fail when type: LineStringZM but it seems to work with this hack
            # Essentially converting it to WKT and then back to JSON solves the problem
            # if featobj['geometry']['type'] == 'Unknown':
            #     geom = ogr.CreateGeometryFromWkt(feat.geometry().ExportToWkt())
            #     featobj = {
            #         "geometry": json.loads(geom.ExportToJson())
            #     }

            fields = {}
            for f in self.fields:
                fields[f] = feat.GetField(f)

            try:
                feats.append({
                    'FID': feat.GetFID(),
                    'geometry': shape(featobj['geometry']) if not featobj["geometry"] is None else None,  # case when feature contains no geometry (rare)
                    'fields': fields
                })
            except ValueError as e:
                # Problem with shapefile geometry should raise the right kind of exception
                raise DataException("Shapefile error: {}".format(e))

        return feats
