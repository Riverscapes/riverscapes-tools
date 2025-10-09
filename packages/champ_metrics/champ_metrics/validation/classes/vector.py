from .dataset import GIS_Dataset
from champmetrics.lib.shapefileloader import Shapefile
from champmetrics.lib.exception import DataException
from .validation_classes import ValidationResult
from champmetrics.lib.raster import Raster
import json
from shapely.geometry import mapping

class CHaMP_Vector(GIS_Dataset):

    minFeatureCount = 1
    geomType = None
    fieldName_Description = "Code"

    def __init__(self, name, filepath):
        GIS_Dataset.__init__(self, name, filepath)
        self.fields = []
        self.demDataExtent = []
        self.dem = None
        self.features = self.get_shapely_feats() if self.exists() else []
        self.featureError = None
        self.spatial_reference_wkt = self.get_spatial_reference() if self.exists() else None
        self.pass_minfeatcount = None

        if not self.field_exists(self.fieldName_Description):
            if self.field_exists(str.lower(self.fieldName_Description)):
                self.fieldName_Description = str.lower(self.fieldName_Description)

    def get_shapefile(self):
        return Shapefile(self.filename)

    def get_shapely_feats(self):
        shp = self.get_shapefile()
        feats = []
        try:
            feats = shp.featuresToShapely()
        except DataException as e:
            featureError = e
        return feats

    def get_features(self):
        shp = self.get_shapefile()
        return shp.features

    def get_spatial_reference(self):
        shp = self.get_shapefile()
        return str(shp.spatialRef)
        
    def field_exists(self, fieldname, fieldtype=None):

            return True if all(fieldname in feat["fields"] for feat in self.features) else False

    def field_values_notnull(self, fieldname):
        values = self.list_attributes(fieldname)
        if None in values or "" in values:  # TODO check how null and empty string attributes are expressed.
            return False
        else:
            return True

    def list_attributes(self, fieldname):
        shp = self.get_shapefile()
        return [value[fieldname] for value in shp.attributesToList([fieldname])]

    def features_on_raster(self, filtercodes=None, raster_extent=None):
        from shapely.geometry import MultiPolygon
        raster_extent = raster_extent if raster_extent else self.demDataExtent

        if filtercodes:
            features = [feat for feat in self.features if self.fieldName_Description in feat['fields'] and feat['fields'][self.fieldName_Description] in filtercodes]
        else:
            features = self.features

        polygons = []
        if isinstance(raster_extent, (list,)):
            polygons = raster_extent
        else:
            polygons.append(raster_extent)

        for feat in features:
            result = []
            for polygon in polygons:
                polygon2 = polygon.buffer(0.1)
                result.append(polygon2.contains(feat['geometry']))

            if not any(result):
                return False

        return True

    def singlepart_features_test(self):
        if self.features:
            if any(feat['geometry'].type in ['MultiLineString', 'MultiPolygon', 'MultiPoint'] for feat in self.features):
                return False
        return True

    def validate(self):

        results = super(CHaMP_Vector, self).validate()

        validate_minfeatcount = ValidationResult(self.__class__.__name__, "MinFeatureCount")

        if len(self.features) >= self.minFeatureCount:
            validate_minfeatcount.status = "Pass"
            self.pass_minfeatcount = True
        else:
            validate_minfeatcount.status = "Error"
            validate_minfeatcount.message = "The number of features found (" + str(len(self.features)) + \
                                            ") is less than the minimum required (" + str(self.minFeatureCount) + ")"
            self.pass_minfeatcount = False

        results.append(validate_minfeatcount.get_dict())

        return results


class CHaMP_Vector_Point(CHaMP_Vector):

    geomType = "Point"

    def get_z_on_dem(self, filterlist=None):
        feats = self.features
        if feats and self.dem:
            r = Raster(self.dem)
            if filterlist:
                feats = [feat for feat in feats if feat['fields'][self.fieldName_Description] in filterlist]
            feats = [feat for feat in feats for poly in self.demDataExtent if poly.contains(feat['geometry'])]
            list_z = [r.getPixelVal([feat['geometry'].x, feat['geometry'].y]) for feat in feats]
            return list_z
        else:
            return []

    def validate(self):
        results = super(CHaMP_Vector_Point, self).validate()
        validate_geomtype = ValidationResult(self.__class__.__name__, "GeometryType")
        if self.exists():
            if self.features: # TODO improve check geom type (at least one feat must exist)
                feat = self.features[0] # Only Checking one feature right now
                if feat["geometry"].geom_type == self.geomType:
                    validate_geomtype.status = "Pass"
                else:
                    validate_geomtype.status = "Error"
                    validate_geomtype.message = "Geometry Type (" + str(self.geomType) + ") is not correct Type (" + \
                                                str(feat["geometry"].geom_type) + ")"
        results.append(validate_geomtype.get_dict())
        return results


class CHaMP_Vector_Point_3D(CHaMP_Vector_Point):

    zValues = True

    def feat_min_z(self, feats, minZ):
        for feat in feats:
            if feat['geometry'].z < minZ:
                return False
        return True

    def range_of_z_values(self, feats):
        listz = [feat['geometry'].z for feat in feats]
        minZ = min(listz)
        maxZ = max(listz)
        return maxZ - minZ

    def get_list_geoms(self):
        return [feat['geometry'] for feat in self.features]

    def validate(self):
        results = super(CHaMP_Vector_Point_3D, self).validate()
        validate_is3d = ValidationResult(self.__class__.__name__, "HasZ")
        if self.exists():
            try:
                if self.features is not None and len(self.features) > 0:
                    feat = self.features[0] # Only Checking one feature right now
                    if feat["geometry"].has_z == self.zValues:
                        validate_is3d.status = "Pass"
                    else:
                        validate_is3d.status = "Error"
                        validate_is3d.message = "Feature class is not 3D"
            except DataException as e:
                validate_is3d.status = "Error"
                validate_is3d.message = str(e)


        results.append(validate_is3d.get_dict())
        return results


class CHaMP_Vector_Polyline(CHaMP_Vector):

    geomType = "LineString"

    def feat_min_length(self, minLength):
        for feat in self.features:
            if feat['geometry'] is None:
                return False
            if feat['geometry'].length < minLength:
                return False
        return True

    def validate(self):

        results = super(CHaMP_Vector_Polyline, self).validate()

        validate_geomtype = ValidationResult(self.__class__.__name__, "GeometryType")

        if self.exists():
            if self.features:
                feat = self.features[0] # Only Checking one feature right now
                if feat["geometry"].geom_type == self.geomType: # todo: change this to all()
                    validate_geomtype.status = "Pass"
                else:
                    validate_geomtype.status = "Error"
                    validate_geomtype.message = "Geometry Type (" + str(self.geomType) + ") is not correct Type (" + \
                                                str(feat["geometry"].geom_type) + ")"

        results.append(validate_geomtype.get_dict())

        return results


class CHaMP_Vector_Polyline_LongLine(CHaMP_Vector_Polyline):

    minLengthError = 1
    minLengthWarning = 6

    def __init__(self, name, filepath):
        CHaMP_Vector_Polyline.__init__(self, name, filepath)

        self.extent_polygon = None
        self.island_polygons = []

    def closed_loop_test(self):
        if len(self.features) > 0:
            for feat in self.features:
                if not feat['geometry'].type == "MultiLineString":
                    if feat['geometry'].coords[0] == feat['geometry'].coords[-1]:
                        return False
        return True

    def start_stop_on_raster(self, raster_extent=None):
        from shapely.geometry import Point, MultiPolygon
        raster_extent = raster_extent if raster_extent else self.demDataExtent
        if self.features and raster_extent:

            # Make an array of all the extent polygons
            polygons = []
            if isinstance(raster_extent, (list,)):
                polygons = raster_extent
            else:
                polygons.append(raster_extent)

            # Loop over all the vector features
            for feat in self.features:
                results = []
                geom = feat['geometry']

                # Loop over all the extent polygons
                for polygon in polygons:

                    # simple check if the feature is entirely within the polygon
                    # this will return false if the feature touches the exterior of the polygon
                    result = geom.within(polygon)
                    if not result:
                        # Make sure that the line starts on or inside the polygon and that it doesn't
                        # cross the polygon

                        result = geom.within(polygon.buffer(0.0001))

                        # startInside = Point(geom.coords[0]).distance polygon.buffer(0.00000001)) or Point(geom.coords[0]).touches(polygon)
                        # endsInside = Point(geom.coords[-1]).within(polygon.buffer(0.00000001)) or Point(geom.coords[-1]).touches(polygon)
                        # result = startInside and endsInside and not geom.crosses(polygon)
                    results.append(result)

                    # gjl = mapping(geom)
                    # print("Line")
                    # print(json.dumps(gjl))
                    #
                    # gj = mapping(polygon)
                    # print("Polygon")
                    # print(json.dumps(gj))

                if not any(results):
                    return False

            return True

    def validate(self):

        results = super(CHaMP_Vector_Polyline_LongLine,self).validate()

        validate_minlength = ValidationResult(self.__class__.__name__, "MinLength")
        validate_singlepart = ValidationResult(self.__class__.__name__, "SinglePartFeatures")
        validate_closedloop = ValidationResult(self.__class__.__name__, "ClosedLoopFeatures")
        validate_startstopraster = ValidationResult(self.__class__.__name__, "FeaturesStartStopOnDEM")
        validate_within_extent = ValidationResult(self.__class__.__name__, "FeaturesWithinChannelExtent")
        validate_not_intersec_islands = ValidationResult(self.__class__.__name__, "FeaturesNotIntersectIslands")

        if self.exists(): #TODO test logic works here, but is not very clear.
            if not self.feat_min_length(self.minLengthError):
                validate_minlength.status = "Error"
                validate_minlength.message = "Feature has length less than minimum required (" + \
                                             str(self.minLengthError) + ")"
            elif not self.feat_min_length(self.minLengthWarning):
                validate_minlength.status = "Warning"
                validate_minlength.message = "Feature has length less than minimum desired (" + \
                                         str(self.minLengthWarning) + ")"
            else:
                validate_minlength.status = "Pass"
            if self.singlepart_features_test():
                validate_singlepart.pass_validation()
                if self.demDataExtent and self.features:
                    if self.start_stop_on_raster():
                        validate_startstopraster.pass_validation()
                    else:
                        validate_startstopraster.warning("One or more line features does not start or stop on the DEM")

                if self.closed_loop_test():
                    validate_closedloop.pass_validation()
                else:
                    validate_closedloop.error("Closed Loop Features found and are not allowed")

                if self.extent_polygon and self.features:

                    if self.start_stop_on_raster(self.extent_polygon):
                        validate_within_extent.pass_validation()
                    else:
                        validate_within_extent.warning("One or more lines not within channel extent.")

                if self.island_polygons and self.features:
                    if not any(feat['geometry'].intersects(island) for island in self.island_polygons for feat in self.features):
                        validate_not_intersec_islands.pass_validation()
                    else:
                        validate_not_intersec_islands.error("one or more lines itersects a qualifying island, which is not allowed.")
            else:
                validate_singlepart.error("Mulitpart Features found and are not allowed.")

        results.append(validate_minlength.get_dict())
        results.append(validate_singlepart.get_dict())
        results.append(validate_closedloop.get_dict())
        results.append(validate_startstopraster.get_dict())
        results.append(validate_within_extent.get_dict())
        results.append(validate_not_intersec_islands.get_dict())

        return results


class CHaMP_Polygon(CHaMP_Vector):

    geomType = ["Polygon", "MultiPolygon"]

    def feat_min_area(self, minArea):
        for feat in self.features:
            if feat['geometry'].area < minArea:
                return False
        return True

    def validate(self):

        results = super(CHaMP_Polygon, self).validate()

        validate_geomtype = ValidationResult(self.__class__.__name__, "GeometryType")

        if self.exists():
            if len(self.features) > 0:
                feat = self.features[0] # Only Checking one feature right now
                if feat["geometry"].geom_type in self.geomType:
                    validate_geomtype.pass_validation()
                else:
                    validate_geomtype.error("Geometry Type (" + str(self.geomType) + ") is not correct Type (" + \
                                                str(feat["geometry"].geom_type) + ")")
            else:
                validate_geomtype.status = "NotTested"
                validate_geomtype.message = "Geometry has no features"

        results.append(validate_geomtype.get_dict())

        return results


def polyinsidepoly(inner, outer):
    """ Tests whether all the polygons in inner are truly inside outer using a tiny buffer"""

    for innerPoly in inner.features:
        results = []

        buffered = outer.buffer(0.001)
        results.append(buffered.contains(innerPoly['geometry']))

        if not any(results):
            return False

    return True