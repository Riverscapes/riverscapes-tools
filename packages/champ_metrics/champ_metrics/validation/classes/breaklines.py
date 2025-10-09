from .vector import CHaMP_Vector_Polyline, CHaMP_Vector_Point_3D
from .validation_classes import ValidationResult

class CHaMP_Breaklines(CHaMP_Vector_Polyline):

    zValues = True
    minLength = 0

    def __init__(self, name, filepath):
        CHaMP_Vector_Polyline.__init__(self, name, filepath)

        self.survey_points = None

    def validate(self):

        results = super(CHaMP_Breaklines,self).validate()

        validate_geomtype = ValidationResult(self.__class__.__name__, "GeometryType")
        validate_is3D = ValidationResult(self.__class__.__name__, "HasZ")
        validate_minlength = ValidationResult(self.__class__.__name__, "MinLength")
        validate_codefield = ValidationResult(self.__class__.__name__, "CodeFieldExists")
        validate_typefield = ValidationResult(self.__class__.__name__, "LineTypeFieldExists")
        validate_vertex_points_xy = ValidationResult(self.__class__.__name__, "VertexOnPoints")
        validate_vetext_points_z = ValidationResult(self.__class__.__name__, "VertexZmatch")

        if self.exists():
            if self.features:
                feat = self.features[0]  # Only Checking one feature right now
                if feat["geometry"].geom_type == self.geomType:
                    validate_geomtype.pass_validation()
                else:
                    validate_geomtype.error("Geometry is the wrong type.")
                if feat["geometry"].has_z is self.zValues:
                    validate_is3D.pass_validation()
                else:
                    validate_is3D.error("Line Geometry is not 3D")
                # if self.survey_points:
                #     from shapely.geometry import Point, MultiPoint
                #     mpoints = MultiPoint(self.survey_points)
                #     for feature in self.features:
                #         for vertex in list(feature['geometry'].coords):
                #             mVertex = Point(vertex)
                #             r = mpoints.contains(mVertex)
                #             pass

                    # if all():
                    #     validate_vertex_points_xy.pass_validation()
                    # else:
                    #     validate_vertex_points_xy.error("Breakline Vertex(s) do not intersect Topo or EoW Points")
                if self.feat_min_length(self.minLength):
                    validate_minlength.pass_validation()
                else:
                    validate_minlength.error("Length of one or more features is less than required minimum (" +
                                             str(self.minLength) + ") or has no geometry.")
                if self.field_exists(self.fieldName_Description):
                    validate_codefield.pass_validation()
                else:
                    validate_codefield.error("Required field '" + self.fieldName_Description + "' does not exist.")
                if self.field_exists("LineType"):
                    validate_typefield.pass_validation()
                else:
                    validate_typefield.error("Required field 'LineType' does not exist.")

        results.append(validate_geomtype.get_dict())
        results.append(validate_typefield.get_dict())
        results.append(validate_codefield.get_dict())
        results.append(validate_minlength.get_dict())
        results.append(validate_is3D.get_dict())
        results.append(validate_vertex_points_xy.get_dict())

        return results


