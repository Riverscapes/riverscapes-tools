from .vector import CHaMP_Vector_Point_3D, CHaMP_Vector
from .validation_classes import ValidationResult

class CHaMP_TopoPoints(CHaMP_Vector_Point_3D):

    code_count_bf_error = 3
    code_count_bf_warning = 20
    code_count_tb_error = 1
    def __init__(self, name, filepath):
        CHaMP_Vector.__init__(self, name, filepath)

    def get_in_point(self):
        pnt = [feat['geometry'] for feat in self.features if self.fieldName_Description in feat['fields'] and feat['fields'][self.fieldName_Description] == "in"]
        return pnt[0] if len(pnt) == 1 else None

    def get_out_point(self):
        pnt = [feat['geometry'] for feat in self.features if self.fieldName_Description in feat['fields'] and feat['fields'][self.fieldName_Description] == "out"]
        return pnt[0] if len(pnt) == 1 else None

    def validate(self, raster=None):

        results = super(CHaMP_TopoPoints, self).validate()

        validate_codefield = ValidationResult(self.__class__.__name__, "CodeFieldExists")
        validate_codenotnull = ValidationResult(self.__class__.__name__, "CodeFieldNotNull")
        validate_bfcount = ValidationResult(self.__class__.__name__, "bfCount")
        validate_tbcount = ValidationResult(self.__class__.__name__, "tbCount")
        validate_incount = ValidationResult(self.__class__.__name__, "inCount")
        validate_outcount = ValidationResult(self.__class__.__name__, "outCount")
        validate_pointsondem = ValidationResult(self.__class__.__name__, "PointsOnDEM")
        validate_tbondem = ValidationResult(self.__class__.__name__, "tbPointsOnDEM")
        validate_bfondem = ValidationResult(self.__class__.__name__, "bfPointsOnDEM")
        validate_inoutpointsDEM = ValidationResult(self.__class__.__name__, "InOutPointsOnDEMwithPosElev")
        validate_inhigherthatnoutpointsDEM = ValidationResult(self.__class__.__name__, "InHigherThanOutPointDEM")

        if self.exists():
            if self.field_exists(self.fieldName_Description):
                validate_codefield.pass_validation()
                if self.field_values_notnull(self.fieldName_Description):
                    validate_codenotnull.pass_validation()
                    codes = self.list_attributes(self.fieldName_Description)
                    if codes.count("bf") < self.code_count_bf_error:
                        validate_bfcount.error("Number of 'bf' points (" + str(codes.count("bf")) +
                                               ") are less than the required amount (" +
                                               str(self.code_count_bf_error) + ").")
                    elif codes.count("bf") < self.code_count_bf_warning:
                        validate_bfcount.warning("Number of 'bf' points (" + str(codes.count("bf")) +
                                               ") are less than the recommended amount (" +
                                               str(self.code_count_bf_warning) + ").")
                    else:
                        validate_bfcount.pass_validation()
                    if codes.count("tb") < self.code_count_tb_error:
                        validate_tbcount.warning("Number of 'tb' points (" + str(codes.count("tb")) +
                                               ") are less than the recommended amount (" +
                                               str(self.code_count_tb_error) + ").")
                    else:
                        validate_tbcount.pass_validation()
                    if codes.count("in") != 1:
                        validate_incount.error("Number of 'in' points (" + str(codes.count("in")) +
                                                ") are not the required amount of 1 point")
                    else:
                        validate_incount.pass_validation()
                    if codes.count("out") != 1:
                        validate_outcount.error("Number of 'out' points (" + str(codes.count("out")) +
                                                ") are not the required amount of 1 point")
                    else:
                        validate_outcount.pass_validation()
                    if codes.count('in') == 1 and codes.count('out') == 1 and self.dem and self.demDataExtent:
                        in_out_z = self.get_z_on_dem(["in"]) + self.get_z_on_dem(["out"])
                        if self.features_on_raster(["in", "out"]):
                            if all(i >= 0 for i in in_out_z):
                                validate_inoutpointsDEM.pass_validation()
                            else:
                                validate_inoutpointsDEM.error("Negative DEM elevations for in/out points found, which are not allowed")
                        else:
                            validate_inoutpointsDEM.error("in/out points not within DEM Data Extent.")
                        if in_out_z[0] > in_out_z[1]:
                           validate_inhigherthatnoutpointsDEM.pass_validation()
                        else:
                            validate_inhigherthatnoutpointsDEM.warning("Elevation of 'in' point (" + str(in_out_z[0]) +
                                                                       "is lower than elevation of 'out' point (" +
                                                                       str(in_out_z[1]) + ").")
                else:
                    validate_codenotnull.error("Null values found in field '" + self.fieldName_Description + "' are not allowed.")
            else:
                validate_codefield.error("Required Field '" + self.fieldName_Description + "' does not exist.")

            if self.demDataExtent:
                if self.features_on_raster():
                    validate_pointsondem.pass_validation()
                else:
                    validate_pointsondem.warning("One or more points are not within DEM data extent.")
                if self.features_on_raster(['tb']):
                    validate_tbondem.pass_validation()
                else:
                    validate_tbondem.warning("One or more tb points are not within DEM data extent.")
                if self.features_on_raster(['bf']):
                    validate_bfondem.pass_validation()
                else:
                    validate_bfondem.warning("One or more bf points are not within DEM data extent.")

        results.append(validate_codefield.get_dict())
        results.append(validate_codenotnull.get_dict())
        results.append(validate_bfcount.get_dict())
        results.append(validate_tbcount.get_dict())
        results.append(validate_incount.get_dict())
        results.append(validate_outcount.get_dict())
        results.append(validate_pointsondem.get_dict())
        results.append(validate_tbondem.get_dict())
        results.append(validate_bfondem.get_dict())
        results.append(validate_inoutpointsDEM.get_dict())
        results.append(validate_inhigherthatnoutpointsDEM.get_dict())

        return results
