from .vector import CHaMP_Vector_Point_3D, CHaMP_Vector
from .validation_classes import ValidationResult


class CHaMP_EdgeofWater_Points(CHaMP_Vector_Point_3D):

    maxRangeZ = 100
    maxRangeZ_dem = 100

    def __init__(self, name, filepath):
        CHaMP_Vector.__init__(self, name, filepath)

    def validate(self):
        results = super(CHaMP_EdgeofWater_Points,self).validate()

        validate_codefield = ValidationResult(self.__class__.__name__, "CodeFieldExists")
        validate_codenotnull = ValidationResult(self.__class__.__name__, "CodeFieldNotNull")
        validate_codes = ValidationResult(self.__class__.__name__, "ValidCodes")
        validate_codecount = ValidationResult(self.__class__.__name__, "EowCodeCount")
        validate_rangeZ = ValidationResult(self.__class__.__name__, "RangeZValues")
        validate_pointsondem = ValidationResult(self.__class__.__name__, "PointsOnDEM")
        validate_rangeZdem = ValidationResult(self.__class__.__name__, "RangeZValuesDEM")
        validate_negatveZdem = ValidationResult(self.__class__.__name__, "NegatveZValuesDEM")

        if self.exists():
            if self.features:
                if self.field_exists(self.fieldName_Description):
                    validate_codefield.pass_validation()
                    codes = self.list_attributes(self.fieldName_Description)
                    if self.field_values_notnull(self.fieldName_Description):
                        validate_codenotnull.pass_validation()

                        if all(x in ["lw", "rw", "mw", "br"] for x in codes):
                            validate_codes.pass_validation()
                        else:
                            validate_codes.warning("Non-standard codes found in layer.")

                        if codes.count("lw") < 3 or codes.count('rw') < 3:
                            validate_codecount.error("Number of 'lw' and 'rw' points ( " + str(codes.count("lw")) + "," +
                                                     str(codes.count("rw")) +
                                                     ") does not meet the minimum number required (3 of each type)")
                        elif codes.count("lw") < 10 or codes.count('rw') < 10:
                            validate_codecount.warning("Number of 'lw' and 'rw' points ( " + str(codes.count("lw")) + "," +
                                                       str(codes.count("rw")) +
                                                       ") does not meet the minimum number recommended (10 of each type)")
                        else:
                            validate_codecount.pass_validation()
                    else:
                        validate_codenotnull.error("Null values found in field '" + self.fieldName_Description +
                                                   "' are not allowed.")
                else:
                    validate_codefield.error("Required Field '" + self.fieldName_Description + "' does not exist.")

                rangeZ = self.range_of_z_values(self.features)
                if rangeZ > self.maxRangeZ:
                    validate_rangeZ.error("Range of Z values (" + str(rangeZ) + ") exceeds maximum allowed (" +
                                          str(self.maxRangeZ) + ")")
                else:
                    validate_rangeZ.pass_validation()
                if self.demDataExtent:
                    if self.features_on_raster():
                        validate_pointsondem.pass_validation()
                    else:
                        validate_pointsondem.warning("One or more points are not within DEM data extent.")
                if self.dem and self.features:
                    dem_z = self.get_z_on_dem()
                    range_dem_z = max(dem_z) - min(dem_z)
                    if range_dem_z < self.maxRangeZ_dem:
                        validate_rangeZdem.pass_validation()
                    else:
                        validate_rangeZdem.error("Range of dem elevations (" + str(range_dem_z) +
                                                 ") is greater than maximum value of " + str(self.maxRangeZ_dem))
                    if all(i >= 0 for i in dem_z):
                        validate_negatveZdem.pass_validation()
                    else:
                        validate_negatveZdem.error("Negative value(s) of dem elevations found, which are not allowed.")

        results.append(validate_codefield.get_dict())
        results.append(validate_codenotnull.get_dict())
        results.append(validate_codes.get_dict())
        results.append(validate_codecount.get_dict())
        results.append(validate_rangeZ.get_dict())
        results.append(validate_pointsondem.get_dict())
        results.append(validate_rangeZdem.get_dict())
        results.append(validate_negatveZdem.get_dict())

        return results