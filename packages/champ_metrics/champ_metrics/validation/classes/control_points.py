from .vector import CHaMP_Vector_Point_3D, CHaMP_Vector
from .validation_classes import ValidationResult


class CHaMP_ControlPoints(CHaMP_Vector_Point_3D):

    minZ = 0
    maxRangeZ = 100

    def __init__(self, name, filepath):
        CHaMP_Vector.__init__(self, name, filepath)

    def validate(self):

        results = super(CHaMP_ControlPoints ,self).validate()

        validate_codefield = ValidationResult(self.__class__.__name__, "CodeFieldExists")
        validate_codenotnull = ValidationResult(self.__class__.__name__, "CodeFieldNotNull")
        validate_typefield = ValidationResult(self.__class__.__name__, "TypeFieldExists")
        validate_rangeZ = ValidationResult(self.__class__.__name__, "RangeZValues")
        validate_minZ = ValidationResult(self.__class__.__name__, "MinimumZValue")
        validate_nofeatures = ValidationResult(self.__class__.__name__, "MinFeaturesCount")

        if self.exists():
            if self.features:
                validate_nofeatures.pass_validation()
                if self.field_exists(self.fieldName_Description):
                    validate_codefield.pass_validation()
                    if self.field_values_notnull(self.fieldName_Description):
                        validate_codenotnull.pass_validation()
                    else:
                        validate_codenotnull.error("Null values found in field '" + self.fieldName_Description +
                                                   "' are not allowed.")
                else:
                    validate_codefield.error("Required Field '" + self.fieldName_Description + "' does not exist.")
                if self.field_exists("Type"):
                    validate_typefield.pass_validation()
                else:
                    validate_typefield.warning("Required Field 'Type' does not exist.")
                if self.feat_min_z(self.features, self.minZ):
                    validate_minZ.pass_validation()
                else:
                    validate_minZ.warning("Minimum Z value less than required value (" + str(self.minZ) + ")")
                if self.range_of_z_values(self.features) > self.maxRangeZ:
                    validate_rangeZ.error("Range of z values is greater than maximum allowed (" + str(self.maxRangeZ) + ")")
                else:
                    validate_rangeZ.pass_validation()
            else:
                validate_nofeatures.warning("No Features Found in Layer.")

        results.append(validate_codefield.get_dict())
        results.append(validate_codenotnull.get_dict())
        results.append(validate_typefield.get_dict())
        results.append(validate_minZ.get_dict())
        results.append(validate_rangeZ.get_dict())
        results.append(validate_nofeatures.get_dict())

        return results