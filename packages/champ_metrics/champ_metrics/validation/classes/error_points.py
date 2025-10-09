from .vector import CHaMP_Vector_Point_3D, CHaMP_Vector
from .validation_classes import ValidationResult


class CHaMP_Error_Points(CHaMP_Vector_Point_3D):

    minFeatureCount = 0

    def __init__(self, name, filepath):
        CHaMP_Vector.__init__(self, name, filepath)
        self.required = False

    def validate(self):
        results = super(CHaMP_Error_Points ,self).validate()
        validate_codefield = ValidationResult(self.__class__.__name__, "CodeFieldExists")
        if self.exists():
            if self.get_shapely_feats():
                if self.field_exists(self.fieldName_Description):
                    validate_codefield.pass_validation()
                else:
                    validate_codefield.error("Required Field '" + self.fieldName_Description + "' does not exist.")
        results.append(validate_codefield.get_dict())
        return results