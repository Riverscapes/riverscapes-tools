from .vector import CHaMP_Polygon
from .validation_classes import ValidationResult

class CHaMP_Islands(CHaMP_Polygon):

    minFeatureCount = 0
    minArea = 1 # m^2

    def __init__(self, name, filepath, type):

        self.type = type
        if type == "Wetted":
            self.name = "WIslands"
        elif type == "Bankfull":
            self.name = "BIslands"
        else:
            self.name = None

        CHaMP_Polygon.__init__(self, name, filepath)

    def get_qualifying_island_polygons(self):

        return [feat['geometry'] for feat in self.features if feat['fields']['IsValid'] == 1] if self.features else None

    def validate(self):

        results = super(CHaMP_Islands, self).validate()

        validate_minarea = ValidationResult(self.__class__.__name__, "MinimumArea")
        validate_field_IsValid = ValidationResult(self.__class__.__name__, "FieldIsValidExists")
        validate_field_qualifying = ValidationResult(self.__class__.__name__, "FieldQualifyingExists")

        if self.exists():
            if self.features:
                if self.feat_min_area(self.minArea):
                    validate_minarea.pass_validation()
                else:
                    validate_minarea.warning("One or more features in dataset is smaller than minimum allowed (" +
                                           str(self.minArea) + ")")
                if self.field_exists("IsValid"):
                    validate_field_IsValid.pass_validation("Required field 'IsValid' is missing or not found.")
                else:
                    validate_field_IsValid.error()
                if self.field_exists("Qualifying"):
                    validate_field_qualifying.pass_validation()
                else:
                    validate_field_qualifying.error("Required field 'Qualifying' is missing or not found.")

        results.append(validate_minarea.get_dict())
        results.append(validate_field_IsValid.get_dict())
        results.append(validate_field_qualifying.get_dict())

        return results