from .vector import CHaMP_Polygon
from .validation_classes import ValidationResult


class CHaMP_WaterExtent(CHaMP_Polygon):

    minArea = 1 # m^2

    def __init__(self, name, filepath, type):

        self.type = type
        if type == "Wetted":
            self.name = "WaterExtent"
        elif type == "Bankfull":
            self.name = "Bankfull"

        CHaMP_Polygon.__init__(self, name, filepath)
        self.topo_in_point = None
        self.topo_out_point = None

    def get_main_extent_polygon(self):
        if self.field_exists("ExtentType"):
            poly = [feat['geometry'] for feat in self.features if feat['fields']['ExtentType'] == "Channel"]
            return poly[0] if len(poly) == 1 else None
        else:
            return None

    def validate(self):
        results = super(CHaMP_WaterExtent, self).validate()
        validate_minarea = ValidationResult(self.__class__.__name__, "MinimumArea")
        validate_fieldextenttype = ValidationResult(self.__class__.__name__, "FieldExtentTypeExists")
        validate_fieldextenttype_channel = ValidationResult(self.__class__.__name__, "FieldExtentTypeChannel")
        validate_singlepart = ValidationResult(self.__class__.__name__, "SinglePartFeatures")
        validate_in_out_within_main_extent = ValidationResult(self.__class__.__name__, "InOutWithinExtent")

        if self.exists():
            if self.features:
                if self.singlepart_features_test():
                    validate_singlepart.pass_validation()
                else:
                    validate_singlepart.error("Multipart Features found and are not allowed")
                if self.feat_min_area(self.minArea):
                    validate_minarea.pass_validation()
                else:
                    validate_minarea.warning("One or more features in dataset is smaller than minimum allowed (" +
                                           str(self.minArea) + ")")
                if self.field_exists("ExtentType"):
                    validate_fieldextenttype.pass_validation()
                    intExtentType = self.list_attributes("ExtentType").count("Channel")
                    if  intExtentType == 1 :
                        validate_fieldextenttype_channel.pass_validation()
                    else:
                        validate_fieldextenttype_channel.error("Number of Main Channel Extents ({}) not equal to the required number (1).".format(str(intExtentType)))
                else:
                    validate_fieldextenttype.error("Required field 'ExtentType' not found.")
                if self.topo_in_point and self.topo_out_point and self.get_main_extent_polygon():
                    if self.topo_in_point.within(self.get_main_extent_polygon()) and self.topo_out_point.within(self.get_main_extent_polygon()):
                        validate_in_out_within_main_extent.pass_validation()
                    else:
                        validate_in_out_within_main_extent.error("Topo 'in' or 'out' point not within main extent.")

        results.append(validate_minarea.get_dict())
        results.append(validate_fieldextenttype.get_dict())
        results.append(validate_fieldextenttype_channel.get_dict())
        results.append(validate_singlepart.get_dict())
        results.append(validate_in_out_within_main_extent.get_dict())

        return results
