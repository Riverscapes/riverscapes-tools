from .vector import CHaMP_Polygon
from .validation_classes import ValidationResult
from champmetrics.lib.channelunits import loadChannelUnitsFromSQLite, loadChannelUnitsFromAPI
from shapely.geometry import MultiPolygon
from .vector import polyinsidepoly

class CHaMP_ChannelUnits(CHaMP_Polygon):

    minArea = 1 # m^2
    fieldName_UnitNumber = "UnitNumber"

    def __init__(self, name, filepath):
        CHaMP_Polygon.__init__(self, name, filepath)
        self.dAuxUnits = None
        self.wetted_extent = None

    def load_attributes_db(self, visitid, sqlitedb):
        self.dAuxUnits = loadChannelUnitsFromSQLite(visitid, sqlitedb)
        return

    def load_attributes(self, visitID):
        self.dAuxUnits = loadChannelUnitsFromAPI(visitID)
        return

    def validate(self):
        results = super(CHaMP_ChannelUnits, self).validate()
        validate_minarea = ValidationResult(self.__class__.__name__, "MinimumArea")
        validate_unitnumber_field = ValidationResult(self.__class__.__name__, "FieldUnitNumberExists")
        validate_unitnumber_field_notnull = ValidationResult(self.__class__.__name__, "FieldUnitNumberNotNull")
        validate_unitnumber_field_positive = ValidationResult(self.__class__.__name__, "FieldUnitNumberPositive")
        validate_unitnumber_field_values_unique = ValidationResult(self.__class__.__name__, "FieldUnitNumberUnique")
        # Attributes
        validate_aux_in_gis = ValidationResult(self.__class__.__name__, "UnitNumbersAuxInGIS")
        validate_gis_in_aux = ValidationResult(self.__class__.__name__, "UnitNumbersGISInAux")
        # Geoms
        validate_inwettedextent = ValidationResult(self.__class__.__name__, "ChannelUnitsWithinWettedExtent")
        validate_no_overlap = ValidationResult(self.__class__.__name__, "ChannelUnitsOverlap")

        if self.exists():
            if self.feat_min_area(self.minArea):
                validate_minarea.pass_validation()
            else:
                validate_minarea.error("One or more features in dataset is smaller than minimum allowed (" +
                                       str(self.minArea) + ")")
            if self.field_exists(self.fieldName_UnitNumber):
                validate_unitnumber_field.pass_validation()
                lUnits = self.list_attributes(self.fieldName_UnitNumber)
                if all(isinstance(value, int) for value in lUnits):
                    validate_unitnumber_field_notnull.pass_validation()
                    if all(value > 0 for value in lUnits):
                        validate_unitnumber_field_positive.pass_validation()
                    else:
                        validate_unitnumber_field_positive.error("0 or negative UnitNumber found, which is not allowed")
                    if len(set(lUnits)) == len(lUnits):
                        validate_unitnumber_field_values_unique.pass_validation()
                    else:
                        validate_unitnumber_field_values_unique.error("Found non-Unique channel units.")
                    if self.dAuxUnits:
                        if all(value in self.dAuxUnits for value in lUnits):
                            validate_gis_in_aux.pass_validation()
                        else:
                            validate_gis_in_aux.error("Unit in GIS layer does not appear in Aux")
                        if all(value in lUnits for value in self.dAuxUnits.keys()):
                            validate_aux_in_gis.pass_validation()
                        else:
                            validate_aux_in_gis.warning("Unit in Aux does not appear in GIS layer")

                    if self.wetted_extent:
                        if polyinsidepoly(self, self.wetted_extent):
                            validate_inwettedextent.pass_validation()
                        else:
                            validate_inwettedextent.warning("One or more channel units outside of wetted extent")

                    if not any(feat['geometry'].crosses(testfeat['geometry']) for testfeat in self.features for feat in self.features):
                        validate_no_overlap.pass_validation()
                    else:
                        validate_no_overlap.warning("Channel Units overlap.")
                else:
                    validate_unitnumber_field_notnull.error("Null or Non-Integer Value found in UnitNumber Field.")
            else:
                validate_unitnumber_field.error("Required field '" + self.fieldName_UnitNumber + "' not found.")

        results.append(validate_minarea.get_dict())
        results.append(validate_unitnumber_field.get_dict())
        results.append(validate_unitnumber_field_notnull.get_dict())
        results.append(validate_unitnumber_field_positive.get_dict())
        results.append(validate_unitnumber_field_values_unique.get_dict())
        results.append(validate_gis_in_aux.get_dict())
        results.append(validate_aux_in_gis.get_dict())
        results.append(validate_inwettedextent.get_dict())
        results.append(validate_no_overlap.get_dict())

        return results