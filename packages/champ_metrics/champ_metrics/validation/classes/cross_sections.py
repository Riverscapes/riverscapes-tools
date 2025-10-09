from .vector import CHaMP_Vector_Polyline, CHaMP_Vector
from .validation_classes import ValidationResult


class CHaMP_CrossSections(CHaMP_Vector_Polyline):

    def __init__(self, name, filepath, type):
        self.type = type
        if type == "Wetted":
            self.name = "WettedXS"
        elif type == "Bankfull":
            self.name = "BankfullXS"
        else:
            self.name = None
        CHaMP_Vector.__init__(self, name, filepath)

    def validate(self):
        results = super(CHaMP_CrossSections, self).validate()
        validate_channelfieldexists = ValidationResult(self.__class__.__name__, "ChannelFieldExists")
        validate_channelfieldvalues = ValidationResult(self.__class__.__name__, "ChannelFieldValidValues")
        validate_isvalidfieldexists = ValidationResult(self.__class__.__name__, "IsValidFieldExists")

        if self.exists():
            if self.field_exists("Channel"):
                validate_channelfieldexists.pass_validation()
                if all(value in ["Main", "Side"] for value in self.list_attributes("Channel")):
                    validate_channelfieldvalues.pass_validation()
                else:
                    validate_channelfieldvalues.error("Null or Invalid value found in Channel Field.")
            else:
                validate_channelfieldexists.error("Required field 'Channel' not found.")
            if self.field_exists("IsValid"):
                validate_isvalidfieldexists.pass_validation()
            else:
                validate_isvalidfieldexists.error("Required field 'IsValid' not found.")

        results.append(validate_isvalidfieldexists.get_dict())
        results.append(validate_channelfieldexists.get_dict())
        results.append(validate_channelfieldvalues.get_dict())
        return results