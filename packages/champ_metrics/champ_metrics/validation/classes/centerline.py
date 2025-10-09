from .vector import CHaMP_Vector_Polyline_LongLine, CHaMP_Vector
from .validation_classes import ValidationResult

class CHaMP_Centerline(CHaMP_Vector_Polyline_LongLine):

    def __init__(self, name, filepath, type):

        self.type = type
        if type == "Wetted":
            self.name = "CenterLine"
        elif type == "Bankfull":
            self.name = "BankfullCL"

        CHaMP_Vector_Polyline_LongLine.__init__(self, name, filepath)
        self.thalweg = None

    def get_main_channel_centerline(self):
        poly = [feat['geometry'] for feat in self.features if feat['fields']['Channel'] == "Main"]
        return poly[0] if len(poly) == 1 else None

    def validate(self):

        results = super(CHaMP_Centerline, self).validate()

        validate_channelfieldexists = ValidationResult(self.__class__.__name__, "ChannelFieldExists")
        validate_channelfieldnotnull = ValidationResult(self.__class__.__name__, "ChannelFieldNotNull")
        validate_channelfieldmain = ValidationResult(self.__class__.__name__, "ChannelFieldMainCount")
        validate_mainchannellength = ValidationResult(self.__class__.__name__, "MainChannelLength")
        validate_mainchannelthalweg = ValidationResult(self.__class__.__name__, "MainChannel10%Thalweg")

        if self.exists():
            if self.field_exists("Channel"):
                validate_channelfieldexists.pass_validation()
                channelfield_attributes = self.list_attributes("Channel")
                if all(value in ["Main", "Side"] for value in channelfield_attributes):
                    validate_channelfieldnotnull.pass_validation()
                    if channelfield_attributes.count("Main") == 1:
                        validate_channelfieldmain.pass_validation()
                        if self.get_main_channel_centerline().length >= 50:
                            validate_mainchannellength.pass_validation()
                        else:
                            validate_mainchannellength.warning("Main channel centerline length (" +
                                                               str(self.get_main_channel_centerline().length) +
                                                               ") is less than recommended value of 50m.")
                        if self.thalweg:
                            len_low = self.thalweg.length - (self.thalweg.length * 0.1)
                            len_high = self.thalweg.length + (self.thalweg.length * 0.1)
                            if len_low <= self.get_main_channel_centerline().length <= len_high:
                                validate_mainchannelthalweg.pass_validation()
                            else:
                                validate_mainchannelthalweg.warning("Centerline length not within 10% of Thalweg length.")
                    else:
                        validate_channelfieldmain.error("Number of Main Channels (" +
                                                        str(channelfield_attributes.count("Main")) +
                                                        ") not equal to the required number of one (1)")
                else:
                    validate_channelfieldnotnull.error("Null or unexpected value found in field 'Channel'")
            else:
                validate_channelfieldexists.error("Required field 'Channel' not found.")

        results.append(validate_channelfieldexists.get_dict())
        results.append(validate_channelfieldnotnull.get_dict())
        results.append(validate_channelfieldmain.get_dict())
        results.append(validate_mainchannellength.get_dict())
        results.append(validate_mainchannelthalweg.get_dict())

        return results
