from os import path
from .validation_classes import ValidationResult


class Dataset(object):

    maxFilenameLength = 256

    def __init__(self, name, filepath):
        self.name = name #path.splitext(path.basename(filepath))[0]
        self.filename = filepath
        self.required = True

    def exists(self):
        return path.isfile(self.filename)

    def validate(self):

        results = []
        validate_exists = ValidationResult(self.__class__.__name__, "Dataset_Exists")
        validate_filelength = ValidationResult(self.__class__.__name__, "Filename_Max_Length")

        if self.exists():
            validate_exists.pass_validation()
            if len(self.filename) < self.maxFilenameLength:
                validate_filelength.pass_validation()
            else:
                validate_filelength.error("Filename length " + str(len(self.filename)) +
                                          " exceeds max length of " + str(self.maxFilenameLength))
        else:
            if self.required:
               validate_exists.error("Required dataset {} not found.".format(self.name))
            else:
                validate_exists.warning("Optional dataset {} not found.".format(self.name))

        results.append(validate_exists.get_dict())
        results.append(validate_filelength.get_dict())

        return results


class GIS_Dataset(Dataset):

    def __init__(self, name, filepath):
        Dataset.__init__(self, name, filepath)

        self.spatial_reference_wkt = None
        self.spatial_reference_dem_wkt = None

    def validate(self):

        results = super(GIS_Dataset, self).validate()
        validate_sr_exists = ValidationResult(self.__class__.__name__, "SpatialReferenceExists")
        validate_sr_dem_match = ValidationResult(self.__class__.__name__, "SpatialReferenceMatchesDEM")

        if self.exists():
            if self.spatial_reference_wkt:
                validate_sr_exists.pass_validation()
                if self.spatial_reference_dem_wkt:
                    if self.spatial_reference_wkt == self.spatial_reference_dem_wkt:
                        validate_sr_dem_match.pass_validation()
                    else:
                        validate_sr_dem_match.error("Spatial reference does not match spatial reference of DEM.")
            else:
                validate_sr_exists.error("Spatial Reference for dataset cannot be found or does not exist.")

        results.append(validate_sr_exists.get_dict())
        results.append(validate_sr_dem_match.get_dict())

        return results