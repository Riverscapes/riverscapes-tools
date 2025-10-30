from os import path
from xml.etree import ElementTree as ET
from .exception import DataException, MissingException
from rscommons import Logger
from champ_metrics.lib.util import getAbsInsensitivePath

# TODO: This shares a lot in common with riverscapes.py. Let's look at refactoring


class TopoProject():
    # Dictionary with layer { layernname : layerxpath }
    LAYERS = {
        "DEM": [
            "./Realizations/Topography/TIN[@active='true']/DEM/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='DEM']/Path"
        ],
        "DetrendedDEM": [
            "./Realizations/Topography/TIN[@active='true']/Detrended/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='Detrended']/Path"
        ],
        "WaterDepth": [
            "./Realizations/Topography/TIN[@active='true']/WaterDepth/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='WaterDepth']/Path"
        ],
        "ErrorSurface": [
            "./Realizations/Topography/TIN[@active='true']/AssocSurfaces/ErrSurface/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='ErrorSurface']/Path"
        ],
        "WaterSurfaceDEM": [
            "./Realizations/Topography/TIN[@active='true']/WaterSurfaceDEM/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='WaterSurfaceDEM']/Path"
        ],
        "AssocPointQuality": [
            "./Realizations/Topography/TIN[@active='true']/AssocSurfaces/PointQuality3D/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='AssocPointQuality']/Path"
        ],
        "AssocSlope": [
            "./Realizations/Topography/TIN[@active='true']/AssocSurfaces/Slope/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='Slope']/Path"
        ],
        "AssocRough": [
            "./Realizations/Topography/TIN[@active='true']/AssocSurfaces/Roughness/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='Roughness']/Path"
        ],
        "AssocPointDensity": [
            "./Realizations/Topography/TIN[@active='true']/AssocSurfaces/PointDensity/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='PointDensity']/Path"
        ],
        "AssocInterpolationError": [
            "./Realizations/Topography/TIN[@active='true']/AssocSurfaces/InterpolationError/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Raster[@id='InterpolationError']/Path"
        ],
        "Topo_Points": [
            "./Realizations/SurveyData[@projected='true']/Vector[@id='topo_points']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Datasets/Vector[@id='topo_points']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Inputs/Vector[@id='topo_points']/Path"
        ],
        "StreamFeatures": [
            "./Realizations/SurveyData[@projected='true']/Vector[@id='stream_features']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Datasets/Vector[@id='stream_features']/Path"
        ],
        "EdgeofWater_Points": [
            "./Realizations/SurveyData[@projected='true']/Vector[@id='eow_points']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Datasets/Vector[@id='eow_points']/Path"
        ],
        "Control_Points": [
            "./Realizations/SurveyData[@projected='true']/Vector[@id='control_points']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Datasets/Vector[@id='control_points']/Path"
        ],
        "Error_Points": [
            "./Realizations/SurveyData[@projected='true']/Vector[@id='error_points']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Datasets/Vector[@id='error_points']/Path"
        ],
        "Breaklines": [
            "./Realizations/SurveyData[@projected='true']/Vector[@id='breaklines']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Datasets/Vector[@id='breaklines']/Path"
        ],
        "WaterExtent": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='wetted'][@type='extent']/Path",
            "./Realizations/Realization/Outputs/Vector[@id='wetted_extent']/Path"
        ],
        "BankfullExtent": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='bankfull'][@type='extent']/Path",
            "./Realizations/Realization/Outputs/Vector[@id='bankfull_extent']/Path"
        ],
        "WettedIslands": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='wetted'][@type='islands']/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='wetted_islands']/Path"

        ],
        "BankfullIslands": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='bankfull'][@type='islands']/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='bankfull_islands']/Path"
        ],
        "ChannelUnits": [
            "./Realizations/Topography/TIN[@active='true']/ChannelUnits/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='ChannelUnits']/Path"
        ],
        "Thalweg": [
            "./Realizations/Topography/TIN[@active='true']/Thalweg/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='thalweg']/Path"
        ],
        "WettedCenterline": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='wetted'][@type='centerline']/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='wetted_centerline']/Path"
        ],
        "BankfullCenterline": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='bankfull'][@type='centerline']/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='bankfull_centerline']/Path"
        ],
        "WettedCrossSections": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='wetted'][@type='crosssections']/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='wetted_crosssections']/Path"
        ],
        "BankfullCrossSections": [
            "./Realizations/Topography/TIN[@active='true']/Stages/Vector[@stage='bankfull'][@type='crosssections']/Path",
            "./Realizations/Realization[@id='topography']/Outputs/Vector[@id='bankfull_crosssections']/Path"
        ],
        "SurveyExtent": [
            "./Realizations/SurveyData/SurveyExtents/Vector[@active='true']/Path",
            "./Realizations/Realization[@id='survey_data']/Outputs/Vector[@id='survey_extent']/Path"
        ],
        "ControlPoints": [
            "./Realizations/SurveyData/Vector[@id='control_points']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Datasets/Vector[@id='control_points']/Path"
        ],
        "TopoTin": "./Realizations/Topography/TIN[@active='true']/Path",
        "Survey_Extent": [
            "./Realizations/SurveyData[@projected='true']/SurveyExtents/Vector[@id='survey_extent']/Path",
            "./Realizations/Realization[@id='survey_data_projected']/Outputs/Vector[@id='survey_extent']/Path"
        ]
    }

    def __init__(self, sProjPath):
        """
        :param sProjPath: Either the folder containing the project.rs.xml or the filepath of the actual project.rs.xml
        """
        log = Logger('TopoProject')
        try:
            if path.isfile(sProjPath):
                self.projpath = path.dirname(sProjPath)
                self.projpathxml = sProjPath
            elif path.isdir(sProjPath):
                self.projpath = sProjPath
                self.projpathxml = path.join(sProjPath, "project.rs.xml")
            else:
                raise MissingException("No project file or directory with the name could be found: {}".format(sProjPath))
        except Exception as e:
            raise MissingException("No project file or directory with the name could be found: {}".format(sProjPath))

        self.isrsproject = False

        if path.isfile(self.projpathxml):
            log.info("Attempting to load topo project file: {}".format(self.projpathxml))
            self.isrsproject = True
            try:
                self.domtree = ET.parse(self.projpathxml)
            except ET.ParseError as e:
                raise DataException("project.rs.xml exists but could not be parsed.")

            self.domroot = self.domtree.getroot()
            log.info("XML Project file loaded")

    def getdir(self, layername):
        return path.dirname(self.getpath(layername))

    def getpath(self, layer_key):
        """
        Turn a relative path into an absolute one.
        :param project_path:
        :param root:
        :param xpath:
        :return:
        """

        if layer_key not in TopoProject.LAYERS:
            raise DataException(f"'{layer_key}' is not a valid layer name key")

        try:
            xpaths = TopoProject.LAYERS[layer_key]
            if isinstance(xpaths, str):
                xpaths = [xpaths]
            elif isinstance(xpaths, list):
                # This could be a list of xpaths. One for the old active tin XML structure and one
                # for the new realization output structure
                pass
            else:
                raise DataException(f"Layer definition for '{layer_key}' is not a string or list.")

            # Find the first xpath that exists in the XML
            node = None
            for xpath in xpaths:
                node = self.domroot.find(xpath)
                if node is not None:
                    break

            if node is not None:
                # Replace any back or forward slashes with the current OS path separator
                # This is because the riverscapes project file always uses forward slashes
                rel_path = node.text.replace("\\", path.sep).replace("/", path.sep)

                final_path = path.join(self.projpath, rel_path)
                if not path.isfile(final_path) and not path.isdir(final_path):
                    # One last, desparate call to see if there's a case error. This is expensive and should not be run as default
                    final_path = getAbsInsensitivePath(final_path, ignoreAbsent=True)
                return final_path
            else:
                raise DataException(f"Could not find layer '{layer_key}' with xpath '{TopoProject.LAYERS[layer_key]}' in project file.")

        except Exception as e:
            raise DataException(f"Error retrieving layer '{layer_key}' from project file.")

    def getMeta(self, metaname):
        """
        Retrieve Meta tags from the project.rs.xml file
        :param metaname:
        :return:
        """
        try:
            return self.domroot.find('./MetaData/Meta[@name="{}"]'.format(metaname)).text
        except Exception as e:
            raise DataException("Error retrieving metadata with name '{}' from project file. {}".format(metaname, self.projpathxml))

    def get_guid(self, layername):
        """
        Get the guid from a given layer
        :param layername:
        :return:
        """
        if layername not in TopoProject.LAYERS:
            raise DataException("'{}' is not a valid layer name".format(layername))

        node = self.domroot.find(TopoProject.LAYERS[layername].rstrip("/Path"))

        if node is not None:
            return node.get("guid")
        else:
            raise DataException("Could not find layer '{}' with xpath '{}'".format(layername, TopoProject.LAYERS[layername]))

    def layer_exists(self, layername):

        node = self.domroot.find(TopoProject.LAYERS[layername])
        return True if node is not None else False
