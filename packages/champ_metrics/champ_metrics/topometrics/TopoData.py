import os
import sys
from os import path
from rscommons import Logger
from champ_metrics.lib.exception import DataException
from champ_metrics.lib.topoproject import TopoProject


class TopoData:

    def __init__(self, topo_project_xml: str, visitID: int) -> None:

        self.directory = topo_project_xml
        self.visitID = visitID
        self.Channels = {'Wetted': Channel(), 'Bankfull': Channel()}
        self.DEM = ""
        self.Detrended = ""
        self.Depth = ""
        self.WaterSurface = ""
        self.ChannelUnits = ""
        self.Thalweg = ""
        self.TopoPoints = ""
        # This object will be empty if there is no project.rs.xml file in the sFolder
        self.riverscapes = TopoProject(topo_project_xml)

    def buildManualFile(self, layerFileName, bMandatory):
        """
        Building a file path using manual layer file naming
        :param layerName:
        :param bMandatory:
        :return:
        """
        path = ""
        log = Logger("buildManualFile")
        try:
            match = next(file for file in os.listdir(self.directory) if file.lower() == layerFileName.lower())
            path = os.path.join(self.directory, match)

        except Exception as e:
            log.warning("The file called '{0}' does not exist in directory: {1}".format(layerFileName, self.directory))
            pass
            # if bMandatory:
            #     log.error("The file called '{0}' does not exist in directory: {1}".format(layerFileName, self.directory))
            #     raise DataException("The file called '{0}' does not exist")
        return path

    def buildProjectFile(self, layer_key: str, bMandatory: bool) -> str:
        """
        Build a file using riverscapes XML
        :param layerName:
        :param bMandatory:
        :return:
        """
        file_path = ""

        try:
            file_path = self.riverscapes.getpath(layer_key)

        except DataException as e:
            pass
            # if bMandatory:
            #     raise e
        return file_path

    def loadlayers(self):
        """
        Load all the layers from the topo project
        """

        # If we have a topo toolbar project with a project.rs.xml file this is what we have to do
        if self.riverscapes.isrsproject:
            self.Channels['Wetted'].Centerline = self.buildProjectFile("WettedCenterline", True)
            self.Channels['Wetted'].CrossSections = self.buildProjectFile("WettedCrossSections", False)
            self.Channels['Wetted'].Extent = self.buildProjectFile("WaterExtent", True)
            self.Channels['Wetted'].Islands = self.buildProjectFile("WettedIslands", False)

            self.Channels['Bankfull'].Centerline = self.buildProjectFile("BankfullCenterline", True)
            self.Channels['Bankfull'].CrossSections = self.buildProjectFile("BankfullCrossSections", False)
            self.Channels['Bankfull'].Extent = self.buildProjectFile("BankfullExtent", True)
            self.Channels['Bankfull'].Islands = self.buildProjectFile("BankfullIslands", False)

            self.DEM = self.buildProjectFile("DEM", True)
            self.Detrended = self.buildProjectFile("DetrendedDEM", True)
            self.Depth = self.buildProjectFile("WaterDepth", True)
            self.WaterSurface = self.buildProjectFile("WaterSurfaceDEM", True)

            self.ChannelUnits = self.buildProjectFile("ChannelUnits", True)
            self.Thalweg = self.buildProjectFile("Thalweg", True)
            self.TopoPoints = self.buildProjectFile("Topo_Points", True)

        # If this is just a folder full of files we have to use filenames
        else:
            self.Channels['Wetted'].Centerline = self.buildManualFile("Centerline.shp", True)
            self.Channels['Wetted'].CrossSections = self.buildManualFile("WettedXS.shp", False)
            self.Channels['Wetted'].Extent = self.buildManualFile("WaterExtent.shp", True)
            self.Channels['Wetted'].Islands = self.buildManualFile("WIslands.shp", False)

            self.Channels['Bankfull'].Centerline = self.buildManualFile("BankfullCL.shp", True)
            self.Channels['Bankfull'].CrossSections = self.buildManualFile("BankfullXS.shp", False)
            self.Channels['Bankfull'].Extent = self.buildManualFile("Bankfull.shp", True)
            self.Channels['Bankfull'].Islands = self.buildManualFile("BIslands.shp", False)

            self.DEM = self.buildManualFile("DEM.tif", True)
            self.Detrended = self.buildManualFile("Detrended.tif", True)
            self.Depth = self.buildManualFile("Water_Depth.tif", True)
            self.WaterSurface = self.buildManualFile("WSEDEM.tif", True)

            self.ChannelUnits = self.buildManualFile("Channel_Units.shp", True)
            self.Thalweg = self.buildManualFile("Thalweg.shp", True)
            self.TopoPoints = self.buildManualFile("Topo_Points.shp", True)

    # def loadlayersproj(self):

    #     tp = TopoProject(self.directory)

    #     self.Channels['Wetted'] = tp['Weted']


class Channel:

    def __init__(self):
        self.Centerline = ""
        self.CrossSections = ""
        self.Extent = ""
        self.Islands = ""
