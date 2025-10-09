""" """
import os
from xml.etree import ElementTree as ET
import classes as vc
from champmetrics.lib.raster import get_data_polygon
from champmetrics.lib.topoproject import TopoProject
from champmetrics.lib.exception import DataException

class CHaMPSurvey(object):

    def __init__(self):

        self.path = ""
        self.spatialRef = ""
        self.extents = ""

        return

    def load_topo_project(self, directory, visitID):
        """
        :param directory: full path to directory of survey, i.e. C://Visit//GISLayers
        :type directory: str
        :param channelunitsjson: path to channel unit json file
        :return: None
        """

        # If we have a project.rs.xml file tp gets a value
        tp = TopoProject(directory)

        def getPath(layername):
            """
            This is a tricky little method. If there is a project.rs.xml file (maude) and we have a tp.getNamePath method
            then run it. Otherwise just return the 'directory' variable (harold)
            """
            try:
                return tp.getpath(layername)
            except Exception as e:
                # This is kind of a weird thing to do. There will be nothing in this path but this will let things
                # fail gracefully
                return os.path.join(directory, "FILENOTFOUND.TIF")

        #CHaMP_Vector.fieldName_Description = "DESCRIPTIO"
        #CHaMP_ChannelUnits.fieldName_UnitNumber = "Unit_Numbe"

        # load expected files, does not mean they exist.
        self.DEM = vc.CHaMP_DEM("DEM", getPath("DEM"))
        self.DetrendedDEM = vc.CHaMP_DetrendedDEM("DetrendedDEM", getPath("DetrendedDEM"))
        self.WaterDepth = vc.CHaMP_WaterDepth("WaterDepth", getPath("WaterDepth"))
        self.ErrorSurface = vc.CHaMP_ErrorSurface("ErrorSurface", getPath("ErrorSurface"))
        self.WaterSurfaceDEM = vc.CHaMP_WaterSurfaceDEM("WaterSurfaceDEM", getPath("WaterSurfaceDEM"))
        self.AssocPointQuality = vc.CHaMP_Associated_3DPointQuality("AssocPointQuality", getPath("AssocPointQuality"))
        self.AssocSlope = vc.CHaMP_Associated_Slope("AssocSlope", getPath("AssocSlope"))
        self.AssocRough = vc.CHaMP_Associated_Roughness("AssocRough", getPath("AssocRough"))
        self.AssocPointDensity = vc.CHaMP_Associated_PointDensity("AssocPointDensity", getPath("AssocPointDensity"))
        self.AssocInterpolationError = vc.CHaMP_Associated_InterpolationError("AssocInterpolationError", getPath("AssocInterpolationError"))

        self.Topo_Points = vc.CHaMP_TopoPoints("Topo_Points", getPath("Topo_Points"))
        self.Topo_Points.dem = self.DEM.filename

        self.StreamFeatures = vc.CHaMP_StreamFeature_Points("StreamFeatures", getPath("StreamFeatures"))
        self.EdgeofWater_Points = vc.CHaMP_EdgeofWater_Points("EdgeofWater_Points", getPath("EdgeofWater_Points"))
        self.EdgeofWater_Points.dem = self.DEM.filename

        self.Control_Points = vc.CHaMP_ControlPoints("Control_Points", getPath("Control_Points"))
        self.Error_Points = vc.CHaMP_Error_Points("Error_Points", getPath("Error_Points"))

        self.Breaklines = vc.CHaMP_Breaklines("Breaklines", getPath("Breaklines"))
        self.Breaklines.survey_points = self.Topo_Points.get_list_geoms() + self.EdgeofWater_Points.get_list_geoms()

        self.WaterExtent = vc.CHaMP_WaterExtent("WaterExtent", getPath("WaterExtent"), "Wetted")
        self.WaterExtent.topo_in_point = self.Topo_Points.get_in_point()
        self.WaterExtent.topo_out_point = self.Topo_Points.get_out_point()

        self.BankfullExtent = vc.CHaMP_WaterExtent("BankfullExtent", getPath("BankfullExtent"), "Bankfull")
        self.BankfullExtent.topo_in_Point = self.Topo_Points.get_in_point()
        self.BankfullExtent.topo_out_point = self.Topo_Points.get_out_point()

        self.WettedIslands = vc.CHaMP_Islands("WettedIslands", getPath("WettedIslands"), "Wetted")
        self.BankfullIslands = vc.CHaMP_Islands("BankfullIslands", getPath("BankfullIslands"), "Bankfull")

        self.ChannelUnits = CHaMP_ChannelUnits("ChannelUnits", getPath("ChannelUnits"))
        self.ChannelUnits.load_attributes(visitID)
        if self.WaterExtent.field_exists("ExtentType"):
            self.ChannelUnits.wetted_extent = self.WaterExtent.get_main_extent_polygon()

        self.Thalweg = CHaMP_Thalweg("Thalweg", getPath("Thalweg"))
        self.Thalweg.dem = self.DEM.filename
        if self.WaterExtent.field_exists("ExtentType"):
            self.Thalweg.extent_polygon = self.WaterExtent.get_main_extent_polygon()
        self.Thalweg.topo_in_point = self.Topo_Points.get_in_point()
        self.Thalweg.topo_out_point = self.Topo_Points.get_out_point()

        self.WettedCenterline = CHaMP_Centerline("WettedCenterline", getPath("WettedCenterline"), "Wetted")
        self.WettedCenterline.thalweg = self.Thalweg.get_thalweg()
        if self.WaterExtent.field_exists("ExtentType"):
            self.WettedCenterline.extent_polygon = self.WaterExtent.get_main_extent_polygon()

        self.BankfullCenterline = CHaMP_Centerline("BankfullCenterline", getPath("BankfullCenterline"), "Bankfull")
        self.BankfullCenterline.thalweg = self.Thalweg.get_thalweg()
        if self.BankfullExtent.field_exists("ExtentType"):
            self.BankfullCenterline.extent_polygon = self.BankfullExtent.get_main_extent_polygon()

        self.WettedCrossSections = CHaMP_CrossSections("WettedCrossSections", getPath("WettedCrossSections"), "Wetted")
        self.BankfullCrossSections = CHaMP_CrossSections("BankfullCrossSections", getPath("BankfullCrossSections"), "Bankfull")

        if self.DEM.exists():
            self.get_dem_attributes()
            self.demDataExtent = get_data_polygon(self.DEM.filename)
            for dataset in self.datasets():
                if isinstance(dataset, CHaMP_Raster):
                    dataset.surveyDEM_Polygon = self.dem_extent
                    dataset.spatial_reference_dem_wkt = self.dem_spatial_reference_WKT
                if isinstance(dataset, CHaMP_Vector):
                    dataset.demDataExtent = self.demDataExtent
        return

    def datasets(self):
        return [self.DEM,
                self.DetrendedDEM,
                self.WaterDepth,
                self.ErrorSurface,
                self.WaterSurfaceDEM,
                self.AssocPointQuality,
                self.AssocSlope,
                self.AssocRough,
                self.AssocPointDensity,
                self.AssocInterpolationError,
                self.Topo_Points,
                self.StreamFeatures,
                self.EdgeofWater_Points,
                self.Control_Points,
                self.Error_Points,
                self.Breaklines,
                self.WaterExtent,
                self.BankfullExtent,
                self.WettedIslands,
                self.BankfullIslands,
                self.ChannelUnits,
                self.Thalweg,
                self.WettedCenterline,
                self.BankfullCenterline,
                self.WettedCrossSections,
                self.BankfullCrossSections]


    def validate(self):
        results = {}
        for dataset in self.datasets():
            results[dataset.name] = dataset.validate()
        return results

    def get_dem_attributes(self):
        gRaster = self.DEM.get_raster()
        self.dem_extent = gRaster.getBoundaryShape()
        self.dem_spatial_reference_WKT = ""
        return



