from .vector import CHaMP_Vector_Polyline_LongLine
from .validation_classes import ValidationResult
# from champmetrics.lib.exception import *
from shapely.geometry import Point
from champmetrics.lib.raster import Raster

class CHaMP_Thalweg(CHaMP_Vector_Polyline_LongLine):

    maxFeatureCount = 1

    def __init__(self, name, filepath):
        CHaMP_Vector_Polyline_LongLine.__init__(self, name, filepath)

        self.wsedemExtent = None
        self.topo_in_point = None
        self.topo_out_point = None
        self.dem = None

    def thalweg_end_pt(self):
        try:
            return Point(self.features[0]['geometry'].coords[-1])
        except IndexError:
            return

    def thalweg_start_pnt(self):
        try:
            return Point(self.features[0]['geometry'].coords[0])
        except IndexError:
            return

    def get_thalweg(self):
        return self.features[0]['geometry'] if len(self.features) == 1 else None

    def validate(self):
        results = super(CHaMP_Thalweg, self).validate()

        validate_maxfeaturecount = ValidationResult(self.__class__.__name__, "MaxFeatureCount")
        validate_wsedemExtent = ValidationResult(self.__class__.__name__, "WithinWSEDEMExtent")
        validate_in_end_dist = ValidationResult(self.__class__.__name__, "InPointNearEnd")
        validate_out_start_dist = ValidationResult(self.__class__.__name__, "OutPointNearStart")
        validate_out_higher_inflow = ValidationResult(self.__class__.__name__, "StartPointLowerEndPoint")
        validate_thalwegstartstopraster = ValidationResult(self.__class__.__name__, "ThalwegStartStopOnDEM")

        if self.exists():
            if len(self.features) > self.maxFeatureCount:
                validate_maxfeaturecount.error("Number of features (" + str(len(self.features)) +
                                               ") exceeds the maximum number allowed ("
                                               + str(self.maxFeatureCount) + ")")
            else:
                validate_maxfeaturecount.pass_validation()
            if self.wsedemExtent:
                if self.start_stop_on_raster(raster_extent=self.wsedemExtent):
                    validate_wsedemExtent.pass_validation()
                else:
                    validate_wsedemExtent.error("Thalweg not entirely contained within WSEDEM.")
            if self.topo_in_point:
                inbuffer = self.topo_in_point.buffer(15)
                if inbuffer.contains(self.thalweg_end_pt()):
                    validate_in_end_dist.pass_validation()
                else:
                    validate_in_end_dist.warning("End point is greater than 15m from Topo 'in' point")
            if self.topo_out_point:
                if self.topo_out_point.buffer(15).contains(self.thalweg_start_pnt()):
                    validate_out_start_dist.pass_validation()
                else:
                    validate_out_start_dist.warning("Start point is greater than 15m from Topo 'out' point")
            if self.dem:
                if self.demDataExtent and self.features:
                    if self.start_stop_on_raster():
                        validate_thalwegstartstopraster.pass_validation()
                    else:
                        validate_thalwegstartstopraster.error("One or more line features does not start or stop on the DEM")
                r = Raster(self.dem)

                tStart = self.thalweg_start_pnt()
                tEnd = self.thalweg_end_pt()

                if tStart is None or tEnd is None:
                    validate_out_higher_inflow.error("Could not determine thalweg start and finish")
                else:
                    z_start = r.getPixelVal([tStart.x, tStart.y])
                    z_end = r.getPixelVal([tEnd.x, tEnd.y])
                    if z_start > z_end + 0.1:
                        validate_out_higher_inflow.error("Thalweg Start (outflow) more than 10cm higher than end (inflow)")
                    else:
                        validate_out_higher_inflow.pass_validation()

        results.append(validate_maxfeaturecount.get_dict())
        results.append(validate_wsedemExtent.get_dict())
        results.append(validate_in_end_dist.get_dict())
        results.append(validate_out_start_dist.get_dict())
        results.append(validate_out_higher_inflow.get_dict())
        results.append(validate_thalwegstartstopraster.get_dict())

        return results
