# Name:     ShapeFile helper methods
#
# Purpose:  Miscellaneous routines for common tasks related to ShapeFiles
#
# Author:   Philip Bailey
#
# Date:     30 May 2019
# -------------------------------------------------------------------------------
import os
import sys
from enum import Enum
import json
import subprocess
import math
from osgeo import ogr, gdal, osr
from copy import copy
from functools import reduce
from shapely.wkb import loads as wkbload
from shapely.ops import unary_union
from shapely.geometry.base import BaseGeometry
from collections.abc import Callable
from shapely.geometry import shape, mapping, Point, MultiPoint, LineString, MultiLineString, GeometryCollection, Polygon, MultiPolygon
from rscommons import Logger, Raster, ProgressBar
from rscommons.util import safe_makedirs, sizeof_fmt, get_obj_size

NO_UI = os.environ.get('NO_UI') is not None

LINE_TYPES = [
    ogr.wkbLineString, ogr.wkbLineString25D, ogr.wkbLineStringM, ogr.wkbLineStringZM,
    ogr.wkbMultiLineString, ogr.wkbMultiLineString25D, ogr.wkbMultiLineStringM, ogr.wkbMultiLineStringZM
]
POLY_TYPES = [
    ogr.wkbPolygon, ogr.wkbPolygon25D, ogr.wkbPolygonM, ogr.wkbPolygonZM,
    ogr.wkbMultiPolygon, ogr.wkbMultiPolygon25D, ogr.wkbMultiPolygonM, ogr.wkbMultiPolygonZM
]
DRIVER_MAP = {
    'shp': 'ESRI Shapefile',
    'gpkg': 'GPKG'
}


class VectorLayer():
    log = Logger('VectorLayer')

    def __init__(self, filepath: str, driver_name: str, layer_name: str):

        self.filepath = filepath
        self.driver_name = driver_name
        self.ogr_layer_name = layer_name

        # This shouldn't be used except to input a new layer
        self.spatial_ref = None
        self.ogr_layer = None
        self.ogr_layer_def = None
        self.ogr_ds = None
        self.ogr_geom_type = None

        # This is just matching extensions
        self.driver = ogr.GetDriverByName(self.driver_name)

    def _create_ds(self):
        """Note: this wipes any existing Datasets (files). It also opens

        Raises:
            Exception: [description]
        """

        if self.driver_name == DRIVER_MAP['shp'] and os.path.exists(self.filepath):
            self.driver.DeleteDataSource(self.filepath)

        # Make a folder if we need to
        ds_dir = os.path.dirname(self.filepath)
        if not os.path.isdir(ds_dir):
            self.log.debug('Creating directory: {}'.format(ds_dir))
            safe_makedirs(ds_dir)

        self.ogr_ds = self.driver.CreateDataSource(self.filepath)

    def _create_layer(self, ogr_geom_type, epsg=None, spatial_ref=None):
        # XOR (^) check to make sure only one of EPSG or spatial_ref are provided
        self.ogr_layer = None
        if epsg is not None ^ spatial_ref is not None:
            raise Exception('Specify either an EPSG or a spatial_ref. Not both')

        if self.driver_name == DRIVER_MAP['shp']:
            pass
        elif self.driver_name == DRIVER_MAP['gpkg']:
            tmplyr = self.ogr_ds.GetLayerByName(self.ogr_layer_name)
            if tmplyr is not None:
                self.ogr_ds.DeleteLayer(self.ogr_layer_name)
        else:
            raise NotImplementedError('Not implemented: {}'.format(self.driver_name))

        if epsg:
            self.spatial_ref = osr.SpatialReference()
            self.spatial_ref.ImportFromEPSG(epsg)
        else:
            self.spatial_ref = spatial_ref

        # Get the output Layer's Feature Definition
        self.ogr_layer = self.ogr_ds.CreateLayer(self.ogr_layer_name, self.spatial_ref, ogr_geom_type=ogr_geom_type)
        self.ogr_geom_type = self.ogr_layer.GetGeomType()
        self.ogr_layer_def = self.ogr_layer.GetLayerDefn()

    def _open_ds(self, allow_write: bool = False):
        permission = 1 if allow_write is True else 0
        self.ogr_ds = self.driver.Open(self.filepath, permission)

    def _open_layer(self):
        if self.driver_name == DRIVER_MAP['shp']:
            self.ogr_layer = self.ogr_ds.GetLayer()
        elif self.driver_name == DRIVER_MAP['gpkg']:
            if self.ogr_layer_name is None:
                raise Exception('For Geopackages you must specify a layer name.')
            self.ogr_layer = self.ogr_ds.GetLayerByName(self.ogr_layer_name)
        else:
            raise Exception("Error opening layer: {}".format(self.ogr_layer_name))

        # Get the output Layer's Feature Definition and spatial ref
        self.ogr_layer_def = self.ogr_layer.GetLayerDefn()
        self.ogr_geom_type = self.ogr_layer.GetGeomType()
        self.spatial_ref = self.ogr_layer.GetSpatialRef()

    def create_field(self, field_name: str, field_type=ogr.OFTReal):
        """
        Remove and then re-add a field to a feature class
        :param layer: Feature class that will receive the attribute field
        :param field_name: Name of the attribute field to be created
        :param log:
        :return: name of the field created (same as function argument)
        """

        if self.ogr_layer is None:
            raise Exception('No open layer to create fields on')
        elif not field_name or len(field_name) < 1:
            raise Exception('Attempting to create field with invalid field name "{}".'.format(field_name))
        elif self.driver_name == DRIVER_MAP['shp'] and len(field_name) > 10:
            raise Exception('Field names in geopackages cannot be greater than 31 characters. "{}" == {}.'.format(field_name, len(field_name)))
        elif field_type == ogr.OFTInteger64:
            self.log.error('ERROR:: ogr.OFTInteger64 is not supported by ESRI!')

        # Delete output column from vector layer if it exists and then recreate it
        for fieldidx in range(0, self.ogr_layer_def.GetFieldCount()):
            if self.ogr_layer_def.GetFieldDefn(fieldidx).GetName() == field_name:
                self.log.info('Deleting existing output field "{}" in vector layer.'.format(field_name))
                self.ogr_layer.DeleteField(fieldidx)
                break

        self.log.info('Creating output field "{}" in layer.'.format(field_name))
        field_def = ogr.FieldDefn(field_name, field_type)

        # Good convention for real valuues
        if field_type == ogr.OFTReal:
            field_def.SetPrecision(10)
            field_def.SetWidth(18)

        self.ogr_layer.CreateField(field_def)
        return field_def

    def verify_field(self, field_name: str):
        """
        Case insensitive search for field in layer. Throw exception if it doesn't exist
        :param layer: Layer in which to search for field
        :param field: The field name that will be searched for
        :return: The actual field name (with correct case) that exists in layer
        """

        for i in range(self.ogr_layer_def.GetFieldCount()):
            actual_name = self.ogr_layer_def.GetFieldDefn(i).GetName()
            if field_name.lower() == actual_name.lower():
                return actual_name

        raise Exception('Missing field {} in {}'.format(field_name, self.ogr_layer.GetName()))

    def iterate_features(
        self, name: str, callback: Callable[[ogr.Feature, int, ProgressBar]], out_layer: VectorLayer = None,
        commit_thresh=1000, attribute_filter: str = None, clip_shape: BaseGeometry = None
    ) -> None:
        """This method allows you to pass in a callback that gets run for each feature in a layer

        Args:
            name (str): [description]
            callback (function): [description]
            out_layer (VectorLayer, optional): [description]. Defaults to None.
            commit_thresh (int, optional): [description]. Defaults to 1000.
        """
        counter = 0

        # If there's a clip geometry provided then limit the features copied to
        # those that intersect (partially or entirely) by this clip feature.
        # Note that this makes the subsequent intersection process a lot more
        # performant because the SetSaptialFilter() uses the ShapeFile's spatial
        # index which is much faster than manually checking if all pairs of features intersect.
        clip_geom = None
        if clip_shape:
            clip_geom = ogr.CreateGeometryFromWkb(clip_shape.wkb)
            self.ogr_layer.SetSpatialFilter(clip_geom)

        if attribute_filter:
            self.ogr_layer.SetAttributeFilter(attribute_filter)

        if out_layer is not None:
            out_layer.StartTransaction()

        # Get an accurate feature count after clipping and filtering
        fcount = self.ogr_layer.GetFeatureCount()
        progbar = ProgressBar(fcount, 50, name)

        for feature in self.ogr_layer:
            counter += 1
            progbar.update(counter)
            callback(feature, counter, progbar)

            if out_layer is not None and counter % commit_thresh == 0:
                out_layer.CommitTransaction()

        if out_layer is not None:
            out_layer.CommitTransaction()

        # Reset the attribute filter
        if attribute_filter:
            self.ogr_layer.SetAttributeFilter('')

        progbar.finish()

    def get_transform(self, out_layer: VectorLayer) -> osr.CoordinateTransformation:
        """Get a transform between this layer and another layer

        Args:
            out_layer ([type]): Vector layer type

        Returns:
            [type]: [description]
        """
        self.log.debug('Input spatial reference is {0}'.format(self.spatial_ref.ExportToProj4()))
        self.log.debug('Output spatial reference is {0}'.format(out_layer.spatial_ref.ExportToProj4()))
        transform = osr.CoordinateTransformation(self.spatial_ref, out_layer.spatial_ref)

        return transform

    def get_transform_from_epsg(self, epsg: int) -> (osr.SpatialReference, osr.CoordinateTransformation):
        """Transform a spatial ref using an epsg code provided

        This is done explicitly and includes a GetAxisMappingStrategy check to
        account for GDAL3's projection differences.

        Args:
            in_spatial_ref ([type]): [description]
            epsg ([type]): [description]

        Returns:
            [type]: [description]
        """
        out_spatial_ref = osr.SpatialReference()
        out_spatial_ref.ImportFromEPSG(int(epsg))

        # https://github.com/OSGeo/gdal/issues/1546
        out_spatial_ref.SetAxisMappingStrategy(self.spatial_ref.GetAxisMappingStrategy())

        VectorLayer.log.debug('Input spatial reference is {0}'.format(self.spatial_ref.ExportToProj4()))
        VectorLayer.log.debug('Output spatial reference is {0}'.format(out_spatial_ref.ExportToProj4()))
        transform = osr.CoordinateTransformation(self.spatial_ref, out_spatial_ref)
        return out_spatial_ref, transform

    def _rough_convert_metres_to_shapefile_units(self, distance: float) -> float:
        extent = self.ogr_layer.GetExtent()
        return VectorLayer._rough_convert_metres_to_dataset_units(self.spatial_ref, extent, distance)

    @staticmethod
    def _rough_convert_metres_to_raster_units(raster_path: str, distance: float) -> float:

        ds = gdal.Open(raster_path)
        in_spatial_ref = osr.SpatialReference()
        in_spatial_ref.ImportFromWkt(ds.GetProjectionRef())
        gt = ds.GetGeoTransform()
        extent = (gt[0], gt[0] + gt[1] * ds.RasterXSize, gt[3] + gt[5] * ds.RasterYSize, gt[3])

        return VectorLayer._rough_convert_metres_to_dataset_units(in_spatial_ref, extent, distance)

    @staticmethod
    def _rough_convert_metres_to_dataset_units(in_spatial_ref: osr.SpatialReference, extent: list, distance: float) -> float:
        """DO NOT USE THIS FOR ACCURATE DISTANCES. IT'S GOOD FOR A QUICK CALCULATION
        WHEN DISTANCE PRECISION ISN'T THAT IMPORTANT

        Arguments:
            shapefile_path {[type]} -- [description]
            distance {[type]} -- [description]

        Returns:
            [type] -- [description]
        """

        # If the ShapeFile uses a projected coordinate system in meters then simply return the distance.
        # If it's projected but in some other units then throw an exception.
        # If it's in degrees then continue with the code below to convert it to metres.
        if in_spatial_ref.IsProjected() == 1:
            if in_spatial_ref.GetAttrValue('unit').lower() in ['meter', 'metre', 'm']:
                return distance
            else:
                raise Exception('Unhandled projected coordinate system linear units: {}'.format(in_spatial_ref.GetAttrValue('unit')))

        # Get the centroid of the Shapefile spatial extent
        extent_ring = ogr.Geometry(ogr.wkbLinearRing)
        extent_ring.AddPoint(extent[0], extent[2])
        extent_ring.AddPoint(extent[1], extent[2])
        extent_ring.AddPoint(extent[1], extent[3])
        extent_ring.AddPoint(extent[0], extent[3])
        extent_ring.AddPoint(extent[0], extent[2])
        extent_poly = ogr.Geometry(ogr.wkbPolygon)
        extent_poly.AddGeometry(extent_ring)
        extent_centroid = extent_poly.Centroid()

        # Go diagonally on the extent rectangle
        pt1_orig = Point(extent[0], extent[2])
        pt2_orig = Point(extent[1], extent[3])
        orig_dist = pt1_orig.distance(pt2_orig)

        # Determine the UTM zone by locating the centroid of the shapefile extent
        # Then get the transformation required to convert to the Shapefile to this UTM zone
        utm_epsg = get_utm_zone_epsg(extent_centroid.GetX())
        in_spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        out_spatial_ref = osr.SpatialReference()
        out_spatial_ref.ImportFromEPSG(int(utm_epsg))
        out_spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        VectorLayer.log.debug('Original spatial reference is : \n       {0} (AxisMappingStrategy:{1})'.format(*VectorLayer.get_srs_debug(in_spatial_ref)))
        VectorLayer.log.debug('Transform spatial reference is : \n       {0} (AxisMappingStrategy:{1})'.format(*VectorLayer.get_srs_debug(out_spatial_ref)))

        transformFwd = osr.CoordinateTransformation(in_spatial_ref, out_spatial_ref)

        pt1_ogr = ogr.CreateGeometryFromWkb(pt1_orig.wkb)
        pt2_ogr = ogr.CreateGeometryFromWkb(pt2_orig.wkb)
        pt1_ogr.Transform(transformFwd)
        pt2_ogr.Transform(transformFwd)

        pt1_proj = wkbload(pt1_ogr.ExportToWkb())
        pt2_proj = wkbload(pt2_ogr.ExportToWkb())

        proj_dist = pt1_proj.distance(pt2_proj)

        output_distance = (orig_dist / proj_dist) * distance

        VectorLayer.log.info('{}m distance converts to {:.10f} using UTM EPSG {}'.format(distance, output_distance, utm_epsg))

        if output_distance > 360:
            raise Exception('Projection Error: \'{:,}\' is larger than the maximum allowed value'.format(output_distance))

        return output_distance

    @ staticmethod
    def get_srs_debug(spatial_ref: osr.SpatialReference) -> [str, str]:
        order = spatial_ref.GetAxisMappingStrategy()
        order_str = str(order)
        if order == 0:
            order_str = 'OAMS_TRADITIONAL_GIS_ORDER'
        elif order == 1:
            order_str = 'OAMS_AUTHORITY_COMPLIANT'
        elif order == 2:
            order_str = 'OAMS_CUSTOM'

        return [spatial_ref.ExportToProj4(), order_str]


def get_utm_zone_epsg(longitude: float) -> int:

    zone_number = math.floor((180.0 + longitude) / 6.0)
    epsg = 26901 + zone_number
    return epsg
