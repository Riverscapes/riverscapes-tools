"""The base VectorBase Class
"""
from __future__ import annotations
import os
import math
import re
from enum import Enum
from osgeo import ogr, gdal, osr
from shapely.wkb import loads as wkbload
from shapely.geometry.base import BaseGeometry
from shapely.geometry import Point
from rscommons import Logger, ProgressBar, Raster
from rscommons.util import safe_makedirs
from rscommons.classes.vector_datasource import DatasetRegistry

# NO_UI = os.environ.get('NO_UI') is not None


class VectorBaseException(Exception):
    """Special exceptions

    Args:
        Exception ([type]): [description]
    """
    pass


class VectorBase():
    """[summary]

    Raises:
        VectorBaseException: Various
    """
    log = Logger('VectorBase')

    class Drivers(Enum):
        Shapefile = 'ESRI Shapefile'
        Geopackage = 'GPKG'

    LINE_TYPES = [
        ogr.wkbLineString, ogr.wkbLineString25D, ogr.wkbLineStringM, ogr.wkbLineStringZM,
        ogr.wkbMultiLineString, ogr.wkbMultiLineString25D, ogr.wkbMultiLineStringM,
        ogr.wkbMultiLineStringZM
    ]
    POLY_TYPES = [
        ogr.wkbPolygon, ogr.wkbPolygon25D, ogr.wkbPolygonM, ogr.wkbPolygonZM,
        ogr.wkbMultiPolygon, ogr.wkbMultiPolygon25D, ogr.wkbMultiPolygonM, ogr.wkbMultiPolygonZM
    ]

    def __init__(self, filepath: str, driver: VectorBase.Drivers, layer_name: str, replace_ds_on_open: bool = False, allow_write=False):
        self.registry = DatasetRegistry()
        self.driver_name = driver.value
        self.filepath = None
        self.ogr_layer_name = layer_name
        self.allow_write = allow_write
        self.replace_ds_on_open = replace_ds_on_open
        self.allow_write = allow_write

        # This shouldn't be used except to input a new layer
        self.spatial_ref = None
        self.ogr_layer = None
        self.ogr_layer_def = None
        self.ogr_ds = None
        self.ogr_geom_type = None

        # This is just matching extensions
        self.driver = ogr.GetDriverByName(self.driver_name)

        # Sometimes the path for the geopackage comes in as /path/to/layer.gpkg/schema.layer
        self.filepath, self.ogr_layer_name = VectorBase.path_sorter(filepath, layer_name)

    def __enter__(self):
        # self.log.debug('__enter__ called')
        self._open_ds()
        self._open_layer()
        return self

    def __exit__(self, _type, _value, _traceback):
        # self.log.debug('__exit__ called. Cleaning up.')
        self.close()

    @staticmethod
    def path_sorter(filepath: str, layer_name: str = None):
        # No path, no work
        if filepath is None or len(filepath.strip()) == 0:
            raise VectorBaseException('Layer filepath must be specified')

        # If layer_name is specified we use it without question
        elif layer_name is not None:
            if len(layer_name.strip()) == 0:
                raise VectorBaseException('Layer name cannot be an empty string. Either a value or None')
            return filepath.strip(), layer_name.strip()

        # If the package is actually there then there is no layer_name
        elif os.path.isfile(filepath):
            return filepath, None

        # Now the fun cases:
        # If this isn't a geopackage just give up.
        if '.gpkg' not in filepath:
            return filepath, None

        matches = re.match(r'(.*\.gpkg)[\\\/]+(.*)', filepath)
        if matches is None:
            return filepath, None
        else:
            return matches[1], matches[2]

    def close(self):
        """Close all file handles and clean up the memory trash for this layer
        """
        self.ogr_layer = None
        self.ogr_layer_def = None
        self.registry.close(self.filepath, self.ogr_layer_name)
        self.ogr_ds = None

    def _create_ds(self):
        """Note: this wipes any existing Datasets (files). It also opens

        Raises:
            Exception: [description]
        """

        # Make a folder if we need to
        ds_dir = os.path.dirname(self.filepath)
        if not os.path.isdir(ds_dir):
            self.log.debug('Creating directory: {}'.format(ds_dir))
            safe_makedirs(ds_dir)

        if os.path.exists(self.filepath):
            self.log.info('Deleting existing dataset: {}'.format(self.filepath))
            self.registry.delete_dataset(self.filepath, self.ogr_layer_name, self.driver)
        else:
            self.log.info('Dataset not found. Creating: {}'.format(self.filepath))

        self.allow_write = True
        self.ogr_ds = self.registry.create(self.filepath, self.ogr_layer_name, self.driver)
        self.log.debug('Dataset created: {}'.format(self.filepath))

    def _open_ds(self):

        if not os.path.exists(self.filepath) or self.replace_ds_on_open is True:
            self._create_ds()
            self.allow_write = True

        elif not os.path.exists(self.filepath):
            raise VectorBaseException('Could not open non-existent dataset: {}'.format(self.filepath))

        permission = 1 if self.allow_write is True else 0

        self.ogr_ds = self.registry.open(self.filepath, self.ogr_layer_name, self.driver, permission)

    def _delete_layer(self):
        """Delete this one layer from the geopackage

        Raises:
            VectorBaseException: [description]
        """
        if self.ogr_layer_name is None:
            raise VectorBaseException('layer name is not specified')
        self.ogr_ds.DeleteLayer(self.ogr_layer_name)
        self.log.info('layer deleted: {} / {}'.format(self.filepath, self.ogr_layer_name))

    def create_layer(self, ogr_geom_type: int, epsg: int = None, spatial_ref: osr.SpatialReference = None, fields: dict = None):
        """[summary]

        Args:
            ogr_geom_type (int): from the enum in ogr i.e. ogr.wkbPolygon
            epsg (int, optional): EPSG Code
            spatial_ref ([osr.SpatialReference], optional): OSR Spatial reference object
            fields (dict, optional): dictionary in the form: {'field name': 4 } where the integer is the ogr.OFTType

        Raises:
            VectorBaseException: [description]
            VectorBaseException: [description]
            NotImplementedError: [description]
        """
        self.ogr_layer = None
        if self.ogr_layer_name is None:
            raise VectorBaseException('No layer name set')
        elif self.ogr_ds is None:
            raise VectorBaseException('Dataset is not open. You must open it first')
        # XOR (^) check to make sure only one of EPSG or spatial_ref are provided
        elif not (epsg is not None) ^ (spatial_ref is not None):
            raise VectorBaseException('Specify either an EPSG or a spatial_ref. Not both')

        elif ogr_geom_type is None:
            raise VectorBaseException('You must specify ogr_geom_type when creating a layer')

        if self.driver_name == VectorBase.Drivers.Shapefile.value:
            pass
        elif self.driver_name == VectorBase.Drivers.Geopackage.value:
            tmplyr = self.ogr_ds.GetLayerByName(self.ogr_layer_name)
            if tmplyr is not None:
                self.ogr_ds.DeleteLayer(self.ogr_layer_name)
        else:
            raise NotImplementedError('Not implemented: {}'.format(self.driver_name))

        if fields is not None:
            self.create_fields(fields)

        if epsg:
            self.spatial_ref = osr.SpatialReference()
            self.spatial_ref.ImportFromEPSG(epsg)
            # NOTE: we hardcode the traditional axis order to help with older datasets
            self.spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        else:
            self.spatial_ref = spatial_ref

        # Throw a warning if our axis mapping strategy is wrong
        self.check_axis_mapping()

        # Get the output Layer's Feature Definition
        self.ogr_layer = self.ogr_ds.CreateLayer(self.ogr_layer_name, self.spatial_ref, geom_type=ogr_geom_type)
        self.ogr_geom_type = self.ogr_layer.GetGeomType()
        self.ogr_layer_def = self.ogr_layer.GetLayerDefn()

    def create_layer_from_ref(self, ref: VectorBase, epsg: int = None):
        """Create a layer by referencing another VectorBase object

        Args:
            input (VectorBase): [description]
            epsg (int, optional): [description]. Defaults to None.
        """
        if epsg is not None:
            self.create_layer(ref.ogr_geom_type, epsg=epsg)
        else:
            self.create_layer(ref.ogr_geom_type, spatial_ref=ref.spatial_ref)

        # We do this instead of a simple key:val dict because we want to capture precision and length info
        for i in range(0, ref.ogr_layer_def.GetFieldCount()):
            fieldDefn = ref.ogr_layer_def.GetFieldDefn(i)
            self.ogr_layer.CreateField(fieldDefn)

    def _open_layer(self):
        if self.driver_name == VectorBase.Drivers.Shapefile.value:
            self.ogr_layer = self.ogr_ds.GetLayer()
        elif self.driver_name == VectorBase.Drivers.Geopackage.value:
            if self.ogr_layer_name is None:
                raise VectorBaseException('For Geopackages you must specify a layer name.')
            self.ogr_layer = self.ogr_ds.GetLayerByName(self.ogr_layer_name)
            # this method is lazy so if there's no layer then just return
            if self.ogr_layer is None:
                self.log.debug('No layer named "{}" found'.format(self.ogr_layer_name))
                return
        else:
            raise VectorBaseException("Error opening layer: {}".format(self.ogr_layer_name))

        # Get the output Layer's Feature Definition and spatial ref
        self.ogr_layer_def = self.ogr_layer.GetLayerDefn()
        self.ogr_geom_type = self.ogr_layer.GetGeomType()
        self.spatial_ref = self.ogr_layer.GetSpatialRef()

        self.check_axis_mapping()

    def check_axis_mapping(self):
        if self.spatial_ref is None:
            raise VectorBaseException('Layer not initialized. No spatial_ref found')
        if self.spatial_ref.GetAxisMappingStrategy() != osr.OAMS_TRADITIONAL_GIS_ORDER:
            _p4, axis_strat = self.get_srs_debug(self.spatial_ref)
            self.log.warning('Axis mapping strategy is: "{}". This may cause axis flipping problems'.format(axis_strat))

    def create_fields(self, fields: dict):
        """Add fields to a layer

        Args:
            fields (dict): dictionary in the form: {'field name': 4 } where the integer is the ogr.OFTType

        Raises:
            VectorBaseException: [description]
        """
        if fields is None or not isinstance(fields, dict):
            raise VectorBaseException('create_fields: Fields must be specified in the form: {\'MyInteger\': ogr.OFTInteger}')
        for fname, ftype in fields.items():
            self.create_field(fname, ftype)

    def get_fields(self) -> dict:
        """[summary]

        Returns:
            dict: Returns a dictionary in the form: {'field name': 4 } where the integer is the ogr.OFTType
        """
        if self.ogr_layer_def is None:
            raise VectorBaseException('get_fields: Layer definition is not defined. Has this layer been created or opened yet?')

        field_dict = {}
        for i in range(0, self.ogr_layer_def.GetFieldCount()):
            field_def = self.ogr_layer_def.GetFieldDefn(i)
            field_dict[field_def.GetName()] = field_def.GetType()

        return field_dict

    def create_field(self, field_name: str, field_type=ogr.OFTReal):
        """
        Remove and then re-add a field to a feature class
        :param layer: Feature class that will receive the attribute field
        :param field_name: Name of the attribute field to be created
        :param log:
        :return: name of the field created (same as function argument)
        """

        if self.ogr_layer is None:
            raise VectorBaseException('No open layer to create fields on')
        elif not field_name or len(field_name) < 1:
            raise VectorBaseException('Attempting to create field with invalid field name "{}".'.format(field_name))
        elif self.driver_name == VectorBase.Drivers.Shapefile.value and len(field_name) > 10:
            raise VectorBaseException('Field names in geopackages cannot be greater than 31 characters. "{}" == {}.'.format(field_name, len(field_name)))
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
        if self.ogr_layer_def is None:
            raise VectorBaseException('Layer not initialized. No ogr_layer_def found')

        for i in range(self.ogr_layer_def.GetFieldCount()):
            actual_name = self.ogr_layer_def.GetFieldDefn(i).GetName()
            if field_name.lower() == actual_name.lower():
                return actual_name

        raise VectorBaseException('Missing field {} in {}'.format(field_name, self.ogr_layer.GetName()))

    def create_feature(self, geom: BaseGeometry, attributes: dict = None):
        """Create a feature from a shapely-type object

        Args:
            out_layer (ogr layer): output feature layer
            feature_def (ogr feature definition): feature definition of the output feature layer
            geom (geometry): geometry to save to feature
            attributes (dict, optional): dictionary of fieldname and attribute values. Defaults to None.
        """
        if self.ogr_layer_def is None:
            raise VectorBaseException('Layer not initialized. No ogr_layer_def found')

        feature = ogr.Feature(self.ogr_layer_def)
        geom_ogr = ogr.CreateGeometryFromWkb(geom.wkb)

        feature.SetGeometry(geom_ogr)

        if attributes:
            for field, value in attributes.items():
                feature.SetField(field, value)

        self.ogr_layer.CreateFeature(feature)
        feature = None

    def iterate_features(
        self, name: str, write_layers: list = None,
        commit_thresh=1000, attribute_filter: str = None, clip_shape: BaseGeometry = None
    ) -> None:
        """This method allows you to pass in a callback that gets run for each feature in a layer

        Args:
            name (str): [description]
            out_layer ([VectorBase], optional): [description]. Defaults to None.
            commit_thresh (int, optional): [description]. Defaults to 1000.
        """

        if self.ogr_layer_def is None:
            raise VectorBaseException('Layer not initialized. No ogr_layer found')

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

        if write_layers is not None:
            done = []
            for lyr in write_layers:
                if lyr.ogr_ds not in done:
                    lyr.ogr_layer.StartTransaction()
                    done.append(lyr.ogr_ds)

        # Get an accurate feature count after clipping and filtering
        fcount = self.ogr_layer.GetFeatureCount()
        progbar = ProgressBar(fcount, 50, name)

        for feature in self.ogr_layer:
            counter += 1
            progbar.update(counter)
            yield (feature, counter, progbar)

            if write_layers is not None and counter % commit_thresh == 0:
                done = []
                for lyr in write_layers:
                    if lyr.ogr_ds not in done:
                        try:
                            lyr.ogr_layer.CommitTransaction()
                        except:
                            pass
                        done.append(lyr.ogr_ds)

        if write_layers is not None:
            done = []
            for lyr in write_layers:
                if lyr.ogr_ds not in done:
                    try:
                        lyr.ogr_layer.CommitTransaction()
                    except:
                        pass
                    done.append(lyr.ogr_ds)

        # Reset the attribute filter
        if attribute_filter:
            self.ogr_layer.SetAttributeFilter('')

        progbar.finish()

    def get_transform(self, out_layer) -> osr.CoordinateTransformation:
        """Get a transform between this layer and another layer

        Args:
            out_layer ([type]): Vector layer type

        Returns:
            [type]: [description]
        """

        if self.spatial_ref is None:
            raise VectorBaseException('No input spatial ref found. Has this layer been created or loaded?')

        elif out_layer.spatial_ref is None:
            raise VectorBaseException('No output spatial ref found. Has this layer been created or loaded?')

        in_proj4, in_ax_strategy = self.get_srs_debug(self.spatial_ref)
        out_proj4, out_ax_strategy = self.get_srs_debug(out_layer.spatial_ref)

        if in_ax_strategy != out_ax_strategy:
            raise VectorBaseException('ERROR: Axis mapping strategy mismatch from "{}" to "{}". This will cause strange x and y coordinates to be transposed.')

        self.log.debug('Input spatial reference is "{}"  Axis Strategy: "{}"'.format(in_proj4, in_ax_strategy))
        self.log.debug('Output spatial reference is "{}"  Axis Strategy: "{}"'.format(out_proj4, out_ax_strategy))

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
        if self.spatial_ref is None:
            raise VectorBaseException('No input spatial ref found. Has this layer been created or loaded?')

        out_spatial_ref = osr.SpatialReference()
        out_spatial_ref.ImportFromEPSG(int(epsg))

        # https://github.com/OSGeo/gdal/issues/1546
        out_spatial_ref.SetAxisMappingStrategy(self.spatial_ref.GetAxisMappingStrategy())

        in_proj4, in_ax_strategy = self.get_srs_debug(self.spatial_ref)
        out_proj4, out_ax_strategy = self.get_srs_debug(out_spatial_ref)

        self.log.debug('Input spatial reference is "{}"  Axis Strategy: "{}"'.format(in_proj4, in_ax_strategy))
        self.log.debug('Output spatial reference is "{}"  Axis Strategy: "{}"'.format(out_proj4, out_ax_strategy))

        transform = osr.CoordinateTransformation(self.spatial_ref, out_spatial_ref)

        return out_spatial_ref, transform

    def rough_convert_metres_to_shapefile_units(self, distance: float) -> float:
        if self.spatial_ref is None:
            raise VectorBaseException('No input spatial ref found. Has this layer been created or loaded?')

        extent = self.ogr_layer.GetExtent()
        return VectorBase.rough_convert_metres_to_dataset_units(self.spatial_ref, extent, distance)

    @ staticmethod
    def rough_convert_metres_to_raster_units(raster_path: str, distance: float) -> float:

        in_ds = gdal.Open(raster_path)
        in_spatial_ref = osr.SpatialReference()
        in_spatial_ref.ImportFromWkt(in_ds.GetProjectionRef())
        gt = in_ds.GetGeoTransform()
        extent = (gt[0], gt[0] + gt[1] * in_ds.RasterXSize, gt[3] + gt[5] * in_ds.RasterYSize, gt[3])

        return VectorBase.rough_convert_metres_to_dataset_units(in_spatial_ref, extent, distance)

    @ staticmethod
    def rough_convert_metres_to_dataset_units(in_spatial_ref: osr.SpatialReference, extent: list, distance: float) -> float:
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
                raise VectorBaseException('Unhandled projected coordinate system linear units: {}'.format(in_spatial_ref.GetAttrValue('unit')))

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

        VectorBase.log.debug('Original spatial reference is : \n       {0} (AxisMappingStrategy:{1})'.format(*VectorBase.get_srs_debug(in_spatial_ref)))
        VectorBase.log.debug('Transform spatial reference is : \n       {0} (AxisMappingStrategy:{1})'.format(*VectorBase.get_srs_debug(out_spatial_ref)))

        transformFwd = osr.CoordinateTransformation(in_spatial_ref, out_spatial_ref)

        pt1_ogr = ogr.CreateGeometryFromWkb(pt1_orig.wkb)
        pt2_ogr = ogr.CreateGeometryFromWkb(pt2_orig.wkb)
        pt1_ogr.Transform(transformFwd)
        pt2_ogr.Transform(transformFwd)

        pt1_proj = wkbload(pt1_ogr.ExportToWkb())
        pt2_proj = wkbload(pt2_ogr.ExportToWkb())

        proj_dist = pt1_proj.distance(pt2_proj)

        output_distance = (orig_dist / proj_dist) * distance

        VectorBase.log.info('{}m distance converts to {:.10f} using UTM EPSG {}'.format(distance, output_distance, utm_epsg))

        if output_distance > 360:
            raise VectorBaseException('Projection Error: \'{:,}\' is larger than the maximum allowed value'.format(output_distance))

        return output_distance

    def verify_raster_spatial_ref(self, raster_path: str):
        if self.spatial_ref is None:
            raise VectorBaseException('No spatial ref found. Has this layer been created or loaded?')
        raster = Raster(raster_path)

        ex = None
        raster_ref = osr.SpatialReference(wkt=raster.proj)
        if not self.spatial_ref.IsSame():
            ex = Exception('ShapeFile and raster spatial references do not match.')
            VectorBase.log.debug('Original spatial reference is : \n       {0} (AxisMappingStrategy:{1})'.format(*VectorBase.get_srs_debug(self.spatial_ref)))
            VectorBase.log.debug('Transform spatial reference is : \n       {0} (AxisMappingStrategy:{1})'.format(*VectorBase.get_srs_debug(raster_ref)))

        raster = None

        if ex:
            raise ex

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
