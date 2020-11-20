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
        """[summary]

        Args:
            filepath (str): path to the dataset. Could be a compound path for gropackages like '/path/to/geopackeg.gpkg/layer_name'
            driver (VectorBase.Drivers): Enum element corresponding to shp or gpkg drivers
            layer_name (str): name of the layer. Required but not used for shapefiles (make something up)
            replace_ds_on_open (bool, optional): Blow away the whole DS and all layers on open. Defaults to False.
            allow_write (bool, optional): Allow writing to this layer. Defaults to False.
        """
        # The Dataset registry is a singleton that keeps track of our datasets
        self.__ds_reg = DatasetRegistry()

        # Sometimes the path for the geopackage comes in as /path/to/layer.gpkg/schema.layer
        # This could be a compound path
        self.filepath = None
        self.ogr_layer_name = None
        self.filepath, self.ogr_layer_name = VectorBase.path_sorter(filepath, layer_name)

        self.driver_name = driver.value
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

    def __enter__(self) -> VectorBase:
        """Behaviour on open when using the "with VectorBase():" Syntax
        """
        # self.log.debug('__enter__ called')
        self._open_ds()
        self._open_layer()
        return self

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with VectorBase():" Syntax
        """
        # self.log.debug('__exit__ called. Cleaning up.')
        self.close()

    @staticmethod
    def path_sorter(filepath: str, layer_name: str = None) -> (str, str):
        """Sometimes the path for the geopackage comes in as /path/to/layer.gpkg/schema.layer

        Args:
            filepath (str): path or compound path to the dataset and/or layer_name
            layer_name (str, optional): layer_name. Use this if it is specified. Defaults to None.

        Raises:
            VectorBaseException: [description]

        Returns:
            (str, str): checked and sanitized (filepath, layername) strings
        """
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

        # take our best guess at separating the file path from the layer name
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
        self.__ds_reg.close(self.filepath, self.ogr_layer_name)
        self.ogr_ds = None

    def _create_ds(self):
        """Note: this wipes any existing Datasets (files). It also creates and
                opens the new dataset. This makes heavy use of the DatasetRegistry

        Raises:
            VectorBaseException: [description]
        """

        # Make a folder if we need to
        ds_dir = os.path.dirname(self.filepath)
        if not os.path.isdir(ds_dir):
            self.log.debug('Creating directory: {}'.format(ds_dir))
            safe_makedirs(ds_dir)

        # Wipe the existing dataset if it exists.
        if os.path.exists(self.filepath):
            self.log.info('Deleting existing dataset: {}'.format(self.filepath))
            self.__ds_reg.delete_dataset(self.filepath, self.driver)
        else:
            self.log.info('Dataset not found. Creating: {}'.format(self.filepath))

        self.allow_write = True
        self.ogr_ds = self.__ds_reg.create(self.filepath, self.ogr_layer_name, self.driver)
        self.log.debug('Dataset created: {}'.format(self.filepath))

    def _open_ds(self):
        """Open a dataset. Makes heavy use of the dataset registry

        Raises:
            VectorBaseException: [description]
        """
        # If the dataset doesn't exist or if we've asked it to be recreated then do that
        if not os.path.exists(self.filepath) or self.replace_ds_on_open is True:
            self._create_ds()
            self.allow_write = True
            return

        # If it doesn't exist at all then throw
        elif not os.path.exists(self.filepath):
            raise VectorBaseException('Could not open non-existent dataset: {}'.format(self.filepath))

        permission = 1 if self.allow_write is True else 0
        self.ogr_ds = self.__ds_reg.open(self.filepath, self.ogr_layer_name, self.driver, permission)

    def _delete_layer(self):
        """Delete this one layer from the geopackage

        Raises:
            VectorBaseException: [description]
        """
        if self.ogr_layer_name is None:
            raise VectorBaseException('layer name is not specified')
        elif self.ogr_ds is None:
            raise VectorBaseException('Dataset is not open. Cannot delete layer')

        self.ogr_ds.DeleteLayer(self.ogr_layer_name)
        self.log.info('layer deleted: {} / {}'.format(self.filepath, self.ogr_layer_name))

    def create_layer(self, ogr_geom_type: int, epsg: int = None, spatial_ref: osr.SpatialReference = None, fields: dict = None):
        """Create a layer in a dataset. Existing layers will be deleted

        Args:
            ogr_geom_type (int): from the enum in ogr i.e. ogr.wkbPolygon
            epsg (int, optional): EPSG Code
            spatial_ref ([osr.SpatialReference], optional): OSR Spatial reference object
            fields (dict, optional): dictionary in the form: {'field name': 4 } where the integer is the ogr.OFTType

        Raises:
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

        # TODO: THis feels fishy. Test the Shapefile recreation workflow
        if self.driver_name == VectorBase.Drivers.Shapefile.value:
            # There is only ever one layer in a shapefile
            pass
        elif self.driver_name == VectorBase.Drivers.Geopackage.value:
            # Delete the layer if it already exists
            if self.ogr_ds.GetLayerByName(self.ogr_layer_name) is not None:
                self.ogr_ds.DeleteLayer(self.ogr_layer_name)
        else:
            # Unrecognized driver
            raise NotImplementedError('Not implemented: {}'.format(self.driver_name))

        # If we've supplied a fields dictionary then use it
        if fields is not None:
            self.create_fields(fields)

        # CHoose the spatial ref one of two ways:
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

    def create_layer_from_ref(self, ref: VectorBase, epsg: int = None, create_fields: bool = True):
        """Create a layer by referencing another VectorBase object.

        Args:
            input (VectorBase): [description]
            epsg (int, optional): [description]. Defaults to None.
            create_fields (bool, optional): Opt out of mirroring the fields. Defaults to True.
        """
        if epsg is not None:
            self.create_layer(ref.ogr_geom_type, epsg=epsg)
        else:
            self.create_layer(ref.ogr_geom_type, spatial_ref=ref.spatial_ref)

        # We do this instead of a simple key:val dict because we want to capture precision and length info
        if create_fields is True:
            for i in range(0, ref.ogr_layer_def.GetFieldCount()):
                fieldDefn = ref.ogr_layer_def.GetFieldDefn(i)
                self.ogr_layer.CreateField(fieldDefn)

    def _open_layer(self):
        """Internal method for opening a layer and getting the ogr_layer, ogr_layer_def, spatial_ref etc.

        Raises:
            VectorBaseException: [description]
        """
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
        # Note: For newly-created shapefiles there won't be a file or an ogr_layer object
        if self.ogr_layer:
            self.ogr_layer_def = self.ogr_layer.GetLayerDefn()
            self.ogr_geom_type = self.ogr_layer.GetGeomType()
            self.spatial_ref = self.ogr_layer.GetSpatialRef()

            self.check_axis_mapping()

    def check_axis_mapping(self):
        """Make sure our Axis Mapping strategy is correct (according to our arbitrary convention)
            We set this in opposition to what GDAL3 Thinks is the default because that's what 
            most of our old Geodatabases and Shapefiles seem to use.

        Raises:
            VectorBaseException: [description]
        """
        if self.spatial_ref is None:
            raise VectorBaseException('Layer not initialized. No spatial_ref found')
        if self.spatial_ref.GetAxisMappingStrategy() != osr.OAMS_TRADITIONAL_GIS_ORDER:
            _p4, axis_strat = self.get_srs_debug(self.spatial_ref)
            self.log.warning('Axis mapping strategy is: "{}". This may cause axis flipping problems'.format(axis_strat))

    def create_fields(self, fields: dict):
        """Add fields to a layer

        Args:
            fields (dict): dictionary in the form:  {'field name': ogr.FieldDefn } OR {'field name': 4 } where the integer is the ogr.OFTType 

        Raises:
            VectorBaseException: [description]
        """
        if fields is None or not isinstance(fields, dict):
            raise VectorBaseException('create_fields: Fields must be specified in the form: {\'MyInteger\': ogr.OFTInteger}')
        for fname, ftype in fields.items():
            self.create_field(fname, ftype)

    def get_fields(self) -> dict:
        """ Get a layer's fields as a simple dictionary

        Returns:
            dict: Returns a dictionary in the form: {'field name': ogr.FieldDefn }
        """
        if self.ogr_layer_def is None:
            raise VectorBaseException('get_fields: Layer definition is not defined. Has this layer been created or opened yet?')

        field_dict = {}
        for i in range(0, self.ogr_layer_def.GetFieldCount()):
            field_def = self.ogr_layer_def.GetFieldDefn(i)
            field_dict[field_def.GetName()] = field_def

        return field_dict

    def create_field(self, field_name: str, field_type: int = None, field_def: ogr.FieldDefn = None):
        """Remove and then re-add a field to a feature class

        Args:
            field_name (str): Name of the attribute field to be created
            field_type (int, optional): ogr type to use. Defaults to None. OR
            field_def (ogr.FieldDefn, optional): [description]. Defaults to None.

        Raises:
            VectorBaseException: [description]
            VectorBaseException: [description]
            VectorBaseException: [description]

        Returns:
            [type]: [description]
        """
        if self.ogr_layer is None:
            raise VectorBaseException('No open layer to create fields on')

        # XOR (^) check to make sure only one of field_type or field_def are provided
        elif not (field_type is not None) ^ (field_def is not None):
            raise VectorBaseException('create_field must have EITHER field_type or field_def as an input')

        elif not field_name or len(field_name) < 1:
            raise VectorBaseException('Attempting to create field with invalid field name "{}".'.format(field_name))

        elif self.driver_name == VectorBase.Drivers.Shapefile.value and len(field_name) > 10:
            raise VectorBaseException('Field names in geopackages cannot be greater than 31 characters. "{}" == {}.'.format(field_name, len(field_name)))

        elif field_type is not None and field_type == ogr.OFTInteger64:
            self.log.error('ERROR:: ogr.OFTInteger64 is not supported by ESRI!')

        # Delete output column from vector layer if it exists and then recreate it
        for fieldidx in range(0, self.ogr_layer_def.GetFieldCount()):
            if self.ogr_layer_def.GetFieldDefn(fieldidx).GetName() == field_name:
                self.log.info('Deleting existing output field "{}" in vector layer.'.format(field_name))
                self.ogr_layer.DeleteField(fieldidx)
                break

        self.log.info('Creating output field "{}" in layer.'.format(field_name))

        if field_def is None:
            field_def = ogr.FieldDefn(field_name, field_type)

        # Good precision convention for Real floating point values
        if field_type is not None and field_type == ogr.OFTReal:
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
        self, name: str = None, write_layers: list = None,
        commit_thresh=1000, attribute_filter: str = None, clip_shape: BaseGeometry = None
    ) -> None:
        """[summary]

        Args:
            name (str): Name for use on the progress bar. If ommitted you won't get a progress bar
            write_layers (list, optional): [description]. Defaults to None.
            commit_thresh (int, optional): Change how often CommitTransaction gets called. Defaults to 1000.
            attribute_filter (str, optional): Attribute Query like "HUC = 17060104". Defaults to None.
            clip_shape (BaseGeometry, optional): Iterate over a subset by clipping to a Shapely-ish geometry. Defaults to None.
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
            # https://gdal.org/python/osgeo.ogr.Layer-class.html#SetSpatialFilter
            self.ogr_layer.SetSpatialFilter(clip_geom)

        if attribute_filter:
            # https://gdal.org/python/osgeo.ogr.Layer-class.html#SetAttributeFilter
            self.ogr_layer.SetAttributeFilter(attribute_filter)

        # For sql-based datasets we use transactions to optimize writing to the file
        VectorBase.__start_transaction(write_layers)

        # Get an accurate feature count after clipping and filtering
        fcount = self.ogr_layer.GetFeatureCount()

        # Initialize the progress bar
        progbar = None
        if name is not None:
            progbar = ProgressBar(fcount, 50, name)

        # Loop over every filtered feature
        for feature in self.ogr_layer:
            counter += 1
            if progbar is not None:
                progbar.update(counter)
            yield (feature, counter, progbar)

            # Write to the file only every N times using transactions
            if counter % commit_thresh == 0:
                VectorBase.__commit_transaction(write_layers)
                VectorBase.__start_transaction(write_layers)

        # If there's anything left to write at the end then write it
        VectorBase.__commit_transaction(write_layers)

        # Reset the attribute filter
        if attribute_filter:
            self.ogr_layer.SetAttributeFilter('')

        # Finalize the progress bar
        if progbar is not None:
            progbar.finish()

    @staticmethod
    def __start_transaction(write_layers: list):
        if write_layers is None:
            return
        done = []
        for lyr in write_layers:
            if lyr.ogr_ds not in done:
                try:
                    lyr.ogr_layer.StartTransaction()
                except Exception as e:
                    print(e)
                done.append(lyr.ogr_ds)

    @staticmethod
    def __commit_transaction(write_layers: list):
        if write_layers is None:
            return
        done = []
        for lyr in write_layers:
            if lyr.ogr_ds not in done:
                try:
                    lyr.ogr_layer.CommitTransaction()
                except Exception as e:
                    print(e)
                done.append(lyr.ogr_ds)

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

    def rough_convert_metres_to_vector_units(self, distance: float) -> float:
        """Convert from Meters into this layer's units

        DO NOT USE THIS FOR ACCURATE DISTANCES. IT'S GOOD FOR A QUICK CALCULATION
        WHEN DISTANCE PRECISION ISN'T THAT IMPORTANT

        Args:
            distance (float): Distance in meters to convert

        Raises:
            VectorBaseException: [description]

        Returns:
            float: Distance in this layer's units
        """
        if self.spatial_ref is None:
            raise VectorBaseException('No input spatial ref found. Has this layer been created or loaded?')

        extent = self.ogr_layer.GetExtent()
        return VectorBase.rough_convert_metres_to_spatial_ref_units(self.spatial_ref, extent, distance)

    @ staticmethod
    def rough_convert_metres_to_raster_units(raster_path: str, distance: float) -> float:
        """Convert from Meters into the units of the supplied raster

        DO NOT USE THIS FOR ACCURATE DISTANCES. IT'S GOOD FOR A QUICK CALCULATION
        WHEN DISTANCE PRECISION ISN'T THAT IMPORTANT

        Args:
            raster_path (str): Path to raster file
            distance (float): Distance in meters

        Returns:
            float: Distance in Raster's units
        """

        if not os.path.isfile(raster_path):
            raise VectorBaseException('Raster file not found: {}'.format(raster_path))

        in_ds = gdal.Open(raster_path)
        in_spatial_ref = osr.SpatialReference()
        in_spatial_ref.ImportFromWkt(in_ds.GetProjectionRef())
        gt = in_ds.GetGeoTransform()
        extent = (gt[0], gt[0] + gt[1] * in_ds.RasterXSize, gt[3] + gt[5] * in_ds.RasterYSize, gt[3])

        return VectorBase.rough_convert_metres_to_spatial_ref_units(in_spatial_ref, extent, distance)

    @ staticmethod
    def rough_convert_metres_to_spatial_ref_units(in_spatial_ref: osr.SpatialReference, extent: list, distance: float) -> float:
        """Convert from meters to the units of a given spatial_ref

        DO NOT USE THIS FOR ACCURATE DISTANCES. IT'S GOOD FOR A QUICK CALCULATION
        WHEN DISTANCE PRECISION ISN'T THAT IMPORTANT

        Args:
            in_spatial_ref (osr.SpatialReference): osr.SpatialRef to use as a reference 
            extent (list): The extent of our dataset: [min_x, min_y, max_x, max_y]. We use this to find the centerpoint
            distance (float): Distance in meters to convert

        Raises:
            VectorBaseException: [description]

        Returns:
            float: Distance in the spatial_ref's units
        """

        # If the input ref uses a projected coordinate system in meters then simply return the distance.
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

        transform_forward = osr.CoordinateTransformation(in_spatial_ref, out_spatial_ref)

        pt1_ogr = ogr.CreateGeometryFromWkb(pt1_orig.wkb)
        pt2_ogr = ogr.CreateGeometryFromWkb(pt2_orig.wkb)
        pt1_ogr.Transform(transform_forward)
        pt2_ogr.Transform(transform_forward)

        pt1_proj = wkbload(pt1_ogr.ExportToWkb())
        pt2_proj = wkbload(pt2_ogr.ExportToWkb())

        proj_dist = pt1_proj.distance(pt2_proj)

        output_distance = (orig_dist / proj_dist) * distance

        VectorBase.log.info('{}m distance converts to {:.10f} using UTM EPSG {}'.format(distance, output_distance, utm_epsg))

        if output_distance > 360:
            raise VectorBaseException('Projection Error: \'{:,}\' is larger than the maximum allowed value'.format(output_distance))

        return output_distance

    def verify_raster_spatial_ref(self, raster_path: str):
        """Make sure our raster's spatial ref matches this layer's ref

        Args:
            raster_path (str): [description]

        Raises:
            VectorBaseException: [description]
            ex: [description]
        """
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
        """useful method for printing spatial ref information to the log

        Args:
            spatial_ref (osr.SpatialReference): [description]

        Returns:
            [str, str]: [description]
        """
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
    """Really crude EPSG lookup method

    Args:
        longitude (float): [description]

    Returns:
        int: [description]
    """
    zone_number = math.floor((180.0 + longitude) / 6.0)
    epsg = 26901 + zone_number
    return epsg
