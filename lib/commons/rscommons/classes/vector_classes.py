""" Derivative classes of VectorBase
"""
# This will let us return a class we're in the middle of defining
# https://stackoverflow.com/questions/33533148/how-do-i-type-hint-a-method-with-the-type-of-the-enclosing-class
from __future__ import annotations
import os
import re
from osgeo import osr, ogr
from rscommons.classes.vector_base import VectorBase


def get_shp_or_gpkg(filepath: str, *args, **kwargs) -> VectorBase:
    if re.match(r'.*\.shp', filepath) is not None:
        return ShapefileLayer(filepath, *args, **kwargs)
    else:
        return GeopackageLayer(filepath, *args, **kwargs)


class ShapefileLayer(VectorBase):
    """Shapefiles
    """

    def __init__(self, filepath: str, delete=False, write: bool = False):
        """[summary]

        Args:
            filepath ([str]): Path to shapefile
            replace_dataset ([str]): Delete the shapefile if it exists and create another
        """
        # layer name isn't important so we hardcode 'layer'
        super(ShapefileLayer, self).__init__(filepath, VectorBase.Drivers.Shapefile, 'layer', replace_ds_on_open=delete, allow_write=write)
        if delete is True:
            self.delete(filepath)

    def create(self, geom_type: int, epsg: int = None, spatial_ref: osr.SpatialReference = None, fields: dict = None):
        """Create a shapefile and associated layer

        Args:
            ogr_geom_type (int): from the enum in ogr i.e. ogr.wkbPolygon
            epsg (int, optional): EPSG Code
            spatial_ref ([osr.SpatialReference], optional): OSR Spatial reference object
            fields (dict, optional): dictionary in the form: {'field name': 4 } where the integer is the ogr.OFTType

        Returns:
            ShapefileLayer: [description]
        """
        # Shape files get deleted first
        self.allow_write = True
        self._create_ds()
        self.create_layer(geom_type, epsg=epsg, spatial_ref=spatial_ref, fields=fields)

    def open(self, write: bool = False):
        """This is implied in the constructor

        Args:
            allow_write (bool, optional): Allow writing to shapefile? Defaults to False.

        Returns:
            ShapefileLayer: [description]
        """
        self.close()
        self.allow_write = write
        self._open_ds()
        self._open_layer()

    def delete_ds(self) -> None:
        """Delete shapefile and associated sidecar files
        """
        if self.ogr_ds is not None:
            self.ogr_ds.Destroy()
        self.delete(self.filepath)

    @staticmethod
    def delete(filepath: str):
        """Static method to safely delete the dataset if it exists

        Args:
            filepath ([type]): [description]
        """
        if os.path.isfile(filepath):
            driver = ogr.GetDriverByName(VectorBase.Drivers.Shapefile.value)
            VectorBase.log.info('Deleting existing dataset: {}'.format(filepath))
            driver.DeleteDataSource(filepath)
        else:
            VectorBase.log.info('Dataset not found. Continuing: {}'.format(filepath))


class GeopackageLayer(VectorBase):
    """Geopackages
    """

    def __init__(self, filepath: str, layer_name: str = None, delete_dataset: bool = False, write: bool = False):
        """[summary]

        Args:
            filepath (str): Path to geopackage
            layer_name (str, optional): Layer name. Warning: If left as None you won't be able to create a layer or do any operations
            delete_dataset (bool, optional): Replace an ENTIRE dataset if an existing one is already found. Defaults to False.
            allow_write (bool, optional): For opening an existing ds only. Allows write access. Defaults to False.
        """
        super(GeopackageLayer, self).__init__(filepath, VectorBase.Drivers.Geopackage, layer_name=layer_name, replace_ds_on_open=delete_dataset, allow_write=write)
        if delete_dataset is True:
            self.delete_ds()

    def create(self, ogr_geom_type: int, epsg: int = None, spatial_ref: osr.SpatialReference = None, fields: dict = None):
        """Create a layer inside a Geopackage

        Args:
            ogr_geom_type (int): from the enum in ogr i.e. ogr.wkbPolygon
            epsg (int, optional): EPSG Code
            spatial_ref ([osr.SpatialReference], optional): OSR Spatial reference object
            fields (dict, optional): dictionary in the form: {'field name': 4 } where the integer is the ogr.OFTType

        Returns:
            GeopackageLayer: [description]
        """
        self.allow_write = True
        if self.ogr_ds is None:
            if os.path.exists(self.filepath):
                self._open_ds()
            else:
                self._create_ds()
        self.create_layer(ogr_geom_type, epsg=epsg, spatial_ref=spatial_ref, fields=fields)

    def delete_layer(self) -> None:
        """Delete this one layer from the geopackage

        Returns:
            GeopackageLayer: [description]
        """
        self._delete_layer()

    def delete_ds(self) -> None:
        """Delete the entire Geopackage including other layers as well
        """
        if self.ogr_ds is not None:
            self.ogr_ds.Destroy()
        self.delete(self.filepath)

    @staticmethod
    def delete(filepath: str):
        """Static method to safely delete the dataset if it exists

        Args:
            filepath ([type]): [description]
        """
        if os.path.isfile(filepath):
            driver = ogr.GetDriverByName(VectorBase.Drivers.Geopackage.value)
            VectorBase.log.info('Deleting existing dataset: {}'.format(filepath))
            driver.DeleteDataSource(filepath)
        else:
            VectorBase.log.info('Dataset not found. Continuing: {}'.format(filepath))


class GeodatabaseLayer(VectorBase):
    """Geopackages
    """

    def __init__(self, filepath: str, layer_name: str = None, delete_dataset: bool = False, write: bool = False):
        """[summary]

        Args:
            filepath (str): Path to geopackage
            layer_name (str, optional): Layer name. Warning: If left as None you won't be able to create a layer or do any operations
            delete_dataset (bool, optional): Replace an ENTIRE dataset if an existing one is already found. Defaults to False.
            allow_write (bool, optional): For opening an existing ds only. Allows write access. Defaults to False.
        """
        super(GeodatabaseLayer, self).__init__(filepath, VectorBase.Drivers.GeoDatabase, layer_name=layer_name, replace_ds_on_open=delete_dataset, allow_write=write)
