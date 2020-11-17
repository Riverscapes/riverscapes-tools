from .vector_base import VectorLayer, DRIVER_MAP
import os
from osgeo import osr


class ShapeLayer(VectorLayer):
    def __init__(self, filepath):
        # layer name isn't important so we hardcode 'layer'
        super(ShapeLayer, self).__init__(filepath, DRIVER_MAP['shp'], 'layer')

    def create(self, geom_type, epsg=None, spatial_ref=None):
        # Shape files get deleted first
        self._create_ds()
        self._create_layer(geom_type, epsg, spatial_ref)

    def open(self, allow_write=False):
        self._open_ds(allow_write=allow_write)
        self._open_layer()

    def delete(self):
        if self.ogr_ds is not None:
            self.ogr_ds.Destroy()
        self.driver.DeleteDataSource(self.filepath)


class GeopackageLayer(VectorLayer):
    def __init__(self, filepath, layer_name: str = None):
        super(GeopackageLayer, self).__init__(filepath, DRIVER_MAP['gpkg'], layer_name=layer_name)

    def create_ds(self):
        self._create_ds()

    def create_layer(self, ogr_geom_type, epsg: int = None, spatial_ref: osr.SpatialReference = None):
        if os.path.exists(self.filepath):
            self._open_ds(allow_write=True)
        else:
            self._create_ds()
        self._create_layer(ogr_geom_type, epsg=epsg, spatial_ref=spatial_ref)

    def delete_layer(self):
        # TODO
        if self.ogr_ds is not None:
            self.ogr_ds.Destroy()
        self.driver.DeleteDataSource(self.filepath)

    def delete_ds(self):
        if self.ogr_ds is not None:
            self.ogr_ds.Destroy()
        self.driver.DeleteDataSource(self.filepath)
