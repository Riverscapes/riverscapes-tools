"""The base VectorBase Class
"""
from __future__ import annotations
from osgeo import ogr
from rscommons.classes.logger import Logger


class DatasetRegistryException(Exception):
    """Special exceptions

    Args:
        Exception ([type]): [description]
    """
    pass


class Dataset():
    def __init__(self, layer_name: str, ds: ogr.DataSource):
        self.ds = ds
        self.layers = [layer_name]


class DatasetRegistry(object):
    _instance = None
    log = Logger('DatasetRegistry')
    _registry = {}

    def __new__(cls):
        if cls._instance is None:
            print('Creating the Vector Datasource Registry')
            cls._instance = super(DatasetRegistry, cls).__new__(cls)
            # Put any initialization here.
        return cls._instance

    # Here are the workers of th singleton

    def __get_ds(self, filepath: str):
        if filepath not in self._registry:
            return None
        return self._registry[filepath]

    # Now some public methods

    def create(self, filepath: str, layer_name: str, driver: ogr.Driver):
        """Note: this wipes any existing Datasets (files). It also opens

        Args:
            filepath (str): [description]
            driver (ogr.Driver): [description]
        """
        if filepath in self._registry:
            raise DatasetRegistryException('Cannot open a dataset twice')

        self._registry[filepath] = Dataset(layer_name, driver.CreateDataSource(filepath))
        return self._registry[filepath]

    def open(self, filepath: str, layer_name: str, driver: ogr.Driver, permission: int):
        existing_ds = self.__get_ds(filepath)

        # Something's here!
        if existing_ds is not None:
            if layer_name in existing_ds.layers:
                raise Exception('Cannot open a layer twice')
            existing_ds.layers.append(layer_name)

        # Nope. Create it
        else:
            self._registry[filepath] = Dataset(layer_name, driver.Open(filepath, permission))
            existing_ds = self._registry[filepath]
            self.log.debug('Dataset opened: {}'.format(filepath))

        # Otherwise open a new handle all the file existence checking is handled elsewhere
        return existing_ds.ds

    def close(self, filepath: str, layer_name: str):
        """Close a single layer from an open dataset

        Args:
            filepath (str): [description]
            layer_name (str): [description]
            driver (ogr.Driver): [description]
        """
        if layer_name is None:
            return
        if layer_name not in self._registry[filepath].layers:
            raise DatasetRegistryException('Close Error. Layer name not in registry')

        # Remove the layer entry
        self._registry[filepath].layers.remove(layer_name)

        # Clean up if this is the last one
        if len(self._registry[filepath].layers) == 0:
            if self._registry[filepath].ds is not None:
                self._registry[filepath].ds.Destroy()
            del self._registry[filepath]
            self.log.debug('Dataset closed: {}'.format(self.filepath))

    def delete_dataset(self, filepath: str, layer_name: str, driver: ogr.Driver):
        """Delete a dataset and remove that entry from the registry
        """
        if layer_name not in self._registry[filepath].layers:
            raise DatasetRegistryException('Close Error. Layer name not in registry')
        elif len(self._registry[filepath].layers) > 1:
            raise DatasetRegistryException('Cannot delete dataset when there are > 1 layers accessing it. {}'.format(self._registry[filepath].layers))

        # Unload the DS
        if self._registry[filepath].ds is not None:
            self._registry[filepath].ds.Destroy()

        # Delete the Dataset
        driver.DeleteDataSource(filepath)

        # Clean up the registry entry
        del self._registry[filepath]
