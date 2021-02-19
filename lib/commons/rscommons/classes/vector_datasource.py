"""The base VectorBase Class
"""
from __future__ import annotations
from osgeo import ogr
from rscommons.classes.logger import Logger
from rscommons.util import safe_remove_file

class DatasetRegistryException(Exception):
    """Special exceptions

    Args:
        Exception ([type]): [description]
    """
    pass


class Dataset():
    """Basic class for holding dataset and its corresponding layers
    """

    def __init__(self, layer_name: str, ds: ogr.DataSource, permission: int):
        """[summary]

        Args:
            layer_name (str): Layer name for the registry
            ds (ogr.DataSource): [description]
            permission (int): 0 = read, 1=write
        """
        self.ds = ds
        self.permission = permission
        self.layers = [layer_name]


class DatasetRegistry(object):
    """Singleton pattern for a dataset registry

    Args:
        object ([type]): [description]

    Raises:
        DatasetRegistryException: [description]
        Exception: [description]

    Returns:
        [type]: [description]
    """
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
        """Note: Create a DATASET this wipes any existing Datasets (files). It also opens a new one.
                This has nothing to do with layers

        Args:
            filepath (str): [description]
            driver (ogr.Driver): [description]
        """
        if filepath in self._registry:
            raise DatasetRegistryException('Cannot open a dataset twice')

        self._registry[filepath] = Dataset(layer_name, driver.CreateDataSource(filepath), permission=1)
        return self._registry[filepath].ds

    def open(self, filepath: str, layer_name: str, driver: ogr.Driver, permission: int):
        """Open a dataset

        Args:
            filepath (str): [description]
            layer_name (str): [description]
            driver (ogr.Driver): [description]
            permission (int): 0 = read, 1 = write

        Raises:
            DatasetRegistryException: [description]

        Returns:
            [type]: [description]
        """
        existing_ds = self.__get_ds(filepath)

        # Something's here. Just return it!
        if existing_ds is not None:
            # If the new handle needs more permissions than the old one then we have to close and re-open
            if existing_ds.permission == 0 and permission > 0:
                raise DatasetRegistryException('You cannot open a geopackage for reading and then open it for writing.')
            if layer_name in existing_ds.layers:
                raise DatasetRegistryException('Cannot open a layer twice')
            existing_ds.layers.append(layer_name)

        # Nope. Create it
        else:
            self._registry[filepath] = Dataset(layer_name, driver.Open(filepath, permission), permission)
            existing_ds = self._registry[filepath]
            # self.log.debug('Dataset opened: {} for {}'.format(filepath, 'READING' if permission == 0 else 'WRITING'))

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
            # self.log.debug('Dataset closed: {}'.format(filepath))

    def delete_dataset(self, filepath: str, driver: ogr.Driver):
        """Delete a dataset and remove that entry from the registry
        """
        # If this dataset is known to the registry then we need to handle it
        if filepath in self._registry:
            if len(self._registry[filepath].layers) > 1:
                raise DatasetRegistryException('Cannot delete dataset when there are > 1 layers accessing it. {}'.format(self._registry[filepath].layers))

            # Unload the DS
            if self._registry[filepath].ds is not None:
                self._registry[filepath].ds.Destroy()
                # Clean up the registry entry
                del self._registry[filepath]

        # Delete the Dataset
        err = driver.DeleteDataSource(filepath)
        
        # If this is a tempfile there's a possibility of failure. 
        # In that case just remove the file normally (or try anyway)
        if err == ogr.OGRERR_FAILURE:
            safe_remove_file(filepath)

