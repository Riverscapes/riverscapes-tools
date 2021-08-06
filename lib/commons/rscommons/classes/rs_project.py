
# Name:        BRAT Project Builder
#
# Purpose:     Gathers and structures the data related to  a BRAT project
#
# Author:      Jordan Gilbert
#
# Created:     09/25/2015
# -------------------------------------------------------------------------------
from __future__ import annotations
from typing import List, Dict
import os
import datetime
import uuid

import rasterio.shutil
from osgeo import ogr
from copy import copy

from rscommons import Logger
from rscommons.rspaths import parse_posix_path
from rscommons.classes.xml_builder import XMLBuilder
from rscommons.util import safe_makedirs
from rscommons.vector_ops import copy_feature_class


_folder_inputs = '01_Inputs'
_folder_analyses = '02_Analyses'

LayerTypes = {
    'DEM': {
        'FileName': 'dem',
        'XMLTag': 'DEM'
    },
    'DA': {
        'FileName': 'drainarea_sqkm'
    },
    'EXVEG': {
        'FileName': 'existing_veg'
    },
    'HISTVEG': {
        'FileName': 'historical_veg'
    },
    'NETWORK': {
        'FileName': 'network'
    },
    'RESULT': {
        'FileName': 'brat'
    }
}


class RSLayer:
    def __init__(self, name: str, id: str, tag: str, rel_path: str, sub_layers: dict = None):
        """[summary]

        Args:
            name (str): The <Name> to be used 
            id (str): The <Name id=""> id to be used
            tag (str): The tag to be used for the vector layer
            rel_path (str): The path relative to the project file
            sub_layers (dict, optional): If this is a geopackage you can . Defaults to None.

        Raises:
            Exception: Name is required
            Exception: id is required
            Exception: rel_path is required
            Exception: Only Geopackages can have sub layers
            Exception: sub_layers must but a list of RSLayer(s)
        """
        if name is None:
            raise Exception('Name is required')
        if id is None:
            raise Exception('id is required')
        if rel_path is None:
            raise Exception('rel_path is required')
        if tag != 'Geopackage' and sub_layers is not None:
            raise Exception('Only Geopackages can have sub layers')
        if sub_layers is not None:
            # Make sure if we're a sub_layer that we've got the right shape
            if not type(sub_layers) == dict or \
                    not all([type(list(sub_layers.values())[0]) == RSLayer for a in sub_layers]):
                raise Exception('sub_layers must but a list of RSLayer(s)')
            self.sub_layers = sub_layers
        else:
            self.sub_layers = None

        self.name = name
        self.id = id
        self.tag = tag
        self.rel_path = rel_path

    def add_sub_layer(self, key: str, layer: RSLayer):
        """Add a geopackage sublayer

        Args:
            key (str): Name of the layer. For lookup purposes only
            layer (RSLayer): RSLayer
        """
        if not self.sub_layers:
            self.sub_layers = {key: layer}
        else:
            self.sub_layers[key] = layer


class RSProject:
    """
    BRAT riverscapes project
    """

    def __init__(self, settings, project_path):
        """The constructor doesn't create anything. It just sets up the class to be able
        to either read or create a new XML file
        Arguments:
            settings {[type]} -- [description]
            project_path {[type]} -- [description]
        Keyword Arguments:
            replace {bool} -- [description] (default: {False})
        """
        self.settings = settings

        # This might be an existing XML file
        if os.path.isfile(project_path):
            self.xml_path = project_path
            self.XMLBuilder = XMLBuilder(self.xml_path)

        # This might be an existing directory
        elif os.path.isdir(project_path):
            new_xml_path = os.path.join(project_path, self.settings.PROJ_XML_FILE)
            self.xml_path = new_xml_path
            self.XMLBuilder = XMLBuilder(self.xml_path)

        # Otherwise just treat it like a new directory
        else:
            self.xml_path = project_path

        self.project_dir = os.path.dirname(self.xml_path)

    def create(self, name, project_type, replace=True):
        """Create or overwrite an existing project xml file

        Arguments:
            name {[type]} -- [description]
            project_type {[type]} -- [description]
            modelVersion {[type]} -- [description]
            xsd_url {[type]} -- [description]
            output_dir {[type]} -- [description]
        """

        if os.path.isfile(self.xml_path):
            if replace:
                os.remove(self.xml_path)
            else:
                raise Exception('Cannot replace existing project. Exiting: {}'.format(self.xml_path))

        safe_makedirs(self.project_dir)

        self.XMLBuilder = XMLBuilder(self.xml_path, 'Project', {
            'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xsi:noNamespaceSchemaLocation': self.settings.XSD_URL
        })
        self.XMLBuilder.add_sub_element(self.XMLBuilder.root, "Name", name)
        self.XMLBuilder.add_sub_element(self.XMLBuilder.root, "ProjectType", project_type)

        self.add_metadata({
            'ModelVersion': self.settings.version,
            'dateCreated': datetime.datetime.now().isoformat()
        })

        self.XMLBuilder.write()
        self.exists = True

    def add_metadata(self, valdict, node=None):
        # log = Logger('add_metadata')
        metadata_element = node.find('MetaData') if node is not None else self.XMLBuilder.find('MetaData')
        for mkey, mval in valdict.items():
            if metadata_element is None:
                if node is not None:
                    metadata_element = self.XMLBuilder.add_sub_element(node, "MetaData")
                else:
                    metadata_element = self.XMLBuilder.add_sub_element(self.XMLBuilder.root, "MetaData")

            found = metadata_element.findall('Meta[@name="{}"]'.format(mkey))
            # Only one key-value pair are allowed with the same name. This cleans up any stragglers
            if len(found) > 0:
                for f in found:
                    metadata_element.remove(f)

            # Note: we don't do a replace=False here because that only verifies the id attribute and we're
            # using 'name' for uniqueness
            self.XMLBuilder.add_sub_element(metadata_element, "Meta", mval, {"name": mkey})

        self.XMLBuilder.write()

    def get_metadata_dict(self, node=None, tag='MetaData'):
        """Reverse lookup to pull Metadata out of the raw XML report

        Args:
            node ([type], optional): [description]. Defaults to None.
            tag (str, optional): [description]. Defaults to 'MetaData'.

        Returns:
            [type]: [description]
        """
        metadata_element = node.find(tag) if node is not None else self.XMLBuilder.find(tag)
        if metadata_element is None:
            return None
        children = list(metadata_element)
        valdict = {}
        for child in children:
            valdict[child.attrib['name']] = child.text

        return valdict

    def get_unique_path(self, folder, name, extension):

        existingPaths = [aPath.text for aPath in self.XMLBuilder.root.iter('Path')]

        file_path = os.path.join(folder, name)
        pre, _ext = os.path.splitext(file_path)
        file_path = '{}.{}'.format(pre, extension)

        i = 1
        while os.path.relpath(file_path, os.path.dirname(self.xml_path)) in existingPaths:
            file_path = '{}_{}.{}'.format(pre, i, extension)
            i += 1

        return file_path

    def get_relative_path(self, abs_path):
        return abs_path[len() + 1:]

    def add_dataset(self, parent_node, path_val, rs_lyr, default_tag, replace=False, rel_path=False):

        xml_tag = rs_lyr.tag if rs_lyr.tag is not None else default_tag
        id = rs_lyr.id if replace else RSProject.unique_type_id(parent_node, xml_tag, rs_lyr.id)

        if replace:
            self.XMLBuilder.delete_sub_element(parent_node, xml_tag, id)

        attribs = {
            'guid': str(uuid.uuid4()),
            'id': id
        }
        nod_dataset = self.XMLBuilder.add_sub_element(parent_node, xml_tag, attribs=attribs)
        self.XMLBuilder.add_sub_element(nod_dataset, 'Name', rs_lyr.name)

        # Sanitize our paths to always produce linux-style slashes
        if rel_path:
            self.XMLBuilder.add_sub_element(nod_dataset, 'Path', parse_posix_path(path_val))
        else:
            posix_path_val = parse_posix_path(os.path.relpath(path_val, os.path.dirname(self.xml_path)))
            self.XMLBuilder.add_sub_element(nod_dataset, 'Path', posix_path_val)
        self.XMLBuilder.write()
        return nod_dataset

    def add_project_vector(self, parent_node, rs_lyr, copy_path=None, replace=False, att_filter=None):
        """NOTE: this is for shapefiles only and we might be phasing it out. Ask yourself "Should I really
                have shapefiles in my project?"

        Args:
            parent_node ([type]): The Eltree XML node to use as the parent
            rs_lyr ([type]): The Layer object to use as an input
            copy_path ([type], optional): Copy this layer to a shapefile. Defaults to None.
            replace (bool, optional): [description]. Defaults to False.
            att_filter ([type], optional): [description]. Defaults to None.

        Returns:
            [type]: [description]
        """
        log = Logger('add_project_vector')

        file_path = os.path.join(os.path.dirname(self.xml_path), rs_lyr.rel_path)
        file_dir = os.path.dirname(file_path)

        # Create the folder if we need to
        safe_makedirs(file_dir)

        if copy_path is not None or replace is True:
            # Delete existing copies so we can re-copy them
            if os.path.exists(file_path):
                log.debug('Existing file found. deleting: {}'.format(file_path))
                driver = ogr.GetDriverByName("ESRI Shapefile")
                driver.DeleteDataSource(file_path)

        if copy_path is not None:
            # TODO: need a good "layer exists" that covers both ShapeFile and GeoPackages
            # if not  os.path.exists(copy_path):
            #     log.error('Could not find mandatory input "{}" shapefile at path "{}"'.format(rs_lyr.name, copy_path))
            log.info('Copying dataset: {}'.format(rs_lyr.name))

            # Rasterio copies datasets efficiently
            copy_feature_class(copy_path, file_path, self.settings.OUTPUT_EPSG, attribute_filter=att_filter)
            log.debug('Shapefile Copied {} to {}'.format(copy_path, file_path))

        nod_dataset = self.add_dataset(parent_node, file_path, rs_lyr, 'Vector', replace)
        return nod_dataset, file_path

    def add_project_raster(self, parent_node, rs_lyr, copy_path=None, replace=False):
        log = Logger('add_project_raster')

        file_path = os.path.join(os.path.dirname(self.xml_path), rs_lyr.rel_path)
        file_dir = os.path.dirname(file_path)

        # Create the folder if we need to
        safe_makedirs(file_dir)

        if copy_path is not None or replace is True:
            # Delete existing copies so we can re-copy them
            if os.path.exists(file_path):
                log.debug('Existing file found. deleting: {}'.format(file_path))
                try:
                    rasterio.shutil.delete(file_path)
                except Exception as e:
                    log.debug(e)
                    log.debug('Raster possibly corrupt. Deleting using file system')
                    os.remove(file_path)

        if copy_path is not None:
            if not os.path.exists(copy_path) or not rs_lyr:
                log.error('Could not find mandatory input "{}" raster at path "{}"'.format(rs_lyr.name, copy_path))

            # Rasterio copies datasets efficiently
            rasterio.shutil.copy(copy_path, file_path, compress='LZW', predictor=2)
            log.info('Raster Copied {} to {}'.format(copy_path, file_path))

        nod_dataset = self.add_dataset(parent_node, file_path, rs_lyr, 'Raster', replace)
        return nod_dataset, file_path

    def add_project_geopackage(self, parent_node, rs_lyr, copy_path=None, replace=False):
        log = Logger('add_project_geopackage')

        file_path = os.path.join(os.path.dirname(self.xml_path), rs_lyr.rel_path)
        file_dir = os.path.dirname(file_path)

        # Create the folder if we need to
        safe_makedirs(file_dir)
        driver = ogr.GetDriverByName("GPKG")

        if copy_path is not None or replace is True:
            # Delete existing copies so we can re-copy them
            if os.path.exists(file_path):
                log.debug('Existing file found. deleting: {}'.format(file_path))
                driver.DeleteDataSource(file_path)

        if copy_path is not None:
            if not os.path.exists(copy_path):
                log.error('Could not find mandatory input "{}" geopackage at path "{}"'.format(rs_lyr.name, copy_path))
            log.info('Copying dataset: {}'.format(rs_lyr.name))
            driver.CopyDataSource(copy_path, file_path)

            # Rasterio copies datasets efficiently
            log.debug('Geopackage Copied {} to {}'.format(copy_path, file_path))

        # Add in our sublayers
        sub_layers = {}
        if rs_lyr.sub_layers is not None and len(rs_lyr.sub_layers) > 0:
            nod_dataset = self.add_dataset(parent_node, file_path, rs_lyr, 'Geopackage', replace)
            layers_node = self.XMLBuilder.add_sub_element(nod_dataset, 'Layers')
            for rssublyr_name, rssublyr in rs_lyr.sub_layers.items():
                sub_abs_path = os.path.join(file_path, rssublyr.rel_path)
                sub_nod = self.add_dataset(layers_node, rssublyr.rel_path, rssublyr, rssublyr.tag, rel_path=True)
                sub_layers[rssublyr_name] = (sub_nod, sub_abs_path)

        return nod_dataset, file_path, sub_layers

    def add_report(self, parent_node, rs_lyr, replace=False):
        log = Logger('add_html_report')
        file_path = os.path.join(os.path.dirname(self.xml_path), rs_lyr.rel_path)
        nod_dataset = self.add_dataset(parent_node, file_path, rs_lyr, 'HTMLFile', replace)
        log.info('Report node created: {}'.format(file_path))
        return nod_dataset, file_path

    @staticmethod
    def getUniqueTypeID(nodParent, xml_tag, IDRoot):

        i = 1
        for nodChild in nodParent.findall(xml_tag):
            if nodChild.attrib['id'][: len(IDRoot)] == IDRoot:
                i += 1

        return '{}{}'.format(IDRoot, i if i > 0 else '')

    @staticmethod
    def unique_type_id(parent, xml_tag, root_id):

        i = 1
        for nodChild in parent.findall(xml_tag):
            if nodChild.attrib['id'][: len(root_id)] == root_id:
                i += 1

        return '{}{}'.format(root_id, i if i > 1 else '')

    @staticmethod
    def prefix_keys(dict_in: Dict[str, str], prefix: str) -> Dict[str, str]:
        """Helper method. Prefix a dictionary's keys

        Args:
            dict_in (Dict[str, str]): [description]
            prefix (str): [description]

        Returns:
            Dict[str, str]: [description]
        """
        if dict_in is None:
            return {}
        return {'{}{}'.format(prefix, key): val for key, val in dict_in.items()}

    def rs_meta_augment(self, in_proj_files: List[str], rs_id_map: Dict[str, str]) -> None:
        """Augment the metadata of specific layers with the input's layers

        Args:
            out_proj_file (str): [description]
            in_proj_files (List[str]): [description]
        """
        wh_prefix = '_rs_wh_'
        proj_prefix = '_rs_prj_'
        lyr_prefix = '_rs_lyr_'

        working_id_list = copy(rs_id_map)

        # Loop over input project.rs.xml files
        found_keys = []
        for in_prj_path in in_proj_files:
            in_prj = RSProject(None, in_prj_path)

            # Define our default, generic warehouse and project meta
            whmeta = self.prefix_keys(in_prj.get_metadata_dict(tag='Warehouse'), wh_prefix)
            projmeta = self.prefix_keys(in_prj.get_metadata_dict(), proj_prefix)

            # look for any valid mappings and move metadata into them
            for id_out, id_in in working_id_list.items():

                lyrnod_in = in_prj.XMLBuilder.find('Realizations').find('.//*[@id="{}"]'.format(id_in))
                lyrmeta = self.prefix_keys(in_prj.get_metadata_dict(lyrnod_in), lyr_prefix)
                lyrnod_out = self.XMLBuilder.find('Realizations').find('.//*[@id="{}"]'.format(id_out))

                if id_out not in found_keys and lyrnod_in is not None and lyrnod_out is not None:
                    print('Found mapping for {}=>{}. Moving metadata'.format(id_in, id_out))
                    found_keys.append(id_out)
                    self.add_metadata({
                        **whmeta,
                        **projmeta,
                        **lyrmeta,
                        "{}projType".format(proj_prefix): in_prj.XMLBuilder.find('ProjectType').text,
                        "{}id".format(lyr_prefix): lyrnod_in.attrib['id'],
                        "{}guid".format(lyr_prefix): lyrnod_in.attrib['guid'],
                        "{}path".format(lyr_prefix): lyrnod_in.find('Path').text,
                    }, lyrnod_out)
