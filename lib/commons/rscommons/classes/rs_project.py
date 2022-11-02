
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
import shutil
import datetime
import uuid

import rasterio.shutil
from osgeo import ogr
from copy import copy
import json

from rscommons import Logger
from rscommons.rspaths import parse_posix_path
from rscommons.classes.xml_builder import XMLBuilder
from rscommons.util import safe_makedirs
from rscommons.vector_ops import copy_feature_class

Path = str

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


class RSMetaTypes:
    """
    This is a helper enumeration class to make sure we only use meta types that are valid.
    These should exactly mirror:
        https://xml.riverscapes.net/Projects/XSD/V1/RiverscapesProject.xsd
    """
    GUID = "guid"
    URL = "url"
    FILEPATH = "filepath"
    IMAGE = "image"
    VIDEO = "video"
    ISODATE = "isodate"
    TIMESTAMP = "timestamp"
    FLOAT = "float"
    INT = "int"
    RICHTEXT = "richtext"
    MARKDOWN = "markdown"
    JSON = "json"


class RSMetaExt:
    """
    This is a helper enumeration class to make sure we only use meta ext that are valid.
    """
    DATASET = 'dataset'
    PROJECT = 'project'
    WAREHOUSE = 'warehouse'


class RSMeta:
    def __init__(self, key: str, value: str, meta_type: str = None, meta_ext=None):
        self.key = key
        self.value = value
        # Do a quick check to make sure we're using correct meta types
        if meta_type is not None and not hasattr(RSMetaTypes, meta_type.upper()):
            raise Exception('Could not find <Meta> type {}'.format(meta_type))

        self.type = meta_type

        if meta_ext is not None:
            if not hasattr(RSMetaExt, meta_ext.upper()):
                raise Exception('Could not find <Meta> ext {}'.format(meta_ext))

        self.ext = meta_ext


class RSLayer:
    def __init__(self, name: str, lyr_id: str, tag: str, rel_path: str, sub_layers: dict = None, lyr_meta: List[RSMeta] = None):
        """[summary]

        Args:
            name (str): The <Name> to be used
            lyr_id (str): The <Name id=""> id to be used
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
            raise Exception('Realization Name is required')
        if lyr_id is None:
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

        self.id = lyr_id
        self.tag = tag
        self.name = name
        self.rel_path = rel_path
        self.lyr_meta = lyr_meta

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

        self.project_type = ''
        self.realizations_node = None
        self.project_extent_node = None
        self.project_dir = os.path.dirname(self.xml_path)

    def create(self, name, project_type, meta: List[RSMeta] = None, meta_dict: Dict[str, str] = None, replace=True):
        """[summary]

        Args:
            name ([type]): [description]
            project_type ([type]): [description]
            meta (List[RSMeta], optional): List of RSMEta types. Defaults to None.
            meta_dict (Dict[str, str], optional): Simple key-value pairs from the command line. Defaults to None.
            replace (bool, optional): [description]. Defaults to True.

        Raises:
            Exception: [description]
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

        self.add_metadata([
            RSMeta('ModelVersion', self.settings.version),
            RSMeta('DateCreated', datetime.datetime.now().isoformat(), RSMetaTypes.ISODATE)
        ])
        if meta is not None:
            self.add_metadata(meta)
        # Now add any simple metadata from the command line
        if meta_dict is not None:
            self.add_metadata_simple(meta_dict)

        # Add a realizations parent node
        self.project_type = project_type
        self.realizations_node = self.XMLBuilder.add_sub_element(self.XMLBuilder.root, 'Realizations')

        self.XMLBuilder.write()
        self.exists = True

    def add_realization(self, name: str, realization_id: str, product_version: str, meta: List[RSMeta] = None, data_nodes: List[str] = None, create_folders=False):

        realization = self.XMLBuilder.add_sub_element(self.realizations_node, "Realization", None, {
            'id': realization_id,
            'dateCreated': datetime.datetime.now().isoformat(),
            # 'guid': str(uuid.uuid4()),
            'productVersion': product_version
        })
        self.XMLBuilder.add_sub_element(realization, 'Name', name)

        if meta is not None:
            self.add_metadata(meta, realization)

        try:
            log = Logger("add_realization")
            logs_node = self.XMLBuilder.add_sub_element(realization, name='Logs')
            log_lyr = RSLayer('Log', 'LOGFILE', 'LogFile', '')
            self.add_dataset(logs_node, log.instance.logpath, log_lyr, 'LogFile')
        except Exception as e:
            print(e)
            pass

        if data_nodes is not None:
            out_nodes = {}
            for node in data_nodes:
                out_nodes[node] = self.XMLBuilder.add_sub_element(realization, node)
                if create_folders is True:
                    safe_makedirs(os.path.join(self.project_dir, node.lower()))

        self.XMLBuilder.write()
        if data_nodes is not None:
            return realization, out_nodes
        else:
            return realization

    def add_metadata_simple(self, metadict: Dict[str, str], node=None):
        """For when you need simple Dict[str,str] key=value metadata with no types

        Args:
            metadict (Dict[str, str]): [description]
        """
        self.add_metadata([RSMeta(k, v) for k, v in metadict.items()], node)

    def add_metadata(self, metavals: List[RSMeta], node=None):
        """Adding metadata to nodes. Note that you need to pass in a list of RSMeta objects for this one
        check out add_metadata_simple if all you have is key-value pairs

        Args:
            metavals (List[RSMeta]): [description]
            node ([type], optional): [description]. Defaults to None.
        """
        metadata_element = node.find('MetaData') if node is not None else self.XMLBuilder.find('MetaData')
        for rsmeta in metavals:
            if metadata_element is None:
                if node is not None:
                    metadata_element = self.XMLBuilder.add_sub_element(node, "MetaData")
                else:
                    metadata_element = self.XMLBuilder.add_sub_element(self.XMLBuilder.root, "MetaData")

            found = metadata_element.findall('Meta[@name="{}"]'.format(rsmeta.key))
            # Only one key-value pair are allowed with the same name. This cleans up any stragglers
            if len(found) > 0:
                for f in found:
                    metadata_element.remove(f)

            # Note: we don't do a replace=False here because that only verifies the id attribute and we're
            # using 'name' for uniqueness
            attrs = {"name": rsmeta.key}
            if rsmeta.ext is not None:
                attrs['ext'] = rsmeta.ext
            if rsmeta.type is not None:
                attrs['type'] = rsmeta.type

            self.XMLBuilder.add_sub_element(metadata_element, "Meta", rsmeta.value, attrs)

        self.XMLBuilder.write()

    def get_metadata_dict(self, node=None, tag='MetaData'):
        """Reverse lookup to pull Metadata out of the raw XML report

        Args:
            node ([type], optional): [description]. Defaults to None.
            tag (str, optional): [description]. Defaults to 'MetaData'.

        Returns:
            [type]: [description]
        """
        meta = self.get_metadata(node, tag)
        if meta is None:
            return None
        return {k: v.value for k, v in meta.items()}

    def get_metadata(self, node=None, tag='MetaData'):
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
            child_name = child.attrib['name']
            child_type = child.attrib['type'] if 'type' in child.attrib else None
            valdict[child_name] = RSMeta(child_name, child.text, child_type)

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

    def add_dataset(self, parent_node, abs_path_val: str, rs_lyr: RSLayer, default_tag: str, replace=False, rel_path=False, sublayer=False):

        xml_tag = rs_lyr.tag if rs_lyr.tag is not None else default_tag
        if not sublayer:
            ds_id = rs_lyr.id if replace else RSProject.unique_type_id(parent_node, xml_tag, rs_lyr.id)

        if replace:
            self.XMLBuilder.delete_sub_element(parent_node, xml_tag, id)

        attribs = {'lyrName': abs_path_val} if sublayer else {
            'id': ds_id
        }
        nod_dataset = self.XMLBuilder.add_sub_element(parent_node, xml_tag, attribs=attribs)
        self.XMLBuilder.add_sub_element(nod_dataset, 'Name', rs_lyr.name)

        # Sanitize our paths to always produce linux-style slashes
        if not sublayer:
            if rel_path:
                self.XMLBuilder.add_sub_element(nod_dataset, 'Path', parse_posix_path(abs_path_val))
            else:
                posix_path_val = parse_posix_path(os.path.relpath(abs_path_val, os.path.dirname(self.xml_path)))
                self.XMLBuilder.add_sub_element(nod_dataset, 'Path', posix_path_val)

        if rs_lyr.lyr_meta is not None:
            self.add_metadata(rs_lyr.lyr_meta, nod_dataset)

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
                sub_nod = self.add_dataset(layers_node, rssublyr.rel_path, rssublyr, rssublyr.tag, rel_path=True, sublayer=True)
                sub_layers[rssublyr_name] = (sub_nod, sub_abs_path)

        return nod_dataset, file_path, sub_layers

    def add_report(self, parent_node, rs_lyr, replace=False):
        log = Logger('add_html_report')
        file_path = os.path.join(os.path.dirname(self.xml_path), rs_lyr.rel_path)
        nod_dataset = self.add_dataset(parent_node, file_path, rs_lyr, 'HTMLFile', replace)
        log.info('Report node created: {}'.format(file_path))
        return nod_dataset, file_path

    def add_project_extent(self, geojson_path: Path, centroid: tuple, bbox: tuple):
        log = Logger('add_project_extents')

        ix = list(self.XMLBuilder.root).index(self.XMLBuilder.root.find("Realizations"))

        project_extent_node = self.XMLBuilder.add_sub_element(self.XMLBuilder.root, name='ProjectBounds', element_position=ix)
        centroid_node = self.XMLBuilder.add_sub_element(project_extent_node, name='Centroid')
        self.XMLBuilder.add_sub_element(centroid_node, name='Lat', text=str(centroid[1]))
        self.XMLBuilder.add_sub_element(centroid_node, name='Lng', text=str(centroid[0]))

        # (minX, maxX, minY, maxY)
        bbox_node = self.XMLBuilder.add_sub_element(project_extent_node, name='BoundingBox')
        self.XMLBuilder.add_sub_element(bbox_node, name='MinLat', text=str(bbox[2]))
        self.XMLBuilder.add_sub_element(bbox_node, name='MinLng', text=str(bbox[0]))
        self.XMLBuilder.add_sub_element(bbox_node, name='MaxLat', text=str(bbox[3]))
        self.XMLBuilder.add_sub_element(bbox_node, name='MaxLng', text=str(bbox[1]))

        geojson_rel_path = os.path.relpath(geojson_path, self.project_dir)
        geojson_node = self.XMLBuilder.add_sub_element(project_extent_node, name='Path', text=geojson_rel_path)

        self.XMLBuilder.write()
        log.info(f'ProjectBounds node added: {geojson_path}')

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
    def prefix_meta_keys(dict_in: Dict[str, str], prefix: str) -> Dict[str, str]:
        """Helper method. Prefix a dictionary's keys

        Args:
            dict_in (Dict[str, str]): [description]
            prefix (str): [description]

        Returns:
            Dict[str, str]: [description]
        """
        if dict_in is None:
            return {}
        new_dict = {}
        for key, val in dict_in.items():
            new_key = '{}{}'.format(prefix, key)
            new_dict[new_key] = RSMeta(new_key, val.value, val.type)
        return new_dict

    @staticmethod
    def meta_keys_ext(dict_in: Dict[str, str], ext: str) -> Dict[str, str]:
        """Helper method. Prefix a dictionary's keys

        Args:
            dict_in (Dict[str, str]): [description]
            prefix (str): [description]

        Returns:
            Dict[str, str]: [description]
        """
        if dict_in is None:
            return {}
        new_dict = {}
        for key, val in dict_in.items():
            new_dict[key] = RSMeta(key, val.value, val.type, meta_ext=ext)
        return new_dict

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

            warehouse_id = in_prj.XMLBuilder.find('Warehouse').attrib['id']
            # Find watershed name in metadata, add if it exists
            watershed_node = in_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
            if watershed_node is not None:
                proj_watershed_node = self.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
                if proj_watershed_node is None:
                    self.add_metadata([RSMeta('Watershed', watershed_node.text)])

            # Define our default, generic warehouse and project meta
            whmeta = self.meta_keys_ext(in_prj.get_metadata(tag='Warehouse'), RSMetaExt.WAREHOUSE)
            projmeta = self.meta_keys_ext(in_prj.get_metadata(), RSMetaExt.PROJECT)

            # look for any valid mappings and move metadata into them
            for id_out, id_in in working_id_list.items():

                lyrnod_in = in_prj.XMLBuilder.find('Realizations').find('Realization').find('.//*[@id="{}"]'.format(id_in))
                lyrmeta = self.meta_keys_ext(in_prj.get_metadata(lyrnod_in), RSMetaExt.DATASET)

                lyrnod_out = self.XMLBuilder.find('Realizations').find('Realization').find('.//*[@id="{}"]'.format(id_out))

                if id_out not in found_keys and lyrnod_in is not None and lyrnod_out is not None:
                    realization_id = in_prj.XMLBuilder.find('Realizations').find('Realization').attrib['id']
                    print('Found mapping for {}=>{}. Moving metadata'.format(id_in, id_out))
                    found_keys.append(id_out)
                    lyrnod_out.attrib['extRef'] = os.path.join(warehouse_id, realization_id, lyrnod_in.attrib['id'])
                    self.add_metadata([
                        *whmeta.values(),
                        *projmeta.values(),
                        *lyrmeta.values(),
                        RSMeta("projType", in_prj.XMLBuilder.find('ProjectType').text, meta_ext=RSMetaExt.PROJECT),
                        RSMeta("id", lyrnod_in.attrib['id'], meta_ext=RSMetaExt.DATASET),
                        RSMeta("path", lyrnod_in.find('Path').text, RSMetaTypes.FILEPATH, meta_ext=RSMetaExt.DATASET),
                    ], lyrnod_out)

    def rs_copy_project_extents(self, in_prj_path):

        in_prj = RSProject(None, in_prj_path)
        project_extent_node = in_prj.XMLBuilder.root.findall("ProjectBounds")[0]

        if self.XMLBuilder.root.find('ProjectBounds') is None:
            ix = list(self.XMLBuilder.root).index(self.XMLBuilder.root.find("Realizations"))
            self.XMLBuilder.root.insert(ix, project_extent_node)

        geojson_filename = project_extent_node.find('Path').text
        in_geojson_path = os.path.join(os.path.dirname(in_prj_path), geojson_filename)
        out_geojson_path = os.path.join(os.path.dirname(self.xml_path), geojson_filename)

        if not os.path.exists(out_geojson_path):
            shutil.copy(in_geojson_path, out_geojson_path)

    def get_project_bounds(self):

        results = {}

        project_bounds = self.XMLBuilder.root.find('ProjectBounds')

        centroid = project_bounds.find("Centroid")
        x = float(centroid.find('Lng').text)
        y = float(centroid.find('Lat').text)
        centroid_geom = ogr.Geometry(ogr.wkbPoint)
        centroid_geom.AddPoint(x, y)
        centroid_geom.FlattenTo2D()
        results['Centroid'] = centroid_geom.ExportToJson()

        bbox = project_bounds.find("BoundingBox")
        xMin = float(bbox.find('MinLng').text)
        yMin = float(bbox.find('MinLat').text)
        xMax = float(bbox.find('MaxLng').text)
        yMax = float(bbox.find('MaxLat').text)

        bbox_ring = ogr.Geometry(ogr.wkbLinearRing)
        bbox_ring.AddPoint(xMin, yMin)
        bbox_ring.AddPoint(xMax, yMin)
        bbox_ring.AddPoint(xMax, yMax)
        bbox_ring.AddPoint(xMin, yMax)
        bbox_ring.AddPoint(xMin, yMin)
        bbox_geom = ogr.Geometry(ogr.wkbPolygon)
        bbox_geom.AddGeometry(bbox_ring)
        bbox_geom.FlattenTo2D()
        results['BoundingBox'] = bbox_geom.ExportToJson()

        json_file = project_bounds.find("Path").text
        with open(os.path.join(self.project_dir, json_file)) as f:
            gj = json.load(f)

        results['Polygon'] = None if gj['features'][0]['geometry']['type'] != 'Polygon' else str(gj['features'][0]['geometry'])

        return results
