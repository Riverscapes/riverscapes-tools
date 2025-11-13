"""Utilities for copying descriptive metadata into Riverscapes projects."""

from __future__ import annotations

from rscommons import RSMeta, RSMetaTypes
from rscommons.classes.raster import get_raster_cell_size
from rscommons.layer_definitions import LayerDefinition, load_layer_definitions


def augment_layermeta(proj_type: str, lyr_descriptions: str, lyr_types: dict):
    """
    For RSContext we've written a JSON file with extra layer meta. We may use this pattern elsewhere but it's just here for now
    proj_type (str): the project type reference used in the documentation websites.
    lyr_descriptions (str): path to a layer_descriptions.json {LayerType ref: [description, sourceurl, productversion]}.
    lyr_types: the LayerTypes dict specified at the beginning of model scripts
    """

    definitions = load_layer_definitions(lyr_descriptions)

    def _lookup(def_key: str, lyr) -> LayerDefinition | None:
        if def_key in definitions:
            return definitions[def_key]
        if lyr and lyr.id in definitions:
            return definitions[lyr.id]
        return None

    for k, lyr in lyr_types.items():
        if lyr.sub_layers is not None:
            for h, sublyr in lyr.sub_layers.items():
                definition = _lookup(h, sublyr)
                if definition is None:
                    continue
                meta_items = []
                if definition.source_url:
                    meta_items.append(RSMeta('SourceUrl', definition.source_url, RSMetaTypes.URL))
                if definition.data_product_version:
                    meta_items.append(RSMeta('DataProductVersion', definition.data_product_version))
                meta_items.append(RSMeta('DocsUrl', f'https://tools.riverscapes.net/{proj_type}/data/#{sublyr.id}', RSMetaTypes.URL))
                if meta_items:
                    sublyr.lyr_meta = meta_items

        definition = _lookup(k, lyr)
        if definition is not None:
            meta_items = []
            if definition.source_url:
                meta_items.append(RSMeta('SourceUrl', definition.source_url, RSMetaTypes.URL))
            if definition.data_product_version:
                meta_items.append(RSMeta('DataProductVersion', definition.data_product_version))
            meta_items.append(RSMeta('DocsUrl', f'https://tools.riverscapes.net/{proj_type}/data/#{lyr.id}', RSMetaTypes.URL))
            if meta_items:
                lyr.lyr_meta = meta_items


def add_layer_descriptions(rsproject, lyr_descriptions: str, lyr_types: dict):

    definitions = load_layer_definitions(lyr_descriptions)

    def _lookup(def_key: str, lyr) -> LayerDefinition | None:
        if def_key in definitions:
            return definitions[def_key]
        if lyr and lyr.id in definitions:
            return definitions[lyr.id]
        return None

    for layer_key, lyr in lyr_types.items():
        if lyr.sub_layers is not None:
            for subid, sublyr in lyr.sub_layers.items():
                definition = _lookup(subid, sublyr)
                if definition is None or not definition.description:
                    continue
                for node in rsproject.XMLBuilder.tree.iter():
                    if 'lyrName' in node.attrib and node.attrib['lyrName'] == sublyr.rel_path:
                        rsproject.XMLBuilder.add_sub_element(node, 'Description', definition.description)

        definition = _lookup(layer_key, lyr)
        if definition is not None and definition.description:
            for node in rsproject.XMLBuilder.tree.iter():
                if 'id' in node.attrib and node.find('Path') is not None and node.find('Path').text == lyr.rel_path:
                    rsproject.XMLBuilder.add_sub_element(node, 'Description', definition.description)

    rsproject.XMLBuilder.write()


def raster_resolution_meta(rsproject, raster_path: str, raster_node):

    resx, resy = get_raster_cell_size(raster_path)

    rsproject.add_metadata([RSMeta('CellSizeX', str(resx)),
                            RSMeta('CellSizeY', str(resy))], raster_node)
