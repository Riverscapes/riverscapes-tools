"""A function to add layer metadata to new layers generated by RS Models.
"""

import json

from rscommons import RSMeta, RSMetaTypes
from rscommons.classes.raster import get_raster_cell_size


def augment_layermeta(proj_type: str, lyr_descriptions: str, lyr_types: dict):
    """
    For RSContext we've written a JSON file with extra layer meta. We may use this pattern elsewhere but it's just here for now
    proj_type (str): the project type reference used in the documentation websites.
    lyr_descriptions (str): path to a layer_descriptions.json {LayerType ref: [description, sourceurl, productversion]}.
    lyr_types: the LayerTypes dict specified at the beginning of model scripts
    """
    with open(lyr_descriptions, 'r') as f:
        json_data = json.load(f)

    for k, lyr in lyr_types.items():
        if lyr.sub_layers is not None:
            for h, sublyr in lyr.sub_layers.items():
                if h in json_data and len(json_data[h]) > 0:
                    sublyr.lyr_meta = [
                        # RSMeta('Description', json_data[h][0]),
                        RSMeta('SourceUrl', json_data[h][1], RSMetaTypes.URL),
                        RSMeta('DataProductVersion', json_data[h][2]),
                        RSMeta('DocsUrl', f'https://tools.riverscapes.net/{proj_type}/data.html#{sublyr.id}', RSMetaTypes.URL)
                    ]

        if k in json_data and len(json_data[k]) > 0:
            lyr.lyr_meta = [
                # RSMeta('Description', json_data[k][0]),
                RSMeta('SourceUrl', json_data[k][1], RSMetaTypes.URL),
                RSMeta('DataProductVersion', json_data[k][2]),
                RSMeta('DocsUrl', f'https://tools.riverscapes.net/{proj_type}/data.html#{lyr.id}', RSMetaTypes.URL)
            ]


def add_layer_descriptions(rsproject, lyr_descriptions: str, lyr_types: dict):

    with open(lyr_descriptions, 'r') as f:
        json_data = json.load(f)

    for id, lyr in lyr_types.items():
        if lyr.sub_layers is not None:
            for subid, sublyr in lyr.sub_layers.items():
                if subid in json_data and len(json_data[subid][0]) > 1:
                    for i in rsproject.XMLBuilder.tree.iter():
                        if 'lyrName' in i.attrib.keys():
                            if i.attrib['lyrName'] == sublyr.rel_path:
                                rsproject.XMLBuilder.add_sub_element(i, 'Description', json_data[subid][0])

        if id in json_data and len(json_data[id][0]) > 1:
            for j in rsproject.XMLBuilder.tree.iter():
                if 'id' in j.attrib.keys() and j.find('Path') is not None:
                    if j.find('Path').text == lyr.rel_path:
                        rsproject.XMLBuilder.add_sub_element(j, 'Description', json_data[id][0])

    rsproject.XMLBuilder.write()


def raster_resolution_meta(rsproject, raster_path: str, raster_node):

    resx, resy = get_raster_cell_size(raster_path)

    rsproject.add_metadata([RSMeta('CellSizeX', str(resx)),
                            RSMeta('CellSizeY', str(resy))], raster_node)