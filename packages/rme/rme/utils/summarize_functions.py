import sqlite3
import os
import numpy as np
from rasterio.mask import mask
from rscommons import GeopackageLayer, Logger


def get_max_value(dgo_ftr, in_line_network, field_name):

    results = []
    with GeopackageLayer(in_line_network) as lyr_lines:
        for feat, *_ in lyr_lines.iterate_features(clip_shape=dgo_ftr):
            results.append(feat.GetField(field_name))
        lyr_lines.ogr_layer.SetSpatialFilter(None)
    if len(results) > 0:
        out_val = str(max(results))
    else:
        out_val = None

    return out_val


def value_from_max_length(dgo_ftr, in_line_network, field_name):
    attributes = {}
    with GeopackageLayer(in_line_network) as lyr_lines:
        for feat, *_ in lyr_lines.iterate_features(clip_shape=dgo_ftr):
            line_geom = feat.GetGeometryRef()
            attribute = str(feat.GetField(field_name))
            geom_section = dgo_ftr.Intersection(line_geom)
            length = geom_section.Length()
            attributes[attribute] = attributes.get(
                attribute, 0) + length
        lyr_lines.ogr_layer.SetSpatialFilter(None)
        lyr_lines = None
    if len(attributes) == 0:
        majority_attribute = None
    else:
        majority_attribute = str(
            max(attributes, key=attributes.get))

    return majority_attribute


def value_from_dgo(dgo_ftr, in_dataset, field_name):
    dgoid = dgo_ftr.GetField("DGOID")
    with sqlite3.connect(os.path.dirname(in_dataset)) as conn:
        curs = conn.cursor()
        curs.execute(
            f"SELECT {field_name} FROM {os.path.basename(in_dataset)} WHERE fid = {dgoid}")
        value = curs.fetchone()[0]

    return value


def value_density_from_dgo(dgo_ftr, in_dataset, field_name):
    dgoid = dgo_ftr.GetField("DGOID")
    with sqlite3.connect(os.path.dirname(in_dataset)) as conn:
        curs = conn.cursor()
        curs.execute(
            f"SELECT {field_name}, centerline_length FROM {os.path.basename(in_dataset)} WHERE fid = {dgoid}")
        val = curs.fetchone()
        if val[0] is not None and val[1] is not None:
            density = val[0] / \
                val[1] if val[1] > 0.0 else None
        else:
            density = None

    return density


def value_from_dataset_area(dgo_ftr, in_dataset, field_name):
    attributes = {}
    with GeopackageLayer(in_dataset) as lyr:
        for feat, *_ in lyr.iterate_features(clip_shape=dgo_ftr):
            geom = feat.GetGeometryRef()
            attribute = feat.GetField(field_name)
            geom_section = dgo_ftr.Intersection(geom)
            area = geom_section.GetArea()
            attributes[attribute] = attributes.get(
                attribute, 0) + area
        lyr = None
    if len(attributes) == 0:
        majority_attribute = None
    else:
        majority_attribute = str(max(attributes, key=attributes.get))

    return majority_attribute


def raster_pixel_value_count(dgo_ftr, in_dataset):
    values = {}
    raw_raster = mask(in_dataset, [dgo_ftr], crop=True)[0]
    mask_raster = np.ma.masked_values(raw_raster, in_dataset.nodata)
    for value in np.unique(mask_raster):
        if value is not np.ma.masked:
            values[value] = np.count_nonzero(mask_raster == value)

    return values


def proportion_of_veg_type(veg_id_dict: dict, veg_class: str, database: str):
    class_count = {}
    for veg_id, count in veg_id_dict.items():
        with sqlite3.connect(database) as conn:
            curs = conn.cursor()
            curs.execute(
                f"SELECT Physiognomy FROM VegetationTypes WHERE veg_id = {veg_id}")
            v_class = curs.fetchone()[0]
            class_count[v_class] = class_count.get(v_class, 0) + count

    proportion = class_count[veg_class] / sum(class_count.values())

    return proportion


def value_by_count(dgo_ftr, in_dataset, field_name, field_value):
    with GeopackageLayer(in_dataset) as lyr_pts:
        count = 0
        for feat, *_ in lyr_pts.iterate_features(clip_shape=dgo_ftr, attribute_filter=f"""{field_name} = {field_value}"""):
            count += 1

    return count


def call_function(func_name, *args):
    return func_name(*args)
