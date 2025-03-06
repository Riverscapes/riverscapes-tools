import sqlite3
from rscommons import GeopackageLayer, Logger


def get_max_value(field_name, in_line_network, dgo_ftr):

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


def value_from_max_length(field_name, in_line_network, dgo_ftr):
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


def value_from_dgo(geopackage, dgoid, layer_name, field_name):
    with sqlite3.connect(geopackage) as conn:
        curs = conn.cursor()
        curs.execute(
            f"SELECT {field_name} FROM {layer_name} WHERE fid = {dgoid}")
        value = curs.fetchone()[0]

    return value


def value_density_from_dgo(geopackage, dgoid, layer_name, field_name):
    with sqlite3.connect(geopackage) as conn:
        curs = conn.cursor()
        curs.execute(
            f"SELECT {field_name}, centerline_length FROM {layer_name} WHERE fid = {dgoid}")
        val = curs.fetchone()
        if val[0] is not None and val[1] is not None:
            density = val[0] / \
                val[1] if val[1] > 0.0 else None
        else:
            density = None

    return density


def value_from_dataset_area(field_name, dataset, dgo_ftr):
    attributes = {}
    with GeopackageLayer(dataset) as lyr:
        for feat, *_ in lyr.iterate_features(clip_shape=dgo_ftr):
            geom_county = feat.GetGeometryRef()
            attribute = feat.GetField(field_name)
            geom_section = dgo_ftr.Intersection(geom_county)
            area = geom_section.GetArea()
            attributes[attribute] = attributes.get(
                attribute, 0) + area
        lyr = None
    if len(attributes) == 0:
        majority_attribute = None
    else:
        majority_attribute = str(max(attributes, key=attributes.get))

    return majority_attribute
