import sqlite3
import os
import numpy as np
from rasterio.mask import mask
from rscommons import GeopackageLayer


def get_max_value(dgo_ftr, in_line_network, field_name):
    """get the maximum value of an attribute on a line network

    Args:
        dgo_ftr (ogr geometry): DGO ogr geometry
        in_line_network (str): path to line network feature class
        field_name (str): the field name from the line network

    Returns:
        _type_: the maximum value of the field
    """

    results = []
    with GeopackageLayer(in_line_network) as lyr_lines:
        for feat, *_ in lyr_lines.iterate_features(clip_shape=dgo_ftr.GetGeometryRef()):
            results.append(feat.GetField(field_name))
        lyr_lines.ogr_layer.SetSpatialFilter(None)
    if len(results) > 0:
        out_val = str(max(results))
    else:
        out_val = None

    return out_val


def value_from_max_length(dgo_ftr, in_line_network, field_name):
    """get the value of an attribute on a line network based on the maximum length of the intersection
    between the line and the dgo feature

    Args:
        dgo_ftr (ogr geometry): DGO ogr geometry
        in_line_network (str): path to line network feature class
        field_name (str): the field name from the line network

    Returns:
        _type_: the value from the line segment with the longest length
    """
    attributes = {}
    dgo_geom = dgo_ftr.GetGeometryRef()
    with GeopackageLayer(in_line_network) as lyr_lines:
        for feat, *_ in lyr_lines.iterate_features(clip_shape=dgo_geom):
            line_geom = feat.GetGeometryRef()
            attribute = str(feat.GetField(field_name))
            geom_section = dgo_geom.Intersection(line_geom)
            length = geom_section.Length()
            attributes[attribute] = attributes.get(
                attribute, 0) + length
        lyr_lines.ogr_layer.SetSpatialFilter(None)
        lyr_lines = None
    if len(attributes) == 0:
        majority_attribute = None
    else:
        majority_attribute = max(attributes, key=attributes.get)

    return majority_attribute


def value_from_dgo(dgo_ftr, in_dataset, field_name):
    """copy a value straight from another DGO

    Args:
        dgo_ftr (ogr geometry): DGO ogr geometry
        in_dataset (str): path to the DGO table
        field_name (str): the field name from the DGO table

    Returns:
        _type_: the value from the DGO table
    """
    dgoid = dgo_ftr.GetFID()
    with sqlite3.connect(os.path.dirname(in_dataset)) as conn:
        curs = conn.cursor()
        curs.execute(
            f"SELECT {field_name} FROM {os.path.basename(in_dataset)} WHERE fid = {dgoid}")
        value = curs.fetchone()[0]

    return value


def value_density_from_dgo(dgo_ftr, in_dataset, field_name):
    """get the value of an attribute from a DGO divided by the DGO centerline

    Args:
        dgo_ftr (ogr geometry): DGO ogr geometry
        in_dataset (str): path to the DGO table
        field_name (str): the field name from the DGO table

    Returns:
        _type_: the value from the DGO table divided by the centerline length
    """
    dgoid = dgo_ftr.GetFID()
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
    """get the value of an attribute from the feature with the largest area

    Args:
        dgo_ftr (ogr geometry): DGO ogr geometry
        in_dataset (str): path to the DGO table
        field_name (str): the field name from the DGO table

    Returns:
        _type_: the value from the DGO table
    """
    attributes = {}
    dgo_geom = dgo_ftr.GetGeometryRef()
    with GeopackageLayer(in_dataset) as lyr:
        for feat, *_ in lyr.iterate_features(clip_shape=dgo_geom):
            geom = feat.GetGeometryRef()
            attribute = feat.GetField(field_name)
            geom_section = dgo_geom.Intersection(geom)
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
    """get a dictionary of raster pixel values and their count

    Args:
        dgo_ftr (ogr geometry): DGO ogr geometry
        in_dataset (rasterio raster): rasterio raster dataset

    Returns:
        dict: a dictionary of pixel values and their count
    """
    values = {}
    raw_raster = mask(in_dataset, [dgo_ftr], crop=True)[0]
    mask_raster = np.ma.masked_values(raw_raster, in_dataset.nodata)
    for value in np.unique(mask_raster):
        if value is not np.ma.masked:
            values[value] = np.count_nonzero(mask_raster == value)

    return values


def value_by_count(dgo_ftr, in_dataset, field_name, field_value):

    with GeopackageLayer(in_dataset) as lyr_pts:
        count = 0
        for feat, *_ in lyr_pts.iterate_features(clip_shape=dgo_ftr.GetGeometryRef(), attribute_filter=f"""{field_name} = '{field_value}'"""):
            count += 1

    return count


def ex_veg_proportion(dgo_ftr, in_dataset, field_name, field_value):

    dgoid = dgo_ftr.GetFID()
    veg_areas = {}
    with sqlite3.connect(os.path.dirname(in_dataset)) as conn:
        curs = conn.cursor()
        curs.execute(f"""SELECT {field_name}, SUM(Area) FROM {os.path.basename(in_dataset)} 
                     LEFT JOIN vegetation_types ON {os.path.basename(in_dataset)}.VegetationID = vegetation_types.VegetationID 
                     WHERE dgoid = {dgoid} AND EpochID = 1 GROUP BY {field_name}""")
        for row in curs.fetchall():
            veg_areas[row[0]] = row[1]
        if field_value in veg_areas:
            proportion = veg_areas[field_value] / sum(veg_areas.values())
        else:
            proportion = 0.0
    return proportion


def hist_veg_proportion(dgo_ftr, in_dataset, field_name, field_value):

    dgoid = dgo_ftr.GetFID()
    veg_areas = {}
    with sqlite3.connect(os.path.dirname(in_dataset)) as conn:
        curs = conn.cursor()
        curs.execute(f"""SELECT {field_name}, SUM(Area) FROM {os.path.basename(in_dataset)} 
                     LEFT JOIN vegetation_types ON {os.path.basename(in_dataset)}.VegetationID = vegetation_types.VegetationID 
                     WHERE dgoid = {dgoid} AND EpochID = 2 GROUP BY {field_name}""")
        for row in curs.fetchall():
            veg_areas[row[0]] = row[1]
        if field_value in veg_areas:
            proportion = veg_areas[field_value] / sum(veg_areas.values())
        else:
            proportion = 0.0
    return proportion


def mw_copy_from_dgo(cursor, dgo_id, table_name, field_name):

    # with sqlite3.connect(os.path.dirname(table_name)) as conn:
    #     curs = conn.cursor()
    cursor.execute(f"""SELECT {field_name} FROM {os.path.basename(table_name)} 
                    WHERE dgoid = {dgo_id}""")
    result = cursor.fetchone()
    if result is None:
        out = None
    else:
        out = result[0]

    return out


def mw_sum(cursor, dgo_ids, table_name, field_name):

    # with sqlite3.connect(os.path.dirname(table_name)) as conn:
    #     curs = conn.cursor()
    cursor.execute(f"""SELECT SUM({field_name}) FROM {os.path.basename(table_name)} 
                    WHERE dgoid IN ({", ".join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if result[0] is None:
        out = None
    else:
        out = result[0]

    return out


def mw_sum_div_length(cursor, dgo_ids, table_name, field_name):

    # with sqlite3.connect(os.path.dirname(table_name)) as conn:
    #     curs = conn.cursor()
    if os.path.basename(table_name) == "dgo_measurements":
        cursor.execute(f"""SELECT SUM({field_name}), SUM(valleng) FROM {os.path.basename(table_name)} 
                        WHERE dgoid IN ({", ".join(map(str, dgo_ids))})""")
    else:
        cursor.execute(f"""SELECT SUM({field_name}), SUM(valleng) FROM {os.path.basename(table_name)} LEFT JOIN dgo_measurements
                    ON {os.path.basename(table_name)}.dgoid = dgo_measurements.dgoid 
                    WHERE {os.path.basename(table_name)}.dgoid IN ({", ".join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        out = None
    elif 'beaver' in table_name or 'geomorph' in table_name:
        out = result[0] / (result[1] / 1000) if result[1] > 0.0 else None
    else:
        out = result[0] / result[1] if result[1] > 0.0 else None

    return out


def mw_sum_div_chan_length(cursor, dgo_ids, table_name, field_name):

    # with sqlite3.connect(os.path.dirname(table_name)) as conn:
    #     curs = conn.cursor()
    cursor.execute(f"""SELECT SUM({field_name}), SUM(strmleng) FROM {os.path.basename(table_name)} LEFT JOIN dgo_measurements
                    ON {os.path.basename(table_name)}.dgoid = dgo_measurements.dgoid 
                    WHERE {os.path.basename(table_name)}.dgoid IN ({", ".join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        out = None
    else:
        out = result[0] / result[1] if result[1] > 0.0 else None

    if field_name in ('confining_margins', 'constricting_margins') and out is not None:
        out = min(out, 1.0)

    return out


def mw_proportion(cursor, dgo_ids, table_name, field_name):

    # with sqlite3.connect(os.path.dirname(table_name)) as conn:
    #     curs = conn.cursor()
    cursor.execute(f"""SELECT SUM({field_name}*segment_area), SUM(segment_area) FROM {os.path.basename(table_name)}
                    LEFT JOIN dgos ON {os.path.basename(table_name)}.dgoid = dgos.dgoid 
                    WHERE {os.path.basename(table_name)}.dgoid IN ({", ".join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        prop = None
    else:
        prop = result[0] / result[1] if result[1] > 0.0 else None

    return prop


def mw_area_weighted_av(cursor, dgo_ids, table_name, field_name):

    # with sqlite3.connect(os.path.dirname(table_name)) as conn:
    #     curs = conn.cursor()
    cursor.execute(f"""SELECT SUM({field_name} * segment_area), SUM(segment_area) FROM {os.path.basename(table_name)}
                    LEFT JOIN dgos ON {os.path.basename(table_name)}.dgoid = dgos.dgoid 
                    WHERE {os.path.basename(table_name)}.dgoid IN ({", ".join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        out = None
    else:
        out = result[0] / result[1] if result[1] > 0.0 else None

    return out


def call_function(func_name, *args):
    return func_name(*args)
