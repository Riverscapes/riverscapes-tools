import numpy as np
import rasterio
from rasterio.mask import mask
import sqlite3
from rscommons import GeopackageLayer, VectorBase


def watershed(huc):
    """get the 10-digit (or less) watershed ID

    Args:
        huc (int): the hydrologic unit code (watershed ID)

    Returns:
        str: watershed ID
    """
    if len(str(huc)) > 10:
        return str(huc[:10])
    else:
        return str(huc)


def headwater(feat_seg_dgo, line_network):
    """determine if a stream reach is a headwater

    Args:
        feat_geom (ogr.Geometry): DGO ogr geometry
        line_network (str): path to the line network feature class 

    Returns:
        _type_: binary headwater classification 
    """
    lp = feat_seg_dgo.GetField('level_path')
    dgo_geom = feat_seg_dgo.GetGeometryRef()
    sum_attributes = {}
    with GeopackageLayer(line_network) as lyr_lines:
        for feat, *_ in lyr_lines.iterate_features(clip_shape=dgo_geom, attribute_filter=f"level_path = {lp}"):
            line_geom = feat.GetGeometryRef()
            attribute = str(feat.GetField('STARTFLAG'))
            if attribute not in ['1', '0']:
                continue
            geom_section = dgo_geom.Intersection(line_geom)
            length = geom_section.Length()
            sum_attributes[attribute] = sum_attributes.get(
                attribute, 0) + length
        lyr_lines.ogr_layer.SetSpatialFilter(None)
        lyr_lines = None
    if sum(sum_attributes.values()) == 0:
        is_headwater = None
    else:
        is_headwater = 1 if sum_attributes.get('1', 0) / sum(sum_attributes.values()) > 0.5 else 0
    return is_headwater


def total_stream_length(feat_geom, line_network, transform):
    """get the total stream length in a DGO

    Args:
        feat_geom (ogr.Geometry): DGO ogr geometry
        line_network (str): path to the line network feature class
        transform (ogr.Transform): a transform to project the line_network for calculating length

    Returns:
        _type_: length (m)
    """
    with GeopackageLayer(line_network) as lyr_lines:
        leng = 0
        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
            geom_flowline_full = feat.GetGeometryRef()
            feat_section = geom_flowline_full.Intersection(feat_geom)
            section_proj = VectorBase.ogr2shapely(feat_section, transform=transform)
            leng += section_proj.length

    return leng


def waterbody_extent(feat_geom, waterbodies, transform):
    """calculate the waterbody extent within a DGO

    Args:
        feat_geom (ogr.Geometry): DGO ogr geometry 
        waterbodies (str): path to the waterbody feature class
        transform (ogr.Transform): a transform to project the waterbodies for calculating area

    Returns:
        _type_: area (m²)
    """
    with GeopackageLayer(waterbodies) as lyr:
        area = 0
        for feat, *_ in lyr.iterate_features(clip_shape=feat_geom):
            geom_wb_full = feat.GetGeometryRef()
            if not geom_wb_full.IsValid():
                geom_wb_full = geom_wb_full.MakeValid()
            feat_section = geom_wb_full.Intersection(feat_geom)
            section_proj = VectorBase.ogr2shapely(feat_section, transform=transform)
            area += section_proj.area

    return area


def calculate_gradient(cursor, dgoid, channel=True):
    """calculate the gradient of a stream reach or centerline

    Args:
        gpkg (str): path to geopackage
        dgoid (int): the DGO ID
        channel (bool, optional): If calcualted channel gradient, True; if centerline, False. Defaults to True.

    Returns:
        _type_: gradient (unitless)
    """
    if channel:
        cursor.execute(
            f"SELECT strmmaxelev, strmminelev, strmleng FROM dgo_measurements WHERE dgoid = {dgoid}")
    else:
        cursor.execute(
            f"SELECT clmaxelev, clminelev, valleng FROM dgo_measurements WHERE dgoid = {dgoid}")
    vals = cursor.fetchone()
    if vals is None:
        return None
    if vals[2] > 0.0:
        gradient = (vals[0] - vals[1]) / vals[2]
    else:
        gradient = None

    return gradient


def rel_flow_length(feat_geom, line_network, transform):
    """Calculate relative flow length

    Args:
        feat_geom (ogr.Geometry): DGO ogr geometry
        line_network (str): path to the line network feature class
        transform (ogr.Transform): a transform to project the line_network for calculating length

    Returns:
        _type_: stream length divided by valley length
    """
    dgo_ftr = feat_geom.GetGeometryRef()
    cl_length = feat_geom.GetField('centerline_length')
    if cl_length is None or cl_length == 0:
        return None
    with GeopackageLayer(line_network) as lyr_lines:
        length = 0
        for feat, *_ in lyr_lines.iterate_features(clip_shape=dgo_ftr):
            geom_flowline_full = feat.GetGeometryRef()
            feat_section = geom_flowline_full.Intersection(dgo_ftr)
            section_proj = VectorBase.ogr2shapely(feat_section, transform=transform)
            length += section_proj.length

    return length


def landfire_classes(feat_geom, cursor, epoch=1):
    """get the landfire vegetation classes for a DGO if they make up more than 30% of the area"""

    classes = {}
    classes_out = []
    dgo_id = feat_geom.GetFID()

    cursor.execute(
        f"""SELECT DGOVegetation.VegetationID, CellCount FROM DGOVegetation LEFT JOIN vegetation_types 
        ON DGOVegetation.VegetationID = vegetation_types.VegetationID WHERE dgoid = {dgo_id} AND EpochID = {epoch}""")
    for row in cursor.fetchall():
        classes[row[0]] = row[1]

    for key, value in classes.items():
        if value / sum(classes.values()) > 0.3:
            classes_out.append(key)

    return classes_out


def get_elevation(feat_geom, dem):
    """get the elevation of a DGO"""

    poly = VectorBase.ogr2shapely(feat_geom)

    with rasterio.open(dem) as src:
        raw_raster, _out_transform = mask(src, [poly], crop=True)
        mask_raster = np.ma.masked_values(raw_raster, src.nodata)
        if not mask_raster.mask.all():
            elevation = float(mask_raster.min())
        else:
            elevation = None

    return elevation


def mw_calculate_gradient(cursor, dgo_ids, channel=True):
    """calculate gradient over a moving window

    Args:
        cursor (sqlite3.Cursor): SQLite cursor to execute queries
        dgo_ids (list(int)): a list of DGO IDs that make up the moving window
        channel (bool, optional): For channel gradient, True; For valley gradient, False. Defaults to True.

    Returns:
        _type_: gradient (unitless)
    """

    if channel:
        cursor.execute(
            f"SELECT MAX(strmmaxelev), MIN(strmminelev), SUM(strmleng) FROM dgo_measurements WHERE dgoid IN ({','.join(map(str, dgo_ids))})")
    else:
        cursor.execute(
            f"SELECT MAX(clmaxelev), MIN(clminelev), SUM(valleng) FROM dgo_measurements WHERE dgoid IN ({','.join(map(str, dgo_ids))})")
    vals = cursor.fetchone()
    if None in vals:
        gradient = None
    elif vals[2] > 0.0:
        gradient = (vals[0] - vals[1]) / vals[2]
    else:
        gradient = None

    return gradient


def mw_calculate_sinuosity(cursor, dgo_ids):
    """calculate sinuosity over a moving window

    Args:
        cursor (sqlite3.Cursor): SQLite cursor to execute queries
        dgo_ids (list(int)): a list of DGO IDs that make up the moving window

    Returns:
        _type_: sinuosity (unitless)
    """
    cursor.execute(
        f"SELECT SUM(strmleng), SUM(strmstrleng) FROM dgo_measurements WHERE dgoid IN ({','.join(map(str, dgo_ids))})")
    vals = cursor.fetchone()
    if None in vals:
        sinuosity = None
    elif vals[1] > 0.0:
        sinuosity = vals[0] / vals[1]
    else:
        sinuosity = None

    return sinuosity


def mw_acres_per_mi(cursor, dgo_ids):
    """calculate acres of valley bottom per mile over a moving window

    Args:
        cursor (sqlite3.Cursor): SQLite cursor to execute queries
        dgo_ids (list(int)): a list of DGO IDs that make up the moving window

    Returns:
        _type_: acres/mi
    """

    cursor.execute(
        f"""SELECT SUM(segment_area), SUM(centerline_length) FROM dgos 
        WHERE dgoid IN ({','.join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        out = None
    elif result[1] > 0.0:
        out = (result[0] * 0.000247105) / (result[1] * 0.000621371)
    else:
        out = None

    return out


def mw_hect_per_km(cursor, dgo_ids):
    """calculate hectares of valley bottom per km over a moving window

    Args:
        cursor (sqlite3.Cursor): SQLite cursor to execute queries
        dgo_ids (list(int)): a list of DGO IDs that make up the moving window

    Returns:
        _type_: hectares/km
    """

    cursor.execute(
        f"""SELECT SUM(segment_area), SUM(centerline_length) FROM dgos 
        WHERE dgoid IN ({','.join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        out = None
    elif result[1] > 0.0:
        out = (result[0] * 0.0001) / (result[1] * 0.001)
    else:
        out = None

    return out


def mw_stream_power(cursor, dgo_ids, q='QLow'):
    """calculate stream power over a moving window

    Args:
        cursor (sqlite3.Cursor): SQLite cursor to execute queries
        dgo_ids (list(int)): a list of DGO IDs that make up the moving window
        q (str, optional): the discharge to use for stream power calculation.

    Returns:
        _type_: stream power (W)
    """

    cursor.execute(
        f"""SELECT MAX(strmmaxelev), MIN(strmminelev), SUM(strmleng), MAX({q}) FROM dgo_measurements
        LEFT JOIN dgo_hydro ON dgo_measurements.dgoid = dgo_hydro.dgoid 
        WHERE dgo_measurements.dgoid IN ({','.join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        out = None
    elif result[2] > 0.0 and result[3] is not None:
        slope = (result[0] - result[1]) / result[2]
        discharge = result[3]
        out = slope * discharge * 0.0283168 * 9810
    else:
        out = None

    return out


def mw_rvd(cursor, dgo_ids):
    """calculate riparian vegetation departure within a moving window

    Args:
        cursor (sqlite3.Cursor): SQLite cursor to execute queries
        dgo_ids (list(int)): a list of DGO IDs that make up the moving window

    Returns:
        _type_: proportion departure (unitless)
    """

    cursor.execute(f"""SELECT SUM(prop_riparian*segment_area), sum(hist_prop_riparian*segment_area) FROM dgo_veg LEFT JOIN dgos
                    ON dgo_veg.dgoid = dgos.dgoid WHERE dgo_veg.dgoid IN ({','.join(map(str, dgo_ids))})""")
    result = cursor.fetchone()
    if None in result:
        out = None
    elif result[1] > 0.0:
        out = 1 - (result[0] / result[1])
    else:
        out = None

    return out
