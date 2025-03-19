import os
import rasterio
import sqlite3
from rscommons import GeopackageLayer, VectorBase
from rme.utils.measurements import get_segment_measurements


def watershed(huc):
    if len(str(huc)) > 10:
        return str(huc[:10])
    else:
        return str(huc)


def headwater(feat_geom, line_network):
    sum_attributes = {}
    with GeopackageLayer(line_network) as lyr_lines:
        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
            line_geom = feat.GetGeometryRef()
            attribute = str(feat.GetField('STARTFLAG'))
            if attribute not in ['1', '0']:
                continue
            geom_section = feat_geom.Intersection(line_geom)
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
    with GeopackageLayer(line_network) as lyr_lines:
        leng = 0
        for feat, *_ in lyr_lines.iterate_features(clip_shape=feat_geom):
            geom_flowline_full = feat.GetGeometryRef()
            feat_section = geom_flowline_full.Intersection(feat_geom)
            section_proj = VectorBase.ogr2shapely(feat_section, transform=transform)
            leng += section_proj.length

    return leng


def waterbody_extent(feat_geom, waterbodies, transform):
    with GeopackageLayer(waterbodies) as lyr:
        area = 0
        for feat, *_ in lyr.iterate_features(clip_shape=feat_geom):
            geom_wb_full = feat.GetGeometryRef()
            feat_section = geom_wb_full.Intersection(feat_geom)
            section_proj = VectorBase.ogr2shapely(feat_section, transform=transform)
            area += section_proj.area

    return area


def calculate_gradient(gpkg, dgoid, channel=True):
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        if channel:
            curs.execute(
                f"SELECT STRMMAXELEV, STRMMINELEV, STRMLENG FROM dgo_measurements WHERE DGOID = {dgoid}")
        else:
            curs.execute(
                f"SELECT CLMAXELEV, CLMINELEV, VALLENG FROM dgo_measurements WHERE DGOID = {dgoid}")
        vals = curs.fetchone()
        if vals is None:
            return None
        if vals[2] > 0.0:
            gradient = (vals[0] - vals[1]) / vals[2]
        else:
            gradient = None

    return gradient


def rel_flow_length(feat_geom, line_network, transform):
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


def landfire_classes(feat_geom, gpkg, epoch=1):
    classes = {}
    classes_out = []
    dgo_id = feat_geom.GetFID()
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        curs.execute(
            f"""SELECT DGOVegetation.VegetationID, CellCount FROM DGOVegetation LEFT JOIN vegetation_types 
            ON DGOVegetation.VegetationID = vegetation_types.VegetationID WHERE DGOID = {dgo_id} AND EpochID = {epoch}""")
        for row in curs.fetchall():
            classes[row[0]] = row[1]

    for key, value in classes.items():
        if value / sum(classes.values()) > 0.3:
            classes_out.append(key)

    return classes_out


def mw_calculate_gradient(gpkg, dgo_ids, channel=True):
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        if channel:
            curs.execute(
                f"SELECT MAX(STRMMAXELEV), MIN(STRMMINELEV), SUM(STRMLENG) FROM dgo_measurements WHERE DGOID IN ({','.join(map(str, dgo_ids))})")
        else:
            curs.execute(
                f"SELECT MAX(CLMAXELEV), MIN(CLMINELEV), SUM(VALLENG) FROM dgo_measurements WHERE DGOID IN ({','.join(map(str, dgo_ids))})")
        vals = curs.fetchone()
        if None in vals:
            return None
        if vals[2] > 0.0:
            gradient = (vals[0] - vals[1]) / vals[2]
        else:
            gradient = None

    return gradient


def mw_calculate_sinuosity(gpkg, dgo_ids):
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        curs.execute(
            f"SELECT SUM(STRMLENG), SUM(STRMSTRLENG) FROM dgo_measurements WHERE DGOID IN ({','.join(map(str, dgo_ids))})")
        vals = curs.fetchone()
        if None in vals:
            return None
        if vals[1] > 0.0:
            sinuosity = vals[0] / vals[1]
        else:
            sinuosity = None

    return sinuosity


def mw_acres_per_mi(dgo_ids, gpkg):
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        curs.execute(
            f"""SELECT SUM(segment_area), SUM(centerline_length) FROM dgos 
            WHERE DGOID IN ({','.join(map(str, dgo_ids))})""")
        result = curs.fetchone()
        if None in result:
            return None
        if result[1] > 0.0:
            out = (result[0] * 0.000247105) / (result[1] * 0.000621371)
        else:
            out = None

    return out


def mw_hect_per_km(dgo_ids, gpkg):
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        curs.execute(
            f"""SELECT SUM(segment_area), SUM(centerline_length) FROM dgos 
            WHERE DGOID IN ({','.join(map(str, dgo_ids))})""")
        result = curs.fetchone()
        if None in result:
            return None
        if result[1] > 0.0:
            out = (result[0] * 0.0001) / (result[1] * 0.001)
        else:
            out = None

    return out


def mw_stream_power(dgo_ids, gpkg, q='QLow'):
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        curs.execute(
            f"""SELECT MAX(STRMMAXELEV), MIN(STRMMINELEV), SUM(STRMLENG) FROM dgo_measurements 
            WHERE DGOID IN ({','.join(map(str, dgo_ids))})""")
        result = curs.fetchone()
        if None in result:
            return None
        if result[2] > 0.0:
            slope = (result[0] - result[1]) / result[2]
            curs.execute(f"SELECT MAX({q}) FROM hydro_dgo WHERE DGOID IN ({','.join(map(str, dgo_ids))})")
            discharge = curs.fetchone()[0]
            return slope * discharge * 0.0283168 * 9810
        else:
            return None


def mw_rvd(dgo_ids, gpkg):
    with sqlite3.connect(gpkg) as conn:
        curs = conn.cursor()
        curs.execute(f"""SELECT riparian_veg_departure, segment_area FROM veg_dgo LEFT JOIN dgos
                     ON veg_dgo.DGOID = dgos.DGOID WHERE dgos.DGOID IN ({','.join(map(str, dgo_ids))})""")
        result = curs.fetchall()
        if len(result) == 0:
            return None
        else:
            return sum([r[0] * r[1] for r in result]) / sum([r[1] for r in result if None not in r])
