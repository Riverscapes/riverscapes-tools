from rscommons import GeopackageLayer, VectorBase


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
