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
