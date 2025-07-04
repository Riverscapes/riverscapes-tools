from rscommons import GeopackageLayer, Logger

from osgeo import ogr


def calculate_confinement(confinement_type_network, segment_network, output_network):

    log = Logger("Calculate Confinement")
    log.info("Starting confinement calculation")

    with GeopackageLayer(segment_network, write=True) as segment_lyr, \
            GeopackageLayer(confinement_type_network, write=True) as confinement_lyr, \
            GeopackageLayer(output_network, write=True) as output_lyr:

        meter_conversion = segment_lyr.rough_convert_metres_to_vector_units(1)
        selection_buffer = 0.01 * meter_conversion

        output_lyr.create_layer_from_ref(segment_lyr, create_fields=False)
        output_lyr.create_fields({
            'confinement_ratio': ogr.FieldDefn("confinement_ratio", ogr.OFTReal),
            'constriction_ratio': ogr.FieldDefn("constriction_ratio", ogr.OFTReal),
            'length': ogr.FieldDefn("approx_leng", ogr.OFTReal),
            'confined_length': ogr.FieldDefn("confin_leng", ogr.OFTReal),
            'constricted_length': ogr.FieldDefn("constr_leng", ogr.OFTReal)
        })
        output_lyr.ogr_layer.StartTransaction()
        for segment_feat, *_ in segment_lyr.iterate_features("Calculating confinement per segment"):
            segment_ogr = segment_feat.GetGeometryRef()
            segment_geom = GeopackageLayer.ogr2shapely(segment_ogr)
            segment_poly = segment_geom.buffer(selection_buffer, cap_style=2)
            confinement_lengths = {c_type: 0.0 for c_type in [
                "Left", "Right", "Both", "None"]}
            for confinement_feat, *_ in confinement_lyr.iterate_features(clip_shape=segment_poly):
                con_type = confinement_feat.GetField("confinement_type")
                confinement_ogr = confinement_feat.GetGeometryRef()
                confinement_geom = GeopackageLayer.ogr2shapely(confinement_ogr)
                confinement_clip = confinement_geom.intersection(segment_poly)
                # if any([confinement_geom.intersects(pt.buffer(selection_buffer))for pt in segment_endpoints]):
                #     for pt in segment_endpoints:
                #         if confinement_geom.intersects(pt.buffer(selection_buffer)):
                #             cut_distance = confinement_geom.project(pt)
                #             split_geoms = cut(confinement_geom, cut_distance)
                #             lgeom = select_geoms_by_intersection(split_geoms, [segment_geom], buffer=selection_buffer)
                #             if len(lgeom) > 0:
                #                 geom = lgeom[0]
                #                 confinement_lengths[con_type] = confinement_lengths[con_type] + geom.length / meter_conversion
                # else:
                #     confinement_lengths[con_type] = confinement_lengths[con_type] + confinement_geom.length / meter_conversion
                if not confinement_clip.is_empty:
                    confinement_lengths[con_type] += confinement_clip.length / \
                        meter_conversion

            # calcuate confimenet parts
            confinement_length = 0.0
            constricted_length = 0.0
            unconfined_length = 0.0
            for con_type, length in confinement_lengths.items():
                if con_type in ['Left', 'Right']:
                    confinement_length += length
                elif con_type in ['Both']:
                    constricted_length += length
                else:
                    unconfined_length += length
            segment_length = sum(
                [confinement_length, constricted_length, unconfined_length])
            confinement_ratio = min((confinement_length + constricted_length) /
                                    segment_length, 1.0) if segment_length > 0.0 else None
            constricted_ratio = constricted_length / \
                segment_length if segment_length > 0.0 else None
            attributes = {
                "confinement_ratio": confinement_ratio,
                "constriction_ratio": constricted_ratio,
                "approx_leng": segment_length,
                "confin_leng": confinement_length + constricted_length,
                "constr_leng": constricted_length
            }
            output_lyr.create_feature(segment_geom, attributes=attributes)
        output_lyr.ogr_layer.CommitTransaction()

    log.info("Finished confinement calculation")

    return


def dgo_confinement(confinement_type_network, out_dgos):

    log = Logger("Calculate Confinement (DGO)")
    log.info("Starting confinement calculation")

    with GeopackageLayer(out_dgos, write=True) as dgo_lyr, \
            GeopackageLayer(confinement_type_network, write=True) as confinement_lyr:

        meter_conversion = dgo_lyr.rough_convert_metres_to_vector_units(1)

        dgo_lyr.ogr_layer.StartTransaction()
        for dgo_feat, *_ in dgo_lyr.iterate_features("Calculating confinement per DGO"):
            level_path = dgo_feat.GetField("level_path")
            if level_path is None:
                continue
            dgo_ogr = dgo_feat.GetGeometryRef()
            dgo_geom = GeopackageLayer.ogr2shapely(dgo_ogr)

            confinement_lengths = {c_type: 0.0 for c_type in [
                "Left", "Right", "Both", "None"]}
            for confinement_feat, *_ in confinement_lyr.iterate_features(clip_shape=dgo_geom, attribute_filter=f'level_path = {level_path}'):
                con_type = confinement_feat.GetField("confinement_type")
                confinement_ogr = confinement_feat.GetGeometryRef()
                confinement_geom = GeopackageLayer.ogr2shapely(confinement_ogr)
                confinement_clip = confinement_geom.intersection(dgo_geom)

                if not confinement_clip.is_empty:
                    confinement_lengths[con_type] += confinement_clip.length / \
                        meter_conversion

            # calcuate confimenet parts
            confinement_length = 0.0
            constricted_length = 0.0
            unconfined_length = 0.0
            for con_type, length in confinement_lengths.items():
                if con_type in ['Left', 'Right']:
                    confinement_length += length
                elif con_type in ['Both']:
                    constricted_length += length
                else:
                    unconfined_length += length
            segment_length = sum(
                [confinement_length, constricted_length, unconfined_length])
            confinement_ratio = min((confinement_length + constricted_length) /
                                    segment_length, 1.0) if segment_length > 0.0 else None
            constricted_ratio = constricted_length / \
                segment_length if segment_length > 0.0 else None

            dgo_feat.SetField(
                "confin_leng", confinement_length + constricted_length)
            dgo_feat.SetField("constr_leng", constricted_length)
            dgo_feat.SetField("approx_leng", segment_length)
            dgo_feat.SetField("confinement_ratio", confinement_ratio)
            dgo_feat.SetField("constriction_ratio", constricted_ratio)

            dgo_lyr.ogr_layer.SetFeature(dgo_feat)

        dgo_lyr.ogr_layer.CommitTransaction()

    log.info("Finished confinement calculation")

    return
