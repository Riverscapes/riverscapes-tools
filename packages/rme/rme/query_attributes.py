"""Add important attributes to IGOs and DGOs to enable querying metrics.

Jordan Gilbert

08/2023
"""

import os

from osgeo import ogr

from rscommons import GeopackageLayer, get_shp_or_gpkg, VectorBase
from rscommons.vector_ops import copy_feature_class
from rscommons.shapefile import get_transform_from_epsg, get_utm_zone_epsg


def copy_attributes(igo, dgo, flowline, ownership, states, counties, out_folder):

    # copy inputs
    # input_gpkg = os.path.join(out_folder, 'inputs.gpkg')
    # copy_feature_class(flowline, os.path.join(input_gpkg, 'flowlines'))
    # copy_feature_class(ownership, os.path.join(input_gpkg, 'ownership'))
    # copy_feature_class(states, os.path.join(input_gpkg, 'states'))
    # copy_feature_class(counties, os.path.join(input_gpkg, 'counties'))

    # copy IGOs / DGOs to output folder
    out_gpkg = os.path.join(out_folder, 'attributes.gpkg')
    out_igo = os.path.join(out_gpkg, 'attributes_igo')
    out_dgo = os.path.join(out_gpkg, 'attributes_dgo')
    copy_feature_class(igo, out_igo)
    copy_feature_class(dgo, out_dgo)

    # add fields to IGOs
    with GeopackageLayer(os.path.join(out_gpkg, 'attributes_dgo'), write=True) as dgo_lyr, \
            GeopackageLayer(flowline) as flowline_lyr, \
            get_shp_or_gpkg(ownership) as ownership_lyr, \
            get_shp_or_gpkg(states) as states_lyr, \
            get_shp_or_gpkg(counties) as counties_lyr:

        ftr1 = dgo_lyr.ogr_layer.GetNextFeature()
        long = ftr1.GetGeometryRef().Centroid().GetX()
        epsg_proj = get_utm_zone_epsg(long)
        _spatial_ref, transform = get_transform_from_epsg(dgo_lyr.spatial_ref, epsg_proj)

        # add desired fields to dgo output
        for i in range(flowline_lyr.ogr_layer_def.GetFieldCount()):
            if flowline_lyr.ogr_layer_def.GetFieldDefn(i).GetName() == 'FCode':
                reachcode_defn = flowline_lyr.ogr_layer_def.GetFieldDefn(i)
        dgo_lyr.ogr_layer.CreateField(reachcode_defn)
        dgo_lyr.ogr_layer.CreateField(ogr.FieldDefn('channel_length', ogr.OFTReal))

        for i in range(ownership_lyr.ogr_layer_def.GetFieldCount()):
            if ownership_lyr.ogr_layer_def.GetFieldDefn(i).GetName() == 'ADMIN_AGEN':
                own_type_defn = ownership_lyr.ogr_layer_def.GetFieldDefn(i)
        dgo_lyr.ogr_layer.CreateField(own_type_defn)
        dgo_lyr.ogr_layer.CreateField(ogr.FieldDefn('ownership_area', ogr.OFTReal))

        dgo_lyr.ogr_layer.CreateField(ogr.FieldDefn('State', ogr.OFTString))
        dgo_lyr.ogr_layer.CreateField(ogr.FieldDefn('County', ogr.OFTString))

        # obtain field values
        dgo_lyr.ogr_layer.StartTransaction()
        for dgo_ftr, *_ in dgo_lyr.iterate_features("Obtaining query attributes"):
            levelpath = dgo_ftr.GetField('LevelPathI')
            distance = dgo_ftr.GetField('seg_distance')
            dgo_ogr = dgo_ftr.GetGeometryRef().Clone()
            dgo_geom = VectorBase.ogr2shapely(dgo_ogr, transform)

            fcodes = {}
            for flowline_ftr, *_ in flowline_lyr.iterate_features(clip_shape=dgo_ftr):
                flowline_ogr = flowline_ftr.GetGeometryRef()
                flowline_g = VectorBase.ogr2shapely(flowline_ogr, transform)
                flowline_geom = flowline_g.intersection(dgo_geom)
                if flowline_ftr.GetField('FCode') in fcodes.keys():
                    fcodes[flowline_ftr.GetField('FCode')] += flowline_geom.length
                else:
                    fcodes[flowline_ftr.GetField('FCode')] = flowline_geom.length
            for fcode, leng in fcodes.items():
                if leng == max(fcodes.values()):
                    dgo_ftr.SetField('FCode', fcode)
                    dgo_ftr.SetField('channel_length', sum(fcodes.values()))  # this process chooses dominant fcode and sets its length to length of all present fcodes...
                    break

            agencies = {}
            for own_ftr, *_ in ownership_lyr.iterate_features(clip_shape=dgo_ftr):
                own_ogr = own_ftr.GetGeometryRef()
                own_g = GeopackageLayer.ogr2shapely(own_ogr, transform)
                own_geom = own_g.intersection(dgo_geom)
                if own_ftr.GetField('ADMIN_AGEN') in agencies.keys():
                    agencies[own_ftr.GetField('ADMIN_AGEN')] += own_geom.area
                else:
                    agencies[own_ftr.GetField('ADMIN_AGEN')] = own_geom.area
            for agency, areas in agencies.items():
                if areas == max(agencies.values()):
                    dgo_ftr.SetField('ADMIN_AGEN', agency)
                    dgo_ftr.SetField('ownership_area', agencies[agency])
                    break

            states_dict = {}
            for state_ftr, *_ in states_lyr.iterate_features(clip_shape=dgo_ftr):
                state_ogr = state_ftr.GetGeometryRef()
                state_g = GeopackageLayer.ogr2shapely(state_ogr, transform)
                state_geom = state_g.intersection(dgo_geom)
                if state_ftr.GetField('NAME') in states_dict.keys():
                    states_dict[state_ftr.GetField('NAME')] += state_geom.area
                else:
                    states_dict[state_ftr.GetField('NAME')] = state_geom.area
            for state, areas in states_dict.items():
                if areas == max(states_dict.values()):
                    dgo_ftr.SetField('State', state)
                    break

            counties_dict = {}
            for county_ftr, *_ in counties_lyr.iterate_features(clip_shape=dgo_ftr):
                county_ogr = county_ftr.GetGeometryRef()
                county_g = GeopackageLayer.ogr2shapely(county_ogr, transform)
                county_geom = county_g.intersection(dgo_geom)
                if county_ftr.GetField('NAME') in counties_dict.keys():
                    counties_dict[county_ftr.GetField('NAME')] += county_geom.area
                else:
                    counties_dict[county_ftr.GetField('NAME')] = county_geom.area
            for county, areas in counties_dict.items():
                if areas == max(counties_dict.values()):
                    dgo_ftr.SetField('County', county)
                    break
            dgo_lyr.ogr_layer.SetFeature(dgo_ftr)

        dgo_lyr.ogr_layer.CommitTransaction()


igo_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/vbet/1601020204/outputs/vbet.gpkg/vbet_igos'
dgo_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/vbet/1601020204/intermediates/vbet_intermediates.gpkg/vbet_dgos'
flowline_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/1601020204/hydrology/nhdplushr.gpkg/NHDFlowline'
ownership_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/1601020204/ownership/ownership.shp'
states_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/1601020204/political_boundaries/states.shp'
counties_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/1601020204/political_boundaries/counties.shp'
out_folder_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/igo_atts'

copy_attributes(igo_in, dgo_in, flowline_in, ownership_in, states_in, counties_in, out_folder_path)
