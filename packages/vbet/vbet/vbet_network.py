# -------------------------------------------------------------------------------
# Name:     Valley Bottom
#
# Purpose:  Perform initial VBET analysis that can be used by the BRAT conservation
#           module
#
# Author:   Philip Bailey
#
# Date:     7 Oct 2019
#
# https://nhd.usgs.gov/userGuide/Robohelpfiles/NHD_User_Guide/Feature_Catalog/Hydrography_Dataset/Complete_FCode_List.htm
# -------------------------------------------------------------------------------
from osgeo import ogr
from shapely.geometry.base import BaseGeometry
from rscommons import Logger, VectorBase
from rscommons.vector_ops import get_geometry_unary_union
from rscommons import get_shp_or_gpkg


def vbet_network(flow_lines_path: str, flow_areas_path: str, out_path: str, epsg: int = None, fcodes: list = None):

    log = Logger('VBET Network')
    log.info('Generating perennial network')
    fcodes = [46006] if fcodes is None else fcodes

    with get_shp_or_gpkg(out_path, write=True) as vbet_net, \
            get_shp_or_gpkg(flow_lines_path) as flow_lines_lyr:

        # Add input Layer Fields to the output Layer if it is the one we want
        vbet_net.create_layer_from_ref(flow_lines_lyr, epsg=epsg)

        # Perennial features
        log.info('Incorporating perennial features')
        fcode_filter = "FCode = " + " or FCode = ".join([f"'{fcode}'" for fcode in fcodes]) if len(fcodes) > 0 else ""  # e.g. "FCode = '46006' or FCode = '55800'"
        fids = include_features(flow_lines_lyr, vbet_net, fcode_filter)

        # Flow area features
        polygon = get_geometry_unary_union(flow_areas_path, epsg=epsg)
        if polygon is not None:
            log.info('Incorporating flow areas.')
            include_features(flow_lines_lyr, vbet_net, "FCode <> '46006'", polygon, excluded_fids=fids)

        fcount = flow_lines_lyr.ogr_layer.GetFeatureCount()

        log.info('VBET network generated with {} features'.format(fcount))


def include_features(source_layer: VectorBase, out_layer: VectorBase, attribute_filter: str = None, clip_shape: BaseGeometry = None, excluded_fids: list = None):

    included_fids = []
    excluded_fids = [] if excluded_fids is None else excluded_fids
    for feature, _counter, _progbar in source_layer.iterate_features('Including Features', write_layers=[out_layer], attribute_filter=attribute_filter, clip_shape=clip_shape):
        out_feature = ogr.Feature(out_layer.ogr_layer_def)

        if feature.GetFID() not in excluded_fids:

            included_fids.append(feature.GetFID())

            # Add field values from input Layer
            for i in range(0, out_layer.ogr_layer_def.GetFieldCount()):
                out_feature.SetField(out_layer.ogr_layer_def.GetFieldDefn(i).GetNameRef(), feature.GetField(i))

            geom = feature.GetGeometryRef()
            out_feature.SetGeometry(geom.Clone())
            out_layer.ogr_layer.CreateFeature(out_feature)

    return included_fids
