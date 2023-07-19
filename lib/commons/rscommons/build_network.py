# Name:     Build Network
#
# Purpose:  Filters out features from a flow line ShapeFile and
#           reprojects the features to the specified spatial reference.
#
#           The features to be removed are identified by NHD FCode.
#           Artifical channels can be retained, removed or subset into
#           those that occur within large waterbodies or big rivers
#           represented as polygons.
#
# Author:   Philip Bailey
#
# Date:     15 May 2019
# -------------------------------------------------------------------------------
from typing import List
from osgeo import ogr
from rscommons import Logger, get_shp_or_gpkg, VectorBase
from rscommons.vector_ops import get_geometry_unary_union

# https://nhd.usgs.gov/userGuide/Robohelpfiles/NHD_User_Guide/Feature_Catalog/Hydrography_Dataset/Complete_FCode_List.htm
FCodeValues = {
    33400: "connector",
    33600: "canal",
    33601: "aqueduct",
    33603: "stormwater",
    46000: "general",
    46003: "intermittent",
    46006: "perennial",
    46007: "ephemeral",
    55800: "artificial"
}

ARTIFICIAL_REACHES = '55800'


def build_network(flowlines_path: str,
                  flowareas_path: str,
                  out_path: str,
                  epsg: int = None,
                  reach_codes: List[str] = None,
                  waterbodies_path: str = None,
                  waterbody_max_size=None,
                  create_layer: bool = True):
    """[summary]

    Args:
        flowlines_path (str): [description]
        flowareas_path (str): [description]
        out_path (str): [description]
        epsg (int, optional): [description]. Defaults to None.
        reach_codes (List[str], optional): [description]. Defaults to None.
        waterbodies_path (str, optional): [description]. Defaults to None.
        waterbody_max_size ([type], optional): [description]. Defaults to None.
        create_layer (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """

    log = Logger('Build Network')

    log.info("Building network from flow lines {0}".format(flowlines_path))

    if reach_codes:
        for r in reach_codes:
            log.info('Retaining {} reaches with code {}'.format(FCodeValues[int(r)], r))
    else:
        log.info('Retaining all reaches. No reach filtering.')

    # Get the transformation required to convert to the target spatial reference
    if epsg is not None:
        with get_shp_or_gpkg(flowareas_path) as flowareas_lyr:
            out_spatial_ref, transform = VectorBase.get_transform_from_epsg(flowareas_lyr.spatial_ref, epsg)

    # Process all perennial/intermittment/ephemeral reaches first
    attribute_filter = None
    if reach_codes and len(reach_codes) > 0:
        _result = [log.info("{0} {1} network features (FCode {2})".format('Retaining', FCodeValues[int(key)], key)) for key in reach_codes]
        attribute_filter = "FCode IN ({0})".format(','.join([key for key in reach_codes]))

    if create_layer is True:
        with get_shp_or_gpkg(flowlines_path) as flowlines_lyr, get_shp_or_gpkg(out_path, write=True) as out_lyr:
            out_lyr.create_layer_from_ref(flowlines_lyr)

    log.info('Processing all reaches')
    process_reaches(flowlines_path, out_path, attribute_filter=attribute_filter)

    # Process artifical paths through small waterbodies
    if waterbodies_path is not None and waterbody_max_size is not None:
        small_waterbodies = get_geometry_unary_union(waterbodies_path, epsg, attribute_filter='AreaSqKm <= ({0})'.format(waterbody_max_size))
        log.info('Retaining artificial features within waterbody features smaller than {0}km2'.format(waterbody_max_size))
        process_reaches(flowlines_path,
                        out_path,
                        transform=transform,
                        attribute_filter='FCode = {0}'.format(ARTIFICIAL_REACHES),
                        clip_shape=small_waterbodies
                        )

    # Retain artifical paths through flow areas
    if flowareas_path:
        flow_polygons = get_geometry_unary_union(flowareas_path, epsg)
        if flow_polygons:
            log.info('Retaining artificial features within flow area features')
            process_reaches(flowlines_path,
                            out_path,
                            transform=transform,
                            attribute_filter='FCode = {0}'.format(ARTIFICIAL_REACHES),
                            clip_shape=flow_polygons
                            )

        else:
            log.info('Zero artifical paths to be retained.')

    with get_shp_or_gpkg(out_path) as out_lyr:
        log.info(('{:,} features written to {:}'.format(out_lyr.ogr_layer.GetFeatureCount(), out_path)))

    log.info('Process completed successfully.')
    return out_spatial_ref


def process_reaches(in_path: str, out_path: str, attribute_filter=None, transform=None, clip_shape=None):
    """[summary]

    Args:
        in_path (str): [description]
        out_path (str): [description]
        attribute_filter ([type], optional): [description]. Defaults to None.
        transform ([type], optional): [description]. Defaults to None.
        clip_shape ([type], optional): [description]. Defaults to None.
    """
    log = Logger('process reaches')

    with get_shp_or_gpkg(in_path) as in_lyr, get_shp_or_gpkg(out_path, write=True) as out_lyr:

        for feature, _counter, _progbar in in_lyr.iterate_features("Processing reaches", attribute_filter=attribute_filter, clip_shape=clip_shape, write_layers=[out_lyr]):
            # get the input geometry and reproject the coordinates
            geom = feature.GetGeometryRef()
            if geom.Length() < 1e-10:
                log.info(f'Feature {feature.GetFID()} has essentally zero length ({geom.Length()}), not being copied')
                continue
            if transform is not None:
                geom.Transform(transform)

            # Create output Feature
            out_feature = ogr.Feature(out_lyr.ogr_layer_def)

            # Add field values from input Layer
            for i in range(0, out_lyr.ogr_layer_def.GetFieldCount()):
                field_name = out_lyr.ogr_layer_def.GetFieldDefn(i).GetNameRef()
                output_field_index = feature.GetFieldIndex(field_name)
                if output_field_index >= 0:
                    out_feature.SetField(field_name, feature.GetField(output_field_index))

            # Add new feature to output Layer
            out_feature.SetGeometry(geom)
            out_lyr.ogr_layer.CreateFeature(out_feature)
