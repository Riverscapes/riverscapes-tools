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
import os
import sqlite3
from typing import List
from osgeo import ogr
from shapely.geometry.base import BaseGeometry
from rscommons import Logger, VectorBase, GeopackageLayer
from rscommons.vector_ops import get_geometry_unary_union
from rscommons import get_shp_or_gpkg


def vbet_network(flow_lines_path: str, flow_areas_path: str, out_path: str, epsg: int = None, fcodes: List[str] = None):

    log = Logger('VBET Network')
    log.info('Generating perennial network')
    fcodes = ["46006"] if fcodes is None else fcodes

    with get_shp_or_gpkg(out_path, write=True) as vbet_net, \
            get_shp_or_gpkg(flow_lines_path) as flow_lines_lyr:

        # Add input Layer Fields to the output Layer if it is the one we want
        vbet_net.create_layer_from_ref(flow_lines_lyr, epsg=epsg)

        # Perennial features
        log.info('Incorporating perennial features')
        fcode_filter = "FCode = " + " or FCode = ".join([f"'{fcode}'" for fcode in fcodes]) if len(fcodes) > 0 else ""  # e.g. "FCode = '46006' or FCode = '55800'"
        fids = include_features(flow_lines_lyr, vbet_net, fcode_filter)

        # Flow area features
        if flow_areas_path is not None:
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
            fid = feature.GetFID()
            included_fids.append(fid)
            excluded_fids.append(fid)
            # Add field values from input Layer
            for i in range(0, out_layer.ogr_layer_def.GetFieldCount()):
                out_feature.SetField(out_layer.ogr_layer_def.GetFieldDefn(i).GetNameRef(), feature.GetField(i))

            geom = feature.GetGeometryRef()
            out_feature.SetGeometry(geom.Clone())
            out_layer.ogr_layer.CreateFeature(out_feature)

    return included_fids


def create_stream_size_zones(catchment_layer, flowlines_layer, join_field, copy_field, zones, out_layer):

    # Load drainage area
    with sqlite3.connect(os.path.dirname(flowlines_layer)) as conn:
        cursor = conn.cursor()
        join_data = cursor.execute(f"""SELECT {join_field}, {copy_field} FROM {os.path.basename(flowlines_layer)}""").fetchall()
        data = {int(value[0]): value[1] for value in join_data}

    with GeopackageLayer(os.path.dirname(catchment_layer), layer_name=os.path.basename(catchment_layer)) as lyr_source, \
            GeopackageLayer(os.path.dirname(out_layer), layer_name=os.path.basename(out_layer), write=True) as lyr_destination:

        # Create Output
        srs = lyr_source.spatial_ref
        lyr_destination.create_layer(ogr.wkbPolygon, spatial_ref=srs)
        lyr_destination.create_field(copy_field, field_type=ogr.OFTReal)
        for attribute_type in zones:
            lyr_destination.create_field(f'{attribute_type}_Zone', field_type=ogr.OFTInteger)

        out_layer_defn = lyr_destination.ogr_layer.GetLayerDefn()
        lyr_destination.ogr_layer.StartTransaction()
        # Build Zones
        for feat_source, *_ in lyr_source.iterate_features("Joining attributes"):

            join_value = feat_source.GetField(join_field)
            if join_value is not None:
                join_id = int(join_value)

                if join_id in data:

                    geom = feat_source.GetGeometryRef()
                    feat_dest = ogr.Feature(out_layer_defn)
                    feat_dest.SetFID(join_id)
                    feat_dest.SetGeometry(geom)

                    feat_dest.SetField(copy_field, data[join_id])  # Set drainage area
                    if data[join_id]:  # if the drainage area is not null

                        out_zones = {}
                        for zone_type, zone_values in zones.items():
                            for i, value in zone_values.items():
                                if value:
                                    if data[join_id] < value:
                                        out_zones[zone_type] = i
                                        break
                                else:
                                    out_zones[zone_type] = i

                        for zone_field, zone_value in out_zones.items():
                            feat_dest.SetField(f'{zone_field}_Zone', zone_value)

                    lyr_destination.ogr_layer.CreateFeature(feat_dest)
        lyr_destination.ogr_layer.CommitTransaction()


def copy_vaa_attributes(destination_layer, vaa_table):

    with sqlite3.connect(os.path.dirname(vaa_table)) as conn_vaa, \
            sqlite3.connect(os.path.dirname(destination_layer)) as conn_dest:
        curs = conn_dest.cursor()

        for line in conn_vaa.iterdump():

            curs.execute(line)

        curs.execute(f"INSERT INTO gpkg_contents (table_name, data_type) VALUES ('{os.path.basename(vaa_table)}', 'attributes');")
        conn_dest.commit()

    return os.path.basename(vaa_table)


def join_attributes(gpkg, name, geom_layer, attribute_layer, join_field, fields, epsg):

    sql = f"CREATE VIEW {name} AS SELECT G.*, {','.join(['A.' + item for item in fields])} FROM {geom_layer} G INNER JOIN {attribute_layer} A ON G.{join_field} = A.{join_field};"

    with sqlite3.connect(gpkg) as conn:

        curs = conn.cursor()
        curs.execute(sql)
        conn.commit()

        curs.execute(f"INSERT INTO gpkg_contents (table_name, identifier, data_type, srs_id) VALUES ('{name}', '{name}', 'features', {epsg});")
        conn.commit()

        curs.execute(f"INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) values ('{name}', 'geom', 'LINESTRING', {epsg}, 0, 0);")
        conn.commit()

    return os.path.join(gpkg, name)


def generate_channel_areas(flowline_network, flow_areas, buffer_field, catchments, out_channel_area, waterbodies=None):

    network_path_buffered = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['VBET_NETWORK_BUFFERED'].rel_path)
    buffer_by_field(flowline_network, network_path_buffered, "BFwidth", cfg.OUTPUT_EPSG, centered=True)

    merge_feature_classes([filtered_waterbody, project_inputs['FLOW_AREA']], flow_polygons)

    flow_polygons = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['FLOW_POLYGONS'].rel_path)
    if "WATERBODY" in project_inputs:
        log.info('Filter and merge waterbody polygons with Flow Areas')
        filtered_waterbody = os.path.join(intermediates_gpkg_path, "waterbody_filtered")
        wb_fcodes = [39000, 39001, 39004, 39005, 39006, 39009, 39010, 39011, 39012, 36100, 46600, 46601, 46602]
        fcode_filter = "FCode = " + " or FCode = ".join([f"'{fcode}'" for fcode in wb_fcodes]) if len(wb_fcodes) > 0 else ""
        copy_feature_class(project_inputs["WATERBODY"], filtered_waterbody, attribute_filter=fcode_filter)
        merge_feature_classes([filtered_waterbody, project_inputs['FLOW_AREA']], flow_polygons)
    else:
        copy_feature_class(project_inputs['FLOW_AREA'], flow_polygons)

    flow_area_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_RASTER'].rel_path)
    rasterize(flow_polygons, flow_area_raster, project_inputs['SLOPE_RASTER'])
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['FLOW_AREA_RASTER'])
    fa_dist_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_DISTANCE'].rel_path)
    proximity_raster(flow_area_raster, fa_dist_raster)
    project.add_project_raster(proj_nodes["Intermediates"], LayerTypes['FLOW_AREA_DISTANCE'])

    log.info('Writing channel raster using slope as a template')
    channel_buffer_raster = os.path.join(project_folder, LayerTypes['CHANNEL_BUFFER_RASTER'].rel_path)
    rasterize(network_path_buffered, channel_buffer_raster, project_inputs['SLOPE_RASTER'])
    project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['CHANNEL_BUFFER_RASTER'])

    log.info('Generating Channel Proximity raster')
    channel_dist_raster = os.path.join(project_folder, LayerTypes['CHANNEL_DISTANCE'].rel_path)
    proximity_raster(channel_buffer_raster, channel_dist_raster)
    project.add_project_raster(proj_nodes["Intermediates"], LayerTypes['CHANNEL_DISTANCE'])

    in_rasters['Channel'] = channel_dist_raster
    out_rasters['NORMALIZED_CHANNEL_DISTANCE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_CHANNEL_DISTANCE'].rel_path)

    in_rasters['Flow Areas'] = fa_dist_raster
    out_rasters['NORMALIZED_FLOWAREA_DISTANCE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_FLOWAREA_DISTANCE'].rel_path)
