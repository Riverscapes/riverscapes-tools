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

Path = str


def vbet_network(flow_lines_path: str, flow_areas_path: str, out_path: str, epsg: int = None, fcodes: List[str] = None, reach_code_field='FCode', flow_areas_path_exclude=None, hard_clip_shape=None):

    log = Logger('VBET Network')
    log.info('Generating perennial network')
    fcodes = ["46006"] if fcodes is None else fcodes

    with get_shp_or_gpkg(out_path, write=True) as vbet_net, \
            get_shp_or_gpkg(flow_lines_path) as flow_lines_lyr:

        # add fcodes from included level paths that aren't already in the list
        stream_names = []
        for line_ftr, *_ in flow_lines_lyr.iterate_features():
            if str(line_ftr.GetField(reach_code_field)) in fcodes:
                if len(stream_names) > 0 and line_ftr.GetField('GNIS_Name') not in stream_names and line_ftr.GetField('GNIS_Name') is not None:
                    stream_names.append(line_ftr.GetField('GNIS_Name'))  # this is NHD only this way

        for line_ftr, *_ in flow_lines_lyr.iterate_features():
            if len(stream_names) > 0 and line_ftr.GetField('GNIS_Name') in stream_names:
                if str(line_ftr.GetField(reach_code_field)) not in fcodes:
                    fcodes.append(str(line_ftr.GetField(reach_code_field)))

        # Add input Layer Fields to the output Layer if it is the one we want
        vbet_net.create_layer_from_ref(flow_lines_lyr, epsg=epsg)

        exclude_fids = []
        if flow_areas_path_exclude is not None:
            with get_shp_or_gpkg(flow_areas_path_exclude) as lyr_exclude:
                for poly_feat, *_ in lyr_exclude.iterate_features("Filtering waterbodies"):
                    poly_geom = poly_feat.GetGeometryRef()
                    for line_feat, *_ in flow_lines_lyr.iterate_features(clip_shape=poly_geom):
                        line_geom = line_feat.GetGeometryRef()
                        if line_geom.Within(poly_geom):
                            fid = line_feat.GetFID()
                            exclude_fids.append(fid)

        # Perennial features
        log.info('Incorporating perennial features')
        fcode_filter = f"{reach_code_field} = " + f" or {reach_code_field} = ".join([f"'{fcode}'" for fcode in fcodes]) if len(fcodes) > 0 else ""  # e.g. "FCode = '46006' or FCode = '55800'"
        fids = include_features(flow_lines_lyr, vbet_net, fcode_filter, excluded_fids=exclude_fids, hard_clip_shape=hard_clip_shape)

        # Flow area features
        if flow_areas_path is not None:
            polygon = get_geometry_unary_union(flow_areas_path, epsg=epsg)
            if polygon is not None:
                log.info('Incorporating flow areas.')
                include_features(flow_lines_lyr, vbet_net, f"{reach_code_field} <> '46006'", polygon, excluded_fids=fids, hard_clip_shape=hard_clip_shape)

        fcount = flow_lines_lyr.ogr_layer.GetFeatureCount()

        log.info('VBET network generated with {} features'.format(fcount))


def include_features(source_layer: VectorBase, out_layer: VectorBase, attribute_filter: str = None, clip_shape: BaseGeometry = None, excluded_fids: list = None, hard_clip_shape=None):

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
            if hard_clip_shape is not None:
                geom = hard_clip_shape.Intersection(geom)
            out_feature.SetGeometry(geom.Clone())
            out_layer.ogr_layer.CreateFeature(out_feature)

    return included_fids


def create_stream_size_zones(catchment_layer: str, id_field: str, data_field: str, zones: dict, out_layer: str):
    """Add stream size zones to catchment layer

    Args:
        catchment_layer (str): source catchment layer
        id_field (str): NHDPlusID or other identifying field
        data_field (str): field to use for zone calculation
        zones (dict): dictionary of zone values
        out_layer (str): output layer
    """

    with GeopackageLayer(os.path.dirname(catchment_layer), layer_name=os.path.basename(catchment_layer)) as lyr_source, \
            GeopackageLayer(os.path.dirname(out_layer), layer_name=os.path.basename(out_layer), write=True) as lyr_destination:

        # Create Output
        lyr_destination.create_layer(ogr.wkbPolygon, spatial_ref=lyr_source.spatial_ref)
        lyr_destination.create_field(data_field, field_type=ogr.OFTReal)
        for attribute_type in zones:
            lyr_destination.create_field(f'{attribute_type}_Zone', field_type=ogr.OFTInteger)

        out_layer_defn: ogr.FeatureDefn = lyr_destination.ogr_layer.GetLayerDefn()
        lyr_destination.ogr_layer.StartTransaction()
        # Build Zones
        feat_source: ogr.Feature = None
        for feat_source, *_ in lyr_source.iterate_features("Building Transform Zones"):
            feat_dest = ogr.Feature(out_layer_defn)
            id_value = feat_source.GetField(id_field)
            feat_dest.SetFID(int(id_value))
            geom: ogr.Geometry = feat_source.GetGeometryRef()
            feat_dest.SetGeometry(geom)
            data_value = feat_source.GetField(data_field)
            feat_dest.SetField(data_field, data_value)
            if data_value is not None:
                out_zones = {}
                for zone_type, zone_values in zones.items():
                    for i, value in zone_values.items():
                        if value:
                            if data_value < value:
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
            if 'FlowlineVAA' not in line:
                continue

            curs.execute(line)

        curs.execute(f"INSERT INTO gpkg_contents (table_name, data_type) VALUES ('{os.path.basename(vaa_table)}', 'attributes');")
        conn_dest.commit()

    return os.path.basename(vaa_table)


def join_attributes(gpkg, name, geom_layer, attribute_layer, join_field, fields, epsg, geom_type='LINESTRING', join_type="INNER"):

    sql_path = 'CASE A.Divergence WHEN 2 THEN A.DnLevelPat ELSE A.LevelPathI END AS vbet_level_path'
    sql = f"CREATE VIEW {name} AS SELECT G.*, {', '.join(['A.' + item for item in fields])}, {sql_path} FROM {geom_layer} G {join_type} JOIN {attribute_layer} A ON G.{join_field} = A.{join_field};"

    with sqlite3.connect(gpkg) as conn:

        curs = conn.cursor()
        curs.execute(sql)
        conn.commit()

        curs.execute(f"INSERT INTO gpkg_contents (table_name, identifier, data_type, srs_id) VALUES ('{name}', '{name}', 'features', {epsg});")
        conn.commit()

        curs.execute(f"INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) values ('{name}', 'geom', '{geom_type}', {epsg}, 0, 0);")
        conn.commit()

    return os.path.join(gpkg, name)


def get_channel_level_path(channel_area, lines, unique_stream_field, unique_reach_field):
    """_summary_

    Args:
        channel_area (_type_): _description_
        lines (_type_): _description_
        vaa_table (_type_): _description_
    """
    log = Logger('Get Channel Level Path')
    # if the values are already there skip this process
    level_path_vals = []
    with GeopackageLayer(channel_area) as lyr_channel:
        if unique_stream_field in [lyr_channel.ogr_layer.GetNextFeature().GetFieldDefnRef(i).GetName() for i in range(lyr_channel.ogr_layer.GetLayerDefn().GetFieldCount())]:
            for ftr, *_ in lyr_channel.iterate_features("Get Level Paths"):
                if ftr.GetField(unique_stream_field) not in level_path_vals and ftr.GetField(unique_stream_field) != None:
                    level_path_vals.append(ftr.GetField(unique_stream_field))
        if len(level_path_vals) > 0:
            log.info('Level Path already exists in channel area')
            return

    # It's faster if we just pull the whole dictionary as a python dict first
    nhidid_lpath_lookup = {}
    with sqlite3.connect(os.path.dirname(lines)) as conn:
        curs = conn.cursor()
        values = curs.execute(f"SELECT {unique_stream_field}, {unique_reach_field} FROM {os.path.basename(lines)}").fetchall()
        for lpath, nhd_id in values:
            nhidid_lpath_lookup[nhd_id] = lpath

    log.debug(f'Found {len(nhidid_lpath_lookup.keys())} level path lookups in flowline network')
    if len(nhidid_lpath_lookup.keys()) == 0:
        log.error('No level paths found in flowline network')
        return

    with GeopackageLayer(channel_area) as lyr_channel, \
            GeopackageLayer(lines) as lyr_intersect:

        for feat, _counter, _progbar in lyr_channel.iterate_features("Get unreferenced paths"):
            nhd_id = feat.GetField(f'{unique_reach_field}')
            if nhd_id not in nhidid_lpath_lookup:
                geom_candidate = feat.GetGeometryRef()
                if not geom_candidate.IsValid():
                    geom_candidate = geom_candidate.MakeValid()
                lengths = {}
                for l_feat, _innercount, _innerprg in lyr_intersect.iterate_features(clip_shape=geom_candidate):
                    line_geom = l_feat.GetGeometryRef()
                    if not line_geom.IsValid():
                        line_geom = line_geom.MakeValid()
                    line_geom = line_geom.Intersection(geom_candidate)
                    line_length = line_geom.Length()
                    line_level_path = l_feat.GetField(f'{unique_stream_field}')
                    lengths[line_level_path] = lengths[line_level_path] + line_length if line_level_path in lengths else line_length
                if len(lengths) > 0:
                    nhidid_lpath_lookup[nhd_id] = max(lengths, key=lengths.get)

    log.debug(f'Found {len(nhidid_lpath_lookup.keys())} total level path lookups')
    skipped = 0
    wrote = 0
    with GeopackageLayer(channel_area, write=True) as lyr_channel:
        lyr_channel.create_field(f"{unique_stream_field}", ogr.OFTString)
        for feat, _counter, _progbar in lyr_channel.iterate_features("Writing Level Paths", write_layers=[lyr_channel]):
            nhd_id = feat.GetField(f'{unique_reach_field}')
            if nhd_id in nhidid_lpath_lookup:
                feat.SetField(f"{unique_stream_field}", nhidid_lpath_lookup[nhd_id])
                lyr_channel.ogr_layer.SetFeature(feat)
                wrote += 1
            else:
                skipped += 1
    log.debug(f'Finished writing level paths: {skipped}/{skipped + wrote} not found')


def get_levelpath_catchment(level_path_id: int, catchments_fc: Path) -> BaseGeometry:

    with sqlite3.connect(os.path.dirname(catchments_fc)) as conn:
        selected = 0
        cursor = conn.cursor()
        level_paths = tuple([level_path_id, '0'])
        out_level_paths = tuple(set([level_path_id]))
        while len(level_paths) != selected:
            selected = len(level_paths)
            down_level_paths = cursor.execute('SELECT LevelPathI from catchments_vaa where DnLevelPat in {}'.format(level_paths)).fetchall()
            level_paths = tuple([str(int(down_path[0])) for down_path in down_level_paths if down_path not in level_paths])
            out_level_paths = tuple(set(level_paths + out_level_paths))

    out_geom = ogr.Geometry(ogr.wkbMultiPolygon)
    with GeopackageLayer(catchments_fc) as lyr:
        for feat, *_ in lyr.iterate_features(attribute_filter="LevelPathI in {}".format(out_level_paths)):
            geom = feat.GetGeometryRef()
            out_geom.AddGeometry(geom)

    out_geom = out_geom.MakeValid()
    return out_geom


def get_distance_lookup(inputs_gpkg, level_paths, level_paths_drainage, vbet_run, conversion=None):

    output = {}
    with sqlite3.connect(inputs_gpkg) as conn:
        curs = conn.cursor()
        for level_path in level_paths:
            if level_path is None:
                continue

            if level_paths_drainage[level_path] < vbet_run['Zones']['Slope'][0]:
                if conversion is not None:
                    output[level_path] = conversion[0]
                else:
                    output[level_path] = 0
            elif vbet_run['Zones']['Slope'][0] <= level_paths_drainage[level_path] < vbet_run['Zones']['Slope'][1]:
                if conversion is not None:
                    output[level_path] = conversion[1]
                else:
                    output[level_path] = 1
            elif vbet_run['Zones']['Slope'][1] <= level_paths_drainage[level_path] < vbet_run['Zones']['Slope'][2]:
                if conversion is not None:
                    output[level_path] = conversion[2]
                else:
                    output[level_path] = 2
            elif vbet_run['Zones']['Slope'][3] != '' and vbet_run['Zones']['Slope'][2] <= level_paths_drainage[level_path] < vbet_run['Zones']['Slope'][3]:
                if conversion is not None:
                    output[level_path] = conversion[3]
                else:
                    output[level_path] = 3
            elif vbet_run['Zones']['Slope'][4] != '' and vbet_run['Zones']['Slope'][3] <= level_paths_drainage[level_path] < vbet_run['Zones']['Slope'][4]:
                if conversion is not None:
                    output[level_path] = conversion[4]
                else:
                    output[level_path] = 4

    return output


# def generate_channel_areas(flowline_network, flow_areas, buffer_field, catchments, out_channel_area, waterbodies=None):

#     network_path_buffered = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['VBET_NETWORK_BUFFERED'].rel_path)
#     buffer_by_field(flowline_network, network_path_buffered, "BFwidth", cfg.OUTPUT_EPSG, centered=True)

#     merge_feature_classes([filtered_waterbody, project_inputs['FLOW_AREA']], flow_polygons)

#     flow_polygons = os.path.join(intermediates_gpkg_path, LayerTypes['INTERMEDIATES'].sub_layers['FLOW_POLYGONS'].rel_path)
#     if "WATERBODY" in project_inputs:
#         log.info('Filter and merge waterbody polygons with Flow Areas')
#         filtered_waterbody = os.path.join(intermediates_gpkg_path, "waterbody_filtered")
#         wb_fcodes = [39000, 39001, 39004, 39005, 39006, 39009, 39010, 39011, 39012, 36100, 46600, 46601, 46602]
#         fcode_filter = "FCode = " + " or FCode = ".join([f"'{fcode}'" for fcode in wb_fcodes]) if len(wb_fcodes) > 0 else ""
#         copy_feature_class(project_inputs["WATERBODY"], filtered_waterbody, attribute_filter=fcode_filter)
#         merge_feature_classes([filtered_waterbody, project_inputs['FLOW_AREA']], flow_polygons)
#     else:
#         copy_feature_class(project_inputs['FLOW_AREA'], flow_polygons)

#     flow_area_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_RASTER'].rel_path)
#     rasterize(flow_polygons, flow_area_raster, project_inputs['SLOPE_RASTER'])
#     project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['FLOW_AREA_RASTER'])
#     fa_dist_raster = os.path.join(project_folder, LayerTypes['FLOW_AREA_DISTANCE'].rel_path)
#     proximity_raster(flow_area_raster, fa_dist_raster)
#     project.add_project_raster(proj_nodes["Intermediates"], LayerTypes['FLOW_AREA_DISTANCE'])

#     log.info('Writing channel raster using slope as a template')
#     channel_buffer_raster = os.path.join(project_folder, LayerTypes['CHANNEL_BUFFER_RASTER'].rel_path)
#     rasterize(network_path_buffered, channel_buffer_raster, project_inputs['SLOPE_RASTER'])
#     project.add_project_raster(proj_nodes['Intermediates'], LayerTypes['CHANNEL_BUFFER_RASTER'])

#     log.info('Generating Channel Proximity raster')
#     channel_dist_raster = os.path.join(project_folder, LayerTypes['CHANNEL_DISTANCE'].rel_path)
#     proximity_raster(channel_buffer_raster, channel_dist_raster)
#     project.add_project_raster(proj_nodes["Intermediates"], LayerTypes['CHANNEL_DISTANCE'])

#     in_rasters['Channel'] = channel_dist_raster
#     out_rasters['NORMALIZED_CHANNEL_DISTANCE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_CHANNEL_DISTANCE'].rel_path)

#     in_rasters['Flow Areas'] = fa_dist_raster
#     out_rasters['NORMALIZED_FLOWAREA_DISTANCE'] = os.path.join(project_folder, LayerTypes['NORMALIZED_FLOWAREA_DISTANCE'].rel_path)
