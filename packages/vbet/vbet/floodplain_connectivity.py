# Name:     Floodplain Connectivity
#
# Purpose:  Identify sections of VBET polygon that are connected/disconnected
#           to floodplain.
#
# Author:   Kelly Whitehead
#
# Date:     December 08, 2020
# -------------------------------------------------------------------------------
import os
# import sys
# import uuid
# import traceback
# import datetime
# import time
# import shutil
# from osgeo import gdal
from osgeo import ogr
# from shapely.wkb import loads as wkb_load
from shapely.geometry import mapping, Polygon, MultiLineString, MultiPolygon
from shapely.ops import unary_union, polygonize, polygonize_full, split, linemerge

# from rscommons.util import safe_makedirs, parse_metadata
from rscommons import RSProject, RSLayer, ModelConfig, ProgressBar, Logger, dotenv, initGDALOGRErrors
from rscommons import GeopackageLayer, VectorBase
from rscommons.vector_ops import get_num_pts, get_num_rings, get_geometry_unary_union, remove_holes, buffer_by_field, copy_feature_class, merge_feature_classes, load_geometries
# from rscommons.vector_classes import get_shp_or_gpkg
# from vbet.vbet_network import vbet_network
# from vbet.__version__ import __version__

initGDALOGRErrors()

Path = str


def floodplain_connectivity(vbet_network: Path, vbet_polygon: Path, roads: Path, railroads: Path, out_polygon: Path, scratch_gpkg: Path = None):
    """[summary]

    Args:
        flowlines (Path): [description]
        vbet_polygon (Path): [description]
        roads (Path): [description]
        railroads (Path): [description]
        out_polygon (Path): [description]

    Returns:
        [type]: [description]
    """

    # Merge Transportation Networks if not empty
    geom_roads = get_geometry_unary_union(roads)
    geom_railroads = get_geometry_unary_union(railroads)
    geom_transportation = geom_roads.union(geom_railroads) if geom_railroads is not None else geom_roads

    # Clip Transportation Network by VBET
    geom_vbet = get_geometry_unary_union(vbet_polygon)
    geom_transporation_clipped = geom_vbet.intersection(geom_transportation)
    if scratch_gpkg:
        with GeopackageLayer(scratch_gpkg, "Clipped_Transportation", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbLineString, epsg=4326)
            progbar = ProgressBar(len(geom_transporation_clipped), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geom_transporation_clipped:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)

    # Merge VBET Boundaries and Transportation network
    geom_edges = MultiLineString([geom.exterior for geom in geom_vbet] + [g for geom in geom_vbet for g in geom.interiors] + [geom for geom in geom_transporation_clipped])
    if scratch_gpkg:
        with GeopackageLayer(scratch_gpkg, "Edges", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbLineString, epsg=4326)
            progbar = ProgressBar(len(geom_edges), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geom_edges:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)

    # recursive polygon splitting by lines
    # geom_boundaries = geom_vbet
    # progbar = ProgressBar(len(geom_transportation), 50, f"splitting vbet by transportation network | polygon count: {len(geom_boundaries)}")
    # counter = 0
    # for line in geom_transportation:
    #     progbar.update(counter)
    #     counter += 1
    #     geom_boundaries = MultiPolygon(split(geom_boundaries, line))

    #geom_boundaries = [geom for geom in polygonize(geom_edges)]
    lines = []

    for geom in geom_edges:
        coords = list(geom.coords)
        for (start, end) in zip(coords[:-1], coords[1:]):
            lines.append(LineString([start, end]))

    areas = polygonize(lines)

    geom_boundaries, dangles, cuts, invalids = polygonize_full(geom_edges)
    if scratch_gpkg:
        with GeopackageLayer(scratch_gpkg, "Split_Polygons", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbPolygon, epsg=4326)
            progbar = ProgressBar(len(geom_boundaries), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geom_boundaries:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)
    if scratch_gpkg:
        with GeopackageLayer(scratch_gpkg, "Dangles", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbLineString, epsg=4326)
            progbar = ProgressBar(len(dangles), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in dangles:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)
    if scratch_gpkg:
        with GeopackageLayer(scratch_gpkg, "Cuts", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbLineString, epsg=4326)
            progbar = ProgressBar(len(cuts), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in cuts:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)

    # Select Polygons by flowline intersection
    geom_vbet_network = get_geometry_unary_union(vbet_network)
    geoms_connected = [geom for geom in geom_boundaries if geom_vbet_network.intersects(geom)]

    # Save Outputs
    if scratch_gpkg:
        with GeopackageLayer(scratch_gpkg, "ConnectedFloodplain", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbPolygon, epsg=4326)
            progbar = ProgressBar(len(geoms_connected), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geoms_connected:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)

    return


if __name__ == "__main__":

    floodplain_connectivity(r"D:\NAR_Data\Data\vbet\17060304\intermediates\vbet_intermediates.gpkg\vbet_network",
                            r"D:\NAR_Data\Data\vbet\17060304\outputs\vbet.gpkg\vbet_50",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\vbet_intermediates.gpkg\roads",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\vbet_intermediates.gpkg\railways",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\vbet_intermediates.gpkg\Floodplain_Connectivity",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\floodplain_connectivity.gpkg")

    # TODO Add transportation networks to vbet inputs
