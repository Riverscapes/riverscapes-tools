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
from osgeo import ogr
from shapely.geometry import MultiPolygon, MultiLineString, LineString, Point
from shapely.ops import polygonize, unary_union

from rscommons import ProgressBar, initGDALOGRErrors, Logger
from rscommons import GeopackageLayer
from rscommons.vector_ops import get_geometry_unary_union
# from vbet.vbet_network import vbet_network
# from vbet.__version__ import __version__

initGDALOGRErrors()

Path = str


def floodplain_connectivity(vbet_network: Path, vbet_polygon: Path, roads: Path, railroads: Path, out_polygon: Path, debug_gpkg: Path = None):
    """[summary]

    Args:
        vbet_network (Path): [description]
        vbet_polygon (Path): [description]
        roads (Path): [description]
        railroads (Path): [description]
        out_polygon (Path): [description]
        debug_gpkg (Path, optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """

    log = Logger('Floodplain Connectivity')
    log.info("Starting Floodplain Connectivity Script")

    # Merge Transportation Networks if not empty
    log.info("Merging Transportation Networks")
    geom_roads = get_geometry_unary_union(roads)
    geom_railroads = get_geometry_unary_union(railroads)
    geom_transportation = geom_roads.union(geom_railroads) if geom_railroads is not None else geom_roads

    # Clip Transportation Network by VBET
    log.info("Clipping Transportation Network by VBET")
    geom_vbet = get_geometry_unary_union(vbet_polygon)
    geom_transporation_clipped = geom_vbet.intersection(geom_transportation)
    if debug_gpkg:
        with GeopackageLayer(debug_gpkg, "Clipped_Transportation", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbLineString, epsg=4326)
            progbar = ProgressBar(len(geom_transporation_clipped), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geom_transporation_clipped:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)

    # Split Valley Edges at transportation intersections
    log.info("Splitting Valley Edges at transportation network intersections")
    geom_vbet_edges = MultiLineString([geom.exterior for geom in geom_vbet] + [g for geom in geom_vbet for g in geom.interiors])  # + [geom for geom in geom_transporation_clipped])
    if debug_gpkg:
        with GeopackageLayer(debug_gpkg, "Valley_Edges", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbLineString, epsg=4326)
            progbar = ProgressBar(len(geom_vbet_edges), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geom_vbet_edges:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)
    pts = geom_transportation.intersection(MultiLineString([geom.exterior for geom in geom_vbet]))
    geom_boundaries = [geom for geom in geom_vbet_edges]
    for pt in pts:
        geom_boundaries = [new_line for line in geom_boundaries for new_line in line_splitter(line, pt)]
    if debug_gpkg:
        with GeopackageLayer(debug_gpkg, "Split_Points", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbPoint, epsg=4326)
            progbar = ProgressBar(len(pts), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in pts:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)
    if debug_gpkg:
        with GeopackageLayer(debug_gpkg, "Valley_Edges_Split", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbLineString, epsg=4326)
            progbar = ProgressBar(len(geom_boundaries), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geom_boundaries:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)

    # Generate Polygons from lines
    log.info("Generating Floodplain Polygons")
    geoms_areas = [geom for geom in polygonize(geom_boundaries + [geom for geom in geom_transporation_clipped])]  # TODO some polys not getting generated
    if debug_gpkg:
        with GeopackageLayer(debug_gpkg, "Split_Polygons", write=True) as out_lyr:
            out_lyr.create_layer(ogr.wkbPolygon, epsg=4326)
            progbar = ProgressBar(len(geoms_areas), 50, f"saving {out_lyr.ogr_layer_name} features")
            counter = 0
            for shape in geoms_areas:
                progbar.update(counter)
                counter += 1
                out_lyr.create_feature(shape)

    # Select Polygons by flowline intersection
    log.info("Selecting connected floodplains")
    geom_vbet_network = get_geometry_unary_union(vbet_network)
    geoms_connected = []
    geoms_disconnected = []
    for geom in geoms_areas:
        if geom_vbet_network.intersects(geom):
            geoms_connected.append(geom)
        else:
            geoms_disconnected.append(geom)

    log.info("Union connected floodplains")
    geoms_connected_output = [geom for geom in unary_union(geoms_connected)]
    geoms_disconnected_output = [geom for geom in unary_union(geoms_disconnected)]

    # Save Outputs
    log.info("Save Floodplain Output")
    with GeopackageLayer(os.path.split(out_polygon)[0], os.path.split(out_polygon)[1], write=True) as out_lyr:
        out_lyr.create_layer(ogr.wkbPolygon, epsg=4326)
        out_lyr.create_field("Connected", ogr.OFTInteger)
        progbar = ProgressBar(len(geoms_connected_output) + len(geoms_disconnected_output), 50, f"saving {out_lyr.ogr_layer_name} features")
        counter = 0
        for shape in geoms_connected_output:
            progbar.update(counter)
            counter += 1
            out_lyr.create_feature(shape, attributes={"Connected": 1})
        for shape in geoms_disconnected_output:
            progbar.update(counter)
            counter += 1
            out_lyr.create_feature(shape, attributes={"Connected": 0})


def line_splitter(line, pt):
    if pt.buffer(0.0000001).intersects(line):
        distance = line.project(pt)
        coords = list(line.coords)
        for i, p in enumerate(coords):
            pd = line.project(Point(p))
            if pd == distance:
                return [
                    LineString(coords[:i + 1]),
                    LineString(coords[i:])]
            if pd > distance:
                cp = line.interpolate(distance)
                return [
                    LineString(coords[:i] + [(cp.x, cp.y)]),
                    LineString([(cp.x, cp.y)] + coords[i:])]
    else:
        return [line]


if __name__ == "__main__":

    floodplain_connectivity(r"D:\NAR_Data\Data\vbet\17060304\intermediates\vbet_intermediates.gpkg\vbet_network",
                            r"D:\NAR_Data\Data\vbet\17060304\outputs\vbet.gpkg\vbet_50",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\vbet_intermediates.gpkg\roads",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\vbet_intermediates.gpkg\railways",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\floodplain_connectivity.gpkg\Floodplains",
                            r"D:\NAR_Data\Data\vbet\17060304\intermediates\floodplain_connectivity.gpkg")

    # TODO Add transportation networks to vbet inputs
