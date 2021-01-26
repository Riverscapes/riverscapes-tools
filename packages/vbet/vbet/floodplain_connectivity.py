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
import sys
from typing import List
import argparse
import traceback
from osgeo import ogr
from shapely.geometry import MultiPolygon, MultiLineString, LineString, Point, MultiPoint, Polygon
from shapely.ops import polygonize, unary_union

from rscommons import ProgressBar, Logger, dotenv, initGDALOGRErrors, GeopackageLayer
from rscommons.util import safe_makedirs
from rscommons.vector_ops import get_geometry_unary_union, load_geometries


Path = str

initGDALOGRErrors()


def floodplain_connectivity(vbet_network: Path, vbet_polygon: Path, roads: Path, railroads: Path, output_dir: Path, debug_gpkg: Path = None):
    """[summary]

    Args:
        vbet_network (Path): Filtered Flowline network used to generate VBET. Final selection is based on this intersection.
        vbet_polygon (Path): Vbet polygons with clipped NHD Catchments
        roads (Path): Road network
        railroads (Path): railroad network
        out_polygon (Path): Output path and layer name for floodplain polygons
        debug_gpkg (Path, optional): geopackage for saving debug layers (may substantially increase processing time). Defaults to None.
    """

    log = Logger('Floodplain Connectivity')
    log.info("Starting Floodplain Connectivity Script")

    out_polygon = os.path.join(output_dir, 'fconn.gpkg/outputs')

    # Prepare vbet and catchments
    geom_vbet = get_geometry_unary_union(vbet_polygon)
    geoms_raw_vbet = list(load_geometries(vbet_polygon, None).values())
    listgeoms = []
    for geom in geoms_raw_vbet:
        if geom.geom_type == "MultiPolygon":
            for g in geom:
                listgeoms.append(g)
        else:
            listgeoms.append(geom)
    geoms_vbet = MultiPolygon(listgeoms)

    # Clip Transportation Network by VBET
    log.info("Merging Transportation Networks")
    # merge_feature_classes([roads, railroads], geom_vbet, os.path.join(debug_gpkg, "Transportation")) TODO: error when calling this method
    geom_roads = get_geometry_unary_union(roads)
    geom_railroads = get_geometry_unary_union(railroads)
    geom_transportation = geom_roads.union(geom_railroads) if geom_railroads is not None else geom_roads
    log.info("Clipping Transportation Network by VBET")
    geom_transportation_clipped = geom_vbet.intersection(geom_transportation)
    if debug_gpkg:
        quicksave(debug_gpkg, "Clipped_Transportation", geom_transportation_clipped, ogr.wkbLineString)

    # Split Valley Edges at transportation intersections
    log.info("Splitting Valley Edges at transportation network intersections")
    geom_vbet_edges = MultiLineString([geom.exterior for geom in geoms_vbet] + [g for geom in geoms_vbet for g in geom.interiors])
    geom_vbet_interior_pts = MultiPoint([Polygon(g).representative_point() for geom in geom_vbet for g in geom.interiors])

    if debug_gpkg:
        quicksave(debug_gpkg, "Valley_Edges_Raw", geom_vbet_edges, ogr.wkbLineString)

    vbet_splitpoints = []
    vbet_splitlines = []
    counter = 0
    for geom_edge in geom_vbet_edges:
        counter += 1
        log.info('Splitting edge features {}/{}'.format(counter, len(geom_vbet_edges)))
        if geom_edge.is_valid:
            if not geom_edge.intersects(geom_transportation):
                vbet_splitlines = vbet_splitlines + [geom_edge]
                continue
            pts = geom_transportation.intersection(geom_edge)
            if pts.is_empty:
                vbet_splitlines = vbet_splitlines + [geom_edge]
                continue
            if isinstance(pts, Point):
                pts = [pts]
            geom_boundaries = [geom_edge]

            progbar = ProgressBar(len(geom_boundaries), 50, "Processing")
            counter = 0
            for pt in pts:
                # TODO: I tried to break this out but I'm not sure
                new_boundaries = []
                for line in geom_boundaries:
                    if line is not None:
                        split_line = line_splitter(line, pt)
                        progbar.total += len(split_line)
                        for new_line in split_line:
                            counter += 1
                            progbar.update(counter)
                            if new_line is not None:
                                new_boundaries.append(new_line)
                geom_boundaries = new_boundaries
                # TODO: Not sure this is having the intended effect
                # geom_boundaries = [new_line for line in geom_boundaries if line is not None for new_line in line_splitter(line, pt) if new_line is not None]
            progbar.finish()
            vbet_splitlines = vbet_splitlines + geom_boundaries
            vbet_splitpoints = vbet_splitpoints + [pt for pt in pts]

    if debug_gpkg:
        quicksave(debug_gpkg, "Split_Points", vbet_splitpoints, ogr.wkbPoint)
        quicksave(debug_gpkg, "Valley_Edges_Split", vbet_splitlines, ogr.wkbLineString)

    # Generate Polygons from lines
    log.info("Generating Floodplain Polygons")
    geom_lines = unary_union(vbet_splitlines + [geom_tc for geom_tc in geom_transportation_clipped])
    geoms_areas = [geom for geom in polygonize(geom_lines) if not any(geom.contains(pt) for pt in geom_vbet_interior_pts)]

    if debug_gpkg:
        quicksave(debug_gpkg, "Split_Polygons", geoms_areas, ogr.wkbPolygon)

    # Select Polygons by flowline intersection
    log.info("Selecting connected floodplains")
    geom_vbet_network = get_geometry_unary_union(vbet_network)
    geoms_connected = []
    geoms_disconnected = []
    progbar = ProgressBar(len(geoms_areas), 50, f"Running polygon selection")
    counter = 0
    for geom in geoms_areas:
        progbar.update(counter)
        counter += 1
        if geom_vbet_network.intersects(geom):
            geoms_connected.append(geom)
        else:
            geoms_disconnected.append(geom)

    log.info("Union connected floodplains")
    geoms_connected_output = [geom for geom in list(unary_union(geoms_connected))]
    geoms_disconnected_output = [geom for geom in list(unary_union(geoms_disconnected))]

    # Save Outputs
    log.info("Save Floodplain Output")
    with GeopackageLayer(out_polygon, write=True) as out_lyr:
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


def line_splitter(line: LineString, pt: Point) -> List[LineString]:
    """Split a shapley line at a point. Return list of LineStrings split at point

    Args:
        line (LineString): Line to split
        pt ([Point]): Point to split line

    Returns:
        List(LineString): List of linestrings
    """
    # TODO: pt. inside line bounding box might be quicker.
    # line.envelope.contains(pt)
    if pt.buffer(0.000001).intersects(line):
        distance = line.project(pt)
        coords = list(line.coords)
        if distance == 0.0 or distance == line.length:
            return [line]
        for i, p in enumerate(coords):
            lim = len(coords) - 1
            pd = line.project(Point(p))
            if pd == distance:
                if i == lim:
                    return [line]
                else:
                    lines = [
                        LineString(coords[:i + 1]),
                        LineString(coords[i:])]
                return lines
            if pd > distance:
                cp = line.interpolate(distance)
                lines = [
                    LineString(coords[:i] + [(cp.x, cp.y)]),
                    LineString([(cp.x, cp.y)] + coords[i:])]
                return lines
        return [line]
    else:
        return [line]


def quicksave(gpkg, name, geoms, geom_type):
    with GeopackageLayer(gpkg, name, write=True) as out_lyr:
        out_lyr.create_layer(geom_type, epsg=4326)
        progbar = ProgressBar(len(geoms), 50, f"saving {out_lyr.ogr_layer_name} features")
        counter = 0
        for shape in geoms:
            progbar.update(counter)
            counter += 1
            out_lyr.create_feature(shape)


def main():
    # TODO Add transportation networks to vbet inputs
    # TODO Prepare clipped NHD Catchments as vbet polygons input

    parser = argparse.ArgumentParser(
        description='Floodplain Connectivity (BETA)',
        # epilog="This is an epilog"
    )
    parser.add_argument('vbet_network', help='Vector line network', type=str)
    parser.add_argument('vbet_polygon', help='Vector polygon layer', type=str)
    parser.add_argument('roads', help='Vector line network', type=str)
    parser.add_argument('railroads', help='Vector line network', type=str)
    parser.add_argument('output_dir', help='Folder where output project will be created', type=str)
    parser.add_argument('--debug_gpkg', help='Debug geopackage', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # make sure the output folder exists
    safe_makedirs(args.output_dir)

    # Initiate the log file
    log = Logger('FLOOD_CONN')
    log.setup(logPath=os.path.join(args.output_dir, 'floodplain_connectivity.log'), verbose=args.verbose)
    log.title('Floodplain Connectivity (BETA)')

    try:
        floodplain_connectivity(args.vbet_network, args.vbet_polygon, args.roads, args.railroads, args.output_dir, args.debug_gpkg)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
