import os
import math
import numpy as np

import ogr
from shapely.geometry import *
from shapely.ops import linemerge, split
from shapely.wkb import loads

from rscommons import GeopackageLayer, ProgressBar
from rscommons.thiessen.geosmoothing import GeoSmoothing
from rscommons.thiessen.shapes import RiverPoint
from rscommons.thiessen.vor import NARVoronoi
# from rscommons.centerline import build_centerline

from vbet.vbet_network import join_attributes


def vbet_centerline(flowlines, vaa_table, vbet_polygons, out_gpkg):

    fields = ['HydroSeq', 'DnHydroSeq', 'UpHydroSeq']
    flowlines_vaa = os.path.join(os.path.dirname(flowlines), "VAA_Centerlines")
    #    flowline_vaa = join_attributes(os.path.dirname(flowlines), "VAA_Centerlines", os.path.basename(flowlines), os.path.basename(vaa_table), 'NHDPlusID', fields, 4326)

    reaches = {}

    out_layer = os.path.join(out_gpkg, "Centerlines_Smooth_04_1m")

    with GeopackageLayer(flowlines_vaa) as lyr,\
            GeopackageLayer(vbet_polygons) as lyr_polygons,\
            GeopackageLayer(out_layer, write=True) as lyr_output:

        srs = lyr.ogr_layer.GetSpatialRef()
        lyr_output.create_layer(ogr.wkbLineString, spatial_ref=srs)
        lyr_output.create_field("HydroSeq", field_type=ogr.OFTReal)
        lyr_output_defn = lyr_output.ogr_layer.GetLayerDefn()

        degree_factor = lyr.rough_convert_metres_to_vector_units(1)

        for feat, *_ in lyr.iterate_features():
            reach = {}
            reach['up'] = feat.GetField('UpHydroSeq')
            reach['down'] = feat.GetField('DnHydroSeq')
            geom = feat.GetGeometryRef()
            reach['geom'] = geom.Clone()
            reaches[feat.GetField('HydroSeq')] = reach

        headwaters = {k: v for k, v in reaches.items() if v['up'] == 0 or v['up'] not in reaches}
        processed = []  # TODO don't add reaches if they are processed
        unioned_reaches = {}

        for HydroSeq in headwaters:
            unioned_geom = reaches[HydroSeq]['geom']
            HydroSeq_next = reaches[HydroSeq]['down']

            lines = ogr.Geometry(ogr.wkbMultiLineString)

            while HydroSeq_next != 0 and HydroSeq_next in reaches:
                union = unioned_geom.Union(reaches[HydroSeq_next]['geom'])
                unioned_geom = union
                lines.AddGeometry(reaches[HydroSeq_next]['geom'])
                if HydroSeq_next in processed:
                    break
                processed.append(HydroSeq_next)
                HydroSeq_next = reaches[HydroSeq_next]['down']

            unioned_reaches[HydroSeq] = unioned_geom

        progbar = ProgressBar(len(unioned_reaches), 50, f"Processing reaches...")
        counter = 0
        progbar.update(counter)

        merged_centerline = None

        for HydroSeq, line in unioned_reaches.items():

            counter += 1
            progbar.update(counter)

            polys = ogr.Geometry(ogr.wkbMultiPolygon)

            for poly_feat, *_ in lyr_polygons.iterate_features():
                poly = poly_feat.GetGeometryRef()

                if poly.Intersects(line):
                    polys.AddGeometry(poly)

            poly_union = polys.UnionCascaded()

            centerlines, merged_centerline = build_centerline(line, poly_union, 20, dist_factor=degree_factor, existing_centerlines=merged_centerline, up_reach=reaches[HydroSeq]['geom'])

            for centerline in centerlines:
                feat_out = ogr.Feature(lyr_output_defn)
                feat_out.SetGeometry(centerline)
                feat_out.SetField('HydroSeq', HydroSeq)
                lyr_output.ogr_layer.CreateFeature(feat_out)
                feat_out = None

            if counter == 400:
                break
    return


def build_centerline(thalweg, bounding_polygon, spacing=None, dist_factor=1, existing_centerlines=None, up_reach=None):

    # Remove Z values
    thalweg.FlattenTo2D()
    bounding_polygon.FlattenTo2D()

    # load the geoms
    g_thalweg_load = loads(thalweg.ExportToWkb())
    if g_thalweg_load.geometryType() == 'LineString':
        g_thalweg_init = g_thalweg_load
    else:
        g_thalweg_init = linemerge(g_thalweg_load)
    g_polygon = loads(bounding_polygon.ExportToWkb())

    processing_extent = g_thalweg_init.buffer(200 * dist_factor)

    # islands?
    rivershape = g_polygon

    # make sure all thalweg coords are within river polygon
    coords = []
    for coord in g_thalweg_init.coords:
        if Point(coord).within(g_polygon):
            coords.append(coord)
    g_thalweg = LineString(coords)

    # Prepare geoms
    if spacing:
        rivershape_smooth = densifyShape(rivershape, spacing * dist_factor)
    else:
        rivershape_smooth = rivershape

    thalwegStart = LineString([g_thalweg.coords[1], g_thalweg.coords[0]])
    thalwegEnd = LineString([g_thalweg.coords[-2], g_thalweg.coords[-1]])

    rivershape_bounds = GetBufferedBounds(rivershape, 5 * dist_factor)

    thalwegStartExt = projToShape(thalwegStart, rivershape_bounds)
    thalwegEndExt = projToShape(thalwegEnd, rivershape_bounds)

    thalweglist = list(g_thalweg.coords)
    thalweglist.insert(0, thalwegStartExt.coords[1])
    thalweglist.append(thalwegEndExt.coords[1])

    newThalweg = LineString(thalweglist)

    bankshapes = splitClockwise(rivershape_bounds, newThalweg)

    points = []

    # Exterior is the shell and there is only ever 1
    for pt in list(rivershape_smooth.exterior.coords):
        g_pt = Point(pt)
        side = 1 if bankshapes[0].contains(g_pt) else -1
        if processing_extent.contains(g_pt):
            points.append(RiverPoint(g_pt, interior=False, side=side))

    for idx, island in enumerate(rivershape_smooth.interiors):
        for pt in list(island.coords):
            g_pt = Point(pt)
            side = 1 if bankshapes[0].contains(g_pt) else -1
            if processing_extent.contains(g_pt):
                points.append(RiverPoint(g_pt, interior=True, side=side, island=idx))

    # log.info("Calculating Voronoi Polygons...")
    myVorL = NARVoronoi(points)

    myVorL.calculate_neighbours()

    centerline_raw = myVorL.collectCenterLines(Polygon(rivershape.exterior))

    smoother = GeoSmoothing(spl_smpar=1 * dist_factor)
    #centerline = smoother.smooth(centerline_raw)
    #geom_centerlines = [ogr.CreateGeometryFromWkb(centerline.to_wkb())]

    #centerlines = chopCenterlineEnds(centerline_raw, rivershape)
    #geom_centerlines = [ogr.CreateGeometryFromWkb(centerlines.to_wkb())]

    centerline_segments = split(centerline_raw, g_polygon)
    centerlines_long = [LineString(segment.coords[1:-1] if len(segment.coords) > 3 else segment.coords) for segment in centerline_segments if segment.interpolate(segment.length / 2).within(rivershape)]
    #centerlines = [segment for segment in centerlines_long if len(segment.coords) > 2]

    rough_centerlines = []
    merged_centerlines = []
    if existing_centerlines:
        g_existing_centerlines = loads(existing_centerlines.ExportToWkb())
        l_existing_centerlines = [g_existing_centerlines] if g_existing_centerlines.type == "LineString" else [g for g in g_existing_centerlines]

        up_reach.FlattenTo2D()
        g_up_reach = loads(up_reach.ExportToWkb())

        for line in centerlines_long:
            new_segment = line.difference(g_existing_centerlines)
            if new_segment.type == 'LineString':
                if new_segment.intersects(g_up_reach):
                    rough_centerlines.append(new_segment)
            else:
                for s in new_segment:
                    if s.intersects(g_up_reach):
                        rough_centerlines.append(s)

        centerlines = [reconnectLine(g_existing_centerlines, smoother.smooth(rough_centerline)) for rough_centerline in rough_centerlines]
        merged_centerlines = linemerge(centerlines + l_existing_centerlines)
    else:
        centerlines = [smoother.smooth(cl) for cl in centerlines_long]
        merged_centerlines = linemerge(centerlines)

    geom_centerlines = [ogr.CreateGeometryFromWkb(centerline.to_wkb()) for centerline in centerlines]
    geom_merged_centerlines = ogr.CreateGeometryFromWkb(merged_centerlines.to_wkb())

    return geom_centerlines, geom_merged_centerlines

# Shapes


def densifyShape(shape, spacing):
    """
    Densifies a shape (including
    :param shape:
    :param spacing: the spacing between points
    :return:
    """
    ext = _densifyRing(shape.exterior, spacing)

    isls = []
    for isl in shape.interiors:
        isls.append(_densifyRing(isl, spacing))

    if len(isls) == 0:
        return ext
    else:
        return ext.difference(MultiPolygon(isls))


def _densifyRing(ring, spacing):
    """
    Densify this particular ring using _densify segment on each segment
    :param ring:
    :param spacing:
    :return:
    """
    # Densify all the line segments. This return a list with the first point and any linear interpolated points
    densesegments = [_densifySegment(LineString([ring.coords[x - 1], ring.coords[x]]), spacing) for x in range(1, len(ring.coords))]
    # Finally add the very last point to complete the ring
    densesegments.append([ring.coords[-1]])
    poly = Polygon(LinearRing([pt for seg in densesegments for pt in seg]))
    return poly


def _densifySegment(segment, spacing):
    """
    A segment is defined as a LineString with two points in it. Spacing is the point spacing we want.
    :param segment:
    :param spacing:
    :return:
    """
    if segment.length < spacing:
        return [segment.coords[0]]

    pts = []
    for currDist in np.arange(0, segment.length, spacing):
        newpt = segment.interpolate(currDist).coords[0]
        if newpt != segment.coords[-1]:
            pts.append(newpt)
    return pts


def GetBufferedBounds(shape, buffer):
    """[summary]

    Args:
        shape ([type]): [description]
        buffer ([type]): [description]

    Returns:
        [type]: [description]
    """

    newExtent = (shape.bounds[0] - buffer, shape.bounds[1] - buffer, shape.bounds[2] + buffer, shape.bounds[3] + buffer)

    return Polygon([
        (newExtent[0], newExtent[1]),
        (newExtent[2], newExtent[1]),
        (newExtent[2], newExtent[3]),
        (newExtent[0], newExtent[3]),
        (newExtent[0], newExtent[1])
    ])


def getDiag(rect):

    return math.sqrt(math.pow((rect.bounds[3] - rect.bounds[1]), 2) + math.pow((rect.bounds[2] - rect.bounds[0]), 2))


def projToShape(line, poly):

    diag = getDiag(poly)
    longLine = getExtrapoledLine(line, diag)
    return poly.intersection(longLine)


def getExtrapoledLine(line, length):

    p1 = line.coords[0]
    p2 = line.coords[1]

    rise = (p2[1] - p1[1])
    run = (p2[0] - p1[0])

    theta = math.atan2(rise, run)

    newX = p2[0] + length * math.cos(theta)
    newY = p2[1] + length * math.sin(theta)

    return LineString([p2, (newX, newY)])


def splitClockwise(rect, thalweg):
    """
    Work clockwise around a rectangle and create two shapes that represent left and right bank
    We do this by adding 4 corners of the rectangle and 2 endpoints of thalweg to a list and then
    sorting it clockwise using the rectangle centroid.
    Then we traverse the clockwise list and switch between shape1 and shape2 when we hit thalweg start/end points
    finally we inject the entire thalweg line into both shape1 and shape2 between where the start and end points
    of the thalweg intersect the rectangle and instantiate the whole mess as two polygons inside a multipolygon
    which we then return
    :param rect:
    :param thalweg: a thalweg with start and end points that intersects the rectangle
    :return:
    """

    # TODO: This might break if the thalweg is reversed or if the thalweg us weird. Lots of testing necessary
    # The thalweg has two points we care about: the first and last points that should intersect the rectangle
    thalwegStart = thalweg.coords[0]
    thalwegEnd = thalweg.coords[-1]

    coordsorter = list(rect.exterior.coords)
    coordsorter.append(thalwegStart)
    coordsorter.append(thalwegEnd)

    # Sort the points clockwise using the centroid as a center point
    def algo(pt):
        return math.atan2(pt[0] - rect.centroid.coords[0][0], pt[1] - rect.centroid.coords[0][1])
    coordsorter.sort(key=algo)

    # Create shape1 and shape2 which will fill up with points shape#idx is the place where the thalweg
    # Should be injected
    shape1 = []
    shape2 = []
    shape1idx = 0
    shape2idx = 0

    # Our boolean switchers
    firstshape = True
    foundfirst = False
    reverseThalweg = False

    # Calculate shape 1 and shape 2 by traversal
    for idx, pt in enumerate(coordsorter):

        # If we hit the thalweg start note it using the idx vars and floop the firstshape.
        if pt == thalwegStart:
            shape1idx = len(shape1)
            shape2idx = len(shape2)
            firstshape = not firstshape
            foundfirst = True

        # At the endpoint we just floop the firstshape.
        elif pt == thalwegEnd:
            firstshape = not firstshape
            # We found the tail before we found the head. Make a note that it's ass-backwards
            if not foundfirst:
                reverseThalweg = True

        # If this is a rectangle corner we add it to the appropriate shape
        elif firstshape:
            shape1.append(pt)
        elif not firstshape:
            shape2.append(pt)

    # Now inject the entire thalweg into the appropriate area (reversed if necessary)
    if reverseThalweg:
        shape1[shape1idx:shape1idx] = reversed(list(thalweg.coords))
        shape2[shape2idx:shape2idx] = list(thalweg.coords)
    else:
        shape1[shape1idx:shape1idx] = list(thalweg.coords)
        shape2[shape2idx:shape2idx] = reversed(list(thalweg.coords))

    return MultiPolygon([Polygon(shape1), Polygon(shape2)])


def chopCenterlineEnds(line, shape):

    #log = Logger("chopCenterlineEnds")

    # Trim to be inside the river shape.
    centerlineChopped = line
    centerlineIntersection = line.intersection(shape)
    # It it's a multiline string that means it crosses over the channel at some point
    if centerlineIntersection.type == "MultiLineString":
        #log.error("Centerline Crosses Channel Boundary. Continuing...")

        # We need to pick out the start and end points so collect them from all
        # the intersections and then sort them by distance from the start of
        # the original line
        endpts = []
        for segline in centerlineIntersection:
            endpts.append(Point(segline.coords[0]))
            endpts.append(Point(segline.coords[-1]))
        endpts.sort(key=lambda pt: line.project(pt))

        # Get the start and endpoints where the line crosses the rivershape
        startdist = line.project(endpts[0])
        enddist = line.project(endpts[-1])

        pts = []
        l = [line] if line.type == 'LineString' else line
        for geom in l:
            for pt in geom.coords:
                projectpt = geom.project(Point(pt))
                if startdist < projectpt < enddist:
                    pts.append(pt)

        # Add the first point in if it isn't already there
        firstpoint = line.interpolate(startdist).coords[0]
        if pts[0] != firstpoint:
            pts.insert(0, firstpoint)

        # Add the last point in if it isn't already there
        lastpoint = line.interpolate(enddist).coords[0]
        if pts[-1] != lastpoint:
            pts.append(lastpoint)

        centerlineChopped = LineString(pts)
    else:
        # If it's just a linestring then we can safely use it
        centerlineChopped = centerlineIntersection

    return centerlineChopped


def reconnectLine(baseline, separateLine):
    """
    Smoothing can separate the centerline from its alternates. This function
    reconnects them using nearest point projection. Do this before smoothing the
    alternate line
    :param baseline: The main line that does not change
    :param separateLine: The line we want to reconnect
    :return:
    """
    # First find the start and end point
    sepLineStart = Point(separateLine.coords[0])
    sepLineEnd = Point(separateLine.coords[-1])

    # Now find their nearest points on the centerline
    newStart = baseline.interpolate(baseline.project(sepLineStart))
    newEnd = baseline.interpolate(baseline.project(sepLineEnd))

    line = list(separateLine.coords)
    line.insert(0, tuple(newStart.coords[0]))
    line.append(tuple(newEnd.coords[0]))

    return LineString(line)


if __name__ == "__main__":

    vbet_centerline(r"D:\NAR_Data\Data\vbet\17060304\centerlines\vbet_inputs.gpkg\flowlines",
                    r"D:\NAR_Data\Data\vbet\17060304\centerlines\vbet_inputs.gpkg\NHDPlusFlowlineVAA",
                    r"D:\NAR_Data\Data\vbet\17060304\outputs\vbet.gpkg\vbet_50",
                    r"D:\NAR_Data\Data\vbet\17060304\centerlines\output.gpkg")
