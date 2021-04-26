from typing import List, Dict, Any
from osgeo import ogr
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import unary_union
from rscommons import Logger, ProgressBar, get_shp_or_gpkg, VectorBase
from rscommons.shapefile import get_transform_from_epsg
from rscommons.vector_ops import export_geojson

Path = str
Transform = ogr.osr.CoordinateTransformation


class RiverPoint:

    def __init__(self, pt, interior=False, side=None, island=None, properties: Dict[str, Any] = None):
        self.point = pt
        self.side = side
        self.interior = interior
        self.island = island
        self.properties = properties


def get_riverpoints(inpath, epsg, attribute_filter=None):
    """[summary]

    Args:
        inpath ([type]): Path to a ShapeFile
        epsg ([type]):  Desired output spatial reference
        attribute_filter ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: List of RiverPoint objects
    """

    log = Logger('get_riverpoints')
    points = []

    with get_shp_or_gpkg(inpath) as in_lyr:

        _out_spatial_ref, transform = get_transform_from_epsg(in_lyr.spatial_ref, epsg)

        for feat, _counter, progbar in in_lyr.iterate_features('Getting points for use in Thiessen', attribute_filter=attribute_filter):

            new_geom = feat.GetGeometryRef()

            if new_geom is None:
                progbar.erase()  # get around the progressbar
                log.warning('Feature with FID={} has no geometry. Skipping'.format(feat.GetFID()))
                continue

            new_geom.Transform(transform)
            new_shape = VectorBase.ogr2shapely(new_geom)

            if new_shape.type == 'Polygon':
                new_shape = MultiPolygon([new_shape])

            for poly in new_shape:
                # Exterior is the shell and there is only ever 1
                for pt in list(poly.exterior.coords):
                    points.append(RiverPoint(pt, interior=False))

                # Now we consider interiors. NB: Interiors are only qualifying islands in this case
                for idx, island in enumerate(poly.interiors):
                    for pt in list(island.coords):
                        points.append(RiverPoint(pt, interior=True, island=idx))

    return points


def midpoints(in_lines):

    out_points = []
    with get_shp_or_gpkg(in_lines) as in_lyr:
        for feat in in_lyr.iterate_features('Getting Midpoints'):
            geom = feat.GetGeometryRef()
            line = VectorBase.ogr2shapely(geom)
            out_points.append(RiverPoint(line.interpolate(0.5, True)))
            feat = None

    return out_points


def centerline_points(in_lines: Path, distance: float = 0.0, transform: Transform = None, fields = None) -> Dict[int, List[RiverPoint]]:
    """Generates points along each line feature at specified distances from the end as well as quarter and halfway

    Args:
        in_lines (Path): path of shapefile with features
        distance (float, optional): distance from ends to generate points. Defaults to 0.0.
        transform (Transform, optional): coordinate transformation. Defaults to None.

    Returns:
        [type]: [description]
    """
    log = Logger('centerline_points')
    with get_shp_or_gpkg(in_lines) as in_lyr:
        out_group = {}
        ogr_extent = in_lyr.ogr_layer.GetExtent()
        extent = Polygon.from_bounds(ogr_extent[0], ogr_extent[2], ogr_extent[1], ogr_extent[3])

        for feat, _counter, progbar in in_lyr.iterate_features("Centerline points"):

            line = VectorBase.ogr2shapely(feat, transform)

            fid = feat.GetFID()
            out_points = []
            # Attach the FID in case we need it later
            props = {'fid': fid}

            if fields:
                for field in fields:
                    divergence = feat.GetField('Divergence')
                    if divergence == 2:
                        value = feat.GetField('DnLevelPat')
                    else:
                        value = feat.GetField(field)
                    props[field] = str(int(value)) if value else None

            pts = [
                line.interpolate(distance),
                line.interpolate(0.5, True),
                line.interpolate(-distance)]

            total = line.length
            interval = distance/total
            current = interval
            while current < 1.0:
                pts.append(line.interpolate(interval, True))
                current = current + interval

            # pts = [
            #     line.interpolate(distance),
            #     line.interpolate(0.5, True),
            #     line.interpolate(-distance)
            # ]

            if line.project(line.interpolate(0.25, True)) > distance:
                pts.append(line.interpolate(0.25, True))
                pts.append(line.interpolate(-0.25, True))

            for pt in pts:
                # Recall that interpolation can have multiple solutions due to pythagorean theorem
                # Throw away anything that's not inside our bounds
                if not extent.contains(pt):
                    progbar.erase()
                    log.warning('Point {} is outside of extent: {}'.format(pt.coords[0], ogr_extent))
                out_points.append(RiverPoint(pt, properties=props))

            out_group[int(fid)] = out_points
            feat = None
        return out_group


def centerline_vertex_between_distance(in_lines, distance=0.0):

    out_group = []
    with get_shp_or_gpkg(in_lines) as in_lyr:
        for feat, _counter, _progbar in in_lyr.iterate_features("Centerline points between distance"):
            line = VectorBase.ogr2shapely(feat)

            out_points = []
            out_points.append(RiverPoint(line.interpolate(distance)))
            out_points.append(RiverPoint(line.interpolate(-distance)))

            max_distance = line.length - distance

            for vertex in list(line.coords):
                test_dist = line.project(Point(vertex))
                if test_dist > distance and test_dist < max_distance:
                    out_points.append(RiverPoint(Point(vertex)))
            out_group.append(out_points)
            feat = None
    return out_group


def load_geoms(in_lines):
    out = []
    with get_shp_or_gpkg(in_lines) as in_lyr:
        for feat, _counter, _progbar in in_lyr.iterate_features("Loading geometry"):
            shapely_geom = VectorBase.ogr2shapely(feat)
            out.append(shapely_geom)

    return out


def clip_polygons(clip_poly, polys):

    progbar = ProgressBar(len(polys), 50, "Clipping Polygons...")
    counter = 0
    progbar.update(counter)
    out_polys = {}
    for key, poly in polys.items():
        counter += 1
        progbar.update(counter)
        out_polys[key] = clip_poly.intersection(poly.buffer(0))

    progbar.finish()
    return out_polys


def dissolve_by_intersection(lines, polys):

    progbar = ProgressBar(len(polys), 50, "Dissolving Polygons...")
    counter = 0
    progbar.update(counter)
    dissolved_polys = []
    for line in lines:
        counter += 1
        progbar.update(counter)
        intersected = [p for p in polys if line.intersects(p)]
        dissolved_polys.append(unary_union(intersected))

    return dissolved_polys


def dissolve_by_points(groups, polys):

    progbar = ProgressBar(len(groups), 50, "Dissolving Polygons...")
    counter = 0
    progbar.update(counter)
    dissolved_polys = {}

    # Original method
    # for key, group in groups.items():
    #     counter += 1
    #     progbar.update(counter)
    #     intersected = [p for p in polys if any([p.contains(pt.point) for pt in group])]
    #     dissolved_polys[key] = unary_union(intersected)

    # This method gradulally speeds up processing by removing polygons from the list.
    for key, group in groups.items():
        counter += 1
        progbar.update(counter)
        intersected = []
        indexes = []
        for i, p in enumerate(polys):
            if any([p.contains(pt.point) for pt in group]):
                intersected.append(p)
                indexes.append(i)
        dissolved_polys[key] = unary_union(intersected)  # MultiPolygon(intersected) #intersected
        polys = [p for i, p in enumerate(polys) if i not in indexes]

    progbar.finish()
    return dissolved_polys
