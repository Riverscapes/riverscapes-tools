from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import linemerge


def continuous_line(line):
    """Makes a discontinuous MultiLineString into a continuous LineString
    (closes small gaps)"""

    if not line.geom_type == 'MultiLineString':
        return line

    line_segs = list(line.geoms)
    comp_dist = min([seg.length for seg in line_segs])/4

    start_points = []
    end_points = []
    for geom in line:
        start_points.append(geom.coords[0])
        end_points.append(geom.coords[-1])

    end_dists = {pnt: [] for pnt in end_points}
    for e_pnt in end_points:
        for s_pnt in start_points:
            end_dists[e_pnt].append(Point(e_pnt).distance(Point(s_pnt)))
    end_dists = {k: min(v) for k, v in end_dists.items()}

    for ept, dist in end_dists.items():
        if dist > 0 and dist < comp_dist:
            for spt in start_points:
                if Point(ept).distance(Point(spt)) == dist:
                    line_segs.append(LineString([ept, spt]))
                    break

    return linemerge(MultiLineString(line_segs))
