from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import linemerge


def continuous_line(line):
    """Makes a discontinuous MultiLineString into a continuous LineString
    (closes small gaps)"""

    if not line.geom_type == 'MultiLineString':
        return line

    line_segs = list(line.geoms)

    start_points = []
    end_points = []
    for geom in line:
        start_points.append(geom.coords[0])
        end_points.append(geom.coords[-1])
    # st_dists = {pnt: [] for pnt in start_points}
    # for s_pnt in start_points:
    #     for e_pnt in end_points:
    #         st_dists[s_pnt].append(Point(s_pnt).distance(Point(e_pnt)))
    # start_dists = {k: min(v) for k, v in st_dists.items()}
    # pnt_start = max(start_dists, key=start_dists.get)

    end_dists = {pnt: [] for pnt in end_points}
    for e_pnt in end_points:
        for s_pnt in start_points:
            end_dists[e_pnt].append(Point(e_pnt).distance(Point(s_pnt)))
    end_dists = {k: min(v) for k, v in end_dists.items()}
