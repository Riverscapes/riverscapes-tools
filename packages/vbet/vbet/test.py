import rasterio
from shapely.geometry import Point
from rscommons import GeopackageLayer, VectorBase
from rscommons.vector_ops import collect_linestring


def get_endpoints_on_raster(raster, geom_line, dist):
    """return a list of endpoints for a linestring or multilinestring

    Args:
        geom (ogr.Geometry): linestring or multilinestring geometry

    Returns:
        list: coords of points
    """
    coords = []

    line = VectorBase.ogr2shapely(geom_line[0])
    iterations = [dist, dist * 2, dist * 3]

    with rasterio.open(raster, 'r') as src:
        start_points = []
        end_points = []
        for geom in geom_line:
            line = VectorBase.ogr2shapely(geom)
            start_points.append(line.coords[0])
            end_points.append(line.coords[-1])
        st_dists = {pnt: [] for pnt in start_points}
        for s_pnt in start_points:
            for e_pnt in end_points:
                st_dists[s_pnt].append(Point(s_pnt).distance(Point(e_pnt)))
        start_dists = {k: min(v) for k, v in st_dists.items()}
        pnt_start = max(start_dists, key=start_dists.get)

        end_dists = {pnt: [] for pnt in end_points}
        for e_pnt in end_points:
            for s_pnt in start_points:
                end_dists[e_pnt].append(Point(e_pnt).distance(Point(s_pnt)))
        end_dists = {k: min(v) for k, v in end_dists.items()}
        pnt_end = max(end_dists, key=end_dists.get)

        for iteration in iterations:
            st_value = list(src.sample([(pnt_start[0], pnt_start[1])]))[0][0]
            if st_value is not None and st_value != 0.0:
                break
            pnt = line.interpolate(iteration)
            pnt_start = (pnt.x, pnt.y)

            end_value = list(src.sample([(pnt_end[0], pnt_end[1])]))[0][0]
            if end_value is not None and end_value != 0.0:
                break
            pnt = line.interpolate(-1 * iteration)
            pnt_end = (pnt.x, pnt.y)

        coords.append(pnt_start)
        coords.append(pnt_end)

        return coords


line = '/workspaces/.codespaces/shared/data/flowlines.gpkg/level_path_70000400011364'
raster = '/workspaces/.codespaces/shared/data/cost_path_70000400011364.tif'
pixel_x = 9.260265974819366342e-05

geom_flowline = collect_linestring(line)
geoms = [geom for geom in geom_flowline]

c = get_endpoints_on_raster(raster, geoms, pixel_x)
print(c)
