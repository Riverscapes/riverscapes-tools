from shapely.ops import split, unary_union, linemerge
from shapely.geometry import MultiLineString
from rscommons import get_shp_or_gpkg
from rscommons.vector_ops import get_geometry_unary_union, VectorBase
import time

vb_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/valley_bottom.shp'
flowlines_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/flowlines.shp'
road_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/roads.shp'
rail_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/rails.shp'
canal_path = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/canals.shp'
paths = {
    road_path: None, rail_path: None, canal_path: None}

network = get_geometry_unary_union(flowlines_path, attribute_filter='FCode = 46006 OR FCode = 46003 OR FCode = 55800 OR FCode = 46007')

disconnected = []

vb_union = get_geometry_unary_union(vb_path)
# road_union = get_geometry_unary_union(road_path)
# rail_union = get_geometry_unary_union(rail_path)
# canal_union = get_geometry_unary_union(canal_path)
# canal_u = linemerge(canal_union)

# for path, label in paths.items():
#    with get_shp_or_gpkg(path) as lyr:
#         ftrs = [lyr.ogr_layer.GetFeature(i) for i, _ in enumerate(lyr.ogr_layer)]
#         sh_ftrs = [VectorBase.ogr2shapely(ftr) for ftr in ftrs]
#         final_ftrs = [f for f in sh_ftrs if f.is_valid and f.type == 'LineString']
#         paths[path] = linemerge(final_ftrs)

infr_geoms = []
for path, lines in paths.items():
    with get_shp_or_gpkg(path) as lyr:
        ftrs = [lyr.ogr_layer.GetFeature(i) for i, _ in enumerate(lyr.ogr_layer)]
        sh_ftrs = [VectorBase.ogr2shapely(ftr) for ftr in ftrs]
        final_ftrs = [f for f in sh_ftrs if f.is_valid and f.type == 'LineString']
        for f in final_ftrs:
            infr_geoms.append(f)

infra = MultiLineString(infr_geoms)
print(f'number of infrastructure features: {len(infra.geoms)}')

start = time.time()
polys = []
for geom in vb_union.geoms:
    polys.append(split(geom, infra))
end = time.time()

print(f'split vb in {end-start}')
