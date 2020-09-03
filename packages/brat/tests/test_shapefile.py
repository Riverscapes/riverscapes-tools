import unittest
import lib.shapefile
from osgeo import osr
# _rough_convert_metres_to_shapefile_units
# _rough_convert_metres_to_raster_units
# _rough_convert_metres_to_dataset_units


class TestShapefile(unittest.TestCase):

    def test__rough_convert_metres_to_dataset_units(self):
        in_spatial_ref = osr.SpatialReference()
        in_spatial_ref.ImportFromEPSG(int(4386))
        in_spatial_ref.SetAxisMappingStrategy(osr.OAMS_AUTHORITY_COMPLIANT)
        extent = (
            -123.14300537109374,
            -123.13510894775389,
            49.30162094735679,
            49.30604223791069
        )

        dist = lib.shapefile._rough_convert_metres_to_dataset_units(in_spatial_ref, extent, 1)
        self.assertLess(dist, 1)


if __name__ == '__main__':
    unittest.main()


# {
#   "type": "FeatureCollection",
#   "features": [
#     {
#       "type": "Feature",
#       "properties": {},
#       "geometry": {
#         "type": "Polygon",
#         "coordinates": [
#           [
#             [
#               -123.14300537109374,
#               49.30162094735679
#             ],
#             [
#               -123.13510894775389,
#               49.30162094735679
#             ],
#             [
#               -123.13510894775389,
#               49.30604223791069
#             ],
#             [
#               -123.14300537109374,
#               49.30604223791069
#             ],
#             [
#               -123.14300537109374,
#               49.30162094735679
#             ]
#           ]
#         ]
#       }
#     }
#   ]
# }
