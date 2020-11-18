""" Testing for the vector ops

"""
import unittest
import os
from rscommons.vector_ops import copy_feature_class
from rscommons.classes.vector_classes import ShapefileLayer, GeopackageLayer
from rscommons.classes.logger import Logger
from rscommons.classes.gdal_errors import initGDALOGRErrors


initGDALOGRErrors()
log = Logger('RSCommons TEST')
log.setup(verbose=True)

datadir = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'data')


class VectorOpsTest(unittest.TestCase):

    def test_copy_feature_class(self):
        in_path = os.path.join(datadir, 'WBDHU12.shp')
        out_path = os.path.join(datadir, 'WBDHU12_copy.gpkg')

        with ShapefileLayer(in_path) as in_lyr, GeopackageLayer(out_path, 'WBDHU12_no_ref', write=True) as out_lyr:
            copy_feature_class(in_lyr, out_lyr, epsg=4326)

        with ShapefileLayer(in_path) as in_lyr, GeopackageLayer(out_path, 'WBDHU12_ref', write=True) as out_lyr:
            copy_feature_class(in_lyr, out_lyr)

        self.assertEqual('foo'.upper(), 'FOO')

    # def test_isupper(self):
    #     self.assertTrue('FOO'.isupper())
    #     self.assertFalse('Foo'.isupper())

    # def test_split(self):
    #     s = 'hello world'
    #     self.assertEqual(s.split(), ['hello', 'world'])
    #     # check that s.split fails when the separator is not a string
    #     with self.assertRaises(TypeError):
    #         s.split(2)


if __name__ == '__main__':
    unittest.main()
