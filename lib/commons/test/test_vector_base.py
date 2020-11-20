""" Testing for the vector base class

"""
import unittest
import os
from tempfile import mkdtemp
from rscommons import Logger, initGDALOGRErrors
from rscommons.classes.vector_base import VectorBase, VectorBaseException
from rscommons.util import safe_remove_dir


initGDALOGRErrors()
log = Logger('RSCommons TEST')
log.setup(verbose=True)

datadir = os.path.join(os.path.dirname(__file__), 'data')


class VectorBaseTest(unittest.TestCase):

    # def setUp(self):
    #     super(VectorBaseTest, self).setUp()
    #     self.outdir = mkdtemp()

    # def tearDown(self):
    #     super(VectorBaseTest, self).tearDown()
    #     safe_remove_dir(self.outdir)

    def test_path_sorter(self):

        # Exception Cases
        self.assertRaises(VectorBaseException, lambda: VectorBase.path_sorter(None, None))
        self.assertRaises(VectorBaseException, lambda: VectorBase.path_sorter(None, ''))
        self.assertRaises(VectorBaseException, lambda: VectorBase.path_sorter('', None))
        self.assertRaises(VectorBaseException, lambda: VectorBase.path_sorter('', ''))
        self.assertRaises(VectorBaseException, lambda: VectorBase.path_sorter('   ', '   '))

        # Simple cases first:
        realfile_path = os.path.join(datadir, 'sample.gpkg')
        self.assertEqual(VectorBase.path_sorter('/path/to/file.gpkg', 'layer_name'), ('/path/to/file.gpkg', 'layer_name'))

        # Real file is there
        self.assertEqual(VectorBase.path_sorter(realfile_path), (realfile_path, None))

        # Now we start to get into the detected cases:
        self.assertEqual(VectorBase.path_sorter('/path/to/file.gpkg/layer_name'), ('/path/to/file.gpkg', 'layer_name'))
        self.assertEqual(VectorBase.path_sorter('/path/to/file.gpkg/layer_name\\/LASDASDAS'), ('/path/to/file.gpkg', 'layer_name\\/LASDASDAS'))
        self.assertEqual(VectorBase.path_sorter('/path/file.gpkg/thing.shp/to/file.gpkg/layer_name\\/LASDASDAS'), ('/path/file.gpkg/thing.shp/to/file.gpkg', 'layer_name\\/LASDASDAS'))
        self.assertEqual(VectorBase.path_sorter('/path/to /file.gpkg\\layer_name'), ('/path/to /file.gpkg', 'layer_name'))
        self.assertEqual(VectorBase.path_sorter('D:\\path\\to\\file.gpkg\\layer_name'), ('D:\\path\\to\\file.gpkg', 'layer_name'))

        print('hi')


if __name__ == '__main__':
    unittest.main()
