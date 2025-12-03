import unittest
from shapely.geometry import LineString, Point
from rscommons.shapely_ops import select_geoms_by_intersection


class Test_Confinement_Functions(unittest.TestCase):

    def setUp(self):
        self.lines = [LineString([(0, 0), (10, 10)]), LineString([(20, 20), (30, 30)])]
        self.point = Point(5, 5)

    def test_selection(self):
        output = select_geoms_by_intersection(self.lines, [self.point])
        self.assertEqual(output, [LineString([(0, 0), (10, 10)])])

    def test_inverse_selection(self):
        output_inverse = select_geoms_by_intersection(self.lines, [self.point], inverse=True)
        self.assertEqual(output_inverse, [LineString([(20, 20), (30, 30)])])


if __name__ == '__main__':
    unittest.main()
