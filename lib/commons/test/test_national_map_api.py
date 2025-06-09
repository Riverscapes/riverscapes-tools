import unittest
from rscommons.national_map_api import TNM
from rscommons import Logger
Logger('TESTER').setup(verbose=True)


class TestNationalMapAPI(unittest.TestCase):
    def test_get_items_real_api(self):
        params = {
            "datasets": "Digital Elevation Model (DEM) 1 meter",
            "prodFormats": "GeoTIFF",
            "polygon": "-117.68862478108554 47.68168589303463,-116.9184937068746 47.68168589303463,-116.9184937068746 48.22079401684147,-117.68862478108554 48.22079401684147,-117.68862478108554 47.68168589303463",
        }
        result = TNM.get_items(params)
        self.assertIn("items", result)
        self.assertIsInstance(result["items"], list)
        self.assertEqual(result["total"], len(result["items"]))
        # Optionally print for manual inspection
        print(f"Fetched {len(result['items'])} of {result['total']} items")
