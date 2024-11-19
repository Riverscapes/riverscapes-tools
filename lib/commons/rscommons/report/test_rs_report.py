
import unittest
import xml.etree.ElementTree as ET
from rscommons.report.rs_report import RSReport


class TestCreateTableFromDictOfMultipleValues(unittest.TestCase):

    def setUp(self):
        # Create a dummy RSReport instance
        self.rs_report = RSReport(rs_project=None, filepath='dummy_path')

    def test_create_table(self):
        values = {
            "Key1": "Value1",
            "Key2": ["Value2a", "Value2b"],
            "Key3": "http://example.com"
        }
        el_parent = ET.Element('div')
        self.rs_report.create_table_from_dict_of_multiple_values(values, el_parent)

        table = el_parent.find('table')
        self.assertIsNotNone(table)
        self.assertEqual(table.attrib['class'], 'dictable')

        rows = table.findall('tbody/tr')
        self.assertEqual(len(rows), 3)

        # Check first row
        self.assertEqual(rows[0].find('th').text, "Key1")
        self.assertEqual(rows[0].find('td').text, "Value1")

        # Check second row
        self.assertEqual(rows[1].find('th').text, "Key2")
        self.assertEqual(rows[1].findall('td')[0].text, "Value2a")
        self.assertEqual(rows[1].findall('td')[1].text, "Value2b")

        # Check third row
        self.assertEqual(rows[2].find('th').text, "Key3")
        self.assertEqual(rows[2].find('td/a').attrib['href'], "http://example.com")
        self.assertEqual(rows[2].find('td/a').text, "http://example.com")


if __name__ == '__main__':
    unittest.main()
