import argparse
import os
import sqlite3
from collections import OrderedDict
from xml.etree import ElementTree as ET

from rsxml import Logger, dotenv
from rscommons import ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import histogram
from confinement.__version__ import __version__


def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class ConfinementReport(RSReport):
    """ Confinement Report
    """

    def __init__(self, database, report_path, rs_project):
        super().__init__(rs_project, report_path)
        self.log = Logger('Confinement Report')
        self.database = database
        self.project_root = rs_project.project_dir

        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)

        self.report_intro()
        self.report_content()

    def report_intro(self):
        """ report introduction
        """
        section = self.section('ReportIntro', 'Introduction')
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()

        row = curs.execute(
            'SELECT Sum(approx_leng) AS TotalLength, Count(*) AS TotalReaches FROM confinement_ratio').fetchone()
        values = {
            'Number of reaches': f'{row["TotalReaches"]:,d}',
            'Total reach length (km)': f'{row["TotalLength"] / 1000:,.0f}',
            'Total reach length (miles)': f'{row["TotalLength"] * 0.000621371:,.0f}'
        }

        self.create_table_from_dict(values, section)

    def report_content(self):
        """ report contents
        """
        realization = self.xml_project.XMLBuilder.find('Realizations').find('Realization')

        section_in = self.section('Inputs', 'Inputs')
        inputs = list(realization.find('Inputs'))
        for lyr in inputs:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section_in, self.project_root)

        section_inter = self.section('Intermediates', 'Intermediates')
        intermediates = list(realization.find('Intermediates'))
        for lyr in intermediates:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section_inter, self.project_root)

        section_out = self.section('Outputs', 'Outputs')

        # charts and tables
        self.raw_confinement(section_out)
        for field in ['confinement_ratio', 'constriction_ratio']:
            format_label = field.replace('_', ' ').title()
            self.confinement_ratio(field, format_label, section_out)

        outputs = list(realization.find('Outputs'))
        for lyr in outputs:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section_out, self.project_root)

    def confinement_ratio(self, db_field, label, parent=None):
        """ Generate a histogram of confinement ratio"""
        section = self.section(None, label, el_parent=parent, level=2)

        conn = sqlite3.connect(self.database)
        curs = conn.cursor()

        curs.execute(f'SELECT {db_field} FROM confinement_ratio')
        data = [row[0] for row in curs.fetchall()]

        image_path = os.path.join(
            self.images_dir, f'{db_field.lower()}.png')
        histogram(data, 10, image_path, 'Confinement Ratio', 'Length (km)')

        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'class': 'boxplot',
            'src': f'{os.path.basename(self.images_dir)}/{os.path.basename(image_path)}',
            'alt': f'{os.path.basename(self.images_dir)}/{os.path.basename(image_path)}'
        })
        img_wrap.append(img)
        section.append(img_wrap)

    def raw_confinement(self, parent=None):
        """ Generate a table summarizing raw confinement types"""
        section = self.section('RawConfinement', 'Raw Confinement', el_parent=parent, level=2)
        keys = OrderedDict()
        keys['Left'] = {
            'label': 'Left Confined',
            'length': 0.0, 'percent': 0.0
        }
        keys['Right'] = {
            'label': 'Right Confinement',
            'length': 0.0, 'percent': 0.0
        }
        keys['None'] = {
            'label': 'Unconfined',
            'length': 0.0,
            'percent': 0.0
        }
        keys['Both'] = {
            'label': 'Constricted - Both Left and Right Confined',
            'length': 0.0,
            'percent': 0.0
        }
        keys['Total'] = {
            'label': 'Total',
            'length': 0.0,
            'percent': 100.0
        }
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory

        curs = conn.cursor()
        curs.execute("""
        SELECT confinement_type, (TypeLength / 1000.0) TypeLength, (100.0 * TypeLength / TotalLength) Ratio, (TotalLength / 1000.0) TotalLength
        FROM
        (SELECT confinement_type, Sum(approx_leng) TypeLength FROM confinement_raw GROUP BY confinement_type)
        JOIN (SELECT Sum(approx_leng) TotalLength FROM confinement_raw)
        """)

        for row in curs.fetchall():
            item = keys[row['confinement_type']]
            item['length'] = row['TypeLength']
            item['percent'] = row['Ratio']

            keys['Total']['length'] = row['TotalLength']

        table_data = [
            (val['label'], val['length'], val['percent'])
            for val in keys.values()
        ]
        self.create_table_from_tuple_list(
            ['Type of Confinement', 'Length (km)', 'Percent'], table_data, section)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the database', type=str)
    parser.add_argument('projectxml', help='Path to the Confinement project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = ConfinementReport(args.database, args.report_path, project)
    report.write()
