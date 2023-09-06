import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET
from collections import OrderedDict

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import xyscatter, box_plot, histogram
from rme.__version__ import __version__


class ConfinementReport(RSReport):

    def __init__(self, database, report_path, rs_project):
        super().__init__(rs_project, report_path)
        self.log = Logger('Confinement Report')
        self.database = database

        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)

        self.report_intro('Confinement')
        # reach_attribute_summary(database, images_dir, inner_div)

        self.raw_confinement()
        [self.confinement_ratio(field, field) for field in [
            'Confinement_Ratio', 'Constriction_Ratio']]

    def report_intro(self, tool_name):
        section = self.section('ReportIntro', 'Introduction')
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()

        row = curs.execute(
            'SELECT Sum(ApproxLeng) AS TotalLength, Count(*) AS TotalReaches FROM Confinement_Ratio').fetchone()
        values = {
            'Number of reaches': '{0:,d}'.format(row['TotalReaches']),
            'Total reach length (km)': '{0:,.0f}'.format(row['TotalLength'] / 1000),
            'Total reach length (miles)': '{0:,.0f}'.format(row['TotalLength'] * 0.000621371)
        }

        self.create_table_from_dict(values, section)

    def confinement_ratio(self, db_field, label):
        section = self.section(None, label)

        conn = sqlite3.connect(self.database)
        curs = conn.cursor()

        curs.execute('SELECT {} FROM Confinement_Ratio'.format(db_field))
        data = [row[0] for row in curs.fetchall()]

        image_path = os.path.join(
            self.images_dir, '{}.png'.format(db_field.lower()))
        histogram(data, 10, image_path)

        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'class': 'boxplot',
            'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(image_path)),
            'alt': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(image_path))
        })
        img_wrap.append(img)
        section.append(img_wrap)

    def raw_confinement(self):
        section = self.section('RawConfinement', 'Raw Confinement')
        keys = OrderedDict()
        keys['Left'] = {'label': 'Left Confined',
                        'length': 0.0, 'percent': 0.0}
        keys['Right'] = {'label': 'Right Confinement',
                         'length': 0.0, 'percent': 0.0}
        keys['None'] = {'label': 'Unconfined', 'length': 0.0, 'percent': 0.0}
        keys['Both'] = {
            'label': 'Constricted - Both Left and Right Confined', 'length': 0.0, 'percent': 0.0}
        keys['Total'] = {'label': 'Total', 'length': 0.0, 'percent': 100.0}
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory

        curs = conn.cursor()
        curs.execute("""
        SELECT Confinement_Type, (TypeLength / 1000.0) TypeLength, (100.0 * TypeLength / TotalLength) Ratio, (TotalLength / 1000.0) TotalLength
        FROM
        (SELECT Confinement_Type, Sum(ApproxLeng) TypeLength FROM Confinement_Raw GROUP BY Confinement_Type)
        JOIN (SELECT Sum(ApproxLeng) TotalLength FROM Confinement_Raw)
        """)

        for row in curs.fetchall():
            item = keys[row['Confinement_Type']]
            item['length'] = row['TypeLength']
            item['percent'] = row['Ratio']

            keys['Total']['length'] = row['TotalLength']

        table_data = [(val['label'], val['length'], val['percent'])
                      for val in keys.values()]
        self.create_table_from_tuple_list(
            ['Type of Confinement', 'Length (km)', 'Percent'], table_data, section)


def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the database', type=str)
    parser.add_argument(
        'projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument(
        'report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig(
        'http://xml.riverscapes.net/Projects/XSD/V1/Confinement.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = ConfinementReport(args.database, args.report_path, project)
    report.write()
