import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET
import json

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.report.get_colors import get_colors
from rscommons.util import safe_makedirs
from rscommons.plotting import pie, horizontal_bar

from rscontext.__version__ import __version__


class RSContextReport(RSReport):

    def __init__(self, report_path, rs_project, project_root):
        super().__init__(rs_project, report_path)
        self.log = Logger('RSContext Report')
        self.project_root = project_root
        # dictionary to store some metrics as json
        self.metrics = {}

        self.colors = get_colors('RSCONTEXT')
        # there will be instances where legend name doesn't include field values used for symbology
        self.labels = {
            'roads': {
                'Expressway': 1,
                'Secondary Hwy': 2,
                'Local Connector': 3,
                'Local Road': 4,
                'Ramp': 5,
                '4WD': 6,
                'Ferry': 7,
                'Tunnel': 8
            }
        }

        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)

        self.flowlines()
        self.layer_summary()

        self.wats_area()

        self.serialize_metrics(os.path.join(os.path.dirname(report_path), 'metrics.json'))

    def layer_summary(self):
        section = self.section('LayerSummary', 'Layer Summary')
        layers = self.xml_project.XMLBuilder.find('Realizations').find('Realization').find('Datasets')

        for lyr in layers:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section, self.project_root)
            if lyr.tag in ['SQLiteDB']:
                self.layerprint(lyr, section, self.project_root)

    def flowlines(self):
        section = self.section('Flowlines', 'Flowlines')
        database = os.path.join(os.path.dirname(self.filepath), 'hydrology/nhdplushr.gpkg')
        conn = sqlite3.connect(database)
        curs = conn.cursor()

        curs.execute("""SELECT FCode, SUM(LengthKM) FROM NHDFlowline GROUP BY FCode""")
        lengths = {row[0]: row[1] for row in curs.fetchall()}
        self.metrics['flowlineLengths'] = lengths
        to_remove = []
        for key, val in lengths.items():
            if str(key)[:3] == '428' and key != 42800:
                lengths[42800] += val
                to_remove.append(key)
        for key in to_remove:
            del lengths[key]
        
        # pie_path = os.path.join(self.images_dir, 'flowlines_pie.png')
        col_keys = []
        for key in lengths.keys():
            if key == 42800:
                col_keys.append('Pipe')
            for k in self.colors.keys():
                if str(key) in k:
                    col_keys.append(k)
        col = [self.colors[key] for key in col_keys]
        
        # pie(lengths.values(), col_keys, 'Flowlines', col, pie_path)

        # plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        # img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        # img = ET.Element('img', attrib={
        #     'src': f'{os.path.basename(self.images_dir)}/{os.path.basename(pie_path)}',
        #     'alt': 'pie_chart'
        # })
        # img_wrap.append(img)
        # plot_wrapper.append(img_wrap)
        # section.append(plot_wrapper)

        bar_path = os.path.join(self.images_dir, 'flowlines_bar.png')
        horizontal_bar(lengths.values(), col_keys, col, 'Length (km)','Flowlines', bar_path, 'Length (mi)')
        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(bar_path)),
            'alt': 'bar_chart'
        })
        img_wrap.append(img)
        plot_wrapper.append(img_wrap)
        section.append(plot_wrapper)

    def wats_area(self):
        database = os.path.join(os.path.dirname(self.filepath), 'hydrology/nhdplushr.gpkg')
        conn = sqlite3.connect(database)
        curs = conn.cursor()

        curs.execute("""SELECT SUM(AreaSqKm) FROM WBDHU10""")
        area = curs.fetchone()[0]
        self.metrics['watershedArea'] = area

    def serialize_metrics(self, filepath):
        with open(filepath, 'w') as f:
            f.write(json.dumps(self.metrics, indent=4))
        

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = RSContextReport(args.report_path, project, os.path.dirname(args.projectxml))
    report.write()
