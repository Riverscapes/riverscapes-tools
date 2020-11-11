import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs, sizeof_fmt
from rscommons.plotting import xyscatter, box_plot

from rscontext.__version__ import __version__


class RSContextReport(RSReport):

    def __init__(self, report_path, rs_project, project_root):
        super().__init__(rs_project, report_path)
        self.log = Logger('RSContext')
        self.project_root = project_root
        self.report_intro()

    def report_intro(self):
        section = self.section('LayerSummary', 'Layer Summary')
        layers = self.xml_project.XMLBuilder.find('Realizations').find('RSContext').getchildren()

        [self.layerprint(lyr, section) for lyr in layers if lyr.tag in ['DEM', 'Raster', 'Vector']]
        [self.layerprint(lyr, section) for lyr in layers if lyr.tag in ['SQLiteDB']]

    def layerprint(self, lyr_el, section):
        tag = lyr_el.tag
        name = lyr_el.find('Name').text

        section = self.section(None, '{}: {}'.format(tag, name), level=2, attrib={'class': 'rsc-layer'})

        meta = self.xml_project.get_metadata_dict(node=lyr_el)
        if meta is not None:
            self.create_table_from_dict(meta, section, attrib={'class': 'fullwidth'})

        path_el = ET.Element('pre', attrib={'class': 'path'})
        pathstr = lyr_el.find('Path').text
        size = 0
        fpath = os.path.join(self.project_root, pathstr)
        if os.path.isfile(fpath):
            size = os.path.getsize(fpath)

        footer = ET.Element('div', attrib={'class': 'layer-footer'})
        path_el.text = 'Project path: {}  ({})'.format(pathstr, sizeof_fmt(size))
        footer.append(path_el)
        section.append(footer)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the BRAT project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RSContext.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = RSContextReport(args.report_path, project, os.path.dirname(args.projectxml))
    report.write()
