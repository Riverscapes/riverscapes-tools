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
        self.log = Logger('RSContext Report')
        self.project_root = project_root
        self.report_intro()

    def report_intro(self):
        section = self.section('LayerSummary', 'Layer Summary')
        layers = self.xml_project.XMLBuilder.find('Realizations').find('RSContext').getchildren()

        [self.layerprint(lyr, section, self.project_root) for lyr in layers if lyr.tag in ['DEM', 'Raster', 'Vector']]
        [self.layerprint(lyr, section, self.project_root) for lyr in layers if lyr.tag in ['SQLiteDB']]


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RSContext.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = RSContextReport(args.report_path, project, os.path.dirname(args.projectxml))
    report.write()
