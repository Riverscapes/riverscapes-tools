import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET
from collections import OrderedDict

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import xyscatter, box_plot, histogram
from rme.__version__ import __version__


class RMEReport(RSReport):

    def __init__(self, database, report_path, rs_project):
        super().__init__(rs_project, report_path)
        self.log = Logger('Riverscapes Metrics Report')
        self.database = database
        self.project_root = rs_project.project_dir

        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)

        self.report_content()

    def report_content(self):
        realization = self.xml_project.XMLBuilder.find('Realizations').find('Realization')

        section_in = self.section('Inputs', 'Inputs')
        inputs = list(realization.find('Inputs'))
        [self.layerprint(lyr, section_in, self.project_root) for lyr in inputs if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']]

        section_inter = self.section('Intermediates', 'Intermediates')
        intermediates = list(realization.find('Intermediates'))
        [self.layerprint(lyr, section_inter, self.project_root) for lyr in intermediates if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']]

        section_out = self.section('Outputs', 'Outputs')
        outputs = list(realization.find('Outputs'))
        [self.layerprint(lyr, section_out, self.project_root) for lyr in outputs if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']]


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the database', type=str)
    parser.add_argument('projectxml', help='Path to the RME project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = RMEReport(args.database, args.report_path, project)
    report.write()
