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

        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the database', type=str)
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/RME.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = RMEReport(args.database, args.report_path, project)
    report.write()
