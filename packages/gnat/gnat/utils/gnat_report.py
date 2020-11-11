import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET
from collections import OrderedDict

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import xyscatter, box_plot, histogram
from gnat.__version__ import __version__


class GNATReport(RSReport):

    def __init__(self, database, report_path, rs_project):
        super().__init__(rs_project, report_path)
        self.log = Logger('GNAT Report')
        self.database = database

        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the database', type=str)
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/GNAT.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = GNATReport(args.database, args.report_path, project)
    report.write()
