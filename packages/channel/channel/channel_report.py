"""Build a report for the channel area
"""
import argparse
# import sqlite3
# from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
# from rscommons.util import safe_makedirs, sizeof_fmt
# from rscommons.plotting import xyscatter, box_plot

from channel.__version__ import __version__


class ChannelReport(RSReport):
    """Channel report

    Args:
        RSReport ([type]): [description]
    """

    def __init__(self, report_path, rs_project: RSProject):
        super().__init__(rs_project, report_path)
        self.log = Logger('Channel Report')
        self.project_root = rs_project.project_dir

        self.layersummary("Inputs", "Inputs")
        self.layersummary("Intermediates", "Intermediates")
        self.layersummary("Outputs", "Outputs")

    def layersummary(self, xml_id: str, name: str):
        """Intro section
        """
        section = self.section('LayerSummary', 'Layer Summary: {}'.format(name))
        layers = self.xml_project.XMLBuilder.find('Realizations').find('ChannelArea').find(xml_id)

        for lyr in layers:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section, self.project_root)
            if lyr.tag in ['SQLiteDB']:
                self.layerprint(lyr, section, self.project_root)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RSContext.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = ChannelReport(args.report_path, project)
    report.write()
