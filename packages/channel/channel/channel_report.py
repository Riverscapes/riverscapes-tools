"""Build a report for the channel area
"""

import argparse
import sqlite3
from xml.etree import ElementTree as ET

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

        self.out_section = self.section('Outputs', 'Outputs')
        p1 = ET.Element('p')
        p1.text = 'The output of the Channel Area Tool is a polygon feature class representing the bankfull width of a channel network. See documentation for the tool '
        aEl1 = ET.SubElement(p1, 'a', {'href': 'https://tools.riverscapes.net/channel/'})
        aEl1.text = 'here.'
        self.out_section.append(p1)
        self.outputs_content()

        in_section = self.section('Inputs', 'Inputs')
        p1in = ET.Element('p')
        p2in = ET.Element('p')
        p1in.text = 'The inputs to the Channel Area tool are a drainage network (polyline) layer, and polygons representing river channels.'
        p2in.text = 'The default inputs for the tool are the NHD Flowline, NHD Area, and NHD Waterbody feature classes.'

        self.layersummary("Inputs", "Inputs")
        self.layersummary("Intermediates", "Intermediates")
        self.layersummary("Outputs", "Outputs")

    def layersummary(self, xml_id: str, name: str):
        """Intro section
        """
        section = self.section('LayerSummary', 'Layer Summary: {}'.format(name))
        layers = self.xml_project.XMLBuilder.find('Realizations').find('Realization').find(xml_id)

        for lyr in layers:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section, self.project_root)
            if lyr.tag in ['SQLiteDB']:
                self.layerprint(lyr, section, self.project_root)

    def outputs_content(self):

        self.section('AreaBreakdown', 'Data Source Breakdown', el_parent=self.out_section, level=2)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/RSContext.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = ChannelReport(args.report_path, project)
    report.write()
