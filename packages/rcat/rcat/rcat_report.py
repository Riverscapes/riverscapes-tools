import argparse

from rsxml import Logger, dotenv
from rscommons import ModelConfig, RSReport, RSProject
from rcat.__version__ import __version__


class RcatReport(RSReport):
    """ RCAT Report
    """

    def __init__(self, report_path, rs_project):
        super().__init__(rs_project, report_path)
        self.log = Logger('RCAT Report')
        self.project_root = rs_project.project_dir
        self.report_intro()

    def report_intro(self):
        """ report intro
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
        outputs = list(realization.find('Outputs'))
        for lyr in outputs:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section_out, self.project_root)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the RCAT project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = RcatReport(args.report_path, project)
    report.write()
