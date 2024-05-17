import argparse

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from anthro.__version__ import __version__


class AnthroReport(RSReport):

    def __init__(self, report_path, rs_project):
        super().__init__(rs_project, report_path)
        self.log = Logger('Anthro Report')
        self.project_root = rs_project.project_dir
        self.report_intro()

    def fix_typo(self, el):
        """Recursively search for and fix specific typo one element's metadata."""

        layers = el.find("Layers")
        meta = self.xml_project.get_metadata_dict(node=el)

        if meta is not None:
            if (
                meta.get("DocsUrl") == "https://tools.riverscapes.net/anthro/data/#ANTRHO_POINTS"
                or meta.get("DocsUrl") == "https://tools.riverscapes.net/anthro/data.html#ANTRHO_POINTS"
            ):
                self.xml_project.add_metadata_simple(
                    {"DocsUrl": "https://tools.riverscapes.net/anthro/data/#ANTHRO_POINTS"},
                    node=el
                )
                return True

        if layers is not None:
            for layer_el in list(layers):
                if self.fix_typo(layer_el):
                    return True

        return False

    def report_intro(self):
        realization = self.xml_project.XMLBuilder.find('Realizations').find('Realization')

        section_in = self.section('Inputs', 'Inputs')
        inputs = list(realization.find('Inputs'))
        for lyr in inputs:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section_in, self.project_root, tool_name="rscontext")

        section_inter = self.section('Intermediates', 'Intermediates')
        intermediates = list(realization.find('Intermediates'))
        for lyr in intermediates:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section_inter, self.project_root, tool_name="anthro")

        section_out = self.section('Outputs', 'Outputs')
        outputs = list(realization.find('Outputs'))
        for lyr in outputs:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.fix_typo(lyr)
                self.layerprint(lyr, section_out, self.project_root, tool_name="anthro")


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the TauDEM project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = AnthroReport(args.report_path, project)
    report.write()
