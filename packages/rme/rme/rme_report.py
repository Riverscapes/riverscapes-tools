import argparse
import sqlite3
import os
from collections import Counter
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import box_plot, vertical_bar
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

    def metrics_section(self, parent_section):
        section = self.section("Metrics", "Metrics", parent_section, level=2)

        self.metrics_table(section)
        self.metrics_plots(section)

    def metrics_table(self, section):
        RSReport.header(4, "Available Metrics", section)

        RSReport.create_table_from_sql(
            ["Metric Name", "Description", "Method for Obtaining"],
            """
            SELECT name, description, method
            FROM metrics
            """,
            self.database,
            section
        )

    def metrics_plots(self, parent_section):
        section = self.section("metric-plots", "Metric Plots", parent_section, level=3)
        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})

        conn = sqlite3.connect(self.database)
        curs = conn.cursor()

        curs.execute("""
            SELECT LOWER(field_name), data_type
            FROM metrics
            WHERE field_name != ''
        """)
        metrics_info = curs.fetchall()

        for metric_name, data_type in metrics_info:
            try:
                curs.execute(f"""
                    SELECT {metric_name}
                    FROM vw_igo_metrics
                """)
                values = [row[0] for row in curs.fetchall()]
            except sqlite3.OperationalError as e:
                self.log.error(f"Error fetching data for metric {metric_name} ({e})")
                continue

            if data_type == "TEXT" or data_type == "INTEGER":
                image_path = os.path.join(self.images_dir, f"{metric_name}_bar.png")
                value_counts = Counter(values)
                vertical_bar(
                    value_counts.values(),
                    [str(i) for i in value_counts.keys()],
                    "count",
                    f"{metric_name.title()} Distribution",
                    image_path
                )

            elif data_type == "REAL":
                # make box plot
                image_path = os.path.join(self.images_dir, f"{metric_name}_box.png")
                box_plot(values, f"{metric_name.title()} Distribution", image_path)

            img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
            img = ET.Element('img', attrib={
                'src': os.path.join(os.path.basename(self.images_dir), os.path.basename(image_path)),
                'alt': 'chart'
            })
            img_wrap.append(img)
            plot_wrapper.append(img_wrap)
        section.append(plot_wrapper)

    def report_content(self):
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
        self.metrics_section(section_out)
        for lyr in outputs:
            if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
                self.layerprint(lyr, section_out, self.project_root)


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
