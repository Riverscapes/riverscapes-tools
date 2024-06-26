import argparse
import sqlite3
import os
from collections import Counter
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import box_plot, vertical_bar
from rme.__version__ import __version__

# NOTE: to change a filter name or add a new filter, you must also add it to the
# `LayerTypes` dictionary at the top of the metric_engine.py file
FILTER_MAP = {
    "perennial": "FCode IN (46006, 55800)",
    "public_perennial": "(FCode IN (46006, 55800)) AND (rme_dgo_ownership NOT IN ('PVT', 'UND', 'BIA', 'LG')) AND (rme_dgo_ownership IS NOT NULL)",
    "blm_lands": "rme_dgo_ownership = 'BLM'",
    "blm_perennial": "(FCode IN (46006, 55800)) AND (rme_dgo_ownership = 'BLM')",
    "usfs_perennial": "(FCode IN (46006, 55800)) AND (rme_dgo_ownership IN ('USFS', '4USFS'))",
    "nps_perennial": "(FCode IN (46006, 55800)) AND (rme_dgo_ownership = 'NPS')",
    'st_perennial': "(FCode IN (46006, 55800)) AND (rme_dgo_ownership = 'ST')",
    'fws_perennial': "(FCode IN (46006, 55800)) AND (rme_dgo_ownership = 'FWS')",
}
FILTER_NAMES = list(FILTER_MAP.keys())


class RMEReport(RSReport):

    def __init__(self, database, report_path, rs_project, filter_name=None):
        super().__init__(rs_project, report_path)
        self.log = Logger('Riverscapes Metrics Report')
        self.database = database
        self.project_root = rs_project.project_dir

        # The report has a core CSS file but we can extend it with our own if we want:
        css_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'rme_report.css')
        self.add_css(css_path)

        self.filter_name = filter_name
        self.sql_filter = FILTER_MAP.get(filter_name)

        if self.filter_name is not None:
            self.images_dir = os.path.join(os.path.dirname(report_path), 'images', self.filter_name)
        else:
            self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)

        self.report_content()

    def metrics_section(self, parent_section):
        section = self.section("Metrics", "Metrics", parent_section, level=2)

        self.metrics_table(section)
        self.metrics_plots(section)

    def metrics_table(self, section):
        RSReport.header(4, "Available Metrics", section)

        table = ET.Element('table')

        thead = ET.Element('thead')
        theadrow = ET.Element('tr')
        thead.append(theadrow)
        table.append(thead)

        col_names = ["Metric Name", "Description", "Method for Obtaining"]
        for col in col_names:
            th = ET.Element('th')
            th.text = col
            theadrow.append(th)

        conn = sqlite3.connect(self.database)
        curs = conn.cursor()
        curs.execute("""
            SELECT name, description, method, LOWER(field_name)
            FROM metrics
            WHERE is_active = 1
        """)

        tbody = ET.Element('tbody')
        table.append(tbody)

        for name, description, method, field_name in curs.fetchall():
            tr = ET.Element('tr')
            tbody.append(tr)

            td = ET.Element("td", attrib={'class': 'text url'})
            section_link = ET.Element(
                'a',
                attrib={'href': f"#{field_name}", 'id': f"{field_name}_header"}
            )
            section_link.text = name
            td.append(section_link)
            tr.append(td)

            for val in [description, method]:
                str_val, class_name = RSReport.format_value(val)
                td = ET.Element('td', attrib={'class': class_name})
                td.text = str_val
                tr.append(td)

        section.append(table)

    def avg_min_max_table(self, data, parent_section):
        table = ET.Element('table')

        thead = ET.Element('thead')
        theadrow = ET.Element('tr')
        thead.append(theadrow)
        table.append(thead)

        col_names = ["Average", "Min", "Max"]
        for col in col_names:
            th = ET.Element('th')
            th.text = col
            theadrow.append(th)

        tbody = ET.Element('tbody')
        table.append(tbody)

        tr = ET.Element('tr')
        tbody.append(tr)

        # Just make it all 0 if there is no data
        clean_data = [0 if x is None else x for x in data] or [0]
        min_val = min(clean_data)
        max_val = max(clean_data)
        avg_val = sum(clean_data) / len(clean_data)

        for val in [avg_val, min_val, max_val]:
            str_val, class_name = RSReport.format_value(val, float)
            td = ET.Element('td', attrib={'class': class_name})
            td.text = str_val
            tr.append(td)

        parent_section.append(table)

    def metrics_plots(self, parent_section):
        section = self.section("metric-plots", "Metric Plots", parent_section, level=3)
        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})

        conn = sqlite3.connect(self.database)
        curs = conn.cursor()

        curs.execute("""
            SELECT LOWER(field_name), data_type, name
            FROM metrics
            WHERE field_name != '' AND is_active = 1
        """)
        metrics_info = curs.fetchall()

        for metric_name, data_type, name in metrics_info:
            try:
                curs.execute(
                    f"""
                    SELECT {metric_name}
                    FROM vw_igo_metrics
                    """
                    + f"WHERE {self.sql_filter}" * (self.sql_filter is not None)
                )
                values = [row[0] for row in curs.fetchall()]
            except sqlite3.OperationalError as e:
                self.log.error(f"Error fetching data for metric {metric_name} ({e})")
                continue

            card = ET.Element('div', attrib={'class': 'metrics-card', 'id': metric_name})
            title = ET.Element(
                'a',
                attrib={'class': 'metrics-card-header', 'href': f"#{metric_name}_header"}
            )
            title.text = name
            card.append(title)

            if data_type == "TEXT" or data_type == "INTEGER":
                image_path = os.path.join(self.images_dir, f"{metric_name}_bar.png")
                value_counts = Counter(values)
                vertical_bar(
                    value_counts.values(),
                    [str(i) for i in value_counts.keys()],
                    "count",
                    f"{name} Distribution",
                    image_path
                )

            elif data_type == "REAL":
                self.avg_min_max_table(values, card)

                image_path = os.path.join(self.images_dir, f"{metric_name}_box.png")
                box_plot(values, f"{name} Distribution", image_path)

            # Get path in form images/(optional subdirectory)/image_name
            relative_dir = os.path.dirname(self.images_dir)
            if os.path.basename(relative_dir) == 'images':
                relative_dir = os.path.dirname(relative_dir)
            image_src = os.path.relpath(image_path, relative_dir)

            img = ET.Element('img', attrib={
                'src': image_src,
                'alt': 'chart'
            })
            card.append(img)
            plot_wrapper.append(card)
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

    # None will run report normally  with no filters
    filter_names = [None] + FILTER_NAMES

    for filter_name in filter_names:
        if filter_name is not None:
            report_path = args.report_path.replace('.html', f'_{filter_name}.html')
        else:
            report_path = args.report_path
        report = RMEReport(args.database, report_path, project, filter_name)
        report.write()
