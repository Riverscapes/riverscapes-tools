from typing import List, Dict, Tuple
import os
import sys
import copy
import argparse
import sqlite3
import json
from collections import defaultdict
from xml.etree import ElementTree as ET
import numpy as np
from shapely.geometry import shape
import matplotlib.pyplot as plt
from osgeo import ogr
from rscommons import Logger, dotenv, RSReport
from rscommons.util import safe_makedirs
from rscommons.plotting import pie, horizontal_bar
from rme.__version__ import __version__


from .utils.hypsometric_curve import hipsometric_curve
from .utils.blm_charts import charts as blm_charts, vegetation_charts, land_ownership_labels, riparian_charts

ACRES_PER_SQ_METRE = 0.000247105
ACRES_PER_SQ_KM = 247.105
MILES_PER_KM = 0.621371
SQ_MILES_PER_SQ_KM = 0.386102159
SQ_MILES_PER_SQ_M = 0.000000386102159
SQ_KM_PER_SQ_M = 0.000001
MILES_PER_M = 0.000621371
ACRES_PER_SQ_M = 0.000247105

BLM_COLOR = '#F7E794'
NON_BLM_COLOR = 'grey'

BAR_WIDTH = 0.35
PLOT_ALPHA = 0.5


class WSCAReport(RSReport):
    """ Watershed Condition Assessment Report """

    def clustered_bar_chart(
        self,
        parent: ET.Element,
        title: str,
        data: List[List[float]],
        series_labels: List[str],
        x_labels: List[str],
        colors: List[str],
        x_label: str,
        y_label: str,
        is_vertical: bool = False
    ) -> None:
        """
        Create and save a plot with multiple series as a clustered bar chart.

        :param parent: The parent XML element to which the image will be added.
        :param title: Title of the plot.
        :param data: List of series data, each series being a list of floats.
        :param series_labels: List of labels for each series.
        :param x_labels: List of labels for the x-axis.
        :param colors: List of colors for the series.
        :param x_label: Label for the x-axis.
        :param y_label: Label for the y-axis.
        """
        plt.clf()
        _fig, ax = plt.subplots(figsize=(10, 6))

        # Number of series and bars in each group
        num_series = len(data)
        num_categories = len(data[0])
        bar_width = 0.8 / num_series  # Adjust bar width to fit all series within a group

        # Calculate the positions for the bars
        indices = np.arange(num_categories)  # Base x positions for the groups
        for i, series in enumerate(data):
            bar_positions = indices + i * bar_width - (num_series * bar_width) / 2 + bar_width / 2

            if is_vertical is True:
                ax.bar(bar_positions, series, bar_width, label=series_labels[i], color=colors[i])
            else:
                ax.barh(bar_positions, series, bar_width, label=series_labels[i], color=colors[i])

        if is_vertical is True:
            ax.set_xticks(indices)
            ax.set_xticklabels(x_labels)

            # Set chart labels and title
            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            ax.set_title(title)
            ax.legend()
            plt.grid(axis='y', linestyle='--', alpha=PLOT_ALPHA)
        else:
            ax.set_yticks(indices)
            ax.set_yticklabels(x_labels)

            # Set chart labels and title
            ax.set_xlabel(y_label)
            ax.set_ylabel(x_label)
            ax.set_title(title)
            ax.legend()
            plt.grid(axis='x', linestyle='--', alpha=PLOT_ALPHA)

        # Save and insert the image
        img_path = os.path.join(self.images_dir, f"{title.replace(' ', '_')}.png")
        plt.tight_layout()
        plt.savefig(img_path)
        plt.close()
        self.insert_image(parent, img_path, title)

    def stacked_clustered_bar_chart(
        self,
        parent: ET.Element,
        title: str,
        data: List[List[float]],
        series_labels: List[str],
        x_labels: List[str],
        colors: List[str],
        x_label: str,
        y_label: str,
        is_vertical: bool = False
    ) -> None:
        """
        Create and save a plot with multiple series as a clustered bar chart.

        Pairs of series are stacked together.

        :param parent: The parent XML element to which the image will be added.
        :param title: Title of the plot.
        :param data: List of series data, each series being a list of floats.
        :param series_labels: List of labels for each series.
        :param x_labels: List of labels for the x-axis.
        :param colors: List of colors for the series.
        :param x_label: Label for the x-axis.
        :param y_label: Label for the y-axis.
        """
        plt.clf()
        _fig, ax = plt.subplots(figsize=(10, 6))

        # Number of series and bars in each group
        num_series = len(data) / 2.0
        num_categories = len(data[0])
        bar_width = 0.8 / num_series  # Adjust bar width to fit all series within a group

        # Calculate the positions for the bars
        indices = np.arange(num_categories)  # Base x positions for the groups
        for i, series in enumerate(data):
            stack = i/2.0
            is_stack = i % 2
            hatch = None if is_stack == 0 else 'x'
            bottom = None if is_stack == 0 else data[i - 1]
            bar_positions = indices + stack * bar_width - (num_series * bar_width) / 2 + (bar_width / 2) * (0.5 if is_stack == 0 else -0.5)
            if is_vertical is True:
                ax.bar(bar_positions, series, bar_width, bottom=bottom, label=series_labels[i], color=colors[i], hatch=hatch, edgecolor='black', linewidth=0.2)
            else:
                ax.barh(bar_positions, series, bar_width, left=bottom, label=series_labels[i], color=colors[i], hatch=hatch, edgecolor='black', linewidth=0.2)

        if is_vertical is True:
            ax.set_xticks(indices)
            ax.set_xticklabels(x_labels)

            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            ax.set_title(title)
            ax.legend()
            plt.grid(axis='y', linestyle='--', alpha=PLOT_ALPHA)
        else:
            ax.set_yticks(indices)
            ax.set_yticklabels(x_labels)

            ax.set_xlabel(y_label)
            ax.set_ylabel(x_label)
            ax.set_title(title)
            ax.legend()
            plt.grid(axis='x', linestyle='--', alpha=PLOT_ALPHA)

        plt.tight_layout()

        # Save and insert the image
        img_path = os.path.join(self.images_dir, f"{title.replace(' ', '_')}.png")
        plt.savefig(img_path)
        plt.close()
        self.insert_image(parent, img_path, title)

    def __init__(self, huc: str, input_dir: str, output_dir: str, report_path: str, verbose: bool = False):
        super().__init__(None, report_path)
        self.huc = huc
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        self.verbose = verbose

        # The report has a core CSS file but we can extend it with our own if we want:
        css_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'rme_report.css')
        self.add_css(css_path)

        safe_makedirs(self.images_dir)

        ##############################################################################################################################
        ws_context_section = self.section('WSContext', 'Watershed Context')

        # data1 = [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]
        # data2 = [[1, 2, 3, 4, 5], [5, 4, 3, 2, 1], [2, 3, 4, 5, 6], [5, 4, 3, 2, 1]]
        # labels = ['A', 'B', 'C', 'D', 'E']
        # # colors = ['red', 'green', 'blue', 'orange', 'purple']
        # colors = [BLM_COLOR, BLM_COLOR, 'grey', 'grey']
        # # self.clustered_bar_chart(ws_context_section, 'Test Plot', data, ['BLM', 'Non-BLM'], labels, colors, 'X Label', 'Y Label')
        # self.stacked_clustered_bar_chart(ws_context_section, 'Test Plot', data2, ['BLM - Perennial', 'BLM - Non-Perennial', 'Non-BLM - Perennial', 'Non-BLM - Non-Perennial'], labels, colors, 'X Label', 'Y Label', False)
        # return

        rs_context_dir = os.path.join(input_dir, 'rs_context', huc)
        rcat_dir = os.path.join(input_dir, 'rcat', huc)
        anthro_dir = os.path.join(input_dir, 'anthro', huc)
        vbet_dir = os.path.join(input_dir, 'vbet', huc)

        rs_context_json = os.path.join(rs_context_dir, 'rscontext_metrics.json')
        rsc_metrics_json = json.load(open(rs_context_json, encoding='utf-8'))
        rsc_nhd_gpkg = os.path.join(rs_context_dir, 'hydrology', 'nhdplushr.gpkg')

        rme_dir = os.path.join(input_dir, 'rme', huc)
        rme_gpkg = os.path.join(rme_dir, 'outputs', 'riverscapes_metrics.gpkg')
        rme_metrics_json = os.path.join(rme_dir, 'rme_metrics.json')
        rme_metrics = json.load(open(rme_metrics_json, encoding='utf-8'))

        brat_dir = os.path.join(input_dir, 'brat', huc)
        brat_gpkg = os.path.join(brat_dir, 'outputs', 'brat.gpkg')

        with sqlite3.connect(rsc_nhd_gpkg) as conn:
            curs = conn.cursor()
            curs.execute('SELECT Name FROM WBDHU10 LIMIT 1')
            self.title = f'Watershed Condition Assessment Report for {curs.fetchone()[0]} ({huc})'

        self.physiography(ws_context_section, rs_context_dir, rsc_nhd_gpkg, rsc_metrics_json)
        self.hydrography(ws_context_section, rs_context_dir, rsc_nhd_gpkg, rsc_metrics_json)
        self.watershed_ownership(ws_context_section, rsc_metrics_json)
        self.riverscape_ownership(ws_context_section, rme_metrics)
        self.land_use(ws_context_section, rs_context_dir, vbet_dir, rcat_dir, anthro_dir, rme_dir)
        self.land_use_intensity(ws_context_section, rme_gpkg)

        ##############################################################################################################################
        s2_section = self.section('Section2', 'Aquatic Resources')

        s2_intro = ET.Element('p')
        s2_intro.text = f'''The BLM administers the following extent and relative distribution of water,
                                     riparian-wetland, and aquatic resources in watershed {huc}.
                                     Many of these landscape features are created and maintained by inter-connected biophysical processes that operate within and across ownership boundaries.
                                     Consequently, the distribution and abundance of BLM administered resources,
                                    relative to non-BLM administered resources, strongly controls the BLM\'s ability to maintain or improve the health and productivity of these areas.'''
        s2_section.append(s2_intro)

        self.acquatic_resources(s2_section, rme_metrics)

        biophysical_section = self.section('Biophysical', 'Biophysical Settings')

        hydro_geo = self.section('HydroGeomorphic', 'Hydro Geomorphic', biophysical_section, level=2)

        self.confinement(hydro_geo, rme_gpkg)
        self.vbet_density(hydro_geo, rme_gpkg)
        self.slope_analysis(biophysical_section, rme_gpkg)
        self.stream_order(biophysical_section, rme_gpkg)
        self.sinuosity(biophysical_section, rme_gpkg)

        ecology_section = self.section('Ecological', 'Ecological', level=2)
        self.vegetation(ecology_section, rcat_dir)
        self.beaver(ecology_section, brat_gpkg)
        self.beaver_unsuitable(ecology_section, brat_gpkg)

        s3_section = self.section('Riparian', 'Section 3 - Conditions: Water, Riparian-wetland, and Aquatic Areas', level=1)
        riparian_section = self.section('Riparian', 'Riparian Conditions', s3_section, level=2)
        self.geomorphic(riparian_section, rme_gpkg)
        self.floodplain_access(riparian_section, rme_gpkg)
        self.starvation(riparian_section, brat_gpkg)

        rcat_riparian_condition_bins = [('Very Poor', 0.2), ('Poor', 0.4), ('Moderate', 0.6), ('Good', 0.85), ('Intact', 1.0)]
        rcat_riparian_departure_bins = [('No Historic Riparian Veg Detected', 0.0), ('Negligible', 0.1), ('Minor', 0.3333), ('Significant', 0.6666), ('Large', 1.0)]
        rcat_anthro_lu_intense_bins = [('Very Low', 0.0), ('Low', 0.33), ('Moderate', 0.66), ('High', 1.0)]

        for field in ['segment_area', 'centerline_length']:
            self.rme_prop_field(s3_section, 'RCAT Riparian Condition', 'rcat_igo_riparian_condition', field, rcat_riparian_condition_bins, rme_gpkg)
            self.rme_prop_field(s3_section, 'RCAT Riparian Departure', 'rcat_igo_riparian_veg_departure', field, rcat_riparian_departure_bins, rme_gpkg)
            self.rme_prop_field(s3_section, 'Anthro Land Use Intensity', 'anthro_igo_land_use_intens', field, rcat_anthro_lu_intense_bins, rme_gpkg)

        rcat_fld_plain_bins = [('< 0.2', 0.2), ('0.2 - 0.4', 0.4), ('0.4 - 0.6', 0.6), ('0.6 - 0.8', 0.8), ('0.8 - 1.0', 1.0)]
        self.rme_prop_field(s3_section, 'RCAT Floodplain Accessibility', 'rcat_igo_fldpln_access', 'rcat_igo_fldpln_access', rcat_fld_plain_bins, rme_gpkg)

    def physiography(self, parent, rs_context_dir: str, rsc_nhd_gpkg: str, rsc_metrics: dict) -> None:

        section = self.section('Physiography', 'Physiographic Attributes', parent, level=2)

        metrics = {
            'Catchment Length': [f"{rsc_metrics['catchmentLength']:,.2f} km", f"{rsc_metrics['catchmentLength'] * MILES_PER_KM:,.2f} miles"],
            'Catchment Area': [f"{rsc_metrics['catchmentArea']:,.2f} km²", f"{rsc_metrics['catchmentArea'] * SQ_MILES_PER_SQ_KM:,.2f} miles²"],
            'Catchment Perimeter': [f"{rsc_metrics['catchmentPerimeter']:,.2f} km", f"{rsc_metrics['catchmentPerimeter']:,.2f} miles"],
            'Circularity Ratio': [f"{rsc_metrics['circularityRatio']:,.2f}"],
            'Elongation Ratio': [f"{rsc_metrics['elongationRatio']:,.2f}"],
            'Form Factor': [f"{rsc_metrics['formFactor']:,.2f}"],
            'Catchment Relief': [f"{rsc_metrics['catchmentRelief']:,.2f} m", f"{rsc_metrics['catchmentRelief'] * 3.28084:,.2f} ft"],
            'Relief Ratio': [f"{rsc_metrics['reliefRatio']:,.2f}"]
        }

        table_wrapper = ET.Element('div', attrib={'class': 'tableWrapper'})
        self.create_table_from_dict_of_multiple_values(metrics, table_wrapper)
        section.append(table_wrapper)

        nhd_gpkg_layer = 'WBDHU10'
        dem_path = os.path.join(rs_context_dir, 'topography', 'dem.tif')
        hipso_curve_path = os.path.join(self.images_dir, 'hypsometric_curve.png')
        hipsometric_curve(hipso_curve_path, rsc_nhd_gpkg, nhd_gpkg_layer, dem_path)
        self.insert_image(section, hipso_curve_path, 'Hypsometric Curve')

    def insert_image(self, parent, image_path: str, alt_text: str) -> None:
        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        relative_dir = os.path.dirname(self.images_dir)
        if os.path.basename(relative_dir) == 'images':
            relative_dir = os.path.dirname(relative_dir)
        image_src = os.path.relpath(image_path, relative_dir)

        img = ET.Element('img', attrib={
            'src': image_src,
            'alt': alt_text
        })

        plot_wrapper.append(img)
        parent.append(plot_wrapper)

    def hydrography(self, parent, rs_context_dir: str, rsc_nhd_gpkg: str, rsc_metrics: dict) -> None:

        section = self.section('Hydrography', 'Hydrographic Attributes', parent, level=2)

        length_section = self.section('StreamLength', 'Stream Length', parent, level=3)

        perl = rsc_metrics['flowlineLengthPerennialKm']
        intl = rsc_metrics['flowlineLengthIntermittentKm']
        ephl = rsc_metrics['flowlineLengthEphemeralKm']
        canl = rsc_metrics['flowlineLengthCanalsKm']
        totl = rsc_metrics['flowlineLengthAllKm']

        self.create_table_from_tuple_list(['', 'Length (km)', 'Length (mi)', 'Length (%)'], [
            ('Perennial', perl, perl * MILES_PER_KM, 100 * perl / totl),
            ('Intermittent', intl, intl * MILES_PER_KM, 100 * intl / totl),
            ('Ephemeral', ephl, ephl * MILES_PER_KM, 100 * ephl / totl),
            ('Canal', canl, canl * MILES_PER_KM, 100 * canl / totl),
            ('Total', totl, totl * MILES_PER_KM, 100)], length_section, None, True)

        pie_values = [
            ('Perennial', rsc_metrics['flowlineLengthPerennialKm'], 'Perennial'),
            ('Intermittent', rsc_metrics['flowlineLengthIntermittentKm'], 'Intermittent'),
            ('Ephemeral', rsc_metrics['flowlineLengthEphemeralKm'], 'Ephemeral'),
            ('Canal', rsc_metrics['flowlineLengthCanalsKm'], 'Canal')
        ]

        pie_path = os.path.join(self.images_dir, 'stream_type_pie.png')
        pie([x[1] for x in pie_values], [x[2] for x in pie_values], 'Stream Length Breakdown', None, pie_path)
        self.insert_image(length_section, pie_path, 'Pie Chart')

        density_section = self.section('DrainageDensity', 'Drainage Density', parent, level=3)

        perd = rsc_metrics['drainageDensityPerennial']
        intd = rsc_metrics['drainageDensityIntermittent']
        ephd = rsc_metrics['drainageDensityEphemeral']
        alld = rsc_metrics['drainageDensityAll']

        self.create_table_from_tuple_list(['', 'Density (km/km²)', 'Density (mi/mi²)'], [
            ('Perennial', perd, perd * SQ_MILES_PER_SQ_KM),
            ('Intermittent', intd, perd * SQ_MILES_PER_SQ_KM),
            ('Ephemeral', ephd, ephd * SQ_MILES_PER_SQ_KM),
            ('Overall', alld, alld * SQ_MILES_PER_SQ_KM)], density_section, None, False)

        self.create_table_from_tuple_list(['Waterbody Type', 'Count', 'Area (km²)', 'Area (mi²)', 'Parecent (%)'], [
            (
                'Lakes/Ponds',
                rsc_metrics['waterbodyLakesPondsFeatureCount'],
                rsc_metrics['waterbodyLakesPondsAreaSqKm'], rsc_metrics['waterbodyLakesPondsAreaSqKm'] * SQ_MILES_PER_SQ_KM,
                100 * rsc_metrics['waterbodyLakesPondsAreaSqKm'] / rsc_metrics['waterbodyAreaSqKm']
            ),
            (
                'Reservoirs', rsc_metrics['waterbodyReservoirFeatureCount'], rsc_metrics['waterbodyReservoirAreaSqKm'], rsc_metrics['waterbodyReservoirAreaSqKm']
                * SQ_MILES_PER_SQ_KM, 100 * rsc_metrics['waterbodyReservoirAreaSqKm'] / rsc_metrics['waterbodyAreaSqKm']),
            ('Estuaries', rsc_metrics['waterbodyEstuariesFeatureCount'], rsc_metrics['waterbodyEstuariesAreaSqKm'], rsc_metrics['waterbodyEstuariesAreaSqKm']
             * SQ_MILES_PER_SQ_KM, 100 * rsc_metrics['waterbodyEstuariesAreaSqKm'] / rsc_metrics['waterbodyAreaSqKm']),
            ('Playa', rsc_metrics['waterbodyPlayaFeatureCount'], rsc_metrics['waterbodyPlayaAreaSqKm'], rsc_metrics['waterbodyPlayaAreaSqKm']
             * SQ_MILES_PER_SQ_KM, 100 * rsc_metrics['waterbodyPlayaAreaSqKm'] / rsc_metrics['waterbodyAreaSqKm']),
            ('Swamp/Marsh', rsc_metrics['waterbodySwampMarshFeatureCount'], rsc_metrics['waterbodySwampMarshAreaSqKm'], rsc_metrics['waterbodySwampMarshAreaSqKm']
             * SQ_MILES_PER_SQ_KM, 100 * rsc_metrics['waterbodySwampMarshAreaSqKm'] / rsc_metrics['waterbodyAreaSqKm']),
            ('Ice/Snow', rsc_metrics['waterbodyIceSnowFeatureCount'], rsc_metrics['waterbodyIceSnowAreaSqKm'],
             rsc_metrics['waterbodyIceSnowAreaSqKm'] * SQ_MILES_PER_SQ_KM, 100 * rsc_metrics['waterbodyIceSnowAreaSqKm'] / rsc_metrics['waterbodyAreaSqKm']),
            ('Total', rsc_metrics['waterbodyFeatureCount'], rsc_metrics['waterbodyAreaSqKm'], rsc_metrics['waterbodyAreaSqKm'] * SQ_MILES_PER_SQ_KM, 100),
        ], section, None, True)

    def watershed_ownership(self, parent, rsc_metrics: dict) -> None:

        total_area = sum([area_m2 for area_m2 in rsc_metrics['ownership'].values()])

        display_data = [(
            land_ownership_labels[owner] if owner != 'BLM' else 'BLM',
            area * SQ_KM_PER_SQ_M,
            area * ACRES_PER_SQ_M,
            100 * area / total_area,
        ) for owner, area in rsc_metrics['ownership'].items()]

        self._ownership_section(parent, 'Watershed Ownership', display_data)

    def riverscape_ownership(self, parent, rme_metrics) -> None:

        # Use a defaultdict to aggregate areas by owner
        area_sums = defaultdict(float)
        total_area = 0.0

        for item in rme_metrics['rme']["ownership"]:
            area_sums[item["owner"] if item["owner"] != 'Unknown' else 'UND'] += item["area"]
            total_area += item["area"]

        # Convert defaultdict to a regular dictionary for display
        area_sums = dict(area_sums)

        # Output the results
        display_data = [(
            land_ownership_labels[owner] if owner != 'BLM' else 'BLM',
            area * SQ_KM_PER_SQ_M,
            area * ACRES_PER_SQ_M,
            100 * area / total_area,
        ) for owner, area in area_sums.items()]

        self._ownership_section(parent, 'Riverscape Ownership', display_data)

    def _ownership_section(self, parent, title, data) -> None:

        title_ns = title.replace(' ', '')
        section = self.section(title_ns, title, parent, level=2)

        total_area = sum([x[1] for x in data])

        sorted_table_data = sorted(data, key=lambda x: x[0])
        sorted_raw_data = sorted(data, key=lambda x: x[0])
        sorted_table_data.append(('Total', total_area, total_area * ACRES_PER_SQ_KM, 100))

        self.create_table_from_tuple_list([title, 'Area (km²)', 'Area (acres)', 'Percent (%)'], sorted_table_data, section, None, True)

        # Get list of default colours and remove any that are close to orange being used for BLM
        default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        default_colors.remove('#ff7f0e')

        # Ensure orange is used for BLM
        labels = [x[0] for x in sorted_raw_data]
        blm_label = next((s for s in labels if 'BLM' in s), None)
        if blm_label is not None:
            i = labels.index(blm_label)
            default_colors.insert(i, BLM_COLOR)

        pie_path = os.path.join(self.images_dir, f'{title_ns}_pie.png')
        pie([x[1] for x in sorted_raw_data], [x[0] for x in sorted_raw_data], f'{title} Breakdown', default_colors, pie_path)
        self.insert_image(section, pie_path, 'Pie Chart')

        keys = [item[0] for item in sorted_raw_data]
        values = [item[2] for item in sorted_raw_data]
        labels = [key for key in keys]
        bar_path = os.path.join(self.images_dir, f'{title_ns}_bar.png')
        horizontal_bar(values, labels, default_colors, 'Area (acres)',  f'{title} Breakdown', bar_path)
        self.insert_image(section, bar_path, 'Bar Chart')

    def land_use(self, parent, rs_context_dir: str, vbet_dir: str, rcat_dir: str, anthro_dir: str, rme_dir: str) -> None:

        land_charts = blm_charts(rs_context_dir, vbet_dir, rcat_dir, anthro_dir, rme_dir, self.images_dir)
        # Kelly produces all the charts in one dictionary. Break them into categories

        for category in ['Land Use Type']:
            section = self.section(category.replace(' ', ''), category, parent, level=2)
            for chart_name, chart_path in land_charts.items():
                if category in chart_name:
                    self.insert_image(section, chart_path, chart_name)

    def acquatic_resources(self, parent, rme_metrics) -> None:

        metrics = []

        blm_area = sum([result['sum'] for result in rme_metrics['rme']['segmentarea'] if result['owner'] == 'BLM' and result['flowType'] == 'All'])
        non_area = sum([result['sum'] for result in rme_metrics['rme']['segmentarea'] if result['owner'] == 'Non-BLM' and result['flowType'] == 'All'])
        metrics.append(('Riverscape Area (acres)', f'{blm_area * ACRES_PER_SQ_METRE:,.2f}', f'{non_area * ACRES_PER_SQ_METRE:,.2f}'))

        for flow_type in ['Perennial', 'Intermittent', 'Ephemeral']:
            blm_length = sum([result['sum'] for result in rme_metrics['rme']['centerlinelength'] if result['owner'] == 'BLM' and result['flowType'] == flow_type])
            non_blm_length = sum([result['sum'] for result in rme_metrics['rme']['centerlinelength'] if result['owner'] == 'Non-BLM' and result['flowType'] == flow_type])
            metrics.append((f'{flow_type} Riverscapes Length (miles)', f'{blm_length * MILES_PER_M :,.2f}', f'{non_blm_length* MILES_PER_M:,.2f}'))

        for flow_type in ['Perennial', 'Non-Perennial']:
            blm_area = sum([result['sum'] for result in rme_metrics['rme']['segmentarea'] if result['owner'] == 'BLM' and result['flowType'] == flow_type])
            non_area = sum([result['sum'] for result in rme_metrics['rme']['segmentarea'] if result['owner'] == 'Non-BLM' and result['flowType'] == flow_type])
            metrics.append((f'{flow_type} Riverscape Area (acres)', f'{blm_area * ACRES_PER_SQ_METRE:,.2f}', f'{non_area * ACRES_PER_SQ_METRE:,.2f}'))

        for flow_type in ['Perennial', 'Non-Perennial']:
            blm_area = sum([result['sum'] for result in rme_metrics['rme']['riparianarea'] if result['owner'] == 'BLM' and result['flowType'] == flow_type])
            non_area = sum([result['sum'] for result in rme_metrics['rme']['riparianarea'] if result['owner'] == 'Non-BLM' and result['flowType'] == flow_type])
            metrics.append((f'{flow_type} Riparian Area (acres)', f'{blm_area * ACRES_PER_SQ_METRE:,.2f}', f'{non_area * ACRES_PER_SQ_METRE:,.2f}'))

        self.create_table_from_tuple_list(['', 'BLM', 'Non-BLM',], metrics, parent)

    def slope_analysis(self, parent, rme_gpkg: str) -> None:

        section = self.section('SlopeAnalysis', 'Slope Analysis', parent, level=2)

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            bins = [0.5, 1.0, 3.0, 8.0, 23.0, 100]
            data = {owner: {flow: [0.0]*len(bins) for flow in ['Perennial', 'Non-Perennial']} for owner in ['BLM', 'Non-BLM']}
            curs.execute('''
                SELECT rme_dgo_ownership, FCode, nhd_dgo_streamlength, rme_igo_prim_channel_gradient
                FROM rme_igos
                WHERE (rme_igo_prim_channel_gradient IS NOT NULL)
                    AND (nhd_dgo_streamlength IS NOT NULL)''')

            for row in curs.fetchall():
                owner = 'BLM' if row[0] == 'BLM' else 'Non-BLM'
                flow = 'Perennial' if row[1] in [46006, 55800] else 'Non-Perennial'
                slope = row[3] * 100.0

                for i, bin_top in enumerate(bins):
                    if slope < bin_top:
                        data[owner][flow][i] += row[2] * MILES_PER_M
                        break

            chart_data = [
                data['BLM']['Perennial'],
                data['BLM']['Non-Perennial'],
                data['Non-BLM']['Perennial'],
                data['Non-BLM']['Non-Perennial']
            ]

            colors = [BLM_COLOR, BLM_COLOR, NON_BLM_COLOR, NON_BLM_COLOR]

            bin_labels = []
            for i, bin_top in enumerate(bins):
                bin_lower = 0.0 if i == 0 else bins[i-1]
                bin_lower_label = f'{bin_lower:.1f}' if bin_lower > 0 and bin_lower < 1 else f'{int(bin_lower):d}'

                bin_upper_label = f'{bin_top:.1f}' if bin_top < 1 else f'{int(bin_top):d}'

                if i == len(bins) - 1:
                    bin_labels.append(f'> {bin_lower_label}')
                else:
                    bin_labels.append(f'{bin_lower_label} - {bin_upper_label}')

            series_labels = ['BLM - Perennial', 'BLM - Non-Perennial', 'Non-BLM - Perennial', 'Non-BLM - Non-Perennial']

            self.stacked_clustered_bar_chart(section, 'Slope Analysis', chart_data, series_labels, bin_labels, colors, 'Slope (%)', 'Stream Length (miles)')

    def land_use_intensity(self, parent, rme_gpkg: str) -> None:

        section = self.section('LandUseIntensity', 'Riverscape Land Use Intensity', parent, level=3)

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            bins = [0.0, 33.0, 66.0, 100.0]
            data = {'BLM': [0.0]*len(bins), 'Non-BLM': [0.0]*len(bins)}

            curs.execute('''SELECT rme_dgo_ownership, anthro_igo_land_use_intens, segment_area FROM rme_dgos
                WHERE segment_area IS NOT NULL AND anthro_igo_land_use_intens IS NOT NULL''')

            for row in curs.fetchall():
                owner = 'BLM' if row[0] == 'BLM' else 'Non-BLM'
                intensity = row[1]
                area = row[2] * ACRES_PER_SQ_METRE
                for i, bin_top in enumerate(bins):
                    if intensity <= bin_top:
                        data[owner][i] += area
                        break

            chart_data = [data['BLM'], data['Non-BLM']]
            self.clustered_bar_chart(section, 'Riverscape Land Use Intensity', chart_data, ['BLM', 'Non-BLM'], ['Very Low', 'Low', 'Moderate', 'High'], [BLM_COLOR, NON_BLM_COLOR], 'Land Use Intensity', 'Area (acres)')

    def riparian_condition(self, riparian_section, rme_dir):

        section = self.section('Riparian', 'Riparian', riparian_section, level=3)

        riparian_gpkg = os.path.join(os.path.dirname(rme_dir), '..', 'blm_riparian.gpkg')

        # Returns a dictionary of ownership, with subdictionaries of riparian class keyed to areas (m2)
        riparian = riparian_charts(rme_dir, riparian_gpkg)

        # Get the unique riparian categories
        unique_categories = []
        for owner, cats in riparian.items():
            unique_categories += list(cats.keys())

        unique_categories = list(set(unique_categories))

        data = []
        for owner in ['BLM', 'Non-BLM']:
            owner_data = riparian[owner]
            owner_series = []
            for cat in unique_categories:
                owner_series.append(owner_data.get(cat, 0) * ACRES_PER_SQ_M)
            data.append(owner_series)

        self.clustered_bar_chart(section, 'Riverscapes Riparian Condition', data, ['BLM', 'Non-BLM'], unique_categories, [BLM_COLOR, NON_BLM_COLOR], '', 'Area (acres)')

    def geomorphic(self, riparian_section, rme_gpkg):
        pass

    def floodplain_access(self, parent, rme_gpkg: str) -> None:

        section = self.section('FloodplainAccessibility', 'Floodplain Accessibility', parent, level=2)

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            curs.execute('''
               SELECT rme_dgo_ownership, sum(rme_dgos.segment_area), sum(rcat_igo_fldpln_access * rme_dgos.segment_area), sum((1- rcat_igo_fldpln_access) * rme_dgos.segment_area)  FROM rme_dgos
                where segment_area is not null and  rcat_igo_fldpln_access is not null and rme_dgo_ownership is not null
                GROUP BY rme_dgo_ownership''')

            total_areas = {'BLM': 0.0, 'Non-BLM': 0.0}
            accessible_areas = {'BLM': 0.0, 'Non-BLM': 0.0}
            inaccessible_areas = {'BLM': 0.0, 'Non-BLM': 0.0}

            for row in curs.fetchall():
                owner = 'BLM' if row[0] == 'BLM' else 'Non-BLM'
                total_areas[owner] += row[1] * ACRES_PER_SQ_METRE
                accessible_areas[owner] += row[2] * ACRES_PER_SQ_METRE
                inaccessible_areas[owner] += row[3] * ACRES_PER_SQ_METRE

            table_data = []
            for label, data in [('Accessible Area', accessible_areas), ('Inaccessible Area', inaccessible_areas)]:
                row_data = [label]
                for owner in ['BLM', 'Non-BLM']:
                    row_data.append(data[owner])
                    row_data.append(100 * data[owner] / total_areas[owner])

                table_data.append(row_data)

            table_data.append(['Total', total_areas['BLM'], None, total_areas['Non-BLM'], None])

            self.create_table_from_tuple_list(['', 'BLM\n(acres)', '\n%', 'Non-BLM (acres)', '\n%'], table_data, section, None, True)

    def starvation(self, parent, brat_gpkg: str) -> None:

        section = self.section('Starvation', 'Structural Starvation', parent, level=2)

        with sqlite3.connect(brat_gpkg) as conn:
            curs = conn.cursor()

            curs.execute("SELECT coalesce(sum(centerline_length),0) FROM vwDgos where Limitation = 'Stream Power Limited'")
            length = curs.fetchone()[0] * MILES_PER_M

            self.create_table_from_dict({'Stream Power Limited Length (miles)': length}, section)

    def stream_order(self, parent, rme_gpkg: str) -> None:

        section = self.section('StreamOrder', 'Stream Order', parent, level=2)

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            # Prepare lists to hold data for both series
            stream_orders = []
            blm_lengths = []
            non_blm_lengths = []
            blm_data = {}
            non_blm_data = {}

            # Loop over ownership categories and fetch data
            for owner_label, owner_condition in [('BLM', "= 'BLM'"), ('Non-BLM', "<> 'BLM'")]:
                curs.execute(f'''
                    SELECT nhd_dgo_streamorder, SUM(nhd_dgo_streamlength)
                    FROM rme_dgos
                    WHERE rme_dgo_ownership {owner_condition}
                    GROUP BY nhd_dgo_streamorder
                    ORDER BY nhd_dgo_streamorder''')

                # Fetch the results and store them in dictionaries for lookup
                if owner_label == 'BLM':
                    blm_data = {row[0]: row[1] * MILES_PER_M for row in curs.fetchall()}  # Convert meters to miles
                else:
                    non_blm_data = {row[0]: row[1] * MILES_PER_M for row in curs.fetchall()}  # Convert meters to miles

            # Combine data from both ownerships
            stream_orders = list(set(blm_data.keys()).union(non_blm_data.keys()))
            for stream_order in sorted(stream_orders):
                blm_length = blm_data.get(stream_order, 0)  # Default to 0 if no data for this stream order
                non_blm_length = non_blm_data.get(stream_order, 0)  # Default to 0 if no data for this stream order

                # Append data to the lists for charting
                blm_lengths.append(blm_length)
                non_blm_lengths.append(non_blm_length)
                stream_orders.append(stream_order)

            # Create the bar chart with two series
            bar_width = 0.35  # Width of each bar
            index = np.arange(len(blm_lengths))  # X-axis positions for bars

            _fig, ax = plt.subplots(figsize=(10, 6))

            # Plot bars for BLM and Non-BLM data, grouped by stream order
            ax.bar(index - bar_width / 2, blm_lengths, bar_width, label='BLM', color=BLM_COLOR)  # , edgecolor='black')
            ax.bar(index + bar_width / 2, non_blm_lengths, bar_width, label='Non-BLM', color=NON_BLM_COLOR)  # , edgecolor='black')

            # Add labels, title, and legend
            ax.set_xlabel('Stream Order')
            ax.set_ylabel('Stream Length (miles)')
            ax.set_title('Stream Order Lengths')
            ax.set_xticks(index)
            ax.set_xticklabels([str(order+1) for order in range(len(blm_lengths))])  # , rotation=45)
            ax.legend()

            plt.grid(axis='y', linestyle='--', alpha=PLOT_ALPHA)

            # Save the chart as an image
            img_path = os.path.join(self.images_dir, 'combined_stream_order_bar_chart.png')
            # plt.tight_layout()  # Adjust layout to prevent clipping of labels
            plt.savefig(img_path)
            plt.close()

            # Insert the image into your report or interface
            self.insert_image(section, img_path, 'Combined Stream Order Length Chart')

    def sinuosity(self, parent, rme_gpkg):

        section = self.section('SinuosityAnalysis', 'Sinuosity Analysis', parent, level=2)

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            # Prepare to collect data for both series
            bin_sums_blm = None
            bin_sums_non_blm = None
            bin_midpoints = None

            for owner, owner_filter in [('BLM', " = 'BLM'",), ('Non-BLM', " <> 'BLM'",)]:
                curs.execute(f'''
                    SELECT
                        nhd_dgo_streamlength, rme_igo_planform_sinuosity
                    FROM rme_igos
                    WHERE (rme_igo_planform_sinuosity IS NOT NULL)
                        AND (rme_dgo_ownership {owner_filter})''')

                raw_data = [(row[0] * MILES_PER_M, row[1]) for row in curs.fetchall()]

                if len(raw_data) == 0:
                    continue

                lengths, gradients = zip(*raw_data)
                num_bins = 5

                # Compute the bins
                bin_edges = np.linspace(min(gradients), max(gradients), num_bins + 1)

                # Sum stream lengths for each gradient bin
                bin_sums = np.histogram(gradients, bins=bin_edges, weights=lengths)[0]

                # Midpoints for bar chart x-axis
                if bin_midpoints is None:
                    bin_midpoints = (bin_edges[:-1] + bin_edges[1:]) / 2

                # Store data for each owner
                if owner == 'BLM':
                    bin_sums_blm = bin_sums
                else:
                    bin_sums_non_blm = bin_sums

            # Check if both series have data
            if bin_sums_blm is not None and bin_sums_non_blm is not None:
                # Plot the bar chart with two series
                bar_width = (bin_edges[1] - bin_edges[0]) / 3  # Smaller width for grouped bars

                plt.clf()
                plt.bar(bin_midpoints - bar_width / 2, bin_sums_blm, width=bar_width, color=BLM_COLOR, label='BLM')
                plt.bar(bin_midpoints + bar_width / 2, bin_sums_non_blm, width=bar_width, color=NON_BLM_COLOR, label='Non-BLM')

                # Add labels, legend, and grid
                plt.xlabel('Sinuosity')
                plt.ylabel('Summed Stream Lengths (miles)')
                plt.title('Stream Lengths Summed by Sinuosity Bins (BLM vs Non-BLM)')
                plt.grid(axis='y', linestyle='--', alpha=PLOT_ALPHA)
                plt.legend()

                # Save the chart as an image
                img_path = os.path.join(self.images_dir, 'combined_sinuosity_histogram.png')
                plt.savefig(img_path, bbox_inches="tight")
                plt.close()

                self.insert_image(section, img_path, 'Combined Sinuosity Histogram')

    def vegetation(self, biophysical_section, rcat_dir):

        vegetation_section = self.section('Vegetation', 'Vegetation', biophysical_section, level=3)
        charts_dict = vegetation_charts(rcat_dir, self.images_dir)

        for key, val in charts_dict.items():
            self.insert_image(vegetation_section, val, key)

    def beaver(self, ecology_section, brat_gpkg):

        section = self.section('Beaver', 'Beaver', ecology_section, level=3)

        with sqlite3.connect(brat_gpkg) as conn:
            curs = conn.cursor()

            # Define bins and labels
            bins = [0, 1, 5, 15, 40]  # Bin edges
            bin_labels = ["0", "0-1", "1-5", "5-15", "15-40"]  # Labels for the bins
            colors = ['red', 'orange', 'yellow', 'green', 'blue']  # Colors for the bins

            # Execute the query
            curs.execute("""
                    SELECT centerline_length, oCC_EX, oCC_HPE
                    FROM vwDGOs
                    WHERE centerline_length > 0
                    AND oCC_EX IS NOT NULL
                    AND oCC_HPE IS NOT NULL
                    AND ReachCode in (46006, 55800)
                """)

            data = curs.fetchall()
            existing_lengths = [(length * MILES_PER_M, occ_ex) for length, occ_ex, occ_hpe in data]
            historic_lengths = [(length * MILES_PER_M, occ_hpe) for length, occ_ex, occ_hpe in data]

            def sum_lengths_by_bins(lengths):
                bin_sums = [0] * len(bins)
                bin_sums[0] = sum([length for length, value in lengths if value == 0])
                bin_sums[1] = sum([length for length, value in lengths if 0 < value <= 1])
                bin_sums[2] = sum([length for length, value in lengths if 1 < value <= 5])
                bin_sums[3] = sum([length for length, value in lengths if 5 < value <= 15])
                bin_sums[4] = sum([length for length, value in lengths if 15 < value <= 40])
                return bin_sums

            # Calculate bin sums for both series
            existing_bin_sums = sum_lengths_by_bins(existing_lengths)
            historic_bin_sums = sum_lengths_by_bins(historic_lengths)

            # Create the bar chart
            bar_width = 0.4
            x = np.arange(len(bin_labels))  # Positions for the bins

            # Create bars with specific colors
            plt.clf()
            for i in range(len(bin_labels)):

                plt.bar(
                    x[i] - bar_width / 2,
                    existing_bin_sums[i],
                    bar_width,
                    label='Existing Capacity' if i == 0 else "",  # Add label only for the first bin
                    color=colors[i],
                    edgecolor='black'
                )

                plt.bar(
                    x[i] + bar_width / 2,
                    historic_bin_sums[i],
                    bar_width,
                    label='Historic Capacity' if i == 0 else "",  # Add label only for the first bin
                    color=colors[i],
                    edgecolor='black',
                    hatch='x'
                )

            # Add labels, title, and legend
            plt.xlabel('Beaver Dam Capacity (dams per km) for Perennial Streams')
            plt.ylabel('Riverscape Length (miles)')
            plt.title('Historic and Existing Beaver Dam Capacity')
            plt.xticks(x, bin_labels)  # Set custom x-axis labels
            plt.legend()

            # Save the chart
            plt.tight_layout()
            img_path = os.path.join(self.images_dir, 'combined_capacity_histogram.png')
            plt.savefig(img_path, bbox_inches="tight")
            plt.close()

            self.insert_image(section, img_path, 'Combined Beaver Data Capacity Histogram')

    def beaver_unsuitable(self, ecology_section, brat_gpkg):

        with sqlite3.connect(brat_gpkg) as conn:
            curs = conn.cursor()

            curs.execute("""
               SELECT DL.Name, coalesce(V.Tally, 0)
FROM DamLimitations DL
         LEFT JOIN
     (SELECT Limitation, sum(centerline_length) Tally
      from vwDgos
      WHERE Limitation is not null
        and Limitation <> 'Dam Building Possible'
        and Limitation <> 'NA'
      group by Limitation) V ON DL.Name = V.Limitation
      WHERE DL.Name <> 'Dam Building Possible'""")

            data = [(row[0], row[1] * MILES_PER_M) for row in curs.fetchall()]

            values = [x[1] for x in data]
            labels = [x[0] for x in data]

            img_path = os.path.join(self.images_dir, 'beaver_unsuitable.png')
            horizontal_bar(values, labels, None, 'Stream Length (miles)', 'Beaver Unsuitable Habitat', img_path)
            self.insert_image(ecology_section, img_path, 'Unsuitable Beaver Habitat')

    def confinement(self, parent, rme_gpkg):

        section = self.section('Confinement', 'Confinement', parent, level=3)
        # self._confinement(section, rme_gpkg, 'Confinement Ratio Lengths', 'Length (miles)', 'centerline_length')
        # self._confinement(section, rme_gpkg, 'Confinement Ratio Areas', 'Area (acres)', 'segment_area')

        bins = [
            ('Laterally Unconfined', 0.1),
            ('Partly Confined - Planform Controlled', 0.5),
            ('Partly Confined - Margin Controlled', 0.9),
            ('> 90% Confined', 1.0)
        ]

        self.rme_prop_field(section, 'Confinement Ratio Lengths', 'conf_igo_confinement_ratio', 'centerline_length', bins, rme_gpkg)
        self.rme_prop_field(section, 'Confinement Ratio Areas', 'conf_igo_confinement_ratio', 'segment_area', bins, rme_gpkg)

    def _confinement(self, section, rme_gpkg, title: str, y_label, field: str) -> None:

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            # 0-0.1 - unconfined, 0.1-0.5 partly confined, planform controlled 0.5-0.85, partly confined valley controlled, 0.85-1 - confined
            bins = [0.1, 0.5, 0.85, 1.0]
            data = {'Perennial': {'BLM': [0.00] * len(bins), 'Non-BLM': [0.00] * len(bins)}, 'Non-Perennial': {'BLM': [0.00] * len(bins), 'Non-BLM': [0.00] * len(bins)}}
            bin_labels = [f'{bins[i-1]}-{bins[i]}' if i != 0 else f'< {bins[i]}' for i in range(len(bins))]

            for flow, flow_filter in [('Perennial', " IN (46006, 55800)"), ('Non-Perennial', " NOT IN (46006, 55800)")]:
                for owner, owner_filter in [('BLM', " = 'BLM'"), ('Non-BLM', " <> 'BLM'")]:
                    curs.execute(f'''
                    SELECT conf_igo_confinement_ratio, centerline_length, segment_area
                        FROM rme_dgos
                        where rme_dgo_ownership {owner_filter}
                            and fcode {flow_filter}
and conf_igo_confinement_ratio is not null and centerline_length is not null and segment_area is not null''')

                    for row in curs.fetchall():
                        confinement_ratio = row[0]
                        # Pick length or area
                        value = row[1] * MILES_PER_M if field == 'centerline_length' else row[2] * ACRES_PER_SQ_METRE
                        for idx, upper_limit in enumerate(bins):
                            if confinement_ratio < upper_limit:
                                data[flow][owner][idx] += value
                                break

            plot_data = [
                data['Perennial']['BLM'],
                data['Non-Perennial']['Non-BLM'],
                data['Perennial']['BLM'],
                data['Non-Perennial']['Non-BLM']
            ]
            self.stacked_clustered_bar_chart(section, f'{title}', plot_data, ['BLM - Perennial', 'BLM Non-Perennial', 'Non-BLM - Perennial', 'Bob-BLM - Non-Perennial'],
                                             bin_labels, [BLM_COLOR, BLM_COLOR, NON_BLM_COLOR, NON_BLM_COLOR], 'Confinement Ratio', y_label)

    def rme_prop_field(self, parent, title, field_name, cumulative_calc, bin_uppers: List[Tuple[str, float]], rme_gpkg):
        """


        Args:
            parent (_type_): _description_
            title (_type_): _description_
            field_name (_type_): _description_
            cumulative_calc (_type_): pass in 'segment_area' or 'centerline_length' for index fields, or 'segment_area * prop_field' or 'centerline_length * prop_field' for proportional fields
            bin_uppers (List[Tuple[str, float]]): _description_
            rme_gpkg (_type_): _description_
        """

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            data = {
                'BLM': {
                    'Perennial': {label: 0.0 for label, _ in bin_uppers},
                    'Non-Perennial': {label: 0.0 for label, _ in bin_uppers}
                },
                'Non-BLM': {
                    'Perennial': {label: 0.0 for label, _ in bin_uppers},
                    'Non-Perennial': {label: 0.0 for label, _ in bin_uppers}
                }
            }

            is_area_calc = 'segment_area' in cumulative_calc
            y_axis_label = 'Area (acres)' if is_area_calc is True else 'Length (miles)'
            unit_conversion = ACRES_PER_SQ_METRE if is_area_calc is True else MILES_PER_M

            curs.execute(f'''
                SELECT
                    rme_dgo_ownership,
                    FCode,
                    {cumulative_calc},
                    {field_name}
                FROM rme_dgos
                WHERE (segment_area is not null)
                    AND (rme_dgo_ownership is not null)
                    AND ({field_name} is not null)
                    AND (centerline_length is not null)''')

            for row in curs.fetchall():
                owner = row[0] if row[0] == 'BLM' else 'Non-BLM'
                flow = 'Perennial' if row[1] in [46006, 55800] else 'Non-Perennial'
                cumulative_field_value = row[2] * unit_conversion
                binning_value = row[3]

                for label, upper in bin_uppers:
                    if binning_value <= upper:
                        data[owner][flow][label] += cumulative_field_value
                        break

            # Prepare data for the chart
            chart_data = []
            series_labels = []
            for owner in ['BLM', 'Non-BLM']:
                for flow in ['Perennial', 'Non-Perennial']:
                    chart_data.append([data[owner][flow][label] for label, _ in bin_uppers])
                    series_labels.append(f'{owner} - {flow}')

            self.stacked_clustered_bar_chart(parent, title, chart_data, series_labels, [label for label, _upper in bin_uppers], [BLM_COLOR, BLM_COLOR, NON_BLM_COLOR, NON_BLM_COLOR], title, y_axis_label)

    def vbet_density(self, parent, rme_gpkg):

        section = self.section('ValleyBottomDensity', 'Valley Bottom Density', parent, level=3)

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            curs.execute('''
                SELECT case when isBLM Then 'BLM' ELSE 'Non-BLM' END, TotalArea / TotalLength
                FROM (
                    SELECT rme_dgo_ownership = 'BLM' IsBLM, sum(segment_area) TotalArea, sum(centerline_length) TotalLength
                    FROM rme_dgos
                    WHERE rme_dgo_ownership is not null
                        and segment_area is not null
                        and centerline_length is not null
                    GROUP BY rme_dgo_ownership = 'BLM')
                         ORDER BY NOT isBLM''')
            densities = [(row[0], row[1]) for row in curs.fetchall()]
            values = [x[1] for x in densities]
            labels = [x[0] for x in densities]

            img_path = os.path.join(self.images_dir, 'vbet_density.png')
            horizontal_bar(values, labels, [BLM_COLOR, 'green'], 'Density (Acres per Mile)', 'Valley Bottom Density Distribution', img_path)
            self.insert_image(section, img_path, 'Valley Bottom Density')


def get_rme_values(rme_gpkg: str) -> dict:

    rme_values = {}
    with sqlite3.connect(rme_gpkg) as conn:
        curs = conn.cursor()

        # Modal Ecoregion III (metric ID 17)
        for level, metric_id in [('Ecoregion III', 17), ('Ecoregion IV', 18)]:
            curs.execute('SELECT metric_value, count(*) frequency FROM dgo_metric_values WHERE metric_id= ? GROUP BY metric_value ORDER by frequency DESC limit 1', [metric_id])
            row = curs.fetchone()
            rme_values[level] = row[0]

        # Stream Gradient
        curs.execute('SELECT AVG(metric_value) FROM dgo_metric_values WHERE metric_id= 4')
        row = curs.fetchone()
        rme_values['Mean Stream Gradient'] = row[0]

    return rme_values


def get_rme_stats(rme_gpkg: str, filter_blm: bool, include_fcodes: bool, fcodes: List[int]) -> dict:

    filter_fcodes = ''
    if fcodes is not None and len(fcodes) > 0:
        fcodes_str = ', '.join([str(fcode) for fcode in fcodes])
        filter_fcodes = f'AND (d.fcode {"" if include_fcodes is True else "NOT"} IN ({fcodes_str}))'

    rme_stats = {}
    with sqlite3.connect(rme_gpkg) as conn:
        curs = conn.cursor()

        sql = f'''
            SELECT m.machine_code,
                sum(mv.metric_value) total,
                sum(mv.metric_value * d.segment_area) total_area,
                avg(mv.metric_value) avg,
                max(mv.metric_value) max,
                min(mv.metric_value) min,
                sum(d.segment_area) raw_area
            FROM dgos d
                inner join dgo_metric_values mv ON d.fid = mv.dgo_id
                inner join metrics m on mv.metric_id = m.metric_id
                inner join (select dgo_id, metric_value from dgo_metric_values WHERE (metric_id = 1)) o ON mv.dgo_id = o.dgo_id
            WHERE (o.metric_value {'=' if filter_blm is True else '<>' } 'BLM')
                {filter_fcodes}
            group by m.machine_code'''

        curs.execute(sql)
        rme_stats = {row[0]: {'total': float(row[1]), 'avg': float(row[2]), 'max': float(row[3]), 'min': float(row[4]), 'raw_area': float(row[5])} for row in curs.fetchall()}

    return rme_stats


def get_vbet_json_metrics(metrics_json_path) -> dict:

    with open(metrics_json_path, 'r', encoding='utf-8') as f:
        metrics_json = json.load(f)

    total_area = float(metrics_json['catchmentArea'])

    formated = {
        'Total Valley Bottom Area per Unit Length (m^2/m)': metrics_json['totalHectaresPerKm'],
        'Average Valley Bottom Area per Unit Length (m^2/m)': metrics_json['averageHectaresPerKm'],
        'Average Valley Bottom Width (m)': metrics_json['avgValleyBottomWidth'],
        'Proportion Riverscape (%)': float(metrics_json['proportionRiverscape']) * 100,
        'Area of Riverscapes (km^2)': float(metrics_json['proportionRiverscape']) * total_area,
        'Area of Non-Riverscapes (km^2)': (1 - float(metrics_json['proportionRiverscape'])) * total_area
    }
    return formated


def get_watershed_boundary_geom(nhd_gpkg: str) -> shape:
    """Takes the NHD GeoPackage and returns the watershed boundary
    as a Shapely Geometry
    """

    # Read the polygon and DEM
    with GeopackageLayer(nhd_gpkg, 'WBDHU10') as polygon_layer:
        geoms = ogr.Geometry(ogr.wkbMultiPolygon)
        for feature, *_ in polygon_layer.iterate_features():
            feature: ogr.Feature
            geom: ogr.Geometry = feature.GetGeometryRef()
            geoms.AddGeometry(geom)

        # rough_units = 1 / polygon_layer.rough_convert_metres_to_vector_units(1.0)
        polygon_json = json.loads(geoms.ExportToJson())

    # Mask the DEM with the polygon
    return shape(polygon_json)


def main():
    """Watershed Assessment Report"""

    parser = argparse.ArgumentParser(description='Riverscapes Watershed Report')
    parser.add_argument('huc', help='HUC identifier', type=str)
    parser.add_argument('input_folder', help='Parent folder inside which whole riverscapes projects are stored', type=str)
    parser.add_argument('report_path', help='Output report file path', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger("Watershed Condition Assessment Report")
    log.setup(logPath=os.path.join(os.path.dirname(args.report_path), "wsca_report.log"), verbose=args.verbose)
    log.title(f'Watershed Condition Assessment Report For HUC: {args.huc}')

    # try:
    report = WSCAReport(args.huc, args.input_folder, os.path.dirname(args.report_path), args.report_path, args.verbose)
    report.write(title=report.title)

    # except Exception as e:
    #     log.error(e)
    #     traceback.print_exc(file=sys.stdout)
    #     sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
