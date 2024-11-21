from typing import List
import os
import sys
import traceback
import argparse
import sqlite3
import json
from collections import Counter, defaultdict
from xml.etree import ElementTree as ET
from shapely.geometry import shape
from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import box_plot, vertical_bar, histogram
from rme.__version__ import __version__
import time
from collections import Counter
import matplotlib.pyplot as plt

from osgeo import ogr, osr
from osgeo import gdal
import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import Point

from rscommons import GeopackageLayer, dotenv, Logger, initGDALOGRErrors, ModelConfig, RSLayer, RSMeta, RSMetaTypes, RSProject, VectorBase, ProgressBar
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.util import parse_metadata, pretty_duration
from rscommons.database import load_lookup_data
from rscommons.geometry_ops import reduce_precision, get_endpoints
from rscommons.vector_ops import copy_feature_class, collect_linestring
from rscommons.vbet_network import copy_vaa_attributes, join_attributes
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions
from rscommons.moving_window import moving_window_dgo_ids
from rscommons.plotting import xyscatter, box_plot, pie, horizontal_bar

from rme.__version__ import __version__
from rme.analysis_window import AnalysisLine

from .utils.hypsometric_curve import hipsometric_curve
from .utils.blm_charts import charts as blm_charts, vegetation_charts, land_ownership_labels

ACRES_PER_SQ_METRE = 0.000247105
ACRES_PER_SQ_KM = 247.105
MILES_PER_KM = 0.621371
SQ_MILES_PER_SQ_KM = 0.386102159
SQ_MILES_PER_SQ_M = 0.000000386102159
SQ_KM_PER_SQ_M = 0.000001
MILES_PER_M = 0.000621371


class WSCAReport(RSReport):
    """ Watershed Condition Assessment Report """

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

        self.physiography(ws_context_section, rs_context_dir, rsc_nhd_gpkg, rsc_metrics_json)
        self.hydrography(ws_context_section, rs_context_dir, rsc_nhd_gpkg, rsc_metrics_json)
        self.watershed_ownership(ws_context_section, rsc_metrics_json)
        self.riverscape_ownership(ws_context_section, rme_metrics)
        self.land_use(ws_context_section, rs_context_dir, vbet_dir, rcat_dir, anthro_dir, rme_dir)

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
        self.hydro_geomorphic(biophysical_section, rme_metrics)
        self.slope_analysis(biophysical_section, rme_gpkg)
        self.stream_order(biophysical_section, rme_gpkg)
        self.sinuosity(biophysical_section, rme_gpkg)
        self.valley_bottom_density(biophysical_section, rme_gpkg)
        self.vegetation(biophysical_section, rcat_dir)

        # intermittent_length = vbet_metrics['drainageDensityIntermittent'] * vbet_metrics['catchmentArea'] * 0.621371
        # ephemeral_length = vbet_metrics['drainageDensityEphemeral'] * vbet_metrics['catchmentArea'] * 0.621371

        # s2_metrics = {
        # 'Valley Bottom Area (acres)': rme_stats[] vbet_metrics['riverscapeArea'] * ACRES_PER_SQ_KM,
        # 'Perennial Stream length (mi)': vbet_metrics['drainageDensityPerennial'] * vbet_metrics['catchmentArea'] * 0.621371,  # km to mi
        # 'Non-Perennial Stream length (mi)': intermittent_length + ephemeral_length,
        # 'Area of Perennial Riverscape (mi)': peren_   # Sum segment_area for DGOs filtered for FCode 46006 + 55800 (sq m)
        # 'Area of Non-Perennial Riverscape (mi)':  # Sum segment_area for DGOs filtered NOT (46006 + 55800) (sq m)
        # 'Riparian area in perennial riverscape (acres)': sum(PROP_RIP * segment_area) filtered by FCode(sq m)
        # 'Riparian area in non-perennial riverscape (acres)': sum(PROP_RIP * segment_area) filtered by NOT FCode(sq m),
        # }

        # natural_waterbodies
        # TODO: sum waterbodty areas for FCode (361%, 390%, 466%, 493%) anything begining with these digits

        # artificial_waterbodies
        # TODO: sum waterbodty areas for FCode (436%)

        # count of dams is count of artifical waterbodies

        table_wrapper3 = ET.Element('div', attrib={'class': 'tableWrapper'})
        # self.create_table_from_dict(s2_metrics, table_wrapper3)
        s2_section.append(table_wrapper3)

    pass

    # self.report_content()

    # def report_content(self):
    #     if self.filter_name is not None:
    #         section_filters = self.section('Filters', 'Filters')
    #         self.filters_section(section_filters)

    #     realization = self.xml_project.XMLBuilder.find('Realizations').find('Realization')

    #     section_in = self.section('Inputs', 'Inputs')
    #     inputs = list(realization.find('Inputs'))
    #     for lyr in inputs:
    #         if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
    #             self.layerprint(lyr, section_in, self.project_root)

    #     section_inter = self.section('Intermediates', 'Intermediates')
    #     intermediates = list(realization.find('Intermediates'))
    #     for lyr in intermediates:
    #         if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
    #             self.layerprint(lyr, section_inter, self.project_root)

    #     section_out = self.section('Outputs', 'Outputs')
    #     outputs = list(realization.find('Outputs'))
    #     self.metrics_section(section_out)
    #     for lyr in outputs:
    #         if lyr.tag in ['DEM', 'Raster', 'Vector', 'Geopackage']:
    #             self.layerprint(lyr, section_out, self.project_root)

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

        metrics = {
            "Perennial Stream Length": [f"{rsc_metrics['flowlineLengthPerennialKm']:,.2f} km", f"{rsc_metrics['flowlineLengthPerennialKm'] * MILES_PER_KM:,.2f} miles", f"{100 * rsc_metrics['flowlineLengthPerennialKm'] / rsc_metrics['flowlineLengthAllKm']:,.2f} %"],
            "Intermittent Stream Length": [f"{rsc_metrics['flowlineLengthIntermittentKm']:,.2f} km", f"{rsc_metrics['flowlineLengthIntermittentKm'] * MILES_PER_KM:,.2f} miles", f"{100 * rsc_metrics['flowlineLengthIntermittentKm'] / rsc_metrics['flowlineLengthAllKm']:,.2f} %"],
            "Ephemeral Stream Length": [f"{rsc_metrics['flowlineLengthEphemeralKm']:,.2f} km", f"{rsc_metrics['flowlineLengthEphemeralKm'] * MILES_PER_KM:,.2f} miles", f"{100 * rsc_metrics['flowlineLengthEphemeralKm'] / rsc_metrics['flowlineLengthAllKm']:,.2f} %"],
            "Canal Length": [f"{rsc_metrics['flowlineLengthCanalsKm']:,.2f} km", f"{rsc_metrics['flowlineLengthCanalsKm'] * MILES_PER_KM:,.2f} miles", f"{100 * rsc_metrics['flowlineLengthCanalsKm'] / rsc_metrics['flowlineLengthAllKm']:,.2f} %"],
            "Total Stream Length": [f"{rsc_metrics['flowlineLengthAllKm']:,.2f} km", f"{rsc_metrics['flowlineLengthAllKm'] * MILES_PER_KM:,.2f} miles"],
            'Perennial Drainage Density': [f"{rsc_metrics['drainageDensityPerennial']:,.2f} km/km²"],
            'Intermittent Drainage Density': [f"{rsc_metrics['drainageDensityIntermittent']:,.2f} km/km²"],
            'Ephemeral Drainage Density': [f"{rsc_metrics['drainageDensityEphemeral']:,.2f} km/km²"],
            'Total Drainage Density': [f"{rsc_metrics['drainageDensityAll']:,.2f} km/km²"],
        }

        table_wrapper = ET.Element('div', attrib={'class': 'tableWrapper'})
        self.create_table_from_dict_of_multiple_values(metrics, table_wrapper)
        section.append(table_wrapper)

        pie_values = [
            ('Perennial', rsc_metrics['flowlineLengthPerennialKm'], 'Perennial Stream Length'),
            ('Intermittent', rsc_metrics['flowlineLengthIntermittentKm'], 'Intermittent Stream Length'),
            ('Ephemeral', rsc_metrics['flowlineLengthEphemeralKm'], 'Ephemeral Stream Length'),
            ('Canal', rsc_metrics['flowlineLengthCanalsKm'], 'Canal Length')
        ]

        pie_path = os.path.join(self.images_dir, 'stream_type_pie.png')
        # col = [self.bratcolors[x[0]] for x in table_data]
        pie([x[1] for x in pie_values], [x[2] for x in pie_values], 'Stream Length Breakdown', None, pie_path)
        self.insert_image(section, pie_path, 'Pie Chart')

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
            land_ownership_labels[owner],
            area * SQ_KM_PER_SQ_M,
            area * SQ_MILES_PER_SQ_M,
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
            land_ownership_labels[owner],
            area * SQ_KM_PER_SQ_M,
            area * SQ_MILES_PER_SQ_M,
            100 * area / total_area,
        ) for owner, area in area_sums.items()]

        self._ownership_section(parent, 'Riverscape Ownership', display_data)

    def _ownership_section(self, parent, title, data) -> None:

        title_ns = title.replace(' ', '')
        section = self.section(title_ns, title, parent, level=2)

        total_area = sum([x[1] for x in data])

        table_data = [(
            owner,
            f'{areakm:,.2f} km²',
            f'{areami:,.2f} mi²',
            f'{percent:,.2f} %'
        ) for owner, areakm, areami, percent in data]

        sorted_table_data = sorted(table_data, key=lambda x: x[0])
        sorted_raw_data = sorted(data, key=lambda x: x[0])

        self.create_table_from_tuple_list([title, 'Area (km²)', 'Area (mi²)', 'Percent (%)'], sorted_table_data, section)

        # sorted_pie_data = sorted(data, key=lambda x: x[0])

        pie_path = os.path.join(self.images_dir, f'{title_ns}_pie.png')
        pie([x[1] for x in sorted_raw_data], [x[0] for x in sorted_raw_data], f'{title} Breakdown', None, pie_path)
        self.insert_image(section, pie_path, 'Pie Chart')

        keys = [item[0] for item in sorted_raw_data]
        values = [item[1] for item in sorted_raw_data]
        labels = [key for key in keys]
        bar_path = os.path.join(self.images_dir, f'{title_ns}_bar.png')
        horizontal_bar(values, labels, None, 'Area (mi²)',  f'{title} Breakdown', bar_path)
        self.insert_image(section, bar_path, 'Bar Chart')

    def land_use(self, parent, rs_context_dir: str, vbet_dir: str, rcat_dir: str, anthro_dir: str, rme_dir: str) -> None:

        land_charts = blm_charts(rs_context_dir, vbet_dir, rcat_dir, anthro_dir, rme_dir, self.images_dir)
        # Kelly produces all the charts in one dictionary. Break them into categories

        for category in ['Land Use Intensity', 'Land Use Type']:
            section = self.section(category.replace(' ', ''), category, parent, level=2)
            for chart_name, chart_path in land_charts.items():
                if category in chart_name:
                    self.insert_image(section, chart_path, chart_name)

    def acquatic_resources(self, parent, rme_metrics) -> None:

        metrics = []

        blm_area = sum([result['sum'] for result in rme_metrics['rme']['segmentarea'] if result['owner'] == 'BLM'])
        non_area = sum([result['sum'] for result in rme_metrics['rme']['segmentarea'] if result['owner'] == 'Non-BLM'])
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

    def hydro_geomorphic(self, parent, rme_metrics):

        section = self.section('HydroGeomorphic', 'Hydro Geomorphic', parent, level=2)

        pass

    def slope_analysis(self, parent, rme_gpkg: str) -> None:

        section = self.section('SlopeAnalysis', 'Slope Analysis', parent, level=2)

        with sqlite3.connect(rme_gpkg) as conn:
            curs = conn.cursor()

            # Prepare lists for the BLM and Non-BLM data
            blm_lengths = []
            blm_gradients = []
            non_blm_lengths = []
            non_blm_gradients = []

            # Loop over ownership and flow categories
            for owner, owner_filter in [('BLM', " = 'BLM'"), ('Non-BLM', " <> 'BLM'")]:
                for flow, flow_filter in [('Perennial', " IN (46003, 55800)"), ('Non-Perennial', " NOT IN (46003, 55800)")]:

                    curs.execute(f'''
                        SELECT
                            nhd_dgo_streamlength, rme_igo_prim_channel_gradient
                        FROM rme_igos
                        WHERE (rme_igo_prim_channel_gradient IS NOT NULL)
                            AND (rme_dgo_ownership {owner_filter})
                            AND (FCode {flow_filter})''')

                    slope_data = [(row[0] * MILES_PER_M, row[1] * 100.0) for row in curs.fetchall()]

                    if len(slope_data) == 0:
                        continue

                    lengths, gradients = zip(*slope_data)

                    # Store data for BLM and Non-BLM separately
                    if owner == 'BLM':
                        blm_lengths.extend(lengths)
                        blm_gradients.extend(gradients)
                    else:
                        non_blm_lengths.extend(lengths)
                        non_blm_gradients.extend(gradients)

            # Now calculate the histogram with both data sets
            num_bins = 5
            bin_edges = np.linspace(min(min(blm_gradients), min(non_blm_gradients)),
                                    max(max(blm_gradients), max(non_blm_gradients)),
                                    num_bins + 1)

            # Sum stream lengths for each gradient bin
            blm_bin_sums = np.histogram(blm_gradients, bins=bin_edges, weights=blm_lengths)[0]
            non_blm_bin_sums = np.histogram(non_blm_gradients, bins=bin_edges, weights=non_blm_lengths)[0]

            # Create labels for the bin ranges (e.g., "0-5%", "5-10%", etc.)
            bin_labels = [f'{int(bin_edges[i]):d}-{int(bin_edges[i+1]):d}%' for i in range(len(bin_edges)-1)]

            # Create the bar chart with two series
            plt.bar(bin_labels, blm_bin_sums, width=0.4, label='BLM', color='blue', align='center')  # , edgecolor='black'
            plt.bar(bin_labels, non_blm_bin_sums, width=0.4, label='Non-BLM', color='green', align='edge')  # , edgecolor='black'

            # Add labels and title
            plt.xlabel('Slope (%)')
            plt.ylabel('Summed Stream Lengths (miles)')
            plt.title('Stream Lengths Summed by Slope Bins (BLM & Non-BLM)')

            # Add a legend to differentiate the two series
            plt.legend()

            # Save the combined chart as an image
            img_path = os.path.join(self.images_dir, 'combined_slope_histogram_with_ranges.png')
            plt.tight_layout()  # Adjust layout to avoid label clipping
            plt.savefig(img_path, bbox_inches="tight")
            plt.close()

            # Insert the image into your report or interface
            self.insert_image(section, img_path, 'Combined Slope Histogram (BLM & Non-BLM)')

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

            fig, ax = plt.subplots(figsize=(10, 6))

            # Plot bars for BLM and Non-BLM data, grouped by stream order
            ax.bar(index - bar_width / 2, blm_lengths, bar_width, label='BLM', color='blue')  # , edgecolor='black')
            ax.bar(index + bar_width / 2, non_blm_lengths, bar_width, label='Non-BLM', color='green')  # , edgecolor='black')

            # Add labels, title, and legend
            ax.set_xlabel('Stream Order')
            ax.set_ylabel('Stream Length (miles)')
            ax.set_title('Stream Order Lengths for BLM and Non-BLM')
            ax.set_xticks(index)
            ax.set_xticklabels([str(order+1) for order in range(len(blm_lengths))])  # , rotation=45)
            ax.legend()

            # Save the chart as an image
            img_path = os.path.join(self.images_dir, 'combined_stream_order_bar_chart.png')
            plt.tight_layout()  # Adjust layout to prevent clipping of labels
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

                plt.bar(bin_midpoints - bar_width / 2, bin_sums_blm, width=bar_width, color='blue', label='BLM', edgecolor='black')
                plt.bar(bin_midpoints + bar_width / 2, bin_sums_non_blm, width=bar_width, color='green', label='Non-BLM', edgecolor='black')

                # Add labels, legend, and grid
                plt.xlabel('Sinuosity')
                plt.ylabel('Summed Stream Lengths (miles)')
                plt.title('Stream Lengths Summed by Sinuosity Bins (BLM vs Non-BLM)')
                plt.grid(axis='y', linestyle='--', alpha=0.7)
                plt.legend()

                # Save the chart as an image
                img_path = os.path.join(self.images_dir, 'combined_sinuosity_histogram.png')
                plt.savefig(img_path, bbox_inches="tight")
                plt.close()

                self.insert_image(section, img_path, 'Combined Sinuosity Histogram')

    def valley_bottom_density(self, parent, rme_gpkg: str) -> None:

        pass

    def vegetation(self, biophysical_section, rcat_dir):

        vegetation_section = self.section('Vegetation', 'Vegetation', biophysical_section, level=2)
        charts_dict = vegetation_charts(rcat_dir, self.images_dir)

        for key, val in charts_dict.items():
            self.insert_image(vegetation_section, val, key)


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
    report.write(title=f'Watershed Condition Assessment Report For HUC: {args.huc}')

    # except Exception as e:
    #     log.error(e)
    #     traceback.print_exc(file=sys.stdout)
    #     sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
