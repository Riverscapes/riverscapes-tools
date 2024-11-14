import os
import sys
import traceback
import argparse
import sqlite3
import json
from collections import Counter
from xml.etree import ElementTree as ET
from shapely.geometry import shape
from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import box_plot, vertical_bar
from rme.__version__ import __version__
import time
from collections import Counter

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
from rscommons.raster_buffer_stats import raster_buffer_stats2

from rme.__version__ import __version__
from rme.analysis_window import AnalysisLine

from .utils.hypsometric_curve import hipsometric_curve


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

        ws_context_section = self.section('WSContext', 'Watershed Context')

        physio_section = self.section('Physiography', 'Physiographic Attributes', ws_context_section)

        rs_context_dir = os.path.join(input_dir, 'rs_context')
        rs_context_json = os.path.join(rs_context_dir, 'rscontext_metrics.json')
        rsc_metrics_json = json.load(open(rs_context_json, encoding='utf-8'))
        rsc_nhd_gpkg = os.path.join(rs_context_dir, 'hydrology', 'nhdplushr.gpkg')

        table_wrapper2 = ET.Element('div', attrib={'class': 'tableWrapper'})

        rsc_metrics_dict = {
            'Catchment Length (km)': rsc_metrics_json['catchmentLength'],
            'Catchment Area (km^2)': rsc_metrics_json['catchmentArea'],
            'Catchment Perimeter (km)': rsc_metrics_json['catchmentPerimeter'],
            'Circularity Ratio': rsc_metrics_json['circularityRatio'],
            'Elongation Ratio': rsc_metrics_json['elongationRatio'],
            'Form Factor': rsc_metrics_json['formFactor'],
            'Catchment Relief (m)': rsc_metrics_json['catchmentRelief'],
            'Relief Ratio': rsc_metrics_json['reliefRatio'],
            'Drainage Density (Perennial, km/km^2)': rsc_metrics_json['drainageDensityPerennial'],
            'Drainage Density (Intermittent, km/km^2)': rsc_metrics_json['drainageDensityIntermittent'],
            'Drainage Density (Ephemeral, km/km^2)': rsc_metrics_json['drainageDensityEphemeral'],
            'Drainage Density (Total, km/km^2)': rsc_metrics_json['drainageDensityAll']
        }

        rme_dir = os.path.join(input_dir, 'rme')
        rme_output_gpkg = os.path.join(rme_dir, 'outputs', 'riverscapes_metrics.gpkg')
        rme_metric_values = get_rme_values(os.path.join(rs_context_dir, rme_output_gpkg))

        for key, val in rme_metric_values.items():
            rsc_metrics_dict[key] = val

        precip_stats = get_precipiation_stats(rsc_nhd_gpkg, os.path.join(rs_context_dir, 'climate', 'precipitation.tif'))

        rsc_metrics_dict['Mean Annual Precipitation (mm)'] = precip_stats['Mean']
        rsc_metrics_dict['Maximum Annual Precipitation (mm)'] = precip_stats['Maximum']
        rsc_metrics_dict['Minimum Annual Precipitation (mm)'] = precip_stats['Minimum']

        self.create_table_from_dict(rsc_metrics_dict, table_wrapper2)
        physio_section.append(table_wrapper2)

        nhd_gpkg = os.path.join(rs_context_dir, 'hydrology', 'nhdplushr.gpkg')
        nhd_gpkg_layer = 'WBDHU10'
        dem_path = os.path.join(rs_context_dir, 'topography', 'dem.tif')
        hipso_curve_path = os.path.join(self.images_dir, 'hypsometric_curve.png')
        hipsometric_curve(hipso_curve_path, nhd_gpkg, nhd_gpkg_layer, dem_path)

        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        relative_dir = os.path.dirname(self.images_dir)
        if os.path.basename(relative_dir) == 'images':
            relative_dir = os.path.dirname(relative_dir)
        image_src = os.path.relpath(hipso_curve_path, relative_dir)

        img = ET.Element('img', attrib={
            'src': image_src,
            'alt': 'chart'
        })

        plot_wrapper.append(img)
        ws_context_section.append(plot_wrapper)

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


def get_rme_values(rme_gpkg: str) -> dict:

    rme_values = {}
    with sqlite3.connect(rme_gpkg) as conn:
        curs = conn.cursor()

        # Modal Ecoregion III (metric ID 17)
        for level, metric_id in [('Ecoregion III', 17), ('Ecoregion IV', 18)]:
            curs.execute('SELECT metric_value, count(*) frequency FROM dgo_metric_values WHERE metric_id= ? GROUP BY metric_value ORDER by frequency DESC limit 1', [metric_id])
            row = curs.fetchone()
            rme_values[level] = row[0]

    return rme_values


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


def get_precipiation_stats(nhd_gpkg: str, precip_raster: str) -> float:

    watershed_boundary = get_watershed_boundary_geom(nhd_gpkg)

    stats = raster_buffer_stats2({1: watershed_boundary}, precip_raster)
    return stats[1]


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
