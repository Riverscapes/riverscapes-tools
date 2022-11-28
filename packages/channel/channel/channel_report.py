"""Build a report for the channel area
"""

import argparse
import os
import sqlite3
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject, VectorBase, get_shp_or_gpkg
from rscommons.classes.vector_base import get_utm_zone_epsg
# from rscommons.util import safe_makedirs, sizeof_fmt
from rscommons.plotting import pie

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
        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')

        # references to colors
        self.colors = {
            'flowarea_filtered': '#20908d',
            'waterbody_filtered': '#4fbfe7',
            'difference_polygons': '#f3a6b2',
            'other_channel': '#8eb4f0'
        }

        self.out_section = self.section('Outputs', 'Outputs')
        p1 = ET.Element('p')
        p1.text = 'The Channel Area Tool is a simple tool for aggregating polygons from different data sources into a single feature class. '
        'The output of the Channel Area Tool is a polygon layer representing the bankfull width of a channel network. See documentation for the tool '
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

        poly_areas = {}
        lyr_label_dict = {'flowarea_filtered': 'NHD Flow Area', 'waterbody_filtered': 'NHD Waterbody', 'difference_polygons': 'Buffered NHD Flowlines', 'other_channels': 'Custom Polygons'}

        intlayers = ['flowarea_filtered', 'waterbody_filtered', 'difference_polygons']
        intpath = os.path.join(self.xml_project.project_dir, 'intermediates/intermediates.gpkg')
        inlayers = ['other_channels']
        inpath = os.path.join(self.xml_project.project_dir, 'inputs/inputs.gpkg')

        for lyr_label in intlayers:
            with get_shp_or_gpkg(intpath, layer_name=lyr_label) as lyr:
                if lyr.ogr_layer:
                    longitude = lyr.ogr_layer.GetExtent()[0]
                    proj_epsg = get_utm_zone_epsg(longitude)
                    sref, transform = VectorBase.get_transform_from_epsg(lyr.spatial_ref, proj_epsg)
                    area = 0
                    for feat, fid, _ in lyr.iterate_features():
                        feature = VectorBase.ogr2shapely(feat, transform)
                        if not feature.is_valid:
                            feature = feature.buffer(0)
                        area += feature.area

                    poly_areas[lyr_label] = float('{:.2f}'.format(area))

        for lyr_label in inlayers:
            with get_shp_or_gpkg(inpath, layer_name=lyr_label) as lyr:
                if lyr.ogr_layer:
                    longitude = lyr.ogr_layer.GetExtent()[0]
                    proj_epsg = get_utm_zone_epsg(longitude)
                    sref, transform = VectorBase.get_transform_from_epsg(lyr.spatial_ref, proj_epsg)
                    area = 0
                    for feat, fid, _ in lyr.iterate_features():
                        feature = VectorBase.ogr2shapely(feat, transform)
                        if not feature.is_valid:
                            feature = feature.buffer(0)
                        area += feature.area

                    poly_areas[lyr_label] = float('{:.2f}'.format(area))

        section = self.section('AreaBreakdown', 'Data Source Breakdown', el_parent=self.out_section, level=2)
        table_dict = {lyr_label_dict[i]: v for i, v in poly_areas.items()}
        RSReport.create_table_from_dict(table_dict, section)
        pie_path = os.path.join(self.images_dir, 'area_breakdown.png')
        pie([values for key, values in poly_areas.items()], [lyr_label_dict[key] for key in poly_areas.keys()], "title", [self.colors[key] for key in poly_areas.keys()], pie_path)
        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(pie_path)),
            'alt': 'pie_chart'
        })
        img_wrap.append(img)
        plot_wrapper.append(img_wrap)
        section.append(plot_wrapper)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/RSContext.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = ChannelReport(args.report_path, project)
    report.write()
