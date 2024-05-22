"""Build a report for the channel area
"""

import argparse
import os
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
        p1.text = 'The Channel Area Tool is a simple tool for generating polygons representing the spatial extent of the drainage network within a watershed. The primary purpose for the tool is that the outputs it produces are used as inputs in other Riverscapes tools. Geospatial tools often use a simple line network to represent streams. Depending on the functions a tool is performing, this can be problematic as a line can represent both a narrow, first order stream as well as large, wide rivers. Many Riverscapes tools analyze areas outside of the channel (for example, to look at streamside vegetation), therefore an accurate representation of the actual channel, not simply a line, is necessary. The tool is comprised of a simple algorithm for combining polygons representing channels with polygons derived from attributes on a drainage network (line). As long as a drainage network has an attribute recording the upstream contributing drainage area for each segment, regional relationships relating channel width to drainage area can be used to buffer the channel segments, and the resulting polygons can be merged with any other available polygons. This gives a first order approximation of the active channel area. As channels are active and constantly moving through time, greater accuracy can be achieved with more recent, high resolution datasets, or with user input (e.g., editing channel positions or channel polygons). For more information, refer to '
        aEl1 = ET.SubElement(p1, 'a', {'href': 'https://tools.riverscapes.net/channelarea/'})
        aEl1.text = 'documentation.'
        self.out_section.append(p1)
        self.outputs_content()

        in_section = self.section('Inputs', 'Inputs')
        p1in = ET.Element('p')
        p2in = ET.Element('p')
        p1in.text = 'The inputs to the Channel Area tool are a drainage network (polyline) layer, and polygons representing river channels.'
        p2in.text = 'The default inputs for the tool are the NHD Flowline, NHD Area, and NHD Waterbody feature classes.'
        in_section.append(p1in)
        in_section.append(p2in)

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

    def custom_table(self, table_contents, el_parent, header_names=None):
        table = ET.Element('table')

        if header_names is not None:
            thead = ET.Element('thead')
            theadrow = ET.Element('tr')
            thead.append(theadrow)
            table.append(thead)

            for name in header_names:
                th = ET.Element('th')
                th.text = name
                theadrow.append(th)

        tbody = ET.Element('tbody')
        table.append(tbody)

        for key, val in table_contents.items():

            tr = ET.Element('tr')
            tbody.append(tr)

            th = ET.Element('th')
            th.text = key
            tr.append(th)

            for item in val:
                # If the value is a URL, make it a link
                if isinstance(item, str) and item.startswith("http"):
                    td = ET.Element('td', attrib={'class': 'text url'})
                    a = ET.Element('a', attrib={'href': item})
                    a.text = item
                    td.append(a)
                else:
                    item, class_name = RSReport.format_value(item)
                    td = ET.Element('td', attrib={'class': class_name})
                    td.text = item
                tr.append(td)

        el_parent.append(table)

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
        table_contents = {
            lyr_label_dict[i]: [v, v / 4046.8564, v / 10000]  # m^2, acres, hectares
            for i, v in poly_areas.items()
        }
        self.custom_table(table_contents, section, header_names=['Data Source', 'Area (m^2)', 'Area (acres)', 'Area (hectares)'])

        pie_path = os.path.join(self.images_dir, 'area_breakdown.png')
        pie(
            [values for key, values in poly_areas.items()],
            [lyr_label_dict[key] for key in poly_areas],
            "",
            [self.colors[key] for key in poly_areas],
            pie_path
        )

        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'src': os.path.join(os.path.basename(self.images_dir), os.path.basename(pie_path)),
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
