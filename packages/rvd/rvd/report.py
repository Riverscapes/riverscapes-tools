import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig
from rscommons.util import safe_makedirs
from rscommons.report_common import create_report, write_report, report_intro, header, format_value, reach_attribute, dict_factory
from rscommons.report_common import create_table_from_tuple_list, create_table_from_sql, create_table_from_dict
from rscommons.plotting import xyscatter, box_plot
from rvd.__version__ import __version__

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/RVD.xsd', __version__)

idCols = [
    'VegetationID',
    'Type ID'
]


def report(database, report_path):

    html, images_dir, inner_div = create_report(database, report_path)

    report_intro(database, images_dir, inner_div, 'Riparian Vegetation Departure (RVD)', cfg.version)
    reach_attribute_summary(database, images_dir, inner_div)

    # TODO: Insert report sections here

    write_report(html, report_path)


def table_of_contents(elParent):
    wrapper = ET.Element('div', attrib={'id': 'TOC'})
    header(3, 'Table of Contents', wrapper)

    ul = ET.Element('ul')

    li = ET.Element('li')
    ul.append(li)

    anchor = ET.Element('a', attrib={'href': '#ownership'})
    anchor.text = 'Ownership'
    li.append(anchor)

    elParent.append(wrapper)


def dam_capacity(database, elParent):

    header(2, 'BRAT Dam Capacity Results', elParent)

    fields = [
        ('Existing complex size', 'Sum(mCC_EX_CT)'),
        ('Historic complex size', 'Sum(mCC_HPE_CT)'),
        ('Existing vegetation capacity', 'Sum((iGeo_len / 1000) * oVC_EX)'),
        ('Historic vegetation capacity', 'Sum((iGeo_len / 1000) * oVC_HPE)'),
        ('Existing capacity', 'Sum((iGeo_len / 1000) * oCC_EX)'),
        ('Historic capacity', 'Sum((iGeo_len / 1000) * oCC_HPE)')
    ]

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT {} FROM Reaches'.format(', '.join([field for label, field in fields])))
    row = curs.fetchone()

    table_dict = {fields[i][0]: row[i] for i in range(len(fields))}
    create_table_from_dict(table_dict, elParent)


def reach_attribute_summary(database, images_dir, elParent):
    wrapper = ET.Element('div', attrib={'id': 'ReachAttributeSummary'})
    header(2, 'Geophysical Attributes', wrapper)

    attribs = [
        ('iGeo_Slope', 'Slope', 'ratio'),
        ('iGeo_ElMax', 'Max Elevation', 'metres'),
        ('iGeo_ElMin', 'Min Elevation', 'metres'),
        ('iGeo_Len', 'Length', 'metres'),
        ('iGeo_DA', 'Drainage Area', 'Sqkm')

    ]
    plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
    [reach_attribute(database, attribute, units, images_dir, plot_wrapper) for attribute, name, units in attribs]

    wrapper.append(plot_wrapper)
    elParent.append(wrapper)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the BRAT database', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    report(args.database, args.report_path)


if __name__ == '__main__':
    main()
