import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig
from rscommons.util import safe_makedirs
from rscommons.report_common import create_report, write_report, report_intro, header, format_value, reach_attribute, dict_factory
from rscommons.report_common import create_table_from_tuple_list, create_table_from_sql, create_table_from_dict
from rscommons.plotting import xyscatter, box_plot
from gnat.__version__ import __version__


def report(database, report_path):

    html, images_dir, inner_div = create_report(database, report_path)

    report_intro(database, images_dir, inner_div, 'Confinement', __version__)
    reach_attribute_summary(database, images_dir, inner_div)

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


def dam_capacity_lengths(database, elParent, capacity_field):

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT Name, MaxCapacity FROM DamCapacities ORDER BY MaxCapacity')
    bins = [(row[0], row[1]) for row in curs.fetchall()]

    curs.execute('SELECT Sum(iGeo_Len) / 1000 FROM Reaches')
    total_length_km = curs.fetchone()[0]

    data = []
    last_bin = 0
    cumulative_length_km = 0
    for name, max_capacity in bins:
        curs.execute('SELECT Sum(iGeo_len) / 1000 FROM Reaches WHERE {} <= {}'.format(capacity_field, max_capacity))
        rowi = curs.fetchone()
        if not rowi or rowi[0] is None:
            bin_km = 0
        else:
            bin_km = rowi[0] - cumulative_length_km
            cumulative_length_km = rowi[0]
        data.append((
            '{}: {} - {}'.format(name, last_bin, max_capacity),
            bin_km,
            bin_km * 0.621371,
            100 * bin_km / total_length_km
        ))

        last_bin = max_capacity

    data.append(('Total', cumulative_length_km, cumulative_length_km * 0.621371, 100 * cumulative_length_km / total_length_km))
    create_table_from_tuple_list((capacity_field, 'Stream Length (km)', 'Stream Length (mi)', 'Percent'), data, elParent)

def confinement_ratio(database, attribute, label):


    





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


def ownership(database, elParent):
    wrapper = ET.Element('div', attrib={'class': 'Ownership'})
    header(2, 'Ownership', wrapper)

    create_table_from_sql(
        ['Ownership Agency', 'Number of Reach Segments', 'Length (km)', '% of Total Length'],
        'SELECT IFNULL(Agency, "None"), Count(ReachID), Sum(iGeo_Len) / 1000, 100* Sum(iGeo_Len) / TotalLength FROM vwReaches'
        ' INNER JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM Reaches) GROUP BY Agency',
        database, wrapper)

    elParent.append(wrapper)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the confinement geopackage', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    report(args.database, args.report_path)


if __name__ == '__main__':
    main()
