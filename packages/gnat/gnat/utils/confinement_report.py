import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET
from collections import OrderedDict

from rscommons import Logger, dotenv, ModelConfig
from rscommons.util import safe_makedirs
from rscommons.report_common import create_report, write_report, header, format_value, dict_factory
from rscommons.report_common import create_table_from_tuple_list, create_table_from_sql, create_table_from_dict
from rscommons.plotting import xyscatter, box_plot, histogram
from gnat.__version__ import __version__


def confinement_report(database, report_path, huc):

    html, images_dir, inner_div = create_report(database, report_path, 'Confinement for {}'.format(huc))

    report_intro(database, images_dir, inner_div, 'Confinement', __version__, huc)
    # reach_attribute_summary(database, images_dir, inner_div)

    raw_confinement(database, inner_div)
    [confinement_ratio(database, images_dir, field, field, inner_div) for field in ['Confinement_Ratio', 'Constriction_Ratio']]

    write_report(html, report_path)


def report_intro(database, images_dir, elParent, tool_name, version, huc):
    wrapper = ET.Element('div', attrib={'id': 'ReportIntro'})
    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory
    curs = conn.cursor()
    # watershed = curs.execute('SELECT WatershedID, Name FROM Watersheds LIMIT 1').fetchone()

    header_bar = ET.Element('div', attrib={'id': 'HeaderBar'})
    wrapper.append(header_bar)

    header(1, '{} for {}'.format(tool_name, huc), header_bar)
    header(4, 'Model Version: {}'.format(version), header_bar)

    # table_of_contents(wrapper)

    header(2, 'Introduction', wrapper)

    row = curs.execute('SELECT Sum(ApproxLeng) AS TotalLength, Count(*) AS TotalReaches FROM Confinement_Ratio').fetchone()
    values = {'Number of reaches': '{0:,d}'.format(row['TotalReaches']), 'Total reach length (km)': '{0:,.0f}'.format(row['TotalLength'] / 1000), 'Total reach length (miles)': '{0:,.0f}'.format(row['TotalLength'] * 0.000621371)}

    # row = curs.execute('SELECT WatershedID "Watershed ID", W.Name "Watershed Name", E.Name Ecoregion, CAST(AreaSqKm AS TEXT) "Area (Sqkm)", States FROM Watersheds W INNER JOIN Ecoregions E ON W.EcoregionID = E.EcoregionID').fetchone()
    # values.update(row)

    table_wrapper = ET.Element('div', attrib={'class': 'tableWrapper'})
    wrapper.append(table_wrapper)

    # create_table_from_dict(values, table_wrapper, attrib={'id': 'SummTable'})

    # curs.execute('SELECT KeyInfo, ValueInfo FROM Metadata')
    # values.update({row['KeyInfo'].replace('_', ' '): row['ValueInfo'] for row in curs.fetchall()})

    # create_table_from_dict(values, table_wrapper, attrib={'id': 'SummTable'})

    # create_table_from_sql(
    #     ['Reach Type', 'Total Length (km)', '% of Total'],
    #     'SELECT ReachType, Sum(iGeo_Len) / 1000 As Length, 100 * Sum(iGeo_Len) / TotalLength AS TotalLength '
    #     'FROM vwReaches INNER JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM Reaches) GROUP BY ReachType',
    #     database, table_wrapper, attrib={'id': 'SummTable'})

    elParent.append(wrapper)


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


# def dam_capacity_lengths(database, elParent, capacity_field):

#     conn = sqlite3.connect(database)
#     curs = conn.cursor()

#     curs.execute('SELECT Name, MaxCapacity FROM DamCapacities ORDER BY MaxCapacity')
#     bins = [(row[0], row[1]) for row in curs.fetchall()]

#     curs.execute('SELECT Sum(iGeo_Len) / 1000 FROM Reaches')
#     total_length_km = curs.fetchone()[0]

#     data = []
#     last_bin = 0
#     cumulative_length_km = 0
#     for name, max_capacity in bins:
#         curs.execute('SELECT Sum(iGeo_len) / 1000 FROM Reaches WHERE {} <= {}'.format(capacity_field, max_capacity))
#         rowi = curs.fetchone()
#         if not rowi or rowi[0] is None:
#             bin_km = 0
#         else:
#             bin_km = rowi[0] - cumulative_length_km
#             cumulative_length_km = rowi[0]
#         data.append((
#             '{}: {} - {}'.format(name, last_bin, max_capacity),
#             bin_km,
#             bin_km * 0.621371,
#             100 * bin_km / total_length_km
#         ))

#         last_bin = max_capacity

#     data.append(('Total', cumulative_length_km, cumulative_length_km * 0.621371, 100 * cumulative_length_km / total_length_km))
#     create_table_from_tuple_list((capacity_field, 'Stream Length (km)', 'Stream Length (mi)', 'Percent'), data, elParent)


def confinement_ratio(database, images_dir, db_field, label, elParent):

    wrapper = ET.Element('div', attrib={'id': 'ratio_{}'.format(db_field)})
    header(3, label, wrapper)

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute('SELECT {} FROM Confinement_Ratio'.format(db_field))
    data = [row[0] for row in curs.fetchall()]

    image_path = os.path.join(images_dir, '{}.png'.format(db_field.lower()))
    histogram(data, 10, image_path)

    img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
    img = ET.Element('img', attrib={'class': 'boxplot', 'src': '{}/{}'.format(os.path.basename(images_dir), os.path.basename(image_path))})
    img_wrap.append(img)
    wrapper.append(img_wrap)

    elParent.append(wrapper)

# def reach_attribute_summary(database, images_dir, elParent):
#     wrapper = ET.Element('div', attrib={'id': 'ReachAttributeSummary'})
#     header(2, 'Geophysical Attributes', wrapper)

#     attribs = [
#         ('iGeo_Slope', 'Slope', 'ratio'),
#         ('iGeo_ElMax', 'Max Elevation', 'metres'),
#         ('iGeo_ElMin', 'Min Elevation', 'metres'),
#         ('iGeo_Len', 'Length', 'metres'),
#         ('iGeo_DA', 'Drainage Area', 'Sqkm')

#     ]
#     plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
#     [reach_attribute(database, attribute, units, images_dir, plot_wrapper) for attribute, name, units in attribs]

#     wrapper.append(plot_wrapper)
#     elParent.append(wrapper)


# def ownership(database, elParent):
#     wrapper = ET.Element('div', attrib={'class': 'Ownership'})
#     header(2, 'Ownership', wrapper)

#     create_table_from_sql(
#         ['Ownership Agency', 'Number of Reach Segments', 'Length (km)', '% of Total Length'],
#         'SELECT IFNULL(Agency, "None"), Count(ReachID), Sum(iGeo_Len) / 1000, 100* Sum(iGeo_Len) / TotalLength FROM vwReaches'
#         ' INNER JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM Reaches) GROUP BY Agency',
#         database, wrapper)

#     elParent.append(wrapper)


def raw_confinement(database, elParent):

    keys = OrderedDict()
    keys['Left'] = {'label': 'Left Confined', 'length': 0.0, 'percent': 0.0}
    keys['Right'] = {'label': 'Right Confinement', 'length': 0.0, 'percent': 0.0}
    keys['None'] = {'label': 'Unconfined', 'length': 0.0, 'percent': 0.0}
    keys['Both'] = {'label': 'Constricted - Both Left and Right Confined', 'length': 0.0, 'percent': 0.0}
    keys['Total'] = {'label': 'Total', 'length': 0.0, 'percent': 100.0}
    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory

    curs = conn.cursor()
    curs.execute("""SELECT Confinement_Type, (TypeLength / 1000.0) TypeLength, (100.0 * TypeLength / TotalLength) Ratio, (TotalLength / 1000.0) TotalLength FROM
        (SELECT Confinement_Type, Sum(ApproxLeng) TypeLength FROM Confinement_Raw GROUP BY Confinement_Type)  JOIN
        (SELECT Sum(ApproxLeng) TotalLength FROM Confinement_Raw)""")

    for row in curs.fetchall():
        item = keys[row['Confinement_Type']]
        item['length'] = row['TypeLength']
        item['percent'] = row['Ratio']

        keys['Total']['length'] = row['TotalLength']

    table_data = [(val['label'], val['length'], val['percent']) for val in keys.values()]
    create_table_from_tuple_list(['Type of Confinement', 'Length (km)', 'Percent'], table_data, elParent)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the confinement geopackage', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    parser.add_argument('huc', help='HUC', type=str)
    args = dotenv.parse_args_env(parser)

    confinement_report(args.database, args.report_path, args.huc)


if __name__ == '__main__':
    main()
