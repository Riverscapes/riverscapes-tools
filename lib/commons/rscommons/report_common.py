import argparse
import sqlite3
import os
import re
from xml.etree import ElementTree as ET
from xml.dom import minidom

from rscommons import Logger, dotenv, ModelConfig
from rscommons.util import safe_makedirs
from rscommons.plotting import xyscatter, box_plot

idCols = [
    'VegetationID',
    'Type ID'
]


def create_report(database, report_path):

    log = Logger('Report')
    log.info('Creating report at {}'.format(report_path))

    images_dir = os.path.join(os.path.dirname(report_path), 'images')

    if os.path.isfile(report_path):
        os.remove(report_path)

    safe_makedirs(images_dir)

    html = ET.Element('html')
    html_header(database, html)

    body = ET.Element('body')
    # Add in our CSS
    body.append(get_css())

    container_div = ET.Element('div', attrib={'id': 'ReportContainer'})
    inner_div = ET.Element('div', attrib={'id': 'ReportInner'})
    # A little div wrapping will help us out later
    html.append(body)
    html.append(container_div)
    container_div.append(inner_div)

    return html, images_dir, inner_div


def write_report(html, report_path):

    xmlstr = minidom.parseString(ET.tostring(html)).toprettyxml(indent="   ")
    with open(report_path, "w") as f:
        f.write(xmlstr)

    log = Logger('Report')
    log.info('Report complete')


def get_css():
    style_tag = ET.Element('style')

    css_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'report.css')
    with open(css_path) as css_file:
        css = css_file.read()
        css = re.sub(r'\n', '\n        ', css)
        style_tag.text = css
    return style_tag


def html_header(database, elParent):
    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory
    curs = conn.cursor()
    watershed = curs.execute('SELECT WatershedID, Name FROM Watersheds LIMIT 1').fetchone()
    head = ET.Element('head')

    title = ET.Element('title')
    title.text = 'BRAT for {} - {}'.format(watershed['WatershedID'], watershed['Name'])

    head.append(title)
    elParent.append(head)


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


def report_intro(database, images_dir, elParent, tool_name, version):
    wrapper = ET.Element('div', attrib={'id': 'ReportIntro'})
    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory
    curs = conn.cursor()
    watershed = curs.execute('SELECT WatershedID, Name FROM Watersheds LIMIT 1').fetchone()

    header_bar = ET.Element('div', attrib={'id': 'HeaderBar'})
    wrapper.append(header_bar)

    header(1, '{} for {} - {}'.format(tool_name, watershed['WatershedID'], watershed['Name']), header_bar)
    header(4, 'Model Version: {}'.format(version), header_bar)

    # table_of_contents(wrapper)

    header(2, 'Introduction', wrapper)

    row = curs.execute('SELECT Sum(iGeo_Len) AS TotalLength, Count(ReachID) AS TotalReaches FROM Reaches').fetchone()
    values = {'Number of reaches': '{0:,d}'.format(row['TotalReaches']), 'Total reach length (km)': '{0:,.0f}'.format(row['TotalLength'] / 1000), 'Total reach length (miles)': '{0:,.0f}'.format(row['TotalLength'] * 0.000621371)}

    row = curs.execute('SELECT WatershedID "Watershed ID", W.Name "Watershed Name", E.Name Ecoregion, CAST(AreaSqKm AS TEXT) "Area (Sqkm)", States FROM Watersheds W INNER JOIN Ecoregions E ON W.EcoregionID = E.EcoregionID').fetchone()
    values.update(row)

    table_wrapper = ET.Element('div', attrib={'class': 'tableWrapper'})
    wrapper.append(table_wrapper)

    # create_table_from_dict(values, table_wrapper, attrib={'id': 'SummTable'})

    curs.execute('SELECT KeyInfo, ValueInfo FROM Metadata')
    values.update({row['KeyInfo'].replace('_', ' '): row['ValueInfo'] for row in curs.fetchall()})

    create_table_from_dict(values, table_wrapper, attrib={'id': 'SummTable'})

    create_table_from_sql(
        ['Reach Type', 'Total Length (km)', '% of Total'],
        'SELECT ReachType, Sum(iGeo_Len) / 1000 As Length, 100 * Sum(iGeo_Len) / TotalLength AS TotalLength '
        'FROM vwReaches INNER JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM Reaches) GROUP BY ReachType',
        database, table_wrapper, attrib={'id': 'SummTable'})

    elParent.append(wrapper)


def reach_attribute(database, attribute, units, images_dir, elParent):
    # Use a class here because it repeats
    wrapper = ET.Element('div', attrib={'class': 'reachAtribute'})
    header(3, attribute, wrapper)

    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory
    curs = conn.cursor()

    # Summary statistics (min, max etc) for the current attribute
    curs.execute('SELECT Count({0}) "Values", Max({0}) Maximum, Min({0}) Minimum, Avg({0}) Average FROM Reaches WHERE {0} IS NOT NULL'.format(attribute))
    values = curs.fetchone()

    reach_wrapper_inner = ET.Element('div', attrib={'class': 'reachAtributeInner'})
    wrapper.append(reach_wrapper_inner)

    # Add the number of NULL values
    curs.execute('SELECT Count({0}) "NULL Values" FROM Reaches WHERE {0} IS NULL'.format(attribute))
    values.update(curs.fetchone())
    create_table_from_dict(values, reach_wrapper_inner)

    # Box plot
    image_path = os.path.join(images_dir, 'attribute_{}.png'.format(attribute))
    curs.execute('SELECT {0} FROM Reaches WHERE {0} IS NOT NULL'.format(attribute))
    values = [row[attribute] for row in curs.fetchall()]
    box_plot(values, attribute, attribute, image_path)

    img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
    img = ET.Element('img', attrib={'class': 'boxplot', 'src': '{}/{}'.format(os.path.basename(images_dir), os.path.basename(image_path))})
    img_wrap.append(img)

    reach_wrapper_inner.append(img_wrap)

    elParent.append(wrapper)


def create_table_from_sql(col_names, sql, database, elParent, attrib=None):
    if attrib is None:
        attrib = {}
    table = ET.Element('table', attrib=attrib)

    thead = ET.Element('thead')
    table.append(thead)

    for col in col_names:
        th = ET.Element('th')
        th.text = col
        thead.append(th)

    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory
    curs = conn.cursor()
    curs.execute(sql)

    tbody = ET.Element('tbody')
    table.append(tbody)

    for row in curs.fetchall():
        tr = ET.Element('tr')
        tbody.append(tr)

        for col, val in row.items():
            val, class_name = format_value(val) if col not in idCols else [str(val), 'idVal']
            td = ET.Element('td', attrib={'class': class_name})

            td.text = val
            tr.append(td)

    elParent.append(table)


def create_table_from_tuple_list(col_names, data, elParent, attrib=None):

    if attrib is None:
        attrib = {}
    table = ET.Element('table', attrib=attrib)

    thead = ET.Element('thead')
    table.append(thead)

    for col in col_names:
        th = ET.Element('th')
        th.text = col
        thead.append(th)

    tbody = ET.Element('tbody')
    table.append(tbody)

    for row in data:
        tr = ET.Element('tr')
        tbody.append(tr)

        for col in row:
            val, class_name = format_value(col)
            td = ET.Element('td', attrib={'class': class_name})

            td.text = val
            tr.append(td)

    elParent.append(table)


def create_table_from_dict(values, elParent, attrib=None):
    """Keys go in first col, values in second

    Arguments:
        values {[type]} - - [description]
        database {[type]} - - [description]
        elParent {[type]} - - [description]

    Returns:
        [type] - - [description]
    """
    if attrib is None:
        attrib = {}
    if 'class' in attrib:
        attrib['class'] = 'dictable {}'.format(attrib['class'])
    else:
        attrib['class'] = 'dictable'

    table = ET.Element('table', attrib=attrib)

    tbody = ET.Element('tbody')
    table.append(tbody)

    for key, val in values.items():

        tr = ET.Element('tr')
        tbody.append(tr)

        th = ET.Element('th')
        th.text = key
        tr.append(th)

        val, class_name = format_value(val)
        td = ET.Element('td', attrib={'class': class_name})
        td.text = val
        tr.append(td)

    elParent.append(table)


def format_value(value):

    formatted = ''
    class_name = ''
    if isinstance(value, str):
        formatted = value
        class_name = 'text'
    elif isinstance(value, float):
        formatted = '{0:,.2f}'.format(value)
        class_name = 'float num'
    elif isinstance(value, int):
        formatted = '{0:,d}'.format(value)
        class_name = 'int num'
    return formatted, class_name


def create_ul(values, elParent, attrib=None):
    if attrib is None:
        attrib = {}
    ul = ET.Element('ul', attrib=attrib)
    elParent.append(ul)

    for key, val in values.items():
        li = ET.Element('li')
        li.text = val
        ul.append(val)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def header(level, text, elParent):

    hEl = ET.Element('h{}'.format(level))
    hEl.text = text

    anchor = ET.Element('a', attrib={'name': text.lower(), 'data-level': str(level), 'data-name': text})
    anchor.text = ' '
    hEl.append(anchor)

    elParent.append(hEl)


# def main():

#     parser = argparse.ArgumentParser()
#     parser.add_argument('database', help='Path to the BRAT database', type=str)
#     parser.add_argument('report_path', help='Output path where report will be generated', type=str)
#     args = dotenv.parse_args_env(parser)

#     report(args.database, args.report_path)


# if __name__ == '__main__':
#     main()
