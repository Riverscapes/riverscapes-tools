import sqlite3
import os
from uuid import uuid4
from xml.etree import ElementTree as ET
from jinja2 import Template
from html5print import HTMLBeautifier, CSSBeautifier
from rscommons import Logger


class RSReport():

    def __init__(self, title, subtitle, filepath):
        self.log = Logger('Report')
        self.template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates')

        self.log.info('Creating report at {}'.format(filepath))
        self.filepath = filepath
        self.title = title
        self.subtitle = subtitle
        self.css_files = []
        self.footer = ''

        if os.path.isfile(self.filepath):
            os.remove(self.filepath)

        self.root_node = ET.Element('html', attrib={'lang': 'en'})
        self.head = self.html_head(title, self.root_node)
        self.body = ET.Element('body')

        # Add in our common CSS. This can be extended
        self.add_css(os.path.join(self.template_path, 'report.css'))

        self.container_div = ET.Element('div', attrib={'id': 'ReportContainer'})
        self.inner_div = ET.Element('main', attrib={'id': 'ReportInner'})

        # A little div wrapping will help us out later
        self.body.append(self.container_div)
        self.root_node.append(self.body)
        self.container_div.append(self.inner_div)

    def write(self):
        css_template = "<style>\n{}\n</style>"
        html_inner = ET.tostring(self.inner_div, method="html", encoding='unicode')
        styles = ''.join([css_template.format(css) for css in self.css_files])
        # Get my HTML templae and render it

        with open(os.path.join(self.template_path, 'template.html')) as t:
            template = Template(t.read())

        final_render = HTMLBeautifier.beautify(template.render(report={
            'title': self.title,
            'subtitle': self.subtitle,
            'head': styles,
            'body': html_inner,
            'footer': self.footer
        }))
        with open(self.filepath, "w") as f:
            f.write(final_render)

        self.log.info('Report Writing Completed')

    def add_css(self, filepath):
        with open(filepath) as css_file:
            css = css_file.read()
        beautiful = CSSBeautifier.beautify(css)
        self.css_files.append(beautiful)

    @staticmethod
    def html_head(report_title, el_parent):

        head = ET.Element('head')
        title = ET.Element('title')
        title.text = report_title
        head.append(title)
        el_parent.append(head)

        return head

    @staticmethod
    def table_of_contents(el_parent):
        wrapper = ET.Element('div', attrib={'id': 'TOC'})
        RSReport.header(3, 'Table of Contents', wrapper)

        ul = ET.Element('ul')

        li = ET.Element('li')
        ul.append(li)

        anchor = ET.Element('a', attrib={'href': '#ownership'})
        anchor.text = 'Ownership'
        li.append(anchor)

        el_parent.append(wrapper)

    @staticmethod
    def create_table_from_sql(col_names, sql, database, el_parent, attrib=None, id_cols=None):
        if attrib is None:
            attrib = {}
        table = ET.Element('table', attrib=attrib)

        thead = ET.Element('thead')
        theadrow = ET.Element('tr')
        thead.append(theadrow)
        table.append(thead)

        for col in col_names:
            th = ET.Element('th')
            th.text = col
            theadrow.append(th)

        conn = sqlite3.connect(database)
        conn.row_factory = RSReport._dict_factory
        curs = conn.cursor()
        curs.execute(sql)

        tbody = ET.Element('tbody')
        table.append(tbody)

        for row in curs.fetchall():
            tr = ET.Element('tr')
            tbody.append(tr)

            for col, val in row.items():
                val, class_name = RSReport.format_value(val) if id_cols and col not in id_cols else [str(val), 'idVal']
                td = ET.Element('td', attrib={'class': class_name})

                td.text = val
                tr.append(td)

        el_parent.append(table)

    @staticmethod
    def create_table_from_tuple_list(col_names, data, el_parent, attrib=None):

        if attrib is None:
            attrib = {}
        table = ET.Element('table', attrib=attrib)

        thead = ET.Element('thead')
        theadrow = ET.Element('tr')
        thead.append(theadrow)
        table.append(thead)

        for col in col_names:
            th = ET.Element('th')
            th.text = col
            theadrow.append(th)

        tbody = ET.Element('tbody')
        table.append(tbody)

        for row in data:
            tr = ET.Element('tr')
            tbody.append(tr)

            for col in row:
                val, class_name = RSReport.format_value(col)
                td = ET.Element('td', attrib={'class': class_name})

                td.text = val
                tr.append(td)

        el_parent.append(table)

    @staticmethod
    def create_table_from_dict(values, el_parent, attrib=None):
        """Keys go in first col, values in second

        Arguments:
            values {[type]} - - [description]
            database {[type]} - - [description]
            el_parent {[type]} - - [description]

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

            val, class_name = RSReport.format_value(val)
            td = ET.Element('td', attrib={'class': class_name})
            td.text = val
            tr.append(td)

        el_parent.append(table)

    @staticmethod
    def format_value(value, val_type=None):
        """[summary]

        Args:
            value ([type]): [description]
            val_type ([type], optional): Type to try and force

        Returns:
            [type]: [description]
        """
        formatted = ''
        class_name = ''

        try:
            if val_type == str or isinstance(value, str):
                formatted = value
                class_name = 'text'
            elif val_type == float or isinstance(value, float):
                formatted = '{0:,.2f}'.format(value)
                class_name = 'float num'
            elif val_type == int or isinstance(value, int):
                formatted = '{0:,d}'.format(value)
                class_name = 'int num'

        except Exception as e:
            print(e)
            return value, 'unknown'

        return formatted, class_name

    @staticmethod
    def create_ul(values, el_parent, attrib=None, ordered=False):
        if attrib is None:
            attrib = {}

        tagname = 'ul' if ordered is False else 'ol'
        outer = ET.Element(tagname, attrib=attrib)

        for key, val in values.items():
            li = ET.Element('li')
            li.text = val
            outer.append(val)

        el_parent.append(outer)

    @staticmethod
    def _dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    @staticmethod
    def header(level, text, el_parent):

        hEl = ET.Element('h{}'.format(level), attrib={'id': str(uuid4())})
        hEl.text = text
        el_parent.append(hEl)

    @staticmethod
    def section(sectionid, title, el_parent, attrib=None):
        if attrib is None:
            attrib = {}
        section = ET.Element('section', attrib={'id': sectionid, 'class': 'report-section'})
        section_inner = ET.Element('div', attrib={'class': 'section-inner'})
        if title:
            RSReport.header(2, title, section)

        section.append(section_inner)
        el_parent.append(section)

        return section_inner
