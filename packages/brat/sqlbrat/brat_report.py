import argparse
import sqlite3
import os
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import xyscatter, box_plot, pie, horizontal_bar

from sqlbrat.__version__ import __version__


id_cols = [
    'VegetationID',
    'Type ID'
]


class BratReport(RSReport):
    """In order to write a report we will extend the RSReport class from the rscommons
    module which has useful styles and building blocks like Tables from lists etc.
    """

    def __init__(self, database, report_path, rs_project):
        # Need to call the constructor of the inherited class:
        super().__init__(rs_project, report_path)

        self.log = Logger('BratReport')
        self.database = database

        # set up dict with references to all colors for plots
        self.bratcolors = {
            'Perennial': '#004da8',
            'Intermittent': '#aaff00',
            'Ephemeral': '#e69800',
            'Canal': '#a3f2f2',
            'Connector': '#ffaa00',
            'Artificial Path': '#cf596a',
            '0 - 160 (Can Build Dam)': '#38a800',
            '160 - 185 (Probably Can Build Dam)': '#f5f500',
            '> 185 (Cannot Build Dam)': '#f50000',
            '0 - 1100 Dam Persists': '#38a800',
            '1100 - 1400 Potential Dam Breach': '#b0e000',
            '1400 - 2200 Potential Dam Blowout': '#ffaa00',
            '> 2200 Dam Blowout': '#ff0000',
            'Very Low': '#267300',
            'Low': '#a4c400',
            'Moderate': '#ffbb00',
            'High': '#ff2600',
            'Immediately Adjacent (0 - 30 m)': '#ff2200',
            'Within Normal Forage Range (30 - 100 m)': '#ff9900',
            'Within Plausable Forage Range (100 - 300 m)': '#ffff00',
            'Outside Range of Concern (300 m - 1 km)': '#7aab00',
            'Not Close (> 1 km)': '#006100',
            'None: 0 dams': '#f50000',
            'Rare: 0-1 dams/km (0-2 dams/mi)': '#ffaa00',
            'Occasional: 1-5 dams/km (2-8 dams/mi)': '#f5f500',
            'Frequent: 5-15 dams/km (8-24 dams/mi)': '#4ce601',
            'Pervasive: 15-40 dams/km (24-64 dams/mi)': '#005ce6',
            'No Dams': '#f50000',
            'Single Dam': '#ffaa00',
            'Small Complex (1-3 Dams)': '#f5f500',
            'Medium Complex (3-5 dams)': '#4ce601',
            'Large Complex (> 5 dams)': '#005cd6',
            'Anthropogenically Limited': '#e6c700',
            'Stream Power Limited': '#00a9e6',
            'Slope Limited': '#ff0000',
            'Potential Reservoir or Landuse': '#ff73df',
            'Naturally Vegetation Limited': '#267300',
            'Stream Size Limited': '#00ad35',
            'Dam Building Possible': '#a5a5a5',
            '...TBD...': '#b9b9b9',
            'Considerable Risk': '#e60000',
            'Some Risk': '#ffaa00',
            'Minor Risk': '#00cf55',
            'Negligible Risk': '#a5a5a5',
            'Easiest - Low-Hanging Fruit': '#00a800',
            'Straight Forward - Quick Return': '#00cf55',
            'Strategic - Long-Term Investment': '#e6cc00',
            'NA': '#b9b9b9',
            'Other': '#b9b9b9',
            'Private': '#94345a',
            'United States Forest Service': '#89f575',
            'Bureau of Land Managment': '#57b2f2',
            'National Park Service': '#d99652',
            'None': '#b2b2b2',
            'United States Army Corps of Engineers': '#e8e461',
            'United States Bureau of Reclamation': '#3b518f',
            'United States Department of Agriculture': '#a86f67',
            'United States Department of Interior': '#f062f5',
            'United States Department of Defense': '#5f8c31',
            'United States Fish and Wildlife Service': '#f26374',
            'State?': '#a9e092',
            'Forest Service?': '#907cd6',
            'Bonneville Power Administration': '#3ba35f',
            'USCG': '#2e7385',
            'BOP': '#e0bd96',
            'IBWC': '#6b3380',
            'FAA': '#745580',
            'United States Department of Justice': '#db7f9f',
            'United States Navy': '#cf5bb7',
            'USMC': '#ad4242',
            'FHA': '#745580',
            'GSA': '#49d1a4',
            'VA': '#95d9f0',
            'United States Army': '#f569a8',
            'United States Air Force': '#cf7557',
            'Unknown': '#b2b2b2'
        }

        # The report has a core CSS file but we can extend it with our own if we want:
        css_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'brat_report.css')
        self.add_css(css_path)

        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)

        # Now we just need to write the sections we want in the report in order
        self.report_intro()
        self.dam_capacity()
        self.conservation()
        self.reach_attribute_summary()
        self.hydrology_plots()
        self.ownership()
        self.vegetation()
        self.reach_attribute_summaries()

    def report_intro(self):
        # Create a section node to start adding things to. Section nodes are added to the table of contents if
        # they have a title. If you don't specify a el_parent argument these sections will simply be added
        # to the report body in the order you call them.
        section = self.section('ReportIntro', 'Introduction')

        # This project has a db so we'll need a connection
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()

        row = curs.execute('SELECT Sum(iGeo_Len) AS TotalLength, Count(ReachID) AS TotalReaches FROM vwReaches').fetchone()
        values = {
            'Number of reaches': '{0:,d}'.format(row['TotalReaches']),
            'Total reach length (km)': '{0:,.0f}'.format(row['TotalLength'] / 1000),
            'Total reach length (miles)': '{0:,.0f}'.format(row['TotalLength'] * 0.000621371)
        }

        row = curs.execute('''
            SELECT WatershedID "Watershed ID", W.Name "Watershed Name", E.Name Ecoregion, CAST(AreaSqKm AS TEXT) "Area (Sqkm)", States
            FROM Watersheds W
            INNER JOIN Ecoregions E ON W.EcoregionID = E.EcoregionID
        ''').fetchone()
        values.update(row)

        # curs.execute('SELECT KeyInfo, ValueInfo FROM Metadata')
        # values.update({row['KeyInfo'].replace('_', ' '): row['ValueInfo'] for row in curs.fetchall()})

        # Here we're creating a new <div> to wrap around the table for stylign purposes
        table_wrapper = ET.Element('div', attrib={'class': 'tableWrapper'})
        RSReport.create_table_from_dict(values, table_wrapper, attrib={'id': 'SummTable'})

        RSReport.create_table_from_sql(
            ['Reach Type', 'Total Length (km)', '% of Total'],
            'SELECT ReachType, ROUND(Sum(iGeo_Len) / 1000, 1) As Length, ROUND(100 * Sum(iGeo_Len) / TotalLength, 1) AS TotalLength '
            'FROM vwReaches INNER JOIN (SELECT ROUND(Sum(iGeo_Len), 1) AS TotalLength FROM vwReaches) GROUP BY ReachType',
            self.database, table_wrapper, attrib={'id': 'SummTable_sql'})

        # Append my table_wrapper div (which now contains both tables above) to the section
        section.append(table_wrapper)

    def reach_attribute(self, attribute, units, parent_el):
        # Use a class here because it repeats
        section = self.section(None, attribute, parent_el, level=2)
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()

        # Summary statistics (min, max etc) for the current attribute
        curs.execute('SELECT Count({0}) "Values", Max({0}) Maximum, Min({0}) Minimum, Avg({0}) Average FROM vwReaches WHERE {0} IS NOT NULL'.format(attribute))
        values = curs.fetchone()

        reach_wrapper_inner = ET.Element('div', attrib={'class': 'reachAtributeInner'})
        section.append(reach_wrapper_inner)

        # Add the number of NULL values
        curs.execute('SELECT Count({0}) "NULL Values" FROM vwReaches WHERE {0} IS NULL'.format(attribute))
        values.update(curs.fetchone())
        RSReport.create_table_from_dict(values, reach_wrapper_inner)

        # Box plot
        image_path = os.path.join(self.images_dir, 'attribute_{}.png'.format(attribute))
        curs.execute('SELECT {0} FROM vwReaches WHERE {0} IS NOT NULL'.format(attribute))
        values = [row[attribute] for row in curs.fetchall()]
        box_plot(values, attribute, attribute, image_path)

        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={'class': 'boxplot', 'alt': 'boxplot', 'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(image_path))})
        img_wrap.append(img)

        reach_wrapper_inner.append(img_wrap)

    def dam_capacity(self):
        section = self.section('DamCapacity', 'BRAT Dam Capacity Results')
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()
        fields = [
            ('Existing capacity (vegetation only)', 'Sum((iGeo_len / 1000) * oVC_EX)'),
            ('Historic capacity (vegetation only)', 'Sum((iGeo_len / 1000) * oVC_HPE)'),
            ('Existing capacity', 'Sum((iGeo_len / 1000) * oCC_EX)'),
            ('Historic capacity', 'Sum((iGeo_len / 1000) * oCC_HPE)')
        ]

        curs.execute('SELECT {} FROM vwReaches'.format(', '.join([field for label, field in fields])))
        row = curs.fetchone()

        table_dict = {fields[i][0]: int(row[fields[i][1]]) for i in range(len(fields))}
        RSReport.create_table_from_dict(table_dict, section)

        self.dam_capacity_lengths('oCC_EX', section)
        self.dam_capacity_lengths('oCC_HPE', section)

    def dam_capacity_lengths(self, capacity_field, elParent):
        conn = sqlite3.connect(self.database)
        curs = conn.cursor()

        curs.execute('SELECT Name, MaxCapacity FROM DamCapacities ORDER BY MaxCapacity')
        bins = [(row[0], row[1]) for row in curs.fetchall()]

        curs.execute('SELECT Sum(iGeo_Len) / 1000 FROM vwReaches')
        total_length_km = curs.fetchone()[0]

        data = []
        last_bin = 0
        cumulative_length_km = 0
        for name, max_capacity in bins:
            curs.execute('SELECT Sum(iGeo_len) / 1000 FROM vwReaches WHERE {} <= {}'.format(capacity_field, max_capacity))
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
        RSReport.create_table_from_tuple_list((capacity_field, 'Stream Length (km)', 'Stream Length (mi)', 'Percent'), data, elParent)

    def hydrology_plots(self):
        section = self.section('HydrologyPlots', 'Hydrology')

        conn = sqlite3.connect(self.database)
        curs = conn.cursor()

        curs.execute('SELECT MaxDrainage, QLow, Q2 FROM Watersheds')
        row = curs.fetchone()
        RSReport.create_table_from_dict({
            'Max Draiange (sqkm)': row[0],
            'Baseflow': row[1],
            'Peak Flow': row[2]
        }, section, attrib={'class': 'fullwidth'})

        RSReport.header(3, 'Hydrological Parameters', section)
        RSReport.create_table_from_sql(
            ['Parameter', 'Data Value', 'Data Units', 'Conversion Factor', 'Equation Value', 'Equation Units'],
            'SELECT Parameter, Value, DataUnits, Conversion, ConvertedValue, EquationUnits FROM vwHydroParams',
            self.database, section, attrib={'class': 'fullwidth'})

        variables = [
            ('iHyd_QLow', 'Baseflow (CFS)'),
            ('iHyd_Q2', 'Peak Flow (CFS)'),
            ('iHyd_SPLow', 'Baseflow Stream Power (Watts)'),
            ('iHyd_SP2', 'Peak Flow Stream Power (Watts)'),
            ('iGeo_Slope', 'Slope (degrees)')
        ]

        plot_wrapper = ET.Element('div', attrib={'class': 'hydroPlotWrapper'})
        section.append(plot_wrapper)

        for variable, ylabel in variables:
            self.log.info('Generating XY scatter for {} against drainage area.'.format(variable))
            image_path = os.path.join(self.images_dir, 'drainage_area_{}.png'.format(variable.lower()))

            curs.execute('SELECT iGeo_DA, {} FROM vwReaches'.format(variable))
            values = [(row[0], row[1]) for row in curs.fetchall()]
            xyscatter(values, 'Drainage Area (sqkm)', ylabel, variable, image_path)

            img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
            img = ET.Element('img', attrib={
                'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(image_path)),
                'alt': 'boxplot'
            })
            img_wrap.append(img)
            plot_wrapper.append(img_wrap)

        for variable, ylabel in [('Base Flow', 'Baseflow (CFS)')]:
            image_path = os.path.join(self.images_dir, 'drainage_area_{}.png'.format(variable.lower()))

    def attribute_table_and_pie(self, attribute_field, bins, elParent):
        """
        Expect the bins as list of dictionaries with keys "label", "lower", "upper"
        """

        RSReport.header(3, '{} Summary'.format(attribute_field), elParent)

        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()

        data = []
        for abin in bins:
            label = abin['label']
            lower = abin['lower'] if 'lower' in abin else None
            upper = abin['upper'] if 'upper' in abin else None
            where_clause = ''
            sql_args = []

            if lower is not None:
                where_clause = ' ({} > ?)'.format(attribute_field)
                sql_args.append(lower)

            if upper is not None:
                if len(where_clause) > 0:
                    where_clause += ' AND '
                sql_args.append(upper)

                where_clause += ' ({} <= ?) '.format(attribute_field)

            curs.execute("""SELECT count(*) ReachCount, (sum(iGeo_Len) / 1000) LengthKM, (sum(igeo_len) * 0.000621371) LengthMiles, (0.1 * sum(iGeo_Len) / t.total_length) Percent
                        FROM ReachAttributes r,
                        (select sum(igeo_len) / 1000 total_length from ReachAttributes) t
                         WHERE {}""".format(where_clause), sql_args)
            row = curs.fetchone()
            data.append((label, row['ReachCount'], row['LengthKM'], row['LengthMiles'], row['Percent']))

        RSReport.create_table_from_tuple_list(['Category', 'Reach Count', 'Length (km)', 'Length (miles)', 'Percent (%)'], data, elParent)

        image_path = os.path.join(self.images_dir, '{}_pie.png'.format(attribute_field.lower()))
        col = [self.bratcolors[x[0]] for x in data]
        pie([x[4] for x in data], [x[0] for x in data], '{} Reach Summary'.format(attribute_field), col, image_path)

        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(image_path)),
            'alt': 'piechart'
        })
        img_wrap.append(img)
        plot_wrapper.append(img_wrap)
        elParent.append(plot_wrapper)

        bar_path = os.path.join(self.images_dir, '{}_bar.png'.format(attribute_field))
        horizontal_bar([x[2] for x in data], [x[0] for x in data], col, 'Reach Length (km)', '{} Reach Summary'.format(attribute_field), bar_path, 'Reach Length (Miles)')

        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(bar_path)),
            'alt': 'bar_chart'
        })
        img_wrap.append(img)
        plot_wrapper.append(img_wrap)
        elParent.append(plot_wrapper)

    def reach_attribute_summaries(self):

        section = self.section('ReachAttSum', 'Reach Attribute Summaries')

        # Low Stream Power
        self.attribute_table_and_pie('iHyd_SPLow', [
            {'label': '0 - 160 (Can Build Dam)', 'upper': 16},
            {'label': '160 - 185 (Probably Can Build Dam)', 'lower': 16, 'upper': 185},
            {'label': '> 185 (Cannot Build Dam)', 'lower': 185}
        ], section)

        # High Stream Power
        self.attribute_table_and_pie('iHyd_SP2', [
            {'label': '0 - 1100 Dam Persists', 'upper': 1100},
            {'label': '1100 - 1400 Potential Dam Breach', 'lower': 1100, 'upper': 1400},
            {'label': '1400 - 2200 Potential Dam Blowout', 'lower': 1400, 'upper': 2200},
            {'label': '> 2200 Dam Blowout', 'lower': 2200}
        ], section)

        # Distance
        self.attribute_table_and_pie('oPC_Dist', [
            {'label': 'Not Close (> 1 km)', 'lower': 1000},
            {'label': 'Outside Range of Concern (300 m - 1 km)', 'lower': 300, 'upper': 1000},
            {'label': 'Within Plausable Forage Range (100 - 300 m)', 'lower': 100, 'upper': 300},
            {'label': 'Within Normal Forage Range (30 - 100 m)', 'lower': 30, 'upper': 100},
            {'label': 'Immediately Adjacent (0 - 30 m)', 'upper': 300}
        ], section)

        # Existing Capacity
        self.attribute_table_and_pie('oCC_EX', [
            {'label': 'None: 0 dams', 'upper': 0},
            {'label': 'Rare: 0-1 dams/km (0-2 dams/mi)', 'lower': 0, 'upper': 1},
            {'label': 'Occasional: 1-5 dams/km (2-8 dams/mi)', 'lower': 1, 'upper': 5},
            {'label': 'Frequent: 5-15 dams/km (8-24 dams/mi)', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive: 15-40 dams/km (24-64 dams/mi)', 'lower': 15}
        ], section)

        # Distance
        self.attribute_table_and_pie('mCC_EX_CT', [
            {'label': 'No Dams', 'upper': 0},
            {'label': 'Single Dam', 'lower': 1, 'upper': 1},
            {'label': 'Small Complex (1-3 Dams)', 'lower': 1, 'upper': 3},
            {'label': 'Medium Complex (3-5 dams)', 'lower': 3, 'upper': 5},
            {'label': 'Large Complex (> 5 dams)', 'lower': 5}
        ], section)

        # Historical Dam Capacity
        self.attribute_table_and_pie('oCC_HPE', [
            {'label': 'None: 0 dams', 'upper': 0},
            {'label': 'Rare: 0-1 dams/km (0-2 dams/mi)', 'lower': 0, 'upper': 1},
            {'label': 'Occasional: 1-5 dams/km (2-8 dams/mi)', 'lower': 1, 'upper': 5},
            {'label': 'Frequent: 5-15 dams/km (8-24 dams/mi)', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive: 15-40 dams/km (24-64 dams/mi)', 'lower': 15}
        ], section)

        # Historical Dam
        self.attribute_table_and_pie('mCC_HPE_CT', [
            {'label': 'No Dams', 'upper': 0},
            {'label': 'Single Dam', 'lower': 1, 'upper': 1},
            {'label': 'Small Complex (1-3 Dams)', 'lower': 1, 'upper': 3},
            {'label': 'Medium Complex (3-5 dams)', 'lower': 3, 'upper': 5},
            {'label': 'Large Complex (> 5 dams)', 'lower': 5}
        ], section)

        # Existing Vegetation Dam Building Capacity
        self.attribute_table_and_pie('oVC_EX', [
            {'label': 'None: 0 dams', 'upper': 0},
            {'label': 'Rare: 0-1 dams/km (0-2 dams/mi)', 'lower': 0, 'upper': 1},
            {'label': 'Occasional: 1-5 dams/km (2-8 dams/mi)', 'lower': 1, 'upper': 5},
            {'label': 'Frequent: 5-15 dams/km (8-24 dams/mi)', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive: 15-40 dams/km (24-64 dams/mi)', 'lower': 15}
        ], section)

        # Historical Vegetation Dam Building Capacity
        self.attribute_table_and_pie('oVC_HPE', [
            {'label': 'None: 0 dams', 'upper': 0},
            {'label': 'Rare: 0-1 dams/km (0-2 dams/mi)', 'lower': 0, 'upper': 1},
            {'label': 'Occasional: 1-5 dams/km (2-8 dams/mi)', 'lower': 1, 'upper': 5},
            {'label': 'Frequent: 5-15 dams/km (8-24 dams/mi)', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive: 15-40 dams/km (24-64 dams/mi)', 'lower': 15}
        ], section)

        # LandUse Intensity
        self.attribute_table_and_pie('iPC_LU', [
            {'label': 'Very Low', 'upper': 0},
            {'label': 'Low', 'lower': 0, 'upper': 0.3333},
            {'label': 'Moderate', 'lower': 0.3333, 'upper': 0.6666},
            {'label': 'High', 'lower': 0.6666}
        ], section)

        # Conservation Opportunity
        self.attribute_table_and_pie('iPC_LU', [
            {'label': 'Very Low', 'upper': 0},
            {'label': 'Low', 'lower': 0, 'upper': 0.3333},
            {'label': 'Moderate', 'lower': 0.3333, 'upper': 0.6666},
            {'label': 'High', 'lower': 0.6666}
        ], section)

    def reach_attribute_summary(self):
        section = self.section('ReachAttributeSummary', 'Geophysical Attributes')

        attribs = [
            ('iGeo_Slope', 'Slope', 'ratio'),
            ('iGeo_ElMax', 'Max Elevation', 'metres'),
            ('iGeo_ElMin', 'Min Elevation', 'metres'),
            ('iGeo_Len', 'Length', 'metres'),
            ('iGeo_DA', 'Drainage Area', 'Sqkm')
        ]
        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        [self.reach_attribute(attribute, units, plot_wrapper) for attribute, name, units in attribs]

        section.append(plot_wrapper)

    def ownership(self):
        section = self.section('Ownership', 'Ownership')

        RSReport.create_table_from_sql(
            ['Ownership Agency', 'Number of Reach Segments', 'Length (km)', '% of Total Length'],
            'SELECT IFNULL(Agency, "None"), Count(ReachID), Sum(iGeo_Len) / 1000, 100* Sum(iGeo_Len) / TotalLength FROM vwReaches'
            ' INNER JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM vwReaches) GROUP BY Agency',
            self.database, section, attrib={'class': 'fullwidth'})

    def vegetation(self):
        section = self.section('Vegetation', 'Vegetation')
        conn = sqlite3.connect(self.database)
        # conn.row_factory = _dict_factory
        curs = conn.cursor()

        for epochid, veg_type in [(2, 'Historic Vegetation'), (1, 'Existing Vegetation')]:

            RSReport.header(3, veg_type, section)

            pEl = ET.Element('p')
            pEl.text = 'The 30 most common {} types within the 100m reach buffer.'.format(veg_type.lower())
            section.append(pEl)

            RSReport.create_table_from_sql(
                ['Vegetation ID', 'Vegetation Type', 'Total Area (sqkm)', 'Default Suitability', 'Override Suitability', 'Effective Suitability'],
                """
                        SELECT VegetationID,
                        Name, (CAST(TotalArea AS REAL) / 1000000) AS TotalAreaSqKm,
                        DefaultSuitability,
                        OverrideSuitability,
                        EffectiveSuitability
                        FROM vwReachVegetationTypes WHERE (EpochID = {}) AND (Buffer = 100) ORDER BY TotalArea DESC LIMIT 30""".format(epochid),
                self.database, section)

            try:
                # Calculate the area weighted suitability
                curs.execute("""
                SELECT WeightedSum / SumTotalArea FROM
                (SELECT Sum(CAST(TotalArea AS REAL) * CAST(EffectiveSuitability AS REAL) / 1000000) WeightedSum FROM vwReachVegetationTypes WHERE EpochID = {0} AND Buffer = 100)
                JOIN
                (SELECT CAST(Sum(TotalArea) AS REAL) / 1000000 SumTotalArea FROM vwReachVegetationTypes WHERE EpochID = {0} AND Buffer = 100)""".format(epochid))
                area_weighted_avg_suitability = curs.fetchone()[0]

                RSReport.header(3, 'Suitability Breakdown', section)
                pEl = ET.Element('p')
                pEl.text = """The area weighted average {} suitability is {}.
                    The breakdown of the percentage of the 100m buffer within each suitability class
                    across all reaches in the watershed.""".format(veg_type.lower(), RSReport.format_value(area_weighted_avg_suitability)[0])
                section.append(pEl)

                RSReport.create_table_from_sql(['Suitability Class', '% with 100m Buffer'],
                                               """
                    SELECT EffectiveSuitability, 100.0 * SArea / SumTotalArea FROM
                    (
                        SELECT CAST(Sum(TotalArea) AS REAL) / 1000000 SArea, EffectiveSuitability
                        FROM vwReachVegetationTypes
                        WHERE EpochID = {0} AND Buffer = 100 GROUP BY EffectiveSuitability
                    )
                    JOIN
                    (
                        SELECT CAST(Sum(TotalArea) AS REAL) / 1000000 SumTotalArea
                        FROM vwReachVegetationTypes
                        WHERE EpochID = {0} AND Buffer = 100
                    )
                    ORDER BY EffectiveSuitability
                    """.format(epochid), self.database, section, id_cols=id_cols)
            except Exception as ex:
                self.log.warning('Error calculating vegetation report')

    def conservation(self):
        section = self.section('Conservation', 'Conservation')

        fields = [
            ('Risk', 'DamRisks', 'RiskID'),
            ('Opportunity', 'DamOpportunities', 'OpportunityID'),
            ('Limitation', 'DamLimitations', 'LimitationID')
        ]

        for label, table, idfield in fields:
            RSReport.header(3, label, section)
            table_data = RSReport.create_table_from_sql(
                [label, 'Total Length (km)', 'Reach Count', '%'],
                'SELECT DR.Name, Sum(iGeo_Len) / 1000, Count(R.{1}), 100 * Sum(iGeo_Len) / TotalLength'
                ' FROM {0} DR LEFT JOIN vwReaches R ON DR.{1} = R.{1}'
                ' JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM vwReaches)'
                ' GROUP BY DR.{1}'.format(table, idfield),
                self.database, section)

            pie_path = os.path.join(self.images_dir, '{}_pie.png'.format(label))
            col = [self.bratcolors[x[0]] for x in table_data]
            pie([x[3] for x in table_data], [x[0] for x in table_data], label, col, pie_path)

            plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
            img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
            img = ET.Element('img', attrib={
                'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(pie_path)),
                'alt': 'pie_chart'
            })
            img_wrap.append(img)
            plot_wrapper.append(img_wrap)
            section.append(plot_wrapper)

            bar_path = os.path.join(self.images_dir, '{}_bar.png'.format(label))
            horizontal_bar([x[1] for x in table_data], [x[0] for x in table_data], col, 'Reach Length (km)', label, bar_path, 'Reach Length (mi)')

            plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
            img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
            img = ET.Element('img', attrib={
                'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(bar_path)),
                'alt': 'bar_chart'
            })
            img_wrap.append(img)
            plot_wrapper.append(img_wrap)
            section.append(plot_wrapper)

        RSReport.header(3, 'Conflict Attributes', section)

        for attribute in ['iPC_Canal', 'iPC_DivPts', 'iPC_Privat']:
            self.reach_attribute(attribute, 'meters', section)


def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the BRAT database', type=str)
    parser.add_argument('projectxml', help='Path to the BRAT project.rs.xml', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/BRAT.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = BratReport(args.database, args.report_path, project)
    report.write()
