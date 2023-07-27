import argparse
from operator import le
import sqlite3
import os
# from turtle import pen
from xml.etree import ElementTree as ET

from rscommons import Logger, dotenv, ModelConfig, RSReport, RSProject
from rscommons.util import safe_makedirs
from rscommons.plotting import xyscatter, box_plot, pie, horizontal_bar
from sympy import sec

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
            'Canal - Ditch': '#a3f2f2',
            'Connector': '#ffaa00',
            'Artificial Path': '#cf596a',
            'Stream': '#20add4',
            'Can Build Dam': '#38a800',
            'Probably Can Build Dam': '#f5f500',
            'Cannot Build Dam': '#f50000',
            'Dam Persists': '#38a800',
            'Potential Dam Breach': '#b0e000',
            'Potential Dam Blowout': '#ffaa00',
            'Dam Blowout': '#ff0000',
            'Very Low': '#267300',
            'Low': '#a4c400',
            'Moderate': '#ffbb00',
            'High': '#ff2600',
            'Immediately Adjacent': '#ff2200',
            'Within Normal Forage Range': '#ff9900',
            'Within Plausable Forage Range': '#ffff00',
            'Outside Range of Concern': '#7aab00',
            'Not Close': '#006100',
            'None': '#f50000',
            'Rare': '#ffaa00',
            'Occasional': '#f5f500',
            'Frequent': '#4ce601',
            'Pervasive': '#005ce6',
            'No Dams': '#f50000',
            'Single Dam': '#ffaa00',
            'Small Complex': '#f5f500',
            'Medium Complex': '#4ce601',
            'Large Complex': '#005cd6',
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
            'Minor Risk': '#00c5ff',
            'Negligible Risk': '#a5a5a5',
            'Easiest - Low-Hanging Fruit': '#00a800',
            'Straight Forward - Quick Return': '#00c5ff',
            'Strategic - Long-Term Investment': '#e6cc00',
            'NA': '#b9b9b9',
            'Other': '#b9b9b9',
            'Private': '#94345a',
            'United States Forest Service': '#89f575',
            'Bureau of Land Management': '#57b2f2',
            'National Park Service': '#d99652',
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
            'Unknown': '#b2b2b2',
            '0 - 0.5%': '#0b2c7a',
            '0.5 - 1%': '#20998f',
            '1 - 3%': '#00db00',
            '3 - 8%': '#ffff00',
            '8 - 23%': '#eda113',
            '>23%': '#c2523c'
        }

        # associate field names with actual titles
        self.f_names = {
            'oCC_EX': 'Existing Capacity',
            'oCC_HPE': 'Historic Capacity',
            'Risk': 'Risk',
            'Opportunity': 'Opportunity',
            'Limitation': 'Limitation',
            'iPC_Canal': 'Distance to Nearest Canal (m)',
            'iPC_DivPts': 'Distance to Nearest Diversion (m)',
            'iPC_Privat': 'Distance to Nearest Privately Owned Land (m)',
            'oPC_Dist': 'Distance to Nearest Potential Conflict',
            'iPC_LU': 'Land Use Intensity',
            'Ownership': 'Ownership',
            'iHyd_SPLow': 'Low Flow Stream Power',
            'iHyd_SP2': 'Flood Flow Stream Power',
            'iGeo_Slope': 'Reach Average Slope',
            'iGeo_ElMax': 'Maximum Elevation of Reach',
            'iGeo_ElMin': 'Minimum Elevation of Reach',
            'iGeo_Len': 'Reach Length'
        }

        # The report has a core CSS file but we can extend it with our own if we want:
        css_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'brat_report.css')
        self.add_css(css_path)

        # create a folder to store the plots in
        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        safe_makedirs(self.images_dir)

        # Now we just need to write the sections we want in the report in order
        outputs_section = self.section('Outputs', 'Model Outputs')
        pEl = ET.Element('p')
        pEl.text = 'This report summarizes the model outputs for a particular run of BRAT, and provides some information on the input data used and intermediate data generated. For more detailed model documentation, visit the '
        aEl = ET.SubElement(pEl, 'a', {'href': 'https://tools.riverscapes.net/brat'})
        aEl.text = 'BRAT Documentation'
        outputs_section.append(pEl)
        self.dam_capacity(outputs_section)
        self.conservation(outputs_section)
        intermediates_section = self.section('Intermediates', 'Model Intermediates')
        self.reach_slope(intermediates_section)
        self.hydrology_plots(intermediates_section)
        self.anthro_intermediates(intermediates_section)
        # self.geophysical_summary(intermediates_section)
        self.ownership(intermediates_section)
        inputs_section = self.section('Inputs', 'Model Inputs')
        self.drainage_network(inputs_section)
        self.vegetation(inputs_section)

        # self.reach_attribute_summaries()

        self.log.info('Finished writing report')

    def dam_capacity(self, parent_sec):

        self.log.info('Summarizing dam capacity outputs')
        section = self.section('DamCapacity', 'BRAT Dam Capacity Results', parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'The BRAT capacity model produces values for each stream segment that represent the density of dams (dams/km or dams/mi) that the segment can support based on the nearby vegetation, the reaches hydrology, and its slope. The output field `oCC_EX` represents total capacity based on these inputs with existing vegetation cover, and `oCC_HPE` represents the capacity based on these inputs and modeled historic vegetation cover. For display output values are categorized based on these bins:'
        section.append(pEl)

        cap_dict = {
            'None': '0 dams',
            'Rare': '0-1 dams/km (0-2 dams/mi)',
            'Occasional': '1-5 dams/km (2-8 dams/mi)',
            'Frequent': '5-15 dams/km (8-24 dams/mi)',
            'Pervasive': '15-40 dams/km (24-64 dams/mi)'
        }
        RSReport.create_table_from_dict(cap_dict, section)

        pEl2 = ET.Element('p')
        pEl2.text = 'The following table contains the total beaver dam capacity for the watershed based on existing and historic vegetation. The vegetation only entries are capacitites based on only the vegetation fuzzy inference system; the others are based on the combined fuzzy inferences system that acocunts for hydrology and slope.'
        section.append(pEl2)

        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()
        fields = [
            ('Existing capacity (vegetation only)', 'Sum((iGeo_Len / 1000) * oVC_EX)'),
            ('Historic capacity (vegetation only)', 'Sum((iGeo_Len / 1000) * oVC_HPE)'),
            ('Existing capacity', 'Sum((iGeo_Len / 1000) * oCC_EX)'),
            ('Historic capacity', 'Sum((iGeo_Len / 1000) * oCC_HPE)')
        ]

        curs.execute('SELECT {} FROM vwReaches'.format(', '.join([field for label, field in fields])))
        row = curs.fetchone()

        table_dict = {fields[i][0]: int(row[fields[i][1]]) for i in range(len(fields))}
        RSReport.create_table_from_dict(table_dict, section)

        # self.dam_capacity_lengths()  # 'oCC_EX', section)
        # self.dam_capacity_lengths()  # 'oCC_HPE', section)

        pEl3 = ET.Element('p')
        pEl3.text = 'The following plots summarize the outputs for existing dam capacity and historic dam capacity.'
        section.append(pEl3)

        subsection = self.section(None, 'Existing Dam Capacity', section, level=3)

        self.attribute_table_and_pie('oCC_EX', [
            {'label': 'None', 'upper': 0},
            {'label': 'Rare', 'lower': 0, 'upper': 1},
            {'label': 'Occasional', 'lower': 1, 'upper': 5},
            {'label': 'Frequent', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive', 'lower': 15}
        ], subsection)

        subsection2 = self.section(None, 'Historic Dam Capacity', section, level=3)

        self.attribute_table_and_pie('oCC_HPE', [
            {'label': 'None', 'upper': 0},
            {'label': 'Rare', 'lower': 0, 'upper': 1},
            {'label': 'Occasional', 'lower': 1, 'upper': 5},
            {'label': 'Frequent', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive', 'lower': 15}
        ], subsection2)

    def conservation(self, parent_sec):

        self.log.info('Summarizing management outputs')
        section = self.section('Conservation', 'Conservation and Management', parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'BRAT produces several conservation and management outputs that can be utilized to inform planning for restoration using beaver or conservation of riverscapes influenced by beaver dam building. These include outputs representing risk of conflict from dam building activity, opportunities for conservation of beaver or restoration using beaver, and factors limiting beaver building activity.'
        section.append(pEl)

        fields = [
            ('Risk', 'DamRisks', 'RiskID'),
            ('Opportunity', 'DamOpportunities', 'OpportunityID'),
            ('Limitation', 'DamLimitations', 'LimitationID')
        ]

        for label, table, idfield in fields:
            # RSReport.header(3, label, section)
            section = self.section(None, label, section, level=3)
            pEl2 = ET.Element('p')
            if label == 'Risk':
                pEl2.text = 'Risk of conflict from dam building activity is based on the possibility of dam building, land use intensity, and proximity to infrastructure.'
            elif label == 'Opportunity':
                pEl2.text = 'Opportunities for restoration/conservation are based on the difference between historic and existing dam building capacity, land use intensity, and risk of undesireable dams.'
            elif label == 'Limitation':
                pEl2.text = 'Limitation categories characterize areas where dam building is not possible, either due to natural or anthropogenic factors.'
            section.append(pEl2)
            table_data = RSReport.create_table_from_sql(
                [label, 'Total Length (km)', 'Total Length (mi)', 'Reach Count', '%'],
                'SELECT DR.Name, ROUND(Sum(iGeo_Len) / 1000, 2), ROUND(Sum(iGeo_Len)/1609, 2), Count(R.{1}), ROUND(100 * Sum(iGeo_Len) / TotalLength, 2)'
                ' FROM {0} DR LEFT JOIN vwReaches R ON DR.{1} = R.{1}'
                ' JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM vwReaches)'
                ' GROUP BY DR.{1}'.format(table, idfield),
                self.database, section)

            pie_path = os.path.join(self.images_dir, '{}_pie.png'.format(label))
            col = [self.bratcolors[x[0]] for x in table_data]
            pie([x[3] for x in table_data], [x[0] for x in table_data], '{} by Stream Length'.format(label), col, pie_path)

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
            horizontal_bar([x[1] for x in table_data], [x[0] for x in table_data], col, 'Reach Length (km)', '{} by Stream Length'.format(label), bar_path, 'Reach Length (mi)')

            plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
            img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
            img = ET.Element('img', attrib={
                'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(bar_path)),
                'alt': 'bar_chart'
            })
            img_wrap.append(img)
            plot_wrapper.append(img_wrap)
            section.append(plot_wrapper)

        # RSReport.header(2, 'Conflict Attributes', section)

        # pEl3 = ET.Element('p')
        # pEl3.text = 'This charts and plots in this section illustrate the statistics for distance to infrastructure and land use intenstiy that go into the risk model. `iPC_Canal` is the distance to the nearest canal for each reach, `iPC_DivPts` is the distance to the nearest stream diversions, and `iPC_Privat` is the distance to private land ownership.'
        # section.append(pEl3)

        # for attribute in ['iPC_Canal', 'iPC_DivPts', 'iPC_Privat']:
        #     self.reach_attribute(attribute, section)

    def reach_slope(self, parent_sec):

        section = self.section('Slope', 'Reach Average Slope', parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'Summary of reach average slopes. At very low slopes, dam building capacity is reduced because a single dam backs water up farther, and at high slopes (>~23 degrees), beaver typically do not build dams.'
        section.append(pEl)

        self.attribute_table_and_pie('iGeo_Slope', [
            {'label': '0 - 0.5%', 'upper': 0.005},
            {'label': '0.5 - 1%', 'lower': 0.005, 'upper': 0.01},
            {'label': '1 - 3%', 'lower': 0.01, 'upper': 0.03},
            {'label': '3 - 8%', 'lower': 0.03, 'upper': 0.08},
            {'label': '8 - 23%', 'lower': 0.08, 'upper': 0.23},
            {'label': '>23%', 'lower': 0.23}
        ], section)

    def hydrology_plots(self, parent_sec):
        self.log.info('Recording hydrology information')
        section = self.section('HydrologyPlots', 'Hydrology', parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'Below are the equations used to estimate baseflow and peak flow (~two-year recurrence interval), as well as the values used for the parameters in those equations.'
        section.append(pEl)

        conn = sqlite3.connect(self.database)
        curs = conn.cursor()

        curs.execute('SELECT MaxDrainage, QLow, Q2 FROM Watersheds')
        row = curs.fetchone()
        RSReport.create_table_from_dict({
            'Drainage area threshold (sqkm) above which dams are not built': row[0],
            'Baseflow equation': row[1],
            'Peak Flow equation': row[2]
        }, section, attrib={'class': 'fullwidth'})

        allequns = row[1] + row[2]

        RSReport.header(3, 'Hydrological Parameters', section)
        RSReport.create_table_from_sql(
            ['Parameter', 'Data Value', 'Data Units', 'Conversion Factor', 'Equation Value', 'Equation Units'],
            'SELECT Parameter, Value, DataUnits, Conversion, ConvertedValue, EquationUnits FROM vwHydroParams WHERE \'{0}\' LIKE \'{1}\'||Parameter||\'{1}\''.format(allequns, '%'),
            self.database, section, attrib={'class': 'fullwidth'})

        variables = [
            ('iHyd_QLow', 'Baseflow (CFS)'),
            ('iHyd_Q2', 'Peak Flow (CFS)'),
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

        # Low Stream Power
        RSReport.header(3, 'Base Flow Stream Power', section)

        pEl2 = ET.Element('p')
        pEl2.text = 'Low flow stream power helps determine whether or not dam building activity can occur at base flows. Below are the stream power values and their associated categories.'
        section.append(pEl2)

        sp_dict = {
            'Can Build Dam': '0 - 160 Watts',
            'Probably Can Build Dam': '160 - 185 Watts',
            'Cannot Build Dam': '>185 Watts'
        }
        RSReport.create_table_from_dict(sp_dict, section)

        self.attribute_table_and_pie('iHyd_SPLow', [
            {'label': 'Can Build Dam', 'upper': 16},
            {'label': 'Probably Can Build Dam', 'lower': 16, 'upper': 185},
            {'label': 'Cannot Build Dam', 'lower': 185}
        ], section)

        # High Stream Power
        RSReport.header(3, 'High Flow Stream Power', section)

        pEl3 = ET.Element('p')
        pEl3.text = 'High flow stream power helps determine whether or not dams can persist at typical flood flows. Below are the stream power values and their associated categories.'
        section.append(pEl3)

        sp_dict2 = {
            'Dam Persists': '0 - 1100 Watts',
            'Potential Dam Breach': '1100 - 1400 Watts',
            'Potentail Dam Blowout': '1400 - 2200 Watts',
            'Dam Blowout': '>2200 Watts'
        }
        RSReport.create_table_from_dict(sp_dict2, section)
        self.attribute_table_and_pie('iHyd_SP2', [
            {'label': 'Dam Persists', 'upper': 1100},
            {'label': 'Potential Dam Breach', 'lower': 1100, 'upper': 1400},
            {'label': 'Potential Dam Blowout', 'lower': 1400, 'upper': 2200},
            {'label': 'Dam Blowout', 'lower': 2200}
        ], section)

    def anthro_intermediates(self, parent_sec):

        section = self.section('Proximity to Infrastructure', title='Proximity to Infrastructure', el_parent=parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'Summary for distance to nearest infrastructure, which is used for modeling risk of undesirable dam building.'
        section.append(pEl)

        dist_dict = {
            'Not Close': '> 1 km',
            'Outside Range of Concern': '300 m - 1 km',
            'Within Plausible Forage Range': '100 m - 300 m',
            'Within Normal Forage Range': '30 m - 100 m',
            'Immediately Adjacent': '0 m - 30 m'
        }
        RSReport.create_table_from_dict(dist_dict, section)

        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()
        curs.execute('SELECT oPC_Dist FROM vwReaches')
        distances = [row for row in curs.fetchall() if row['oPC_Dist'] is not None]
        if len(distances) > 0:
            self.attribute_table_and_pie('oPC_Dist', [
                {'label': 'Not Close', 'lower': 1000},
                {'label': 'Outside Range of Concern', 'lower': 300, 'upper': 1000},
                {'label': 'Within Plausable Forage Range', 'lower': 100, 'upper': 300},
                {'label': 'Within Normal Forage Range', 'lower': 30, 'upper': 100},
                {'label': 'Immediately Adjacent', 'upper': 30}
            ], section)
        else:
            pEl2 = ET.Element('p')
            pEl2.text = 'No infrastructure present in this watershed.'
            section.append(pEl2)

        section2 = self.section('Land Use Intensity', title='Land Use Intensity', el_parent=parent_sec, level=2)

        pEl2 = ET.Element('p')
        pEl2.text = 'Summary for land use intensity averaged over the zone surrounding each stream reach, used in modeling potential for conflict from dam building.'
        section2.append(pEl2)

        self.attribute_table_and_pie('iPC_LU', [
            {'label': 'Very Low', 'upper': 0},
            {'label': 'Low', 'lower': 0, 'upper': 33},
            {'label': 'Moderate', 'lower': 33, 'upper': 66},
            {'label': 'High', 'lower': 66}
        ], section2)

    def ownership(self, parent_sec):
        section = self.section('Ownership', 'Ownership', parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'This section summarizes the length of stream reaches that intersect different land ownership.'
        section.append(pEl)

        table_data = RSReport.create_table_from_sql(
            ['Ownership Agency', 'Number of Reach Segments', 'Length (km)', 'Length (mi)', '% of Total Length'],
            'SELECT IFNULL(Agency, "None"), Count(ReachID), ROUND(Sum(iGeo_Len) / 1000, 2), ROUND(Sum(iGeo_Len)/1609, 2), ROUND(100* Sum(iGeo_Len) / TotalLength, 2) FROM vwReaches'
            ' INNER JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM vwReaches) GROUP BY Agency',
            self.database, section, attrib={'class': 'fullwidth'})

        bar_path = os.path.join(self.images_dir, 'Ownership_bar.png')
        col = []
        for x in table_data:
            if x[0] in self.bratcolors:
                col.append(self.bratcolors[x[0]])
            else:
                col.append('#b2b2b2')
        horizontal_bar([i[2] for i in table_data], [i[0] for i in table_data], col, 'Reach Length (km)', 'Ownership by Stream Length', bar_path, 'Reach_Length (mi)')

        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(bar_path)),
            'alt': 'bar_chart'
        })
        img_wrap.append(img)
        plot_wrapper.append(img_wrap)

        section.append(plot_wrapper)

    def drainage_network(self, parent_sec):
        # Create a section node to start adding things to. Section nodes are added to the table of contents if
        # they have a title. If you don't specify a el_parent argument these sections will simply be added
        # to the report body in the order you call them.

        self.log.info('Summarizing watershed and drainage area information')
        section = self.section('ReportIntro', 'Drainage Network', parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'This section contains information on the drainage network used in this run of BRAT, as well as some information about the watershed.'
        section.append(pEl)

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

        table_data = RSReport.create_table_from_sql(
            ['Reach Type', 'Total Length (km)', '% of Total'],
            'SELECT ReachType, ROUND(Sum(iGeo_Len) / 1000, 1) As Length, ROUND(100 * Sum(iGeo_Len) / TotalLength, 1) AS TotalLength '
            'FROM vwReaches INNER JOIN (SELECT ROUND(Sum(iGeo_Len), 1) AS TotalLength FROM vwReaches) GROUP BY ReachType',
            self.database, table_wrapper, attrib={'id': 'SummTable_sql'})

        # create a list of colors from lables using the color dictionary
        col = [self.bratcolors[x[0]] for x in table_data]
        bar_path = os.path.join(self.images_dir, 'Reach_type_bar.png')
        horizontal_bar([x[1] for x in table_data], [x[0] for x in table_data], col, 'Reach Length (km)', 'Reach Types', bar_path, 'Reach Length (mi)')

        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={
            'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(bar_path)),
            'alt': 'bar_chart'
        })
        img_wrap.append(img)
        plot_wrapper.append(img_wrap)

        # Append my table_wrapper div (which now contains both tables above) to the section
        section.append(table_wrapper)
        section.append(plot_wrapper)

    def vegetation(self, parent_sec):
        self.log.info('Recording vegetation information')
        section = self.section('Vegetation', 'Vegetation', parent_sec, level=2)
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

    def reach_attribute(self, attribute, parent_el):
        # Use a class here because it repeats
        # section = self.section(None, attribute, parent_el, level=2)
        RSReport.header(3, attribute, parent_el)
        conn = sqlite3.connect(self.database)
        conn.row_factory = _dict_factory
        curs = conn.cursor()

        # Summary statistics (min, max etc) for the current attribute
        curs.execute('SELECT Count({0}) "Values", Max({0}) Maximum, Min({0}) Minimum, Avg({0}) Average FROM vwReaches WHERE {0} IS NOT NULL'.format(attribute))
        values = curs.fetchone()

        reach_wrapper_inner = ET.Element('div', attrib={'class': 'reachAtributeInner'})
        parent_el.append(reach_wrapper_inner)

        # Add the number of NULL values
        curs.execute('SELECT Count({0}) "NULL Values" FROM vwReaches WHERE {0} IS NULL'.format(attribute))
        values.update(curs.fetchone())
        RSReport.create_table_from_dict(values, reach_wrapper_inner)

        # Box plot
        image_path = os.path.join(self.images_dir, 'attribute_{}.png'.format(attribute))
        curs.execute('SELECT {0} FROM vwReaches WHERE {0} IS NOT NULL'.format(attribute))
        values = [row[attribute] for row in curs.fetchall()]
        box_plot(values, attribute, self.f_names[attribute], image_path)

        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={'class': 'boxplot', 'alt': 'boxplot', 'src': '{}/{}'.format(os.path.basename(self.images_dir), os.path.basename(image_path))})
        img_wrap.append(img)

        reach_wrapper_inner.append(img_wrap)

    def attribute_table_and_pie(self, attribute_field, bins, elParent):
        """
        Expect the bins as list of dictionaries with keys "label", "lower", "upper"
        """

        RSReport.header(3, '{} Summary'.format(self.f_names[attribute_field]), elParent)

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

        RSReport.create_table_from_tuple_list(['Category', 'Reach Count', 'Length (km)', 'Length (mi)', 'Percent (%)'], data, elParent)

        image_path = os.path.join(self.images_dir, '{}_pie.png'.format(attribute_field.lower()))
        col = [self.bratcolors[x[0]] for x in data]
        pie([x[4] for x in data], [x[0] for x in data], '{} by Stream Length'.format(self.f_names[attribute_field]), col, image_path)

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
        horizontal_bar([x[2] for x in data], [x[0] for x in data], col, 'Reach Length (km)', '{} by Stream Length'.format(self.f_names[attribute_field]), bar_path, 'Reach Length (Miles)')

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
            {'label': 'Can Build Dam', 'upper': 16},
            {'label': 'Probably Can Build Dam', 'lower': 16, 'upper': 185},
            {'label': 'Cannot Build Dam', 'lower': 185}
        ], section)

        # High Stream Power
        self.attribute_table_and_pie('iHyd_SP2', [
            {'label': 'Dam Persists', 'upper': 1100},
            {'label': 'Potential Dam Breach', 'lower': 1100, 'upper': 1400},
            {'label': 'Potential Dam Blowout', 'lower': 1400, 'upper': 2200},
            {'label': 'Dam Blowout', 'lower': 2200}
        ], section)

        # Distance
        self.attribute_table_and_pie('oPC_Dist', [
            {'label': 'Not Close', 'lower': 1000},
            {'label': 'Outside Range of Concern', 'lower': 300, 'upper': 1000},
            {'label': 'Within Plausable Forage Range', 'lower': 100, 'upper': 300},
            {'label': 'Within Normal Forage Range', 'lower': 30, 'upper': 100},
            {'label': 'Immediately Adjacent', 'upper': 300}
        ], section)

        # Existing Capacity
        self.attribute_table_and_pie('oCC_EX', [
            {'label': 'None', 'upper': 0},
            {'label': 'Rare', 'lower': 0, 'upper': 1},
            {'label': 'Occasional', 'lower': 1, 'upper': 5},
            {'label': 'Frequent', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive', 'lower': 15}
        ], section)

        # Distance
        self.attribute_table_and_pie('mCC_EX_CT', [
            {'label': 'No Dams', 'upper': 0},
            {'label': 'Single Dam', 'lower': 1, 'upper': 1},
            {'label': 'Small Complex', 'lower': 1, 'upper': 3},
            {'label': 'Medium Complex', 'lower': 3, 'upper': 5},
            {'label': 'Large Complex', 'lower': 5}
        ], section)

        # Historical Dam Capacity
        self.attribute_table_and_pie('oCC_HPE', [
            {'label': 'None', 'upper': 0},
            {'label': 'Rare', 'lower': 0, 'upper': 1},
            {'label': 'Occasional', 'lower': 1, 'upper': 5},
            {'label': 'Frequent', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive', 'lower': 15}
        ], section)

        # Historical Dam
        self.attribute_table_and_pie('mCC_HPE_CT', [
            {'label': 'No Dams', 'upper': 0},
            {'label': 'Single Dam', 'lower': 1, 'upper': 1},
            {'label': 'Small Complex', 'lower': 1, 'upper': 3},
            {'label': 'Medium Complex', 'lower': 3, 'upper': 5},
            {'label': 'Large Complex', 'lower': 5}
        ], section)

        # Existing Vegetation Dam Building Capacity
        self.attribute_table_and_pie('oVC_EX', [
            {'label': 'None', 'upper': 0},
            {'label': 'Rare', 'lower': 0, 'upper': 1},
            {'label': 'Occasional', 'lower': 1, 'upper': 5},
            {'label': 'Frequent', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive', 'lower': 15}
        ], section)

        # Historical Vegetation Dam Building Capacity
        self.attribute_table_and_pie('oVC_HPE', [
            {'label': 'None', 'upper': 0},
            {'label': 'Rare', 'lower': 0, 'upper': 1},
            {'label': 'Occasional', 'lower': 1, 'upper': 5},
            {'label': 'Frequent', 'lower': 5, 'upper': 15},
            {'label': 'Pervasive', 'lower': 15}
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

    def geophysical_summary(self, parent_sec):
        self.log.info('Summarizing geophysical reach attributes')
        section = self.section('ReachAttributeSummary', 'Geophysical Attributes', parent_sec, level=2)

        pEl = ET.Element('p')
        pEl.text = 'This section summarizes some of the geophysical attributes of the drainage network: slope, elevation, and network segment length.'
        section.append(pEl)

        attribs = [
            ('iGeo_Slope', 'Slope', 'ratio'),
            ('iGeo_ElMax', 'Max Elevation', 'metres'),
            ('iGeo_ElMin', 'Min Elevation', 'metres'),
            ('iGeo_Len', 'Length', 'metres'),
        ]
        plot_wrapper = ET.Element('div', attrib={'class': 'plots'})
        [self.reach_attribute(attribute, plot_wrapper) for attribute, name, units in attribs]

        section.append(plot_wrapper)


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

    cfg = ModelConfig('http://xml.riverscapes.net/Projects/XSD/V1/BRAT.xsd', __version__)
    project = RSProject(cfg, args.projectxml)
    report = BratReport(args.database, args.report_path, project)
    report.write()
