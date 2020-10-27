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

idCols = [
    'VegetationID',
    'Type ID'
]


def report(database, report_path):

    html, images_dir, inner_div = create_report(database, report_path)

    report_intro(database, images_dir, inner_div, 'BRAT', __version__)
    reach_attribute_summary(database, images_dir, inner_div)
    dam_capacity(database, inner_div)
    dam_capacity_lengths(database, inner_div, 'oCC_EX')
    dam_capacity_lengths(database, inner_div, 'oCC_HPE')

    hydrology_plots(database, images_dir, inner_div)
    ownership(database, inner_div)
    vegetation(database, images_dir, inner_div)
    conservation(database, images_dir, inner_div)

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


def hydrology_plots(database, images_dir, elParent):
    wrapper = ET.Element('div', attrib={'id': 'ReportIntro'})
    log = Logger('Report')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    header(2, 'Hydrology', wrapper)

    curs.execute('SELECT MaxDrainage, QLow, Q2 FROM Watersheds')
    row = curs.fetchone()
    create_table_from_dict({'Max Draiange (sqkm)': row[0], 'Baseflow': row[1], 'Peak Flow': row[2]}, wrapper)

    header(3, 'Hydrological Parameters', wrapper)
    create_table_from_sql(
        ['Parameter', 'Data Value', 'Data Units', 'Conversion Factor', 'Equation Value', 'Equation Units'],
        'SELECT Parameter, Value, DataUnits, Conversion, ConvertedValue, EquationUnits FROM vwHydroParams',
        database, wrapper)

    variables = [
        ('iHyd_QLow', 'Baseflow (CFS)'),
        ('iHyd_Q2', 'Peak Flow (CSF)'),
        ('iHyd_SPLow', 'Baseflow Stream Power (Watts'),
        ('iHyd_SP2', 'Peak Flow Stream Power (Watts)'),
        ('iGeo_Slope', 'Slope (degrees)')
    ]

    plot_wrapper = ET.Element('div', attrib={'class': 'hydroPlotWrapper'})
    wrapper.append(plot_wrapper)

    for variable, ylabel in variables:
        log.info('Generating XY scatter for {} against drainage area.'.format(variable))
        image_path = os.path.join(images_dir, 'drainage_area_{}.png'.format(variable.lower()))

        curs.execute('SELECT iGeo_DA, {} FROM Reaches'.format(variable))
        values = [(row[0], row[1]) for row in curs.fetchall()]
        xyscatter(values, 'Drainage Area (sqkm)', ylabel, variable, image_path)

        img_wrap = ET.Element('div', attrib={'class': 'imgWrap'})
        img = ET.Element('img', attrib={'src': '{}/{}'.format(os.path.basename(images_dir), os.path.basename(image_path))})
        img_wrap.append(img)
        plot_wrapper.append(img_wrap)

    elParent.append(wrapper)


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


def vegetation(database, image_dir, elParent):

    wrapper = ET.Element('div', attrib={'class': 'Vegetation'})
    for epochid, veg_type in [(2, 'Historic Vegetation'), (1, 'Existing Vegetation')]:

        header(2, veg_type, wrapper)

        pEl = ET.Element('p')
        pEl.text = 'The 30 most common {} types within the 100m reach buffer.'.format(veg_type.lower())
        wrapper.append(pEl)

        create_table_from_sql(['Vegetation ID', 'Vegetation Type', 'Total Area (sqkm)', 'Default Suitability', 'Override Suitability', 'Effective Suitability'],
                              """SELECT VegetationID,
                              Name, (CAST(TotalArea AS REAL) / 1000000) AS TotalAreaSqKm,
                              DefaultSuitability,
                              OverrideSuitability,
                              EffectiveSuitability
                              FROM vwReachVegetationTypes WHERE (EpochID = {}) AND (Buffer = 100) ORDER BY TotalArea DESC LIMIT 30""".format(epochid), database, wrapper)

        try:
            # Calculate the area weighted suitability
            conn = sqlite3.connect(database)
            curs = conn.cursor()
            curs.execute("""SELECT WeightedSum / SumTotalArea FROM
                        (SELECT Sum(CAST(TotalArea AS REAL) * CAST(EffectiveSuitability AS REAL) / 1000000) WeightedSum FROM vwReachVegetationTypes WHERE EpochID = {0} AND Buffer = 100)
                        JOIN
                        (SELECT CAST(Sum(TotalArea) AS REAL) / 1000000 SumTotalArea FROM vwReachVegetationTypes WHERE EpochID = {0} AND Buffer = 100)""".format(epochid))
            area_weighted_avg_suitability = curs.fetchone()[0]

            header(3, 'Suitability Breakdown', wrapper)
            pEl = ET.Element('p')
            pEl.text = """The area weighted average {} suitability is {}.
                The breakdown of the percentage of the 100m buffer within each suitability class across all reaches in the watershed.""".format(veg_type.lower(), format_value(area_weighted_avg_suitability)[0])
            wrapper.append(pEl)

            create_table_from_sql(['Suitability Class', '% with 100m Buffer'],
                                  """SELECT EffectiveSuitability, 100.0 * SArea / SumTotalArea FROM 
                (SELECT CAST(Sum(TotalArea) AS REAL) / 1000000 SArea, EffectiveSuitability FROM vwReachVegetationTypes WHERE EpochID = {0} AND Buffer = 100 GROUP BY EffectiveSuitability)
                JOIN
                (SELECT CAST(Sum(TotalArea) AS REAL) / 1000000 SumTotalArea FROM vwReachVegetationTypes WHERE EpochID = {0} AND Buffer = 100)
                ORDER BY EffectiveSuitability""".format(epochid), database, wrapper)
        except Exception as ex:
            log = Logger('Report')
            log.warning('Error calculating vegetation report')

    elParent.append(wrapper)


def conservation(database, images_dir, elParent):
    wrapper = ET.Element('div', attrib={'class': 'Conservation'})
    header(2, 'Conservation', wrapper)

    fields = [
        ('Risk', 'DamRisks', 'RiskID'),
        ('Opportunity', 'DamOpportunities', 'OpportunityID'),
        ('Limitation', 'DamLimitations', 'LimitationID')
    ]

    for label, table, idfield in fields:

        header(3, label, wrapper)

        create_table_from_sql(
            [label, 'Total Length (km)', 'Reach Count', '%'],
            'SELECT DR.Name, Sum(iGeo_Len) / 1000, Count(R.{1}), 100 * Sum(iGeo_Len) / TotalLength'
            ' FROM {0} DR LEFT JOIN Reaches R ON DR.{1} = R.{1}'
            ' JOIN (SELECT Sum(iGeo_Len) AS TotalLength FROM Reaches)'
            ' GROUP BY DR.{1}'.format(table, idfield),
            database, wrapper)

    header(3, 'Conflict Attributes', wrapper)

    for attribute in ['iPC_Canal', 'iPC_DivPts', 'iPC_Privat']:
        reach_attribute(database, attribute, 'meters', images_dir, wrapper)

    elParent.append(wrapper)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to the BRAT database', type=str)
    parser.add_argument('report_path', help='Output path where report will be generated', type=str)
    args = dotenv.parse_args_env(parser)

    report(args.database, args.report_path)


if __name__ == '__main__':
    main()
