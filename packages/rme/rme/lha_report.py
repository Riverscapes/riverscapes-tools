"""
Land Health Assessment Report (LHA) for Riverscapes.
May 2025. For Alden.
This is the next version of the Watershed Health Condition Assessment Report (WHCA).
"""
from typing import List, Dict, Tuple
import os
import sys
import argparse
import sqlite3
import json
from xml.etree import ElementTree as ET
import matplotlib.pyplot as plt
from rsxml import Logger, dotenv
from rscommons import RSReport
from rscommons.util import safe_makedirs
from rscommons.plotting import pie, horizontal_bar
from rme.__version__ import __version__

from .utils.hypsometric_curve import hipsometric_curve
from .utils.blm_charts import charts as blm_charts, vegetation_charts, land_ownership_labels, riparian_charts

ACRES_PER_SQ_METRE = 0.000247105
ACRES_PER_SQ_KM = 247.105
MILES_PER_KM = 0.621371
SQ_MILES_PER_SQ_KM = 0.386102159
SQ_MILES_PER_SQ_M = 0.000000386102159
SQ_KM_PER_SQ_M = 0.000001
MILES_PER_M = 0.000621371
ACRES_PER_SQ_M = 0.000247105

BLM_COLOR = '#F7E794'
NON_BLM_COLOR = 'grey'

BAR_WIDTH = 0.35
PLOT_ALPHA = 0.5

huc10_data = [
    {
        "key": "1705010303",
        "name": "Castle Creek",
        "AreaAcres": 198177.64
    },
    {
        "key": "1705010304",
        "name": "Swan Falls-Snake River",
        "AreaAcres": 207184.9
    },
    {
        "key": "1705010413",
        "name": "Headwaters Deep Creek",
        "AreaAcres": 98010.25
    },
    {
        "key": "1705010416",
        "name": "Deep Creek",
        "AreaAcres": 74883.88
    },
    {
        "key": "1705010417",
        "name": "Red Canyon-Owyhee River",
        "AreaAcres": 93008.51
    },
    {
        "key": "1705010703",
        "name": "Middle Fork Owyhee River",
        "AreaAcres": 70690.22
    },
    {
        "key": "1705010704",
        "name": "North Fork Owyhee River",
        "AreaAcres": 141409.03
    },
    {
        "key": "1705010801",
        "name": "Rock Creek",
        "AreaAcres": 106105.49
    },
    {
        "key": "1705010802",
        "name": "Big Boulder Creek",
        "AreaAcres": 85647.5
    }
]

huc8_data = [
    {
        "key": "17050103",
        "name": "Middle Snake-Succor",
        "areaacres": 1498752.95
    },
    {
        "key": "17050107",
        "name": "Middle Owyhee",
        "areaacres": 956880.59
    },
    {
        "key": "17050108",
        "name": "Jordan",
        "areaacres": 836556.7
    },
    {
        "key": "17050104",
        "name": "Upper Owyhee",
        "areaacres": 1371846.37
    }
]


cols = ['huc10', 'state', 'county', 'ownership_name', 'ALLOT_NAME', 'ALLOT_NO', 'PAST_NAME', 'PAST_NO', 'GIS_ACRES', 'centerline_length', 'segment_area', 'FCode', 'fcode_name',
        'dgoid', 'ownership', 'state', 'county', 'drainage_area', 'watershed_id', 'stream_name', 'stream_order', 'headwater', 'stream_length', 'waterbody_type', 'waterbody_extent',
        'ecoregion3', 'ecoregion4', 'ownership_name', 'dgoid', 'brat_capacity', 'brat_hist_capacity', 'brat_risk', 'brat_opportunity', 'brat_limitation', 'brat_complex_size',
        'brat_hist_complex_size', 'dam_setting', 'dgoid', 'prim_channel_gradient', 'valleybottom_gradient', 'rel_flow_length', 'confluences', 'diffluences', 'tributaries', 'tribs_per_km',
        'planform_sinuosity', 'lowlying_area', 'elevated_area', 'channel_area', 'floodplain_area', 'integrated_width', 'active_channel_ratio', 'low_lying_ratio', 'elevated_ratio', 'floodplain_ratio',
        'acres_vb_per_mile', 'hect_vb_per_km', 'channel_width', 'confinement_ratio', 'constriction_ratio', 'confining_margins', 'constricting_margins', 'dgoid', 'qlow', 'q2', 'splow', 'sphigh', 'dgoid',
        'road_len', 'road_dens', 'rail_len', 'rail_dens', 'land_use_intens', 'road_dist', 'rail_dist', 'div_dist', 'canal_dist', 'infra_dist', 'fldpln_access', 'access_fldpln_extent', 'dgoid', 'strmminelev', 'strmmaxelev', 'clminelev', 'clmaxelev',
        'strmleng', 'valleng', 'strmstrleng', 'dgoid', 'lf_evt', 'lf_bps', 'lf_agriculture_prop', 'lf_agriculture', 'lf_conifer_prop',
        'lf_conifer', 'lf_conifer_hardwood_prop', 'lf_conifer_hardwood', 'lf_developed_prop', 'lf_developed', 'lf_exotic_herbaceous_prop', 'lf_exotic_herbaceous',
        'lf_exotic_tree_shrub_prop', 'lf_exotic_tree_shrub', 'lf_grassland_prop', 'lf_grassland', 'lf_hardwood_prop', 'lf_hardwood', 'lf_riparian_prop',
        'lf_riparian', 'lf_shrubland_prop', 'lf_shrubland', 'lf_sparsely_vegetated_prop', 'lf_sparsely_vegetated', 'lf_hist_conifer_prop',
        'lf_hist_conifer', 'lf_hist_conifer_hardwood_prop', 'lf_hist_conifer_hardwood', 'lf_hist_grassland_prop', 'lf_hist_grassland',
        'lf_hist_hardwood_prop', 'lf_hist_hardwood', 'lf_hist_hardwood_conifer_prop', 'lf_hist_hardwood_conifer', 'lf_hist_peatland_forest_prop',
        'lf_hist_peatland_forest', 'lf_hist_peatland_nonforest_prop', 'lf_hist_peatland_nonforest', 'lf_hist_riparian_prop', 'lf_hist_riparian',
        'lf_hist_savanna_prop', 'lf_hist_savanna', 'lf_hist_shrubland_prop', 'lf_hist_shrubland', 'lf_hist_sparsely_vegetated_prop',
        'lf_hist_sparsely_vegetated', 'ex_riparian', 'hist_riparian', 'prop_riparian', 'hist_prop_riparian', 'riparian_veg_departure',
        'ag_conversion', 'develop', 'grass_shrub_conversion', 'conifer_encroachment', 'invasive_conversion', 'riparian_condition']

sum_fields = [
    'centerline_length',
    'segment_area'
]

stat_fields = [
    'brat_capacity',
    'riparian_veg_departure',
    'hist_prop_riparian',
    'prim_channel_gradient', 'valleybottom_gradient', 'rel_flow_length', 'confluences', 'diffluences', 'tributaries', 'tribs_per_km',
    'planform_sinuosity', 'lowlying_area', 'elevated_area', 'channel_area', 'floodplain_area', 'integrated_width', 'active_channel_ratio', 'low_lying_ratio', 'elevated_ratio', 'floodplain_ratio',
    'acres_vb_per_mile', 'hect_vb_per_km', 'channel_width', 'confinement_ratio', 'constriction_ratio', 'confining_margins', 'constricting_margins',
    'lf_evt', 'lf_bps', 'lf_agriculture_prop', 'lf_agriculture', 'lf_conifer_prop',
    'lf_conifer', 'lf_conifer_hardwood_prop', 'lf_conifer_hardwood', 'lf_developed_prop', 'lf_developed', 'lf_exotic_herbaceous_prop', 'lf_exotic_herbaceous',
    'lf_exotic_tree_shrub_prop', 'lf_exotic_tree_shrub', 'lf_grassland_prop', 'lf_grassland', 'lf_hardwood_prop', 'lf_hardwood', 'lf_riparian_prop',
    'lf_riparian', 'lf_shrubland_prop', 'lf_shrubland', 'lf_sparsely_vegetated_prop', 'lf_sparsely_vegetated', 'lf_hist_conifer_prop',
    'lf_hist_conifer', 'lf_hist_conifer_hardwood_prop', 'lf_hist_conifer_hardwood', 'lf_hist_grassland_prop', 'lf_hist_grassland',
    'lf_hist_hardwood_prop', 'lf_hist_hardwood', 'lf_hist_hardwood_conifer_prop', 'lf_hist_hardwood_conifer', 'lf_hist_peatland_forest_prop',
    'lf_hist_peatland_forest', 'lf_hist_peatland_nonforest_prop', 'lf_hist_peatland_nonforest', 'lf_hist_riparian_prop', 'lf_hist_riparian',
    'lf_hist_savanna_prop', 'lf_hist_savanna', 'lf_hist_shrubland_prop', 'lf_hist_shrubland', 'lf_hist_sparsely_vegetated_prop',
    'lf_hist_sparsely_vegetated', 'ex_riparian', 'hist_riparian', 'prop_riparian', 'hist_prop_riparian', 'riparian_veg_departure',
    'ag_conversion', 'develop', 'grass_shrub_conversion', 'conifer_encroachment', 'invasive_conversion', 'riparian_condition'
]

fcode_names = ['Perennial', 'Intermittent', 'Ephemeral', 'Canals, Pipes and Ditches']
pivot_fields = [f"SUM(CASE WHEN fcode_name = '{fcode}' THEN centerline_length ELSE 0 END) AS \"{fcode}\"" for fcode in fcode_names]


all_fields = []

all_fields.extend([{
    'field': field_name,
    'methods': ['sum'],
    'conversion': MILES_PER_M
} for field_name in sum_fields])

all_fields.extend([{
    'field': field_name,
    'methods': ['sum', 'min', 'max', 'avg'],
} for field_name in stat_fields])

pivot_fields = [
    {
        'english': 'Perennial Stream Length (mi)',
        'name': 'perennial',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Perennial'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    },
    {
        'english': 'Intermittent Stream Length (mi)',
        'name': 'intermittent',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Intermittent'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    },
    {
        'english': 'Ephemeral Stream Length (mi)',
        'name': 'ephemeral',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Ephemeral'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    },
    {
        'english': 'Canals, Pipes and Ditches Stream Length (mi)',
        'name': 'Canals, Pipes and Ditches',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Canals, Pipes and Ditches'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    },
    {
        'english': 'BLM Perennial Stream Length (mi)',
        'name': 'blm_perennial',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Perennial' and ownership_name = 'BLM'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    },
    {
        'english': 'BLM Intermittent Stream Length (mi)',
        'name': 'blm_intermittent',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Intermittent' and ownership_name = 'BLM'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    },
    {
        'english': 'Non-BLM Perennial Stream Length (mi)',
        'name': 'non_blm_perennial',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Perennial' and ownership_name <> 'BLM'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    },
    {
        'english': 'Non-BLM Intermittent Stream Length (mi)',
        'name': 'non_blm_intermittent',
        'field': 'centerline_length',
        'whereClause': "fcode_name = 'Intermittent' and ownership_name <> 'BLM'",
        'methods': ['pivot'],
        'conversion': MILES_PER_M
    }
]
all_fields.extend(pivot_fields)

for risk_value in ["Minor Risk", "Negligible Risk", "Some Risk", "Considerable Risk"]:
    all_fields.append({
        'english': f'BRAT {risk_value} Stream Length (mi)',
        'name': risk_value,
        'field': 'centerline_length',
        'methods': ['pivot'],
        'whereClause': f"brat_risk = '{risk_value}'",
        'conversion': MILES_PER_M
    })

for limit_value in ["Anthropogenically Limited", "Dam Building Possible", "Naturally Vegetation Limited", "Other", "Slope Limited", "Stream Power Limited", "Stream Size Limited"]:
    all_fields.append({
        'english': f'BRAT {limit_value} Stream Length (mi)',
        'name': limit_value,
        'field': 'centerline_length',
        'methods': ['pivot'],
        'whereClause': f"brat_limitation = '{limit_value}'",
        'conversion': MILES_PER_M
    })


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class LHAReport(RSReport):
    """ Land Health Assessment Report """

    def __init__(self, rme_scrape: str, input_dir: str, output_dir: str, report_path: str, verbose: bool = False):
        super().__init__(None, report_path)
        self.huc = None
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.images_dir = os.path.join(os.path.dirname(report_path), 'images')
        self.verbose = verbose

        # Load JSON field aliases
        field_aliases = {}
        with open('/workspaces/data/rme_v3_fields.json', 'r', encoding='utf-8') as f:
            field_aliases = json.load(f)
        field_aliases = {f['fieldName']: f['fullName'] for f in field_aliases}

        # The report has a core CSS file but we can extend it with our own if we want:
        css_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'rme_report.css')
        self.add_css(css_path)

        safe_makedirs(self.images_dir)

        where_clauses = [
            ('BLM', "(ownership_name = 'BLM')"),
            ('Non-BLM', "(ownership_name <> 'BLM')"),
            ('Perennial', "(fcode_name = 'Perennial')"),
            ('Non-Perennial', "(fcode_name <> 'Perennial')"),
        ]

        for clause_name, clause in where_clauses:
            print(f'Processing {clause_name}...')

            data = {}
            with sqlite3.connect(rme_scrape) as conn:
                conn.row_factory = dict_factory
                curs = conn.cursor()

                fields_list = []
                for field in all_fields:
                    field_name = field.get('name', field['field'])
                    conversion = field.get('conversion', 1)
                    for method in field['methods']:
                        if method == 'pivot':
                            fields_list.append(f'SUM(CASE WHEN {field["whereClause"]} THEN {conversion} * {field["field"]} ELSE 0 END) AS "pivot_{field_name}"')
                        else:
                            fields_list.append(f'{conversion} * {method}({field["field"]}) as {method}_{field_name}')

                curs.execute(f'''
                    SELECT
                        ALLOT_NAME,
                        PAST_NAME,
                        {",".join(fields_list)}
                    FROM dgos d
                            inner join dgo_desc dd on d.dgoid = dd.dgoid
                            inner join igos_pastures i on d.level_path = i.level_path and d.seg_distance = i.seg_distance
                    inner join dgo_beaver db on d.dgoid = db.dgoid
                    inner join dgo_geomorph dg on d.dgoid = dg.dgoid
                    inner join dgo_hydro dh on d.dgoid = dh.dgoid
                    inner join dgo_impacts di on d.dgoid = di.dgoid
                    inner join dgo_measurements dm on d.DGOID = dm.dgoid
                    inner join dgo_veg dv on d.dgoid = dv.dgoid
                    where i.ALLOT_NAME is not null
                        and {clause}
                    group by ALLOT_NAME, PAST_NAME''')

                for row in curs.fetchall():
                    allotment = row['ALLOT_NAME']
                    pasture = row['PAST_NAME']
                    data.setdefault(allotment, {})
                    data[allotment].setdefault(pasture, {})

                    for field in all_fields:
                        for method in field['methods']:
                            col_name = f'{method}_{field.get("name", field["field"])}'
                            out_name = method + ' ' + field.get('english', field_aliases.get(field['field'], field.get('name', field['field'])))
                            out_name = out_name.replace('pivot', 'sum')
                            data[allotment][pasture][out_name] = row[col_name]

            # write the data to a JSON file
            with open(f'/workspaces/data/{clause_name}-metrics.json', 'w', encoding='utf-8') as f:
                f.write(json.dumps(data, indent=4))

            # Load your JSON data (assuming it's stored in a variable called `data`)
            # with open('your_file.json', 'r') as f:
            #     data = json.load(f)

            # Output tab-delimited file
            output_file = os.path.join(os.path.dirname(rme_scrape), f'{clause_name}_output.tsv')
            with open(output_file, 'w', encoding='utf8') as out:
                # Write header
                headers = ["Allotment", "Pasture"]
                # Get all metric keys from the first pasture found
                for allotment in data.values():
                    for pasture_data in allotment.values():
                        headers.extend(pasture_data.keys())
                        break
                    break
                out.write("\t".join(headers) + "\n")

                # Write data rows
                for allotment_name, pastures in data.items():
                    for pasture_name, metrics in pastures.items():
                        row = [allotment_name, pasture_name]
                        for key in headers[2:]:
                            row.append(str(metrics.get(key, "")))  # Use empty string if key is missing
                        out.write("\t".join(row) + "\n")

        # for allot, allot_data in data.items():
        #     for past, past_data in allot_data.items():
        #         for metric, value in past_data:

        return

        #############################################################################
        curs.execute('''
SELECT d.huc10,
       dd.state,
       dd.county,
       dd.ownership_name,
       i.ALLOT_NAME,
       i.ALLOT_NO,
       i.PAST_NAME,
       i.PAST_NO,
       i.GIS_ACRES,
       d.centerline_length,
       d.segment_area,
       d.FCode,
       i.fcode_name,
       dd.*,
       db.*,
       dg.*,
       dh.*,
       di.*,
       dm.*,
       dv.*
FROM dgos d
         inner join dgo_desc dd on d.dgoid = dd.dgoid
         inner join igos_pastures i on d.level_path = i.level_path and d.seg_distance = i.seg_distance
inner join dgo_beaver db on d.dgoid = db.dgoid
inner join dgo_geomorph dg on d.dgoid = dg.dgoid
inner join dgo_hydro dh on d.dgoid = dh.dgoid
inner join dgo_impacts di on d.dgoid = di.dgoid
inner join dgo_measurements dm on d.DGOID = dm.dgoid
inner join dgo_veg dv on d.dgoid = dv.dgoid
where i.ALLOT_NAME is not null;
                         ''')

        # rows = curs.fetchall()

        # huc10_names = {row['key']: row['name'] for row in huc10_data}

        # # get dictionary of keys
        # keys = [col[0] for col in curs.description]
        # data = []
        #  for row in rows:

        #       row_data = {
        #            'HUC10': row['huc10'],
        #             'HUC10Name': huc10_names[row['huc10']],
        #             'County': row['county'],
        #             'Ownership': row['ownership_name'],
        #             'Allotment': row['ALLOT_NAME'],
        #             'AllotmentNo': row['ALLOT_NO'],
        #             'Pasture': row['PAST_NAME'],
        #             'PastureNo': row['PAST_NO'],
        #            }

        #        for field in sum_fields:
        #             row_data[field] = row[field]

        #         data.append(row_data)

        # allotments = {}
        # for row in data:
        #     if row['Allotment'] not in allotments:
        #         allotments[row['Allotment']] = {}

        #     allotment = allotments[row['Allotment']]
        #     if row['Pasture'] not in allotment:
        #         allotment[row['Pasture']] = {}

        #     pasture = allotment[row['Pasture']]
        #     for field in sum_fields:
        #         metric_name = f'Total {field_aliases[field]}'
        #         pasture[metric_name] = pasture[field] + row[field] if field in pasture else row[field]

        #     pass

        # my_data = {"fred": 1, "andrew": 2}

        # ws_context_section = self.section('WSContext', 'Watershed Context')

        # self.create_table_from_dict(my_data, ws_context_section)
        # report_scopes = []
        # with sqlite3.connect(rme_scrape) as conn:
        #     curs = conn.cursor()
        #     for scope in scopes:
        #         scope_section = self.section(scope['title'].replace(' ', ''), scope['title'],)
        #         if 'sql' in scope:
        #             curs.execute(scope['sql'])
        #             for row in curs.fetchall():
        #                 name = row[0]
        #                 if 'names' in scope:
        #                     names_lookup = {name['key']: name['name'] for name in scope['names']}
        #                     if name in names_lookup:
        #                         name = f'{names_lookup[name]} ({name})'
        #                 sub_section = self.section(name.replace(' ', ''), name, scope_section, 2)
        #                 self.hydrology_section(curs, sub_section, scope['ids'], row[0])

        #         else:
        #             self.scope = 'Owyhee'
        #             self.hydrology_section(curs, scope_section, scope['ids'], 0)

    def hydrology_section(self, curs, parent, ids_sql, filter_key):
        """
        Create a section for hydrology data.
        """

        s2_intro = ET.Element('p')
        s2_intro.text = 'Hydrology'
        parent.append(s2_intro)

        curs.execute(f'''
            SELECT FCode, SUM(segment_area), SUM(centerline_length)
            FROM
            (
                SELECT v.*
                FROM vw_dgo_desc_metrics v
                    INNER JOIN ({ids_sql}) f ON v.igoid = f.igoid
            )
            GROUP BY FCode
        ''', [filter_key])

        raw_data = {row[0]: {'area': row[1], 'length': row[2]} for row in curs.fetchall()}
        total_area = sum([row['area'] for row in raw_data.values()])
        total_length = sum([row['length'] for row in raw_data.values()])

        table_data = []
        for key, value in fcode_lookup.items():
            if key in raw_data:
                area = raw_data[key]['area']
                length = raw_data[key]['length']
            else:
                area = 0
                length = 0

            # Convert to acres and miles
            area_acres = area * ACRES_PER_SQ_M
            length_miles = length * MILES_PER_M
            area_percent = 100 * area / total_area if area > 0 and total_area > 0 else 0
            length_percent = 100 * length / total_length if length > 0 and total_length > 0 else 0
            table_data.append((value, area_acres, area_percent, length_miles, length_percent))

        table_data.append(('Total', total_area * ACRES_PER_SQ_M, 100, total_length * MILES_PER_M, 100))

        self.create_table_from_tuple_list(['', 'Area (ac)', 'Area (%)', 'Length (mi)', 'Length (%)'], table_data, parent, None, True)

        # Ownership

        curs.execute(f'''
            SELECT ownership, sum(segment_area), sum(centerline_length)
            FROM (
                SELECT v.*
                FROM vw_dgo_desc_metrics v
                    INNER JOIN ({ids_sql}) f ON v.igoid = f.igoid
            ) group by ownership
        ''', [filter_key])

        raw_data = {row[0]: {'area': row[1], 'length': row[2]} for row in curs.fetchall()}
        total_area = sum([row['area'] for row in raw_data.values()])
        total_length = sum([row['length'] for row in raw_data.values()])

        table_data = []
        for key, value in ownership_lookup.items():
            if key in raw_data:
                area = raw_data[key]['area']
                length = raw_data[key]['length']
            else:
                area = 0
                length = 0

            # Convert to acres and miles
            area_acres = area * ACRES_PER_SQ_M
            length_miles = length * MILES_PER_M
            area_percent = 100 * area / total_area if area > 0 and total_area > 0 else 0
            length_percent = 100 * length / total_length if length > 0 and total_length > 0 else 0
            table_data.append((value, area_acres, area_percent, length_miles, length_percent))

        table_data.append(('Total', total_area * ACRES_PER_SQ_M, 100, total_length * MILES_PER_M, 100))

        self.create_table_from_tuple_list(['', 'Area (ac)', 'Area (%)', 'Length (mi)', 'Length (%)'], table_data, parent, None, True)

        # pie_values = [
        #     ('Perennial', rsc_metrics['flowlineLengthPerennialKm'], 'Perennial'),
        #     ('Intermittent', rsc_metrics['flowlineLengthIntermittentKm'], 'Intermittent'),
        #     ('Ephemeral', rsc_metrics['flowlineLengthEphemeralKm'], 'Ephemeral'),
        #     ('Canal', rsc_metrics['flowlineLengthCanalsKm'], 'Canal')
        # ]

        # pie_path = os.path.join(self.images_dir, 'stream_type_pie.png')
        # pie([x[1] for x in pie_values], [x[2] for x in pie_values], 'Stream Length Breakdown', None, pie_path)
        # self.insert_image(length_section, pie_path, 'Pie Chart')

        pass

    def clustered_bar_chart(
        self,
        parent: ET.Element,
        title: str,
        data: List[List[float]],
        series_labels: List[str],
        x_labels: List[str],
        colors: List[str],
        x_label: str,
        y_label: str,
        is_vertical: bool = False
    ) -> None:
        """
        Create and save a plot with multiple series as a clustered bar chart.

        :param parent: The parent XML element to which the image will be added.
        :param title: Title of the plot.
        :param data: List of series data, each series being a list of floats.
        :param series_labels: List of labels for each series.
        :param x_labels: List of labels for the x-axis.
        :param colors: List of colors for the series.
        :param x_label: Label for the x-axis.
        :param y_label: Label for the y-axis.
        """
        plt.clf()
        _fig, ax = plt.subplots(figsize=(10, 6))


def get_rme_values(rme_gpkg: str) -> dict:

    rme_values = {}
    with sqlite3.connect(rme_gpkg) as conn:
        curs = conn.cursor()

        # Modal Ecoregion III (metric ID 17)
        for level, metric_id in [('Ecoregion III', 17), ('Ecoregion IV', 18)]:
            curs.execute('SELECT metric_value, count(*) frequency FROM dgo_metric_values WHERE metric_id= ? GROUP BY metric_value ORDER by frequency DESC limit 1', [metric_id])
            row = curs.fetchone()
            rme_values[level] = row[0]

        # Stream Gradient
        curs.execute('SELECT AVG(metric_value) FROM dgo_metric_values WHERE metric_id= 4')
        row = curs.fetchone()
        rme_values['Mean Stream Gradient'] = row[0]

    return rme_values


def get_rme_stats(rme_gpkg: str, filter_blm: bool, include_fcodes: bool, fcodes: List[int]) -> dict:

    filter_fcodes = ''
    if fcodes is not None and len(fcodes) > 0:
        fcodes_str = ', '.join([str(fcode) for fcode in fcodes])
        filter_fcodes = f'AND (d.fcode {"" if include_fcodes is True else "NOT"} IN ({fcodes_str}))'

    rme_stats = {}
    with sqlite3.connect(rme_gpkg) as conn:
        curs = conn.cursor()

        sql = f'''
            SELECT m.machine_code,
                sum(mv.metric_value) total,
                sum(mv.metric_value * d.segment_area) total_area,
                avg(mv.metric_value) avg,
                max(mv.metric_value) max,
                min(mv.metric_value) min,
                sum(d.segment_area) raw_area
            FROM dgos d
                inner join dgo_metric_values mv ON d.fid = mv.dgo_id
                inner join metrics m on mv.metric_id = m.metric_id
                inner join (select dgo_id, metric_value from dgo_metric_values WHERE (metric_id = 1)) o ON mv.dgo_id = o.dgo_id
            WHERE (o.metric_value {'=' if filter_blm is True else '<>'} 'BLM')
                {filter_fcodes}
            group by m.machine_code'''

        curs.execute(sql)
        rme_stats = {row[0]: {'total': float(row[1]), 'avg': float(row[2]), 'max': float(row[3]), 'min': float(row[4]), 'raw_area': float(row[5])} for row in curs.fetchall()}

    return rme_stats


def get_vbet_json_metrics(metrics_json_path) -> dict:

    with open(metrics_json_path, 'r', encoding='utf-8') as f:
        metrics_json = json.load(f)

    total_area = float(metrics_json['catchmentArea'])

    formated = {
        'Total Valley Bottom Area per Unit Length (m^2/m)': metrics_json['totalHectaresPerKm'],
        'Average Valley Bottom Area per Unit Length (m^2/m)': metrics_json['averageHectaresPerKm'],
        'Average Valley Bottom Width (m)': metrics_json['avgValleyBottomWidth'],
        'Proportion Riverscape (%)': float(metrics_json['proportionRiverscape']) * 100,
        'Area of Riverscapes (km^2)': float(metrics_json['proportionRiverscape']) * total_area,
        'Area of Non-Riverscapes (km^2)': (1 - float(metrics_json['proportionRiverscape'])) * total_area
    }
    return formated


def main():
    """Watershed Assessment Report"""

    parser = argparse.ArgumentParser(description='Riverscapes Watershed Report')
    parser.add_argument('rme_scrape', help='RME Scrape GeoPackage', type=str)
    parser.add_argument('input_folder', help='Parent folder inside which whole riverscapes projects are stored', type=str)
    parser.add_argument('report_path', help='Output report file path', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    title = 'Owyhee Land Health Assessment Report'
    log = Logger(title)
    log.setup(log_path=os.path.join(os.path.dirname(args.report_path), "wsca_report.log"), verbose=args.verbose)
    log.title(title)

    # try:
    report = LHAReport(args.rme_scrape, args.input_folder, os.path.dirname(args.report_path), args.report_path, args.verbose)
    report.write(title=title)

    # except Exception as e:
    #     log.error(e)
    #     traceback.print_exc(file=sys.stdout)
    #     sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
