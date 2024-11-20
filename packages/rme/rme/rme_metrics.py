"""
Generate Metrics JSON for a single Riverscapes Context Project
"""
import sqlite3
import argparse
import os
import traceback
import sys
import json
from rscommons import GeopackageLayer, dotenv, Logger, RSProject, RSLayer, ShapefileLayer

PERENNIAL = [46006, 55800]
INERMITTENT = [46003]
EPHEMERAL = [46007]

owner_clauses = {
    'All': '1=1',
    'BLM': "rme_dgo_ownership = 'BLM'",
    'nonBLM': "rme_dgo_ownership != 'BLM'",
    'Federal': "rme_dgo_ownership IN ('BLM', 'USFS', 'NPS', 'FWS', 'USFWS', 'USBR', 'USGS', 'USACE', 'USDA', 'USDOI', 'DoD')"
}

flow_type_clauses = {
    'All': '1=1',
    'Perennial': f'FCode IN ({",".join([str(x) for x in PERENNIAL])})',
    'Intermittent': f'FCode IN ({",".join([str(x) for x in INERMITTENT])})',
    'Ephemeral': f'FCode IN ({",".join([str(x) for x in EPHEMERAL])})'
}


def rme_metrics(project_path, vbet_proj_path):
    """Calculate summary watershed level RME metrics in JSON for this project."""

    log = Logger('rme_metrics')
    log.info('Calculating metrics for RME project')

    if not os.path.exists(os.path.join(vbet_proj_path, 'vbet_metrics.json')):
        raise FileNotFoundError(f'vbet_metrics.json not found in {vbet_proj_path}')

    # TODO: metrics here
    rme_metrics = {}

    with sqlite3.connect(os.path.join(project_path, 'outputs', 'riverscapes_metrics.gpkg')) as conn:
        conn.row_factory = dict_factory
        curs = conn.cursor()

        curs.execute('SELECT COUNT(*), SUM(segment_area) FROM rme_dgos')
        dgo_count, total_area = curs.fetchone()

        curs.execute("""
            SELECT
            COALESCE(rme_dgo_ownership, 'Unknown') AS owner,
            CASE
                WHEN FCode IN (46003, 55800) THEN 'Perennial'
                WHEN FCode = 46003 THEN 'Intermittent'
                WHEN FCode = 46007 THEN 'Ephemeral'
                ELSE 'Unknown'
            END AS flowType,
            COALESCE(SUM(segment_area), 0) area,
            COALESCE(SUM(centerline_length), 0) length,
        FROM
            rme_dgos
        GROUP BY
            Ownership, FlowType""")

        rme_metrics['ownership'] = [{'owner': row['owner'], 'flowTpe': row['flowType'], 'area': row['area'], 'length': row['length']} for row in curs.fetchall()]

        curs.execute("SELECT name, type FROM pragma_table_info('rme_dgos') WHERE LOWER(type) IN ('real', 'float', 'double', 'decimal')")
        decimal_fields = [row['name'] for row in curs.fetchall()]

        for owner_name, owner_clause in owner_clauses.items():

            group_by = []
            if owner_name == "'All'":
                owner_field = 'All'
            elif owner_name == 'Federal':
                owner_field = "'Federal'"
                group_by.append('Ownership')
            else:
                owner_field = "COALESCE(rme_dgo_ownership, 'Unknown')"
                group_by.append('Ownership')

            for flow_name, flow_clause in flow_type_clauses.items():

                if flow_type != 'All':
                    flow_type_where_clause = f"AND FCode IN ({','.join([str(x) for x in PERENNIAL])})"
                    group_by.append('FlowType')

                for field in decimal_fields:

                    curs.execute(f"""
                        SELECT
                            MIN({field}) AS MinValue,
                            MAX({field}) AS MaxValue,
                            AVG(rme_igo_prim_channel_gradient) AS AvgValue,
                            COUNT(*) AS Tally,
                            Sum({field}) AS SumValue
                        FROM
                            rme_dgos
                        WHERE
                            {owner_clause}
                            AND {flow_clause}
                    """)

        curs.execute("SELECT name, type FROM pragma_table_info('your_table_name') WHERE LOWER(type) IN ('integer', 'int', 'smallint', 'tinyint', 'bigint', 'unsigned big int')")
        integer_fields = [row['name'] for row in curs.fetchall()]


")

    with open(os.path.join(vbet_proj_path, 'vbet_metrics.json'), encoding='utf8') as json_file:
        metrics= json.load(json_file)

    metrics['rme']= rme_metrics

    with open(os.path.join(project_path, 'rme_metrics.json'), 'w', encoding='utf8') as f:
        json.dump(metrics, f, indent=2)

    # proj = RSProject(None, os.path.join(project_path, 'project.rs.xml'))
    # datasets_node = proj.XMLBuilder.find('Realizations').find('Realization').find('Datasets')
    # proj.add_dataset(datasets_node, os.path.join(project_path, 'rme_metrics.json'), RSLayer('Metrics', 'Metrics', 'File', 'rme_metrics.json'), 'File')
    # proj.XMLBuilder.write()


def dict_factory(cursor, row):
    d= {}
    for idx, col in enumerate(cursor.description):
        d[col[0]]= row[idx]
    return d


def main():
    """Run this method to generate the summary JSON metrics for the RME project."""

    parser= argparse.ArgumentParser()
    parser.add_argument('project_path', help='Path to project directory', type=str)
    parser.add_argument('vbet_path', help='Path to VBET project directory', type=str)
    args= dotenv.parse_args_env(parser)

    try:
        rme_metrics(args.project_path, args.vbet_path)
    except Exception as e:
        Logger('rme_metrics').error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
