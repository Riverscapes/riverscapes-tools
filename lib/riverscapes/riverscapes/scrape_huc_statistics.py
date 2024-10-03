"""
Scrapes RME GeoPackage from Data Exchange and extracts statistics for a single HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.
Philip Bailey
"""
from typing import Dict
import shutil
import sys
import re
import os
import copy
import sqlite3
import argparse
import uuid
from rscommons import Logger, dotenv

# Metric summary methods used in dictionary below
LENGTH_WEIGHTED_AVG = 'length_weighted_avg'
AREA_WEIGHTED_AVG = 'area_weighted_avg'
SUM_METRIC = 'sum_metric_value'
MULTIPLIED_BY_LENGTH = 'multiplied_by_length'
MULTIPLIED_BY_AREA = 'multiplied_by_area'

# Sums the segment area for DGOs that have the corresponding metric value equal to zero
SUM_AREA_ZERO_COUNT = 'sum_area_zero_count'

# These are RME metrics than can be scraped. The items in each Tuple are:
# 1. The name of the metric in the RME database (not used by this code)
# 2. The metric ID in the RME database
# 3. The type of summary to use
# 4. The key to use in the output dictionary
rme_metric_defs = (
    ('rme_igo_prim_channel_gradient',	4,	LENGTH_WEIGHTED_AVG,		'channel_gradient'),
    ('rme_igo_valley_bottom_gradient',	5,	LENGTH_WEIGHTED_AVG,		'valley_gradient'),
    ('nhd_dgo_streamlength',	16,	SUM_METRIC,		'channel_length'),
    ('vbet_dgo_lowlying_area',	19,	SUM_METRIC,		'low_lying_area'),
    ('vbet_dgo_elevated_area',	20,	SUM_METRIC, 'elevated_area'),
    ('vbet_dgo_channel_area',	21,	SUM_METRIC,		'channel_area'),
    ('vbet_igo_integrated_width',	23, LENGTH_WEIGHTED_AVG,		'valley_width'),
    # ('conf_igo_confinement_ratio',	31,	LENGTH_WEIGHTED_AVG,	'confinement'),
    # ('conf_igo_constriction_ratio', 32,	LENGTH_WEIGHTED_AVG,		'constriction'),
    ('anthro_igo_road_dens',	35,	MULTIPLIED_BY_LENGTH,		'road_length'),
    ('anthro_igo_rail_dens',	36,	MULTIPLIED_BY_LENGTH,		'rail_length'),
    ('anthro_igo_land_use_intens',	37,	AREA_WEIGHTED_AVG,		'land_use_intensity'),
    ('rcat_igo_fldpln_access',	38,	MULTIPLIED_BY_AREA,		'accessible_floodplain_area'),
    ('rcat_igo_prop_riparian',	39,	MULTIPLIED_BY_AREA,		'riparian_area'),
    ('rcat_igo_riparian_veg_departure',	40,	AREA_WEIGHTED_AVG,		'riparian_departure'),
    ('rcat_igo_riparian_ag_conversion',	41,	MULTIPLIED_BY_AREA,		'riparian_ag_conv_area'),
    ('rcat_igo_riparian_develop',	42, MULTIPLIED_BY_AREA,		'riparian_developed_area'),
    ('rcat_lui_zero_count',	37,	SUM_AREA_ZERO_COUNT,		'lui_zero_area')
    # ('brat_igo_capacity',	43,	SUM_METRIC,		'beaver_dam_capacity')
)

# Conversion factors
# 3 Oct 2024 - decided to keep the units in the database and not convert them here
# METRES_TO_MILES = 0.000621371
# SQMETRES_TO_ACRES = 0.000247105


def scrape_huc_statistics(huc: str, rme_gpkg: str, output_db: str) -> None:
    """
    Scrape the RME statistics for a single HUC.

    """

    log = Logger('Scrape HUC')

    # Load the foreign key look up tables for owners and flows
    owners = load_filters(output_db, 'owners')
    flows = load_filters(output_db, 'flows')
    states = load_filters(output_db, 'us_states')

    # Get an empty template from the output db for the data to be scraped
    data_template = get_data_template(output_db)

    log.info(f'Scraping metrics for HUC {huc}')

    huc_metrics = []
    with sqlite3.connect(rme_gpkg) as rme_conn:
        rme_conn.row_factory = dict_factory
        rme_curs = rme_conn.cursor()

        for __state_name, state_data in states.items():

            for __flow_name, flow_data in flows.items():

                # Without an owner filter we get statistics for all owners for a certain FCode
                data = copy.deepcopy(data_template)
                data['state_id'] = state_data['id']
                data['flow_id'] = flow_data['id']
                data['huc10'] = huc
                scrape_rme_statistics(rme_curs, state_data, flow_data, None, data)

                if data['dgo_count'] > 0:
                    huc_metrics.append(data)

                for __owner_name, owner_data in owners.items():

                    data = copy.deepcopy(data_template)
                    data['state_id'] = state_data['id']
                    data['owner_id'] = owner_data['id']
                    data['flow_id'] = flow_data['id']
                    data['huc10'] = huc

                    # Statistics with both owner and flow filters
                    scrape_rme_statistics(rme_curs, state_data, flow_data, owner_data, data)

                    if data['dgo_count'] > 0:
                        huc_metrics.append(data)

    # Store the output HUC metrics
    keys = huc_metrics[0].keys()
    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.execute('INSERT INTO hucs (huc10, rme_project_guid, rcat_project_guid) VALUES (?, ?, ?)', [huc, None, None])
        curs.executemany(f'INSERT INTO metrics ({", ".join(keys)}) VALUES ({", ".join(["?" for _ in keys])})', [tuple(m[k] for k in keys) for m in huc_metrics])
        secondary_metrics(curs)
        conn.commit()


def secondary_metrics(curs: sqlite3.Cursor) -> None:
    """ After the metrics have been scraped, calculate the secondary metrics with simple SQL updates
    """

    curs.execute('UPDATE metrics SET hist_riparian_area = riparian_area / (1 - riparian_departure)')
    curs.execute('UPDATE metrics SET relative_flow_length = channel_length / riverscape_length')
    curs.execute('UPDATE metrics SET acres_vb_per_mile = (riverscape_area * 0.000247105) / (riverscape_length * 0.000621371)')
    curs.execute('UPDATE metrics SET road_density = road_length / riverscape_length')
    curs.execute('UPDATE metrics SET rail_density = rail_length / riverscape_length')
    curs.execute('UPDATE metrics SET riparian_ag_conversion_proportion = riparian_ag_conv_area / riverscape_area')
    curs.execute('UPDATE metrics SET riparian_developed_proportion = riparian_developed_area / riverscape_area')
    # curs.execute('UPDATE metrics SET beaver_dam_density = beaver_dam_capacity / riverscape_length')


def get_data_template(output_db: str) -> Dict[str, float]:
    """
    Get the data template from the destination cursor
    """
    with sqlite3.connect(output_db) as dest_conn:
        dest_conn.row_factory = dict_factory
        dest_cursor = dest_conn.cursor()
        dest_cursor.execute('PRAGMA table_info(metrics)')
        columns = [info['name'] for info in dest_cursor.fetchall()]
        return {col: None for col in columns}


def copy_file_with_unique_name(file_path):
    """
    Deduce a new, unique file name from the original file name and copy the file to the new file name.
    """

    folder = os.path.dirname(file_path)
    original_filename = os.path.basename(file_path)
    name, ext = os.path.splitext(original_filename)

    # Generate a unique filename using uuid
    unique_filename = f"{name}_{uuid.uuid4().hex}{ext}"
    new_file_path = os.path.join(folder, unique_filename)

    # Copy the file to the new file path
    shutil.copy2(file_path, new_file_path)

    # print(f"File copied to: {new_file_path}")
    return new_file_path


def copy_table_between_cursors(src_cursor, dest_cursor, table_name):
    """
    Copy a table structure and data from the source cursor to destination cursor
    """

    # Get table schema from the source database
    src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    create_table_sql = src_cursor.fetchone()['sql']
    dest_cursor.execute(create_table_sql)

    # Get all data from the source table
    src_cursor.execute(f"SELECT * FROM {table_name}")
    rows = src_cursor.fetchall()

    # Get the column names from the source table
    src_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info['name'] for info in src_cursor.fetchall()]  # info[1] gives the column names
    columns_str = ', '.join(columns)

    # Insert data into the destination table
    placeholders = ', '.join(['?' for _ in columns])  # Create placeholders for SQL insert
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    row_tuples = [[row[col] for col in columns] for row in rows]
    dest_cursor.executemany(insert_sql, row_tuples)


def scrape_rme_statistics(curs: sqlite3.Cursor, state: Dict[str, str], flow: Dict[str, str], owner: Dict[str, str], output: Dict[str, float]) -> None:
    """
    Scrape statistics from the RME output. The owner and flow filters are optional.
    The output of this function is to insert several RME statistics into the "data" dictionary.
    """

    base_sql = '''
        SELECT
            count(*) dgo_count,
            coalesce(sum(d.centerline_length),0) riverscape_length,
            coalesce(sum(d.segment_area), 0) riverscape_area
        FROM dgos d
        LEFT JOIN dgo_metric_values dms ON d.fid = dms.dgo_id
        '''

    if owner is not None:
        base_sql += ' LEFT JOIN dgo_metric_values dmo ON d.fid = dmo.dgo_id'

    final_sql = add_where_clauses(base_sql, state, flow, owner)
    curs.execute(final_sql)
    row = curs.fetchone()

    output['dgo_count'] = row['dgo_count']
    output['riverscape_length'] = row['riverscape_length']  # * METRES_TO_MILES
    output['riverscape_area'] = row['riverscape_area']  # * SQMETRES_TO_ACRES

    # Now process the individual RME metrics
    for __metric_name, metric_id, summary_method, output_key in rme_metric_defs:
        output[output_key] = get_rme_metric_summary(curs, state, flow, owner, metric_id, summary_method)


def get_rme_metric_summary(curs: sqlite3.Cursor, state: Dict[str, str], flow: Dict[str, str], owner: Dict[str, str], metric_id: int, summary_method: str) -> float:
    """
    For a given metric (by metric_id) this method generates several summary metrics:
        - Length weighted average
        - Area weighted average
        - Sum of metric values
        - Sum of metric values multiplied by centerline length
        - Sum of metric values multiplied by segment area
        - Sum of metric values where they are zero

    The caller specifies which of these metrics they want returned by passing in the summary_method.

    The caller can filter by state, flow and optionally owner.
    """

    owner_table_join = '' if owner is None else ' LEFT JOIN dgo_metric_values dmo ON d.fid = dmo.dgo_id'

    base_sql = f'''
        SELECT
            SUM(dmv.metric_value * d.centerline_length) / SUM(d.centerline_length) AS {LENGTH_WEIGHTED_AVG},
            SUM(dmv.metric_value * d.segment_area) / SUM(d.segment_area) AS {AREA_WEIGHTED_AVG},
            SUM(dmv.metric_value) AS {SUM_METRIC},
            SUM(dmv.metric_value * d.centerline_length) AS {MULTIPLIED_BY_LENGTH},
            SUM(dmv.metric_value * d.segment_area) AS {MULTIPLIED_BY_AREA},
            COALESCE(sum(CASE WHEN dmv.metric_value = 0 THEN d.segment_area ELSE 0 END), 0) {SUM_AREA_ZERO_COUNT}
        FROM dgos d
                INNER JOIN dgo_metric_values dmv ON d.fid = dmv.dgo_id
                LEFT JOIN dgo_metric_values dms ON d.fid = dms.dgo_id
                {owner_table_join}
        WHERE dmv.metric_id = ?
        '''

    final_sql = add_where_clauses(base_sql, state, flow, owner)
    curs.execute(final_sql, [metric_id])
    row = curs.fetchone()
    return row[summary_method]


def add_where_clauses(base_sql: str, state: Dict[str, str], flow: Dict[str, str], owner: Dict[str, str]) -> str:
    """
    Add WHERE clauses to the SQL query based on the state, owner and flow.
    Note that owner is the only filter than can be None!
    """

    final_sql = base_sql
    final_sql += ' WHERE ' if 'WHERE' not in base_sql else ' AND '

    s_clause = ','.join([f"'{s}'" for s in state['where_clause'].split(",")])
    final_sql += f'( dms.metric_id = 2 AND dms.metric_value IN ({s_clause}))'

    f_clause = ','.join([f"'{f}'" for f in flow['where_clause'].split(",")])
    final_sql += f' AND (d.FCode IN ({f_clause}))'

    if owner is not None:
        o_clause = ','.join([f"'{o}'" for o in owner['where_clause'].split(",")])
        final_sql += f' AND (dmo.metric_id = 1 AND dmo.metric_value IN ({o_clause}))'

    return final_sql


def get_matching_file(parent_dir: str, regex: str) -> str:
    """
    Get the path to the first file in the parent directory that matches the regex.
    Returns None if no file is found.
    This is used to check if the output GeoPackage has already been downloaded and
    to avoid downloading it again.
    """

    regex = re.compile(regex)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def load_filters(output_db: str, table_name: str) -> Dict[str, Dict[str, str]]:
    '''
    Load the filters from the output database for a particular table.
    This is used for both ownerships and flows lookups
    '''

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.execute(f'SELECT name, id, where_clause FROM {table_name}')
        return {f[0]: {'id': f[1], 'where_clause': f[2]} for f in curs.fetchall()}


def continue_with_huc(huc: str, output_db: str) -> bool:
    '''
    Check if the HUC already exists in the output GeoPackage. 
    This is used to determine if the HUC has already been scraped and whether it
    can be skipped.
    '''

    if not os.path.isfile(output_db):
        return True

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        curs.execute('SELECT huc10 FROM hucs WHERE huc10 = ? LIMIT 1', [huc])
        return curs.fetchone() is None

    return False


def create_output_db(output_db: str) -> None:
    """ 
    Build the output SQLite database by running the schema file.
    """
    log = Logger('Create Output DB')
    log.info(f'Creating output database: {output_db}')

    # As a precaution, do not overwrite or delete the output database.
    # Force the user to delete it manually if they want to rebuild it.
    if os.path.isfile(output_db):
        log.error('Output database already exists. Skipping creation.')
        return

    schema_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'packages', 'rme', 'rme', 'database')
    if not os.path.isdir(schema_dir):
        raise FileNotFoundError(f'Could not find database schema directory {schema_dir}')

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        log.info('Creating output database schema')
        with open(os.path.join(schema_dir, 'rme_scrape_huc_statistics.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            curs.executescript(sql_commands)
            conn.commit()

    log.info('Output database created')


def dict_factory(cursor, row):
    """Apply the dictionary factory to the cursor so that columns can be accessed by name"""

    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def main():
    """
    Scrape RME metrics for a single HUC
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('huc', help='HUC code for the scrape', type=str)
    parser.add_argument('rme_gpkg', help='RME output GeoPackage path', type=str)
    parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true')
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger('RME Scrape')

    log_dir = os.path.join(os.path.dirname(args.rme_gpkg))
    log.setup(logPath=os.path.join(log_dir, 'rme_scrape.log'), verbose=args.verbose)
    log.title(f'RME scrape for HUC: {args.huc}')

    if not os.path.isfile(args.rme_gpkg):
        log.error(f'RME output GeoPackage cannot be found: {args.rme_gpkg}')
        sys.exit(1)

    # Place the output RME scrape database in the same directory as the RME GeoPackage
    output_db = os.path.join(os.path.dirname(args.rme_gpkg), 'rme_scrape.sqlite')
    log.info(f'Output database: {output_db}')

    try:
        create_output_db(output_db)
        scrape_huc_statistics(args.huc, args.rme_gpkg, output_db)
    except Exception as e:
        log.error(f'Error scraping HUC {args.huc}: {e}')
        sys.exit(1)

    log.info('Process complete')


if __name__ == '__main__':
    main()
