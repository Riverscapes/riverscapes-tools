import sqlite3
import json
import argparse
import os
from rscommons import dotenv


def huc_summary_stats(database):

    data = {}

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute("SELECT CapacityID, Name, MaxCapacity FROM DamCapacities ORDER BY MaxCapacity")
    capacities = [(row[0], row[1], row[2]) for row in curs.fetchall()]

    summarize_id_field(curs, 'Risks', 'RiskID', data)
    summarize_id_field(curs, 'Limitations', 'LimitationID', data)
    summarize_id_field(curs, 'Opportunities', 'OpportunityID', data)

    summarize_capacity_field(curs, 'Existing', 'oCC_EX', data, capacities)
    summarize_capacity_field(curs, 'Historic', 'oCC_HPE', data, capacities)

    print(data)
    return data


def summarize_id_field(curs, key, field, data):

    curs.execute('SELECT WatershedID, {0}, Sum(iGeo_Len) / 1000 FROM Reaches WHERE {0} IS NOT NULL GROUP BY WatershedID, {0}'.format(field))
    for row in curs.fetchall():
        if row[0] not in data:
            data[row[0]] = {'Risks': {}, 'Limitations': {}, 'Opportunities': {}, 'Existing': {}, 'Historic': {}}

        data[row[0]][key][row[1]] = row[2]


def summarize_capacity_field(curs, key, field, data, capacities):

    min_capacity = 0
    for capacity_id, name, max_capacity in capacities:

        curs.execute('SELECT WatershedID, Sum(iGeo_Len) / 1000 FROM Reaches WHERE ({0} IS NOT NULL) AND ({0} > ?) AND ({0} <= ?) GROUP BY WatershedID'.format(field),
                     [min_capacity, max_capacity])

        for row in curs.fetchall():
            if row[0] not in data:
                data[row[0]] = {'Risks': {}, 'Limitations': {}, 'Opportunities': {}, 'Existing': {}, 'Historic': {}}

            data[row[0]][key][capacity_id] = row[1]

        min_capacity = max_capacity


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to SQLite database', type=str)
    parser.add_argument('output', help='Path to output JSON file', type=str)
    args = dotenv.parse_args_env(parser)

    result = huc_summary_stats(args.database)

    with open(args.output, 'w') as jf:
        json.dump(result, jf)

    print('Process complete. Output written to', args.output)


if __name__ == '__main__':
    main()
