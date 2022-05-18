import os
import argparse
import json
import sqlite3
import datetime
from rscommons import dotenv


def warehouse_to_json(sql_path, json_path):
    """Path to the input SQLite dump of the riverscapes warehouse
    Path to the output JSON file that will be produced"""

    dbconn = sqlite3.connect(sql_path)
    dbcurs = dbconn.cursor()

    data = []
    dbcurs.execute('SELECT * FROM projects')
    for row in dbcurs.fetchall():

        meta = json.loads(row['meta'])

        data.append({
            "guid": row['guid'],
            "title": row['name'],
            "createdOn": row['createdOn'],
            'updatedOn': row['updatedOn'],
            'projectType': row['projType'],
            'metadata': row['metadata']
        })

    with open(json_path, 'w') as fout:
        print(json.dumps(data, indent=4), file=fout)

    print('Process Complete.')


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('sql_path', help='Path to existing SQLite database dump of the riverscapes warehouse', type=str)
    parser.add_argument('json_path', help='Path to the output JSON file that will be created', type=str)
    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '.env'))

    warehouse_to_json(args.sql_path, args.json_path)


if __name__ == "__main__":
    main()
