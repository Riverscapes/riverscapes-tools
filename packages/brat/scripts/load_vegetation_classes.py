import argparse
import csv
import sqlite3


def load_landfire_classes(conn, lookup, table, fields):

    field_list = fields.replace(' ', '').split(',')

    values = []
    with open(lookup, mode='r') as infile:
        csv_values = csv.DictReader(infile)
        for row in csv_values:
            values.append(tuple([row[field] for field in field_list]))

    print('{:,} vegetation classes loaded from CSV file.'.format(len(values)))

    # Remove all existing lookup values
    deleted = conn.execute('DELETE FROM {}'.format(table))
    print('{:,} vegetation classes deleted from {} table.'.format(deleted.rowcount, table))

    # Insert all the new lookup values
    inserted = conn.executemany('INSERT INTO {} ({}) VALUES ({})'.format(table, ','.join(field_list), ','.join('?' * len(field_list))), values)
    print('{:,} vegetation classes inserted into {}.'.format(inserted.rowcount, table))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Path to SQLite database', type=argparse.FileType('r'))
    parser.add_argument('lookup', help='Path to Landfire CSV lookup definitions', type=argparse.FileType('r'))
    parser.add_argument('table', help='Database table where classes will be stored', type=str)
    parser.add_argument('fields', help='Comma separated list of fields to import. Must include Value and ClassName. Optionally land use', type=str)
    args = parser.parse_args()

    # Open connection to SQLite database.
    conn = sqlite3.connect(args.database.name)

    try:
        load_landfire_classes(conn, args.lookup.name, args.table, args.fields)
        conn.commit()
        print('Process completed successfully.')

    except Exception as e:
        conn.rollback()
        print(e)
        print('Process completed with errors.')

    conn.close()


if __name__ == '__main__':
    main()
