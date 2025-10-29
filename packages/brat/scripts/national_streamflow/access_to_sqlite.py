# Philip Bailey
# 27 Sep 2019
# See readme.md in same folder
# Remember ot install pyodbc
import pyodbc
import sqlite3
from collections import namedtuple
import re
import os
import argparse
from rsxml import dotenv


def convert(in_path, out_path):

    cnxn = pyodbc.connect('Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};Dbq={};'.format(in_path))

    cursor = cnxn.cursor()

    if os.path.isfile(out_path):
        os.remove(out_path)

    conn = sqlite3.connect(out_path)
    c = conn.cursor()

    Table = namedtuple('Table', ['cat', 'schem', 'name', 'type'])

    # get a list of tables
    tables = []
    for row in cursor.tables():
        if row.table_type == 'TABLE':
            t = Table(row.table_cat, row.table_schem, row.table_name, row.table_type)
            tables.append(t)

    for t in tables:
        print(t.name)

        # SQLite tables must being with a character or _
        t_name = t.name
        if not re.match('[a-zA-Z]', t.name):
            t_name = '_' + t_name

        # get table definition
        columns = []

        # I couldn't get the following code to work so I converted it to get a list of
        # columns by selecting data. Inefficient but effective. Could be improved with a LIMIT clause
        # for row in cursor.columns(t.name.decode('utf-16', errors='replace')):
        #    print '    {} [{}({})]'.format(row.column_name, row.type_name, row.column_size)
        #    col_name = re.sub('[^a-zA-Z0-9]', '_', row.column_name)
        #    columns.append('{} {}({})'.format(col_name, row.type_name, row.column_size))
        cursor = cnxn.cursor()
        cursor.execute("SELECT * FROM {}".format(t.name))  # .decode('utf-16', errors='replace')))
        columns = [column[0] for column in cursor.description]
        cols = ', '.join(columns)

        # create the table in SQLite
        c.execute('DROP TABLE IF EXISTS "{}"'.format(t_name))
        c.execute('CREATE TABLE "{}" ({})'.format(t_name, cols))

        # copy the data from MDB to SQLite
        cursor.execute('SELECT * FROM "{}"'.format(t.name))
        for row in cursor:
            values = []
            for value in row:
                if value is None:
                    values.append(u'NULL')
                else:
                    if isinstance(value, bytearray):
                        value = sqlite3.Binary(value)
                    else:
                        value = u'{}'.format(value)
                    values.append(value)
            v = ', '.join(['?'] * len(values))
            sql = 'INSERT INTO "{}" VALUES(' + v + ')'
            c.execute(sql.format(t_name), values)

    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('access', help='Input Access database path', type=argparse.FileType('r'))
    parser.add_argument('sqlite', help='Output SQLite database path', type=str)

    args = dotenv.parse_args_env(parser)

    convert(args.access.name, args.sqlite)


if __name__ == '__main__':
    main()
