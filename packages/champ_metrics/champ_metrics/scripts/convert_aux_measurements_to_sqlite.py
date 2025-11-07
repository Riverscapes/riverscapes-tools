"""
Script to build a SQLite version of the CHaMP All Measurements database.
The original database is in MS Access (*.mdb) format, so this script uses CSV exports
of the Access tables to import into SQLite.

Philip Bailey
6 Nov 2025

Steps
1. Use mdb-export (brew install mdb-export)to write Access schema to file in SQLite syntax:

mdb-schema mydatabase.mdb sqlite > schema.sql

2. Use mdb-tables to list the tables in the Access database.

mdb-tables mydatabase.mdb > tables.txt

3. Reformat the list of tables to make a shell script to export each table to CSV.
I did this reformatting in GSheets. The resulting shell script looks like this:

mdb-export /Users/philipbailey/GISData/riverscapes/champ/CHaMP_All_Measurements.MDB AirTemperatureLogger > AirTemperatureLogger.csv
mdb-export /Users/philipbailey/GISData/riverscapes/champ/CHaMP_All_Measurements.MDB Benchmark > Benchmark.csv
mdb-export /Users/philipbailey/GISData/riverscapes/champ/CHaMP_All_Measurements.MDB ChannelUnit > ChannelUnit.csv

4. Use the Python script below to create the SQLite database and import the CSV data files.
"""

import sqlite3
import csv
import os

# Path to new SQLite database
db_path = "/Users/philipbailey/GISData/riverscapes/champ/CHaMP_All_Measurements3.sqlite"

# Path to SQL file with DDL statements
ddl_file = "/Users/philipbailey/GISData/riverscapes/champ/CHaMP_All_Measurements_sqlite.sql"

# Folder containing CSV files
csv_folder = "/Users/philipbailey/GISData/riverscapes/champ/access_dump_csv"

# Create/connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Run DDL SQL file
with open(ddl_file, "r", encoding="utf-8") as f:
    ddl_sql = f.read()
cursor.executescript(ddl_sql)
print(f"Executed DDL from {ddl_file}")

# Get a list of the columns in the visits table.
cursor.execute("PRAGMA table_info(Visits)")
visit_columns = [f'`{column[1]}`' for column in cursor.fetchall()]
print(f"Columns in 'Visits' table: {visit_columns}")

# Loop over CSV files and import data
for file_name in os.listdir(csv_folder):
    if file_name.lower().endswith(".csv"):
        table_name = os.path.splitext(file_name)[0]
        file_path = os.path.join(csv_folder, file_name)

        print(f"Importing {file_path} into table '{table_name}'")

        try:

            with open(file_path, "r", encoding="utf-8-sig") as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)  # Assume first row is header
                headers = [f'`{h.replace(' % ','')}`' for h in headers]
                placeholders = ",".join("?" * len(headers))
                insert_sql = f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({placeholders})"

                rows = list(reader)
                cursor.executemany(insert_sql, rows)
                conn.commit()
                print(f"Imported {len(rows)} rows into '{table_name}'")

                # Now insert the distinct visits from this table into the Visits table
                # including a do nothing on conflict clause to avoid duplicates
                visit_insert_sql = f"""
                INSERT INTO Visits ({','.join(visit_columns)})
                SELECT {','.join(visit_columns)} FROM {table_name}
                """
                cursor.execute(visit_insert_sql)

                # Drop all but the VisitID column from the current table to save space
                drop_columns = [col for col in visit_columns if "VisitID" not in col]
                for col in drop_columns:
                    drop_sql = f"ALTER TABLE {table_name} DROP COLUMN {col}"
                    try:
                        cursor.execute(drop_sql)
                    except sqlite3.OperationalError as e:
                        print(f"Could not drop column {col} from {table_name}: {e}")

                # Loop over the fields and set them NULL if they are empty strings
                cursor.execute(f"PRAGMA table_info({table_name})")
                table_info = cursor.fetchall()
                text_columns = [f'`{col[1]}`' for col in table_info if col[2].upper()]  # in ("TEXT", "VARCHAR")]
                for col in text_columns:
                    update_sql = f"UPDATE {table_name} SET {col} = NULL WHERE {col} = ''"
                    cursor.execute(update_sql)

                # Create visit index on VisitID in the current table
                index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table_name.lower()}_visitid ON {table_name} (VisitID)"
                cursor.execute(index_sql)

                # Now drop
                conn.commit()

        except Exception as e:
            print(f"Error importing {file_path}: {e}")
            continue

# Create index on VisitID in Visits table for faster lookups
cursor.execute("CREATE INDEX IF NOT EXISTS idx_visits_visitid ON Visits (VisitID)")
conn.commit()

# Keep only the first occurrence of each VisitID in the Visits table
cursor.execute("""
DELETE FROM Visits
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM Visits
    GROUP BY VisitID
)
""")
conn.commit()
print("Removed duplicate VisitID entries from Visits table.")

# Perform a vacuum to optimize the database
cursor.execute("VACUUM")
print("Database vacuumed.")

conn.close()
print(f"All done. Database saved to {db_path}")
