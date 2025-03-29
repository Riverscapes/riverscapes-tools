import sqlite3


def create_thematic_table(gpkg_path, table_name, metric_group_id):
    """creates a dgo and igo table for a given them of metric

    Args:
        gpkg_path (str): path to the geopackage for the tables
        table_name (str): the name for the table
        metric_group_id (int): the metric group ID from the metrics table
    """
    conn = sqlite3.connect(gpkg_path)
    c = conn.cursor()
    c.execute(f"SELECT field_name, data_type FROM metrics WHERE metric_group_id = {metric_group_id} AND is_active = 1")
    data = c.fetchall()
    dgo_columns = "dgoid INTEGER PRIMARY KEY NOT NULL REFERENCES dgos(dgoid) ON DELETE CASCADE, "+", ".join([f"{field_name} {data_type}" for field_name, data_type in data])
    c.execute(f"CREATE TABLE dgo_{table_name}  ({dgo_columns})")
    igo_columns = "igoid INTEGER PRIMARY KEY NOT NULL REFERENCES igos(igoid) ON DELETE CASCADE, "+", ".join([f"{field_name} {data_type}" for field_name, data_type in data])
    c.execute(f"CREATE TABLE igo_{table_name} ({igo_columns})")
    c.execute(f"INSERT INTO gpkg_contents (table_name, data_type) VALUES ('dgo_{table_name}', 'attributes')")
    c.execute(f"INSERT INTO gpkg_contents (table_name, data_type) VALUES ('igo_{table_name}', 'attributes')")
    conn.commit()
    conn.close()


def create_measurement_table(gpkg_path):
    """create a table to store measurements for dgos

    Args:
        gpkg_path (str): path to the geopackage for the table
    """
    conn = sqlite3.connect(gpkg_path)
    c = conn.cursor()
    c.execute("SELECT machine_code, data_type FROM measurements WHERE is_active = 1")
    columns = "dgoid INTEGER PRIMARY KEY NOT NULL REFERENCES dgos(DGOID) ON DELETE CASCADE, "+", ".join([f"{machine_code} {data_type}" for machine_code, data_type in c.fetchall()])
    c.execute(f"CREATE TABLE dgo_measurements ({columns})")
    c.execute("INSERT INTO gpkg_contents (table_name, data_type) VALUES ('dgo_measurements', 'attributes')")
    conn.commit()
    conn.close()
