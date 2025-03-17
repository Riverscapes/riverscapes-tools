import sqlite3


def create_thematic_table(gpkg_path, table_name, metric_group_id):
    conn = sqlite3.connect(gpkg_path)
    c = conn.cursor()
    c.execute(f"SELECT field_name, data_type FROM metrics WHERE metric_group_id = {metric_group_id} AND is_active = 1")
    columns = "DGOID, "+", ".join([f"{field_name} {data_type}" for field_name, data_type in c.fetchall()])
    c.execute(f"CREATE TABLE {table_name} ({columns})")
    c.execute(f"INSERT INTO gpkg_contents (table_name, data_type) VALUES ('{table_name}', 'attributes')")
    c.execute(f"CREATE INDEX ix_dgoid_{table_name} ON {table_name} (DGOID)")
    conn.commit()
    conn.close()


def create_measurement_table(gpkg_path):
    conn = sqlite3.connect(gpkg_path)
    c = conn.cursor()
    c.execute("SELECT machine_code, data_type FROM measurements WHERE is_active = 1")
    columns = "DGOID, "+", ".join([f"{machine_code} {data_type}" for machine_code, data_type in c.fetchall()])
    c.execute(f"CREATE TABLE dgo_measurements ({columns})")
    c.execute("INSERT INTO gpkg_contents (table_name, data_type) VALUES ('dgo_measurements', 'attributes')")
    c.execute("CREATE INDEX ix_dgoid_meas ON dgo_measurements (DGOID)")
    conn.commit()
    conn.close()
