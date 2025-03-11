CREATE TABLE metric_groups (
    metric_group_id INTEGER PRIMARY KEY NOT NULL,
    metric_group_name TEXT,
    description TEXT
);

CREATE TABLE metric_calculations (
    metric_calculation_id INTEGER PRIMARY KEY NOT NULL,
    calculation_method TEXT,
    description TEXT
);

CREATE TABLE metrics (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT UNIQUE NOT NULL,
    machine_code TEXT UNIQUE NOT NULL,
    data_type TEXT NOT NULL,
    field_name TEXT,
    description TEXT,
    method TEXT,
    small REAL,
    medium REAL,
    large REAL,
    metric_group_id INTEGER,
    metric_calculation_id INTEGER,
    is_active BOOLEAN,
    docs_url TEXT,

    CONSTRAINT fk_metric_group_id FOREIGN KEY (metric_group_id) REFERENCES metric_groups (metric_group_id),
    CONSTRAINT fk_metric_calculation_id FOREIGN KEY (metric_calculation_id) REFERENCES metric_calculations (metric_calculation_id)
);

CREATE TABLE input_datasets (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    dataset TEXT,
    field_name TEXT,
    field_value TEXT,
    CONSTRAINT fk_input_datasets_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id)
);

CREATE TABLE vegetation_types (
    VegetationID INTEGER PRIMARY KEY NOT NULL,
    EpochID INTEGER,
    Name TEXT,
    Physiognomy TEXT
);

CREATE TABLE measurements (
    measurement_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT UNIQUE NOT NULL,
    machine_code TEXT UNIQUE NOT NULL,
    data_type TEXT NOT NULL,
    description TEXT,
    is_active INTEGER
);

CREATE TABLE measurement_values (
    dgo_id INTEGER NOT NULL,
    measurement_id INTEGER NOT NULL,
    measurement_value REAL,
    metadata TEXT,
    qaqc_date TEXT,
    PRIMARY KEY (dgo_id, measurement_id),
    
    CONSTRAINT fk_measurement_values_dgo_id FOREIGN KEY (dgo_id) REFERENCES dgos (fid),
    CONSTRAINT fk_measurement_values_measurement_id FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id)
);

CREATE TABLE dgo_vegetation(
    DGOID INTEGER PRIMARY KEY NOT NULL,
    physiognomy TEXT,
    proportion INTEGER,
    CONSTRAINT fk_dgo_vegetation_DGOID FOREIGN KEY (DGOID) REFERENCES dgos (fid)
);

CREATE TABLE dgo_hist_vegetation(
    DGOID INTEGER PRIMARY KEY NOT NULL,
    physiognomy TEXT,
    proportion INTEGER,
    CONSTRAINT fk_dgo_vegetation_DGOID FOREIGN KEY (DGOID) REFERENCES dgos (fid)
);

CREATE INDEX fx_measurement_values_measurement_id ON measurement_values (measurement_id);

-- CREATE INDEX ix_dgo_metric_values_metric_id ON dgo_metric_values (metric_id);
-- CREATE INDEX ix_igo_metric_values_metric_id ON igo_metric_values (metric_id);

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_groups', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vegetation_types', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('input_datasets', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurements', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurement_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('dgo_vegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('dgo_hist_vegetation', 'attributes');
