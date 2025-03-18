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

CREATE TABLE window_calculations (
    calculation_id INTEGER PRIMARY KEY NOT NULL,
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
    small REAL,
    medium REAL,
    large REAL,
    metric_group_id INTEGER,
    metric_calculation_id INTEGER,
    primary_metric INTEGER,
    moving_window INTEGER,
    window_calc_id INTEGER,
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

CREATE TABLE mw_input_datasets (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    dataset TEXT,
    field_name TEXT,
    CONSTRAINT fk_mw_input_datasets_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id)
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

CREATE INDEX ix_metric_group_metrics ON metrics (metric_group_id);
CREATE INDEX ix_metric_group_groups ON metric_groups (metric_group_id);
CREATE INDEX ix_metric_calculation_metrics ON metrics (metric_calculation_id);
CREATE INDEX ix_window_calculation ON window_calculations (calculation_id);
CREATE INDEX ix_metric_calculation_calcs ON metric_calculations (metric_calculation_id);
CREATE INDEX ix_input_datasets ON input_datasets (metric_id);
CREATE INDEX ix_mw_input_datasets ON mw_input_datasets (metric_id);
CREATE INDEX ix_vegetation_types ON vegetation_types (VegetationID);


-- CREATE INDEX ix_dgo_metric_values_metric_id ON dgo_metric_values (metric_id);
-- CREATE INDEX ix_igo_metric_values_metric_id ON igo_metric_values (metric_id);

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_groups', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('window_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vegetation_types', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('input_datasets', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('mw_input_datasets', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurements', 'attributes');
