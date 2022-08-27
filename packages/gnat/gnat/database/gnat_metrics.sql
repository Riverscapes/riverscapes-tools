CREATE TABLE metrics (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT,
    machine_code TEXT,
    data_type TEXT,
    description TEXT,
    method TEXT,
    small REAL,
    medium REAL,
    large REAL,
    metric_group_id INTEGER,
    is_active BOOLEAN,
    docs_url TEXT
);

CREATE TABLE metric_values (
    point_id INTEGER NOT NULL,
    metric_id INTEGER NOT NULL,
    metric_value REAL,
    metadata TEXT,
    qaqc_date TEXT,
    PRIMARY KEY (point_id, metric_id)

    CONSTRAINT fk_point_id FOREIGN KEY (point_id) REFERENCES points (fid),
    CONSTRAINT fk_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id)
);

CREATE TABLE measurements (
    measurement_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT,
    machine_code TEXT,
    description TEXT
);

CREATE TABLE measurement_values (
    point_id INTEGER NOT NULL,
    measurement_id INTEGER NOT NULL,
    measurement_value REAL,
    metadata TEXT,
    qaqc_date TEXT,
    PRIMARY KEY (point_id, measurement_id)

    CONSTRAINT fk_point_id FOREIGN KEY (point_id) REFERENCES points (fid),
    CONSTRAINT fk_measurement_id FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id)
);

CREATE INDEX ix_metric_values ON metric_values (point_id);

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurement_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurements', 'attributes');

