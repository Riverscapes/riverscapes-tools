CREATE TABLE metrics (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    machine_code TEXT NOT NULL,
    data_type TEXT,
    field_name TEXT,
    description TEXT,
    method TEXT,
    small REAL,
    medium REAL,
    large REAL,
    metric_group_id INTEGER,
    is_active BOOLEAN,
    docs_url TEXT
);

CREATE TABLE dgo_metric_values (
    dgo_id INTEGER NOT NULL,
    metric_id INTEGER NOT NULL,
    metric_value TEXT,
    metadata TEXT,
    qaqc_date TEXT,
    PRIMARY KEY (dgo_id, metric_id)

    CONSTRAINT fk_point_id FOREIGN KEY (dgo_id) REFERENCES dgos (fid) ON DELETE CASCADE,
    CONSTRAINT fk_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id) ON DELETE CASCADE
);

CREATE TABLE igo_metric_values (
    igo_id INTEGER NOT NULL,
    metric_id INTEGER NOT NULL,
    metric_value TEXT,
    metadata TEXT,
    qaqc_date TEXT,
    PRIMARY KEY (igo_id, metric_id)

    CONSTRAINT fk_igo_id FOREIGN KEY (igo_id) REFERENCES igos (fid),
    CONSTRAINT fk_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id)
);

CREATE TABLE measurements (
    measurement_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT,
    machine_code TEXT,
    data_type TEXT,
    description TEXT,
    is_active INTEGER
);

CREATE TABLE measurement_values (
    dgo_id INTEGER NOT NULL,
    measurement_id INTEGER NOT NULL,
    measurement_value REAL,
    metadata TEXT,
    qaqc_date TEXT,
    PRIMARY KEY (dgo_id, measurement_id)

    CONSTRAINT fk_point_id FOREIGN KEY (dgo_id) REFERENCES dgos (fid),
    CONSTRAINT fk_measurement_id FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id)
);

CREATE INDEX ix_dgo_metric_values_metric_id ON dgo_metric_values (metric_id);
CREATE INDEX ix_igo_metric_values_metric_id ON igo_metric_values (metric_id);
CREATE INDEX ix_active_fp_area ON dgos (active_floodplain_area);
CREATE INDEX ix_active_chan_area ON dgos (active_channel_area);
CREATE INDEX ix_active_seg_area ON dgos (segment_area);
CREATE INDEX ix_active_center_length ON dgos (centerline_length);
CREATE INDEX ix_fp_area ON dgos (floodplain_area);

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('dgo_metric_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('igo_metric_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurement_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurements', 'attributes');
