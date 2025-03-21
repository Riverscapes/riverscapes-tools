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
    primary_metric BOOLEAN,
    moving_window BOOLEAN,
    window_calc_id INTEGER,
    is_active BOOLEAN,
    docs_url TEXT,

    CONSTRAINT fk_metric_group_id FOREIGN KEY (metric_group_id) REFERENCES metric_groups (metric_group_id),
    CONSTRAINT fk_metric_calculation_id FOREIGN KEY (metric_calculation_id) REFERENCES metric_calculations (metric_calculation_id),
    CONSTRAINT fk_window_calc_id FOREIGN KEY (window_calc_id) REFERENCES window_calculations (calculation_id)
);

CREATE TABLE input_datasets (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    dataset TEXT,
    field_name TEXT,
    field_value TEXT,
    CONSTRAINT fk_input_datasets_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id)
);

CREATE TABLE input_datasets_mw (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    dataset TEXT,
    field_name TEXT,
    CONSTRAINT fk_input_datasets_mw_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id)
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
CREATE INDEX ix_window_metric_cals ON metrics (window_calc_id);
CREATE INDEX ix_metric_calculation_calcs ON metric_calculations (metric_calculation_id);
CREATE INDEX ix_input_datasets ON input_datasets (metric_id);
CREATE INDEX ix_input_datasets_mw ON input_datasets_mw (metric_id);
CREATE INDEX ix_vegetation_types ON vegetation_types (VegetationID);

CREATE VIEW vw_dgo_desc_metrics AS SELECT dgo_desc.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM dgo_desc 
INNER JOIN dgos ON dgo_desc.dgoid = dgos.dgoid;

CREATE VIEW vw_igo_desc_metrics AS SELECT igo_desc.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM igo_desc
INNER JOIN igos ON igo_desc.igoid = igos.igoid;

CREATE VIEW vw_dgo_geomorph_metrics AS SELECT dgo_geomorph.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM dgo_geomorph
INNER JOIN dgos ON dgo_geomorph.dgoid = dgos.dgoid;

CREATE VIEW vw_igo_geomorph_metrics AS SELECT igo_geomorph.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM igo_geomorph
INNER JOIN igos ON igo_geomorph.igoid = igos.igoid;

CREATE VIEW vw_dgo_veg_metrics AS SELECT dgo_veg.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM dgo_veg
INNER JOIN dgos ON dgo_veg.dgoid = dgos.dgoid;

CREATE VIEW vw_igo_veg_metrics AS SELECT igo_veg.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM igo_veg
INNER JOIN igos ON igo_veg.igoid = igos.igoid;

CREATE VIEW vw_dgo_hydro_metrics AS SELECT dgo_hydro.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM dgo_hydro
INNER JOIN dgos ON dgo_hydro.dgoid = dgos.dgoid;

CREATE VIEW vw_igo_hydro_metrics AS SELECT igo_hydro.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM igo_hydro
INNER JOIN igos ON igo_hydro.igoid = igos.igoid;

CREATE VIEW vw_dgo_impacts_metrics AS SELECT dgo_impacts.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM dgo_impacts
INNER JOIN dgos ON dgo_impacts.dgoid = dgos.dgoid;

CREATE VIEW vw_igo_impacts_metrics AS SELECT igo_impacts.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM igo_impacts
INNER JOIN igos ON igo_impacts.igoid = igos.igoid;

CREATE VIEW vw_dgo_beaver_metrics AS SELECT dgo_beaver.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM dgo_beaver
INNER JOIN dgos ON dgo_beaver.dgoid = dgos.dgoid;

CREATE VIEW vw_igo_beaver_metrics AS SELECT igo_beaver.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM igo_beaver
INNER JOIN igos ON igo_beaver.igoid = igos.igoid;

CREATE VIEW vw_dgo_metrics AS SELECT dgo_desc.*, dgo_geomorph.*, dgo_veg.*, dgo_hydro.*, dgo_impacts.*, dgo_beaver.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM dgo_desc
INNER JOIN dgo_geomorph ON dgo_desc.dgoid = dgo_geomorph.dgoid
INNER JOIN dgo_veg ON dgo_desc.dgoid = dgo_veg.dgoid
INNER JOIN dgo_hydro ON dgo_desc.dgoid = dgo_hydro.dgoid
INNER JOIN dgo_impacts ON dgo_desc.dgoid = dgo_impacts.dgoid
INNER JOIN dgo_beaver ON dgo_desc.dgoid = dgo_beaver.dgoid
INNER JOIN dgos ON dgo_desc.dgoid = dgos.dgoid;

CREATE VIEW vw_igo_metrics AS SELECT igo_desc.*, igo_geomorph.*, igo_veg.*, igo_hydro.*, igo_impacts.*, igo_beaver.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM igo_desc
INNER JOIN igo_geomorph ON igo_desc.igoid = igo_geomorph.igoid
INNER JOIN igo_veg ON igo_desc.igoid = igo_veg.igoid
INNER JOIN igo_hydro ON igo_desc.igoid = igo_hydro.igoid
INNER JOIN igo_impacts ON igo_desc.igoid = igo_impacts.igoid
INNER JOIN igo_beaver ON igo_desc.igoid = igo_beaver.igoid
INNER JOIN igos ON igo_desc.igoid = igos.igoid;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_groups', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('window_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vegetation_types', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('input_datasets', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('input_datasets_mw', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurements', 'attributes');
