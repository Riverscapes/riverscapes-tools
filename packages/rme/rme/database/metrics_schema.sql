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
CREATE INDEX ix_window_metric_cals ON metrics (window_calc_id);
CREATE INDEX ix_metric_calculation_calcs ON metric_calculations (metric_calculation_id);
CREATE INDEX ix_input_datasets ON input_datasets (metric_id);
CREATE INDEX ix_mw_input_datasets ON mw_input_datasets (metric_id);
CREATE INDEX ix_vegetation_types ON vegetation_types (VegetationID);

CREATE VIEW vw_dgo_desc_metrics AS SELECT desc_dgo.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM desc_dgo 
INNER JOIN dgos ON desc_dgo.DGOID = dgos.DGOID;

CREATE VIEW vw_igo_desc_metrics AS SELECT desc_igo.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM desc_igo
INNER JOIN igos ON desc_igo.IGOID = igos.IGOID;

CREATE VIEW vw_dgo_geomorph_metrics AS SELECT geomorph_dgo.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM geomorph_dgo
INNER JOIN dgos ON geomorph_dgo.DGOID = dgos.DGOID;

CREATE VIEW vw_igo_geomorph_metrics AS SELECT geomorph_igo.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM geomorph_igo
INNER JOIN igos ON geomorph_igo.IGOID = igos.IGOID;

CREATE VIEW vw_dgo_veg_metrics AS SELECT veg_dgo.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM veg_dgo
INNER JOIN dgos ON veg_dgo.DGOID = dgos.DGOID;

CREATE VIEW vw_igo_veg_metrics AS SELECT veg_igo.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM veg_igo
INNER JOIN igos ON veg_igo.IGOID = igos.IGOID;

CREATE VIEW vw_dgo_hydro_metrics AS SELECT hydro_dgo.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM hydro_dgo
INNER JOIN dgos ON hydro_dgo.DGOID = dgos.DGOID;

CREATE VIEW vw_igo_hydro_metrics AS SELECT hydro_igo.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM hydro_igo
INNER JOIN igos ON hydro_igo.IGOID = igos.IGOID;

CREATE VIEW vw_dgo_impacts_metrics AS SELECT impacts_dgo.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM impacts_dgo
INNER JOIN dgos ON impacts_dgo.DGOID = dgos.DGOID;

CREATE VIEW vw_igo_impacts_metrics AS SELECT impacts_igo.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM impacts_igo
INNER JOIN igos ON impacts_igo.IGOID = igos.IGOID;

CREATE VIEW vw_dgo_beaver_metrics AS SELECT beaver_dgo.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM beaver_dgo
INNER JOIN dgos ON beaver_dgo.DGOID = dgos.DGOID;

CREATE VIEW vw_igo_beaver_metrics AS SELECT beaver_igo.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM beaver_igo
INNER JOIN igos ON beaver_igo.IGOID = igos.IGOID;

CREATE VIEW vw_dgo_metrics AS SELECT desc_dgo.*, geomorph_dgo.*, veg_dgo.*, hydro_dgo.*, impacts_dgo.*, beaver_dgo.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
FROM desc_dgo
INNER JOIN geomorph_dgo ON desc_dgo.DGOID = geomorph_dgo.DGOID
INNER JOIN veg_dgo ON desc_dgo.DGOID = veg_dgo.DGOID
INNER JOIN hydro_dgo ON desc_dgo.DGOID = hydro_dgo.DGOID
INNER JOIN impacts_dgo ON desc_dgo.DGOID = impacts_dgo.DGOID
INNER JOIN beaver_dgo ON desc_dgo.DGOID = beaver_dgo.DGOID
INNER JOIN dgos ON desc_dgo.DGOID = dgos.DGOID;

CREATE VIEW vw_igo_metrics AS SELECT desc_igo.*, geomorph_igo.*, veg_igo.*, hydro_igo.*, impacts_igo.*, beaver_igo.*, igos.geom, igos.level_path, igos.seg_distance, igos.FCode
FROM desc_igo
INNER JOIN geomorph_igo ON desc_igo.IGOID = geomorph_igo.IGOID
INNER JOIN veg_igo ON desc_igo.IGOID = veg_igo.IGOID
INNER JOIN hydro_igo ON desc_igo.IGOID = hydro_igo.IGOID
INNER JOIN impacts_igo ON desc_igo.IGOID = impacts_igo.IGOID
INNER JOIN beaver_igo ON desc_igo.IGOID = beaver_igo.IGOID
INNER JOIN igos ON desc_igo.IGOID = igos.IGOID;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_groups', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('window_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vegetation_types', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('input_datasets', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('mw_input_datasets', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurements', 'attributes');
