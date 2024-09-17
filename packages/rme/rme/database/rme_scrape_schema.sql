-- This schema file is for the RME **SCRAPE** database.
-- It is NOT for the actual RME model.

CREATE TABLE hucs (
    huc TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    scraped_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    is_active BOOLEAN,
    docs_url TEXT
);

-- Technically we don't need the measurements table, but it makes loading the lookup data easier
CREATE TABLE measurements (
    measurement_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT UNIQUE NOT NULL,
    machine_code TEXT UNIQUE NOT NULL,
    data_type TEXT NOT NULL,
    description TEXT,
    is_active INTEGER
);

-- -- IGOs are are a feature class and created by OGR.
-- -- But DGOs are non-spatial in the scrape and need to be created here.
create table dgos
(
    huc                       TEXT NOT NULL,
    level_path                TEXT NOT NULL,
    seg_distance              REAL NOT NULL,
    FCode                     INTEGER,
    low_lying_floodplain_area REAL,
    low_lying_floodplain_prop REAL,
    active_channel_area       REAL,
    active_channel_prop       REAL,
    elevated_floodplain_area  REAL,
    elevated_floodplain_prop  REAL,
    floodplain_area           REAL,
    floodplain_prop           REAL,
    centerline_length         REAL,
    segment_area              REAL,
    integrated_width          REAL
);

CREATE TABLE dgo_metric_values (
    huc TEXT NOT NULL,
    level_path TEXT NOT NULL,
    seg_distance TEXT NOT NULL,
    metric_id INTEGER NOT NULL,
    metric_value TEXT,

    CONSTRAINT fk_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id) ON DELETE CASCADE
);

CREATE index dgo_metric_values_huc_level_path_seg_distance_metric_id_index
    on dgo_metric_values (huc, level_path, seg_distance, metric_id);

CREATE TABLE igo_metric_values (
    huc TEXT NOT NULL,
    level_path TEXT NOT NULL,
    seg_distance TEXT NOT NULL,
    metric_id INTEGER NOT NULL,
    metric_value TEXT,

    CONSTRAINT fk_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id) ON DELETE CASCADE
);

CREATE INDEX igo_metric_values_huc_level_path_seg_distance_metric_id_index
    ON igo_metric_values (huc, level_path, seg_distance, metric_id);