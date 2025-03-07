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

CREATE TABLE VegetationTypes (
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

CREATE TABLE dgo_dataset_metrics (
    DGOID INTEGER PRIMARY KEY NOT NULL,
    ownership TEXT,
    state TEXT,
    county TEXT,
    drainage_area REAL,
    watershed_id TEXT,
    stream_name TEXT,
    stream_order INTEGER,
    headwater INTEGER,
    stream_type INTEGER,
    stream_length REAL,
    waterbody_type INTEGER,
    waterbody_extent REAL,
    ecoregion3 TEXT,
    ecoregion4 TEXT,
    landfire_evt TEXT,
    landfire_bps TEXT,
    ex_agriculture REAL,
    ex_prop_agriculture REAL,
    ex_conifer REAL,
    ex_prop_conifer REAL,
    ex_conifer_hardwood REAL,
    ex_prop_conifer_hardwood REAL,
    ex_developed REAL,
    ex_prop_developed REAL,
    ex_exotic_herbaceous REAL,
    ex_prop_exotic_herbaceous REAL,
    ex_exotic_tree_shrub REAL,
    ex_prop_exotic_tree_shrub REAL,
    ex_grassland REAL,
    ex_prop_grassland REAL,
    ex_hardwood REAL,
    ex_prop_hardwood REAL,
    ex_riparian REAL,
    ex_prop_riparian REAL,
    ex_shrubland REAL,
    ex_prop_shrubland REAL,
    ex_sparsely_vegetated REAL,
    ex_prop_sparsely_vegetated REAL,
    hist_conifer REAL,
    hist_prop_conifer REAL,
    hist_conifer_hardwood REAL,
    hist_prop_conifer_hardwood REAL,
    hist_grassland REAL,
    hist_prop_grassland REAL,
    hist_hardwood REAL,
    hist_prop_hardwood REAL,
    hist_hardwood_conifer REAL,
    hist_peatland_forest REAL,
    hist_prop_peatland_forest REAL,
    hist_peatland_nonforest REAL,
    hist_prop_peatland_nonforest REAL,
    hist_riparian REAL,
    hist_prop_riparian REAL,
    hist_savanna REAL,
    hist_prop_savanna REAL,
    hist_shrubland REAL,
    hist_prop_shrubland REAL,
    hist_sparsely_vegetated REAL,
    hist_prop_sparsely_vegetated REAL
);

CREATE TABLE dgo_rme_metrics (
    DGOID INTEGER PRIMARY KEY NOT NULL,
    prim_channel_gradient REAL,
    valley_gradient REAL,
    relative_flow_length REAL,
    confluences INTEGER,
    diffluences INTEGER,
    trib_junctions REAL,
    planform_sinuosity REAL
);


CREATE INDEX fx_measurement_values_measurement_id ON measurement_values (measurement_id);

-- CREATE INDEX ix_dgo_metric_values_metric_id ON dgo_metric_values (metric_id);
-- CREATE INDEX ix_igo_metric_values_metric_id ON igo_metric_values (metric_id);

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_groups', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_calculations', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurements', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('measurement_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('dgo_dataset_metrics', 'attributes');
