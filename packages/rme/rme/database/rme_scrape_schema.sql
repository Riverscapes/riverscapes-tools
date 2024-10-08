-- This schema file is for the RME **SCRAPE** database.
-- It is NOT for the actual RME model.

CREATE TABLE hucs (
    huc TEXT PRIMARY KEY NOT NULL,
    rme_project_id TEXT,
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

CREATE TABLE dgo_metric_values (
    huc TEXT NOT NULL,
    level_path TEXT NOT NULL,
    seg_distance TEXT NOT NULL,
    metric_id INTEGER NOT NULL,
    metric_value TEXT,

    CONSTRAINT fk_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id) ON DELETE CASCADE
);

CREATE index dgo_metric_values_huc_level_path_seg_distance_metric_id_index
    on dgo_metric_values (huc, level_path, seg_distance);

CREATE index dgo_metric_values_metric_id ON dgo_metric_values (metric_id);


-----------------------------------------------------------------------------------------------------------------------
-- Views. This SQL was copied from an example RME project because the SQL is generated in code
-- and not stored in the database. This is a temporary solution until we can generate the SQL

-------------------------------
-- DGOs

CREATE VIEW dgo_num_metrics
AS
SELECT huc,
       level_path,
       seg_distance,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 4) AS REAL)  rme_igo_prim_channel_gradient,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 5) AS REAL)  rme_igo_valleybottom_gradient,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 6) AS REAL)  rme_igo_rel_flow_length,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 8) AS INT)   rme_dgo_confluences,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 9) AS INT)   rme_dgo_diffluences,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 10) AS REAL) rme_igo_trib_per_km,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 11) AS REAL) rme_igo_planform_sinuosity,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 12) AS REAL) rme_dgo_drainage_area,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 13) AS INT)  nhd_dgo_streamorder,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 14) AS INT)  nhd_dgo_headwater,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 16) AS REAL) nhd_dgo_streamlength,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 19) AS REAL) vbet_dgo_lowlying_area,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 20) AS REAL) vbet_dgo_elevated_area,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 21) AS REAL) vbet_dgo_channel_area,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 22) AS REAL) vbet_dgo_floodplain_area,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 23) AS REAL) vbet_igo_integrated_width,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 24) AS REAL) vbet_igo_active_channel_ratio,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 25) AS REAL) vbet_igo_low_lying_ratio,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 26) AS REAL) vbet_igo_elevated_ratio,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 27) AS REAL) vbet_igo_floodplain_ratio,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 28) AS REAL) vbet_igo_acres_vb_per_mile,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 29) AS REAL) vbet_igo_hect_vb_per_km,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 30) AS REAL) vbet_dgo_streamsize,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 31) AS REAL) conf_igo_confinement_ratio,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 32) AS REAL) conf_igo_constriction_ratio,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 33) AS REAL) conf_dgo_confining_margins,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 35) AS REAL) anthro_igo_road_dens,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 36) AS REAL) anthro_igo_rail_dens,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 37) AS REAL) anthro_igo_land_use_intens,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 38) AS REAL) rcat_igo_fldpln_access,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 39) AS REAL) rcat_igo_prop_riparian,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 40) AS REAL) rcat_igo_riparian_veg_departure,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 41) AS REAL) rcat_igo_riparian_ag_conversion,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 42) AS REAL) rcat_igo_riparian_develop,
       CAST(SUM(metric_value) FILTER (WHERE metric_id == 43) AS REAL) brat_igo_capacity
FROM dgo_metric_values
GROUP BY huc, level_path, seg_distance;


CREATE VIEW dgo_text_metrics
AS
SELECT d.huc,
       d.level_path,
       d.seg_distance,
       o.ownership  rme_dgo_ownership,
       s.us_state   rme_dgo_state,
       c.county     rme_dgo_county,
       e.ecoregion3 epa_dgo_ecoregion3,
       f.ecoregion4 epa_dgo_ecoregion4,
       br.bratrisk  brat_igo_risk,
       bo.bratopp   brat_igo_opportunity
FROM dgos d
         LEFT JOIN
     (SELECT huc, level_path, seg_distance, metric_value AS ownership FROM dgo_metric_values WHERE metric_id = 1) o
     ON o.huc = d.huc and o.level_path = d.level_path and o.seg_distance = d.seg_distance
         LEFT JOIN
     (SELECT huc, level_path, seg_distance, metric_value AS us_state FROM dgo_metric_values WHERE metric_id = 2) s
     ON s.huc = d.huc and s.level_path = d.level_path and s.seg_distance = d.seg_distance
         LEFT JOIN
     (SELECT huc, level_path, seg_distance, metric_value AS county FROM dgo_metric_values WHERE metric_id = 3) c
     ON c.huc = d.huc and c.level_path = d.level_path and c.seg_distance = d.seg_distance
         LEFT JOIN
     (SELECT huc, level_path, seg_distance, metric_value AS ecoregion3 FROM dgo_metric_values WHERE metric_id = 17) e
     ON e.huc = d.huc and e.level_path = d.level_path and e.seg_distance = d.seg_distance
         LEFT JOIN
     (SELECT huc, level_path, seg_distance, metric_value AS ecoregion4 FROM dgo_metric_values WHERE metric_id = 18) f
     ON f.huc = d.huc and f.level_path = d.level_path and f.seg_distance = d.seg_distance
         LEFT JOIN
     (SELECT huc, level_path, seg_distance, metric_value AS bratrisk FROM dgo_metric_values WHERE metric_id = 44) br
     ON br.huc = d.huc and br.level_path = d.level_path and br.seg_distance = d.seg_distance
         LEFT JOIN
     (SELECT huc, level_path, seg_distance, metric_value AS bratopp FROM dgo_metric_values WHERE metric_id = 45) bo
     ON bo.huc = d.huc and bo.level_path = d.level_path and bo.seg_distance = d.seg_distance;


CREATE VIEW dgo_metrics_pivot AS
SELECT *
FROM dgo_num_metrics
         JOIN dgo_text_metrics USING (huc, level_path, seg_distance);

CREATE VIEW vw_dgo_metrics AS
SELECT i.fid, i.geom, d.*
FROM igos i
         inner join dgo_metrics_pivot d
                    on i.huc = d.huc and i.level_path = d.level_path and i.seg_distance = d.seg_distance;

-- Spatial View Data

insert into gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y,
                           srs_id)
select 'vw_dgo_metrics_with_geom',
       data_type,
       'vw_dgo_metrics_with_geom',
       description,
       last_change,
       min_x,
       min_y,
       max_x,
       max_y,
       srs_id
From gpkg_contents
where table_name = 'igos';

insert into gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
select 'vw_dgo_metrics_with_geom', column_name, geometry_type_name, srs_id, z, m
from gpkg_geometry_columns
where table_name = 'igos';