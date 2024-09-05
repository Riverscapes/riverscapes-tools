------------------------------------------------------------------
-- WAREHOUSE Projects Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rs_projects
(
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id      TEXT NOT NULL UNIQUE,
    name            TEXT,
    project_type_id TEXT,
    tags            TEXT,
    huc10           TEXT,
    model_version   TEXT,
    created_on      INTEGER,
    owned_by_id     TEXT,
    owned_by_name   TEXT,
    owned_by_type   TEXT
);
CREATE INDEX IF NOT EXISTS ix_rs_projects_project_type_id ON rs_projects (project_type_id);
CREATE INDEX IF NOT EXISTS ix_rs_projects_created_on ON rs_projects (created_on);
CREATE INDEX IF NOT EXISTS ix_rs_projects_huc10 ON rs_projects (huc10);
CREATE INDEX IF NOT EXISTS ix_rs_projects_model_version ON rs_projects (model_version);

CREATE TABLE IF NOT EXISTS rs_project_meta
(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES rs_projects (id) ON DELETE CASCADE,
    key        TEXT,
    value      TEXT
);
CREATE INDEX IF NOT EXISTS ix_rs_project_meta ON rs_project_meta(project_id, key);
CREATE INDEX IF NOT EXISTS ix_rs_project_meta_key ON rs_project_meta(key, value);

------------------------------------------------------------------
-- VIEWS
------------------------------------------------------------------


-- DROP VIEW IF EXISTS vw_cc_huc_status;
-- CREATE VIEW vw_cc_huc_status
--     as
-- SELECT DISTINCT huc.fid, huc.geom, t.status, j.task_script_id
--       from Huc10_conus huc

--                INNER join cc_taskenv te ON huc.HUC10 = te.value
--                INNER JOIN cc_tasks t ON te.task_id = t.id
--                INNER JOIN cc_jobs j ON t.job_id = j.id
--                INNER JOIN wbdhu10_attributes w10a on huc.huc10 = w10a.HUC10
--       WHERE key = 'HUC'
--         AND te.value <> 'FAILED'
--         and w10a.STATES != 'CN'
--         and w10a.STATES != 'MX';

-- INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y,
--                            srs_id)
-- SELECT 'vw_cc_huc_status',
--        data_type,
--        'vw_cc_huc_status',
--        'Cyber Castor Status View',
--        last_change,
--        min_x,
--        min_y,
--        max_x,
--        max_y,
--        srs_id
-- FROM gpkg_contents
-- WHERE table_name = 'Huc10_conus'
-- ON CONFLICT DO NOTHING;

-- INSERT INTO gpkg_geometry_columns
-- SELECT 'vw_cc_huc_status', column_name, geometry_type_name, srs_id, z, m
-- FROM gpkg_geometry_columns
-- WHERE table_name = 'Huc10_conus'
-- ON CONFLICT DO NOTHING;


-----------------------------------------------------------------------------
-- Warehouse Projects
DROP VIEW IF EXISTS vw_projects;
CREATE VIEW vw_projects as
    select r.id,
       h.geom,
       r.project_id,
       r.project_type_id,
       r.name,
       h.huc10,
       h.name                                     huc_name,
       h.states,
       r.model_version,
       r.created_on,
       r.tags,
       datetime(r.created_on / 1000, 'unixepoch') created_on_dt,
       owned_by_id,
       owned_by_name,
       owned_by_type
from rs_projects r
         inner join vw_conus_hucs h on r.huc10 = h.huc10;


INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y,
                           srs_id)
SELECT 'vw_projects',
       data_type,
       'vw_projects',
       'Warehouse Projects View',
       last_change,
       min_x,
       min_y,
       max_x,
       max_y,
       srs_id
FROM gpkg_contents
WHERE table_name = 'wbdhu10'
ON CONFLICT DO NOTHING;

INSERT INTO gpkg_geometry_columns
SELECT 'vw_projects', column_name, geometry_type_name, srs_id, z, m
FROM gpkg_geometry_columns
WHERE table_name = 'wbdhu10'
ON CONFLICT DO NOTHING;

DROP VIEW IF EXISTS vw_project_huc4_status;
CREATE VIEW vw_project_huc4_status as
select row_number() over (order by p.project_type_id),
       w.geom,
       p.project_type_id,
       p.model_version,
       p.count_project,
       w.h10count,
       cast((100 * coalesce(p.count_project,0) / w.h10count) as real) projectpc
from vw_huc4_with_10counts w
         left join
     (select project_type_id, substr(huc10, 0, 5) huc4, model_version, count(*) count_project
      from vw_projects
      group by project_type_id, substr(huc10, 0, 5), model_version) p on w.huc4 = p.huc4;

INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y,
                           srs_id)
SELECT 'vw_project_huc4_status',
       data_type,
       'vw_project_huc4_status',
       'Warehouse Projects View Summed into HUC4',
       last_change,
       min_x,
       min_y,
       max_x,
       max_y,
       srs_id
FROM gpkg_contents
WHERE table_name = 'wbdhu10'
ON CONFLICT DO NOTHING;

INSERT INTO gpkg_geometry_columns
SELECT 'vw_project_huc4_status', column_name, geometry_type_name, srs_id, z, m
FROM gpkg_geometry_columns
WHERE table_name = 'wbdhu10'
ON CONFLICT DO NOTHING;

