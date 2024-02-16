------------------------------------------------------------------
-- CYBERCASTOR Tables
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS engine_scripts
(
    id                INTEGER PRIMARY KEY,
    guid              TEXT UNIQUE NOT NULL,
    name              TEXT        NOT NULL,
    description       TEXT,
    local_script_path TEXT,
    task_vars         TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_engine_scripts_guid ON engine_scripts (guid);

------------------------------------------------------------------
-- CYBERCASTOR Jobs Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cc_jobs
(
    id             integer PRIMARY KEY,
    guid           TEXT UNIQUE NOT NULL,
    created_by     TEXT,
    created_on     INTEGER,
    description    TEXT,
    name           TEXT,
    status         TEXT,
    task_def_id    TEXT,
    task_script_id TEXT
);
CREATE INDEX IF NOT EXISTS ix_cc_jobs ON cc_jobs (created_on);

------------------------------------------------------------------
-- CYBERCASTOR Metadata Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cc_job_metadata
(
    id     integer PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES cc_jobs (id) ON DELETE CASCADE,
    key    TEXT    NOT NULL,
    value  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cc_job_metadata ON cc_job_metadata (job_id, key);
CREATE INDEX IF NOT EXISTS ix_cc_job_metadata_key ON cc_job_metadata (key);


------------------------------------------------------------------
-- CYBERCASTOR Tasks Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cc_tasks
(
    id             integer PRIMARY KEY,
    job_id         INTEGER     NOT NULL REFERENCES cc_jobs (id) ON DELETE CASCADE,
    guid           TEXT UNIQUE NOT NULL,
    created_by     TEXT,
    created_on     INTEGER,
    ended_on       INTEGER,
    log_stream     TEXT,
    log_url        TEXT,
    cpu            INTEGER,
    memory         INTEGER,
    name           TEXT,
    queried_on     INTEGER,
    started_on     INTEGER,
    status         TEXT,
    task_def_props TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cc_tasks ON cc_tasks (job_id, guid);
CREATE INDEX IF NOT EXISTS ix_cc_tasks ON cc_tasks (created_on);

------------------------------------------------------------------
-- CYBERCASTOR Job Environment Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cc_jobenv
(
    id     integer PRIMARY KEY,
    job_id INTEGER REFERENCES cc_jobs (id) ON DELETE CASCADE,
    key    TEXT NOT NULL,
    value  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cc_jobenv ON cc_jobenv (job_id, key);


------------------------------------------------------------------
-- CYBERCASTOR Task Environment Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cc_taskenv
(
    id      integer PRIMARY KEY,
    task_id INTEGER REFERENCES cc_tasks (id) ON DELETE CASCADE,
    key     TEXT NOT NULL,
    value   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cc_taskenv ON cc_taskenv (task_id, key);

------------------------------------------------------------------
-- WAREHOUSE Projects Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rs_projects
(
    id              INTEGER PRIMARY KEY,
    project_id      TEXT NOT NULL UNIQUE,
    name            TEXT,
    project_type_id TEXT,
    tags            TEXT,
    created_on      INTEGER,
    owned_by_id     TEXT,
    owner_by_name   TEXT,
    owner_by_type   TEXT
);
CREATE INDEX IF NOT EXISTS ix_rs_projects_project_type_id ON rs_projects (project_type_id);
CREATE INDEX IF NOT EXISTS is_rs_projects_created_on ON rs_projects (created_on);

CREATE TABLE IF NOT EXISTS rs_project_meta
(
    id         INTEGER PRIMARY KEY,
    project_id INTEGER REFERENCES rs_projects (id) ON DELETE CASCADE,
    key        TEXT,
    value      TEXT
);
CREATE INDEX IF NOT EXISTS ix_rs_project_meta ON rs_project_meta(project_id, key);
CREATE INDEX IF NOT EXISTS ix_rs_project_meta_key ON rs_project_meta(key, value);

------------------------------------------------------------------
-- VIEWS
------------------------------------------------------------------
DROP VIEW IF EXISTS vw_cc_huc_status;
CREATE VIEW vw_cc_huc_status
    as
SELECT DISTINCT huc.fid, huc.geom, t.status, j.task_script_id
      from Huc10_conus huc

               INNER join cc_taskenv te ON huc.HUC10 = te.value
               INNER JOIN cc_tasks t ON te.task_id = t.id
               INNER JOIN cc_jobs j ON t.job_id = j.id
               INNER JOIN wbdhu10_attributes w10a on huc.huc10 = w10a.HUC10
      WHERE key = 'HUC'
        AND te.value <> 'FAILED'
        and w10a.STATES != 'CN'
        and w10a.STATES != 'MX';

        INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y,
                           srs_id)
SELECT 'vw_cc_huc_status',
       data_type,
       'vw_cc_huc_status',
       'Cyber Castor Status View',
       last_change,
       min_x,
       min_y,
       max_x,
       max_y,
       srs_id
FROM gpkg_contents
WHERE table_name = 'Huc10_conus';

INSERT INTO gpkg_geometry_columns
SELECT 'vw_cc_huc_status', column_name, geometry_type_name, srs_id, z, m
FROM gpkg_geometry_columns
WHERE table_name = 'Huc10_conus'
ON CONFLICT DO NOTHING;


-----------------------------------------------------------------------------
-- Warehouse Projects
DROP VIEW IF EXISTS vw_projects;
CREATE VIEW vw_projects as
    select p.id, huc.geom, p.project_id, p.name, p.project_type_id, p.tags, p.created_on, p.owner_by_name
from rs_projects p
         INNER JOIN
     (select project_id, value from rs_project_meta WHERE key = 'Model Version') mv on p.id = mv.project_id
         inner join
     (select project_id, value
      from rs_project_meta
      WHERE key = 'HUC10') mh on p.id = mh.project_id

         inner join Huc10_conus huc on huc.HUC10 = mh.value;

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
WHERE table_name = 'Huc10_conus';

INSERT INTO gpkg_geometry_columns
SELECT 'vw_projects', column_name, geometry_type_name, srs_id, z, m
FROM gpkg_geometry_columns
WHERE table_name = 'Huc10_conus'
ON CONFLICT DO NOTHING;
