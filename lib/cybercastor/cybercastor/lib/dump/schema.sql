
------------------------------------------------------------------
-- RIVERSCAPES EXCHANGE: Projects Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rs_projects
                (
                pid INTEGER PRIMARY KEY,
                id TEXT,
                name TEXT,
                project_type_id TEXT,
                tags TEXT,
                created_on INTEGER,
                owned_by_id TEXT,
                owner_by_name TEXT,
                owner_by_type TEXT);

------------------------------------------------------------------
-- RIVERSCAPES EXCHANGE: Metadata Table
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rs_project_meta
                (id INTEGER PRIMARY KEY,
                project_id INTEGER,
                key TEXT,
                value TEXT);

CREATE INDEX idx_rs_project_meta_key_value ON rs_project_meta (key, value);
CREATE INDEX idx_rs_project_meta_project_id ON rs_project_meta (project_id);
CREATE INDEX idx_rs_projects_pid ON rs_projects (pid);

------------------------------------------------------------------
-- CYBERCASTOR Engine Table
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
CREATE INDEX ix_cc_jobs ON cc_jobs (created_on);

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
CREATE UNIQUE INDEX ux_cc_job_metadata ON cc_job_metadata (job_id, key);
CREATE INDEX ix_cc_job_metadata_key ON cc_job_metadata (key);


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
CREATE UNIQUE INDEX ux_cc_tasks ON cc_tasks (job_id, guid);
CREATE INDEX ix_cc_tasks ON cc_tasks (created_on);

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
CREATE UNIQUE INDEX ux_cc_jobenv ON cc_jobenv (job_id, key);


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
CREATE UNIQUE INDEX ux_cc_taskenv ON cc_taskenv (task_id, key);


------------------------------------------------------------------
-- Custom view
------------------------------------------------------------------
CREATE VIEW vw_cc_huc_status as
SELECT DISTINCT huc.fid, huc.geom, t.status
from Huc10_conus huc
         left join cc_taskenv te ON huc.HUC10 = te.value
         INNER JOIN cc_tasks t ON te.task_id = t.id
WHERE key = 'HUC'
  AND te.value <> 'FAILED';

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
WHERE table_name = 'Huc10_conus'

INSERT INTO gpkg_geometry_columns
SELECT 'vw_cc_huc_status', column_name, geometry_type_name, srs_id, z, m
FROM gpkg_geometry_columns
WHERE table_name = 'Huc10_conus';