------------------------------------------------------------------
-- CYBERCASTOR Tables
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS engine_scripts
(
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
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
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
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
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
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
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
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
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
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
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER REFERENCES cc_tasks (id) ON DELETE CASCADE,
    key     TEXT NOT NULL,
    value   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cc_taskenv ON cc_taskenv (task_id, key);


------------------------------------------------------------------
-- VIEWS
------------------------------------------------------------------

DROP VIEW IF EXISTS vw_cc_jobs;

CREATE VIEW vw_cc_jobs AS
SELECT j.id,
       j.guid,
       j.status,
       count(ct.job_id)                           task_count,
       j.created_by,
       j.created_on,
       datetime(j.created_on / 1000, 'unixepoch') created_on_dt,
       description,
       j.name,
       task_def_id,
       task_script_id
from cc_jobs j
         inner join cc_tasks ct on j.id = ct.job_id
group by j.id,
         j.guid,
         j.status,
         j.created_by,
         j.created_on,
         datetime(j.created_on / 1000, 'unixepoch'),
         description,
         j.name,
         task_def_id,
         task_script_id;

DROP VIEW IF EXISTS vw_cc_tasks;

CREATE VIEW vw_cc_tasks AS
SELECT t.id,
       t.guid,
       t.name,
       j.name,
       j.task_script_id,
       t.status,
       datetime(t.started_on / 1000, 'unixepoch') started_on_dt,
       j.name                                     job_name,
       t.name                                     task_name,
       t.cpu,
       t.memory,
       t.log_url,
       t.task_def_props,
        t.created_by,
       t.created_on,
       datetime(t.created_on / 1000, 'unixepoch') created_on_dt,
       datetime(t.queried_on / 1000, 'unixepoch') queried_on_dt,
       t.log_stream
from cc_jobs j
         inner join cc_tasks t on j.id = t.job_id;
