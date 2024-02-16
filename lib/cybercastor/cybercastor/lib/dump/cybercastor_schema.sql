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
