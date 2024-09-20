
CREATE TABLE hucs (
    huc10 TEXT NOT NULL PRIMARY KEY,
    rme_project_guid TEXT NOT NULL,
    rcat_project_guid TEXT NOT NULL,
    scraped_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE owners (
    id INT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    where_clause TEXT NOT NULL,
    description TEXT
);

INSERT INTO owners (id, name, where_clause) VALUES
    (1, 'Federal', 'USFS,FWS,BLM,USDA,USBR,NPS,DOD,USACE,VA,DOE,FAA,ARMY,USMC,USAF'),
    (2, 'BLM', 'BLM'),
    (3, 'USFS', 'USFS'),
    (4, 'FWS', 'FWS'),
    (5, 'NPS', 'NPS'),
    (6, 'DOD', 'DOD'),
    (7, 'State', 'ST'),
    (8, 'Tribal', 'BIA'),
    (9, 'Private', 'PVT');
    -- (10, 'Other', NULL);

CREATE TABLE flows (
    id INT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    where_clause TEXT NOT NULL,
    description TEXT
);

INSERT INTO flows (id, name, where_clause) VALUES
    (1, 'Perennial', '46006,55800,33600'),
    (2, 'Intermittent', '46003'),
    (3, 'Ephemeral', '46007');

CREATE TABLE metrics (
    huc10 INT NOT NULL REFERENCES hucs(huc10) ON DELETE CASCADE,
    owner_id INT REFERENCES owners(id) ON DELETE CASCADE,
    flow_id INT REFERENCES flows(id) ON DELETE CASCADE,
    dgo_count REAL,
    dgo_area_acres REAL,
    dgo_length_miles REAL,
    active_area REAL,
    floodplain_access_area REAL,
    lui_zero_count REAL,
    hist_riparian_area REAL,

    PRIMARY KEY(huc10, owner_id, flow_id)
);

CREATE INDEX owner_id ON metrics(owner_id);
CREATE INDEX flow_id ON metrics(flow_id);

CREATE VIEW vw_metrics AS
SELECT
    huc10,
    o.name AS ownership,
    f.name AS fcode,
    dgo_count,
    dgo_area_acres,
    dgo_length_miles,
    active_area,
    floodplain_access_area,
    lui_zero_count,
    hist_riparian_area
FROM
    metrics m
    INNER JOIN flows f ON m.flow_id = f.id
    LEFT JOIN owners o ON m.owner_id = o.id;