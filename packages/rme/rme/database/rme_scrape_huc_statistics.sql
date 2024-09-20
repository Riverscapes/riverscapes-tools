
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

CREATE TABLE us_states (
    id INT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    where_clause TEXT NOT NULL UNIQUE
);

INSERT INTO us_states(id, name, where_clause)
VALUES 
    (1,'Alabama','AL'),
    (2,'Alaska','AK'),
    (3,'Arizona','AZ'),
    (4,'Arkansas','AR'),
    (5,'California','CA'),
    (6,'Colorado','CO'),
    (7,'Connecticut','CT'),
    (8,'Delaware','DE'),
    (9,'Florida','FL'),
    (10,'Georgia','GA'),
    (11,'Hawaii','HI'),
    (12,'Idaho','ID'),
    (13,'Illinois','IL'),
    (14,'Indiana','IN'),
    (15,'Iowa','IA'),
    (16,'Kansas','KS'),
    (17,'Kentucky','KY'),
    (18,'Louisiana','LA'),
    (19,'Maine','ME'),
    (20,'Maryland','MD'),
    (21,'Massachusetts','MA'),
    (22,'Michigan','MI'),
    (23,'Minnesota','MN'),
    (24,'Mississippi','MS'),
    (25,'Missouri','MO'),
    (26,'Montana','MT'),
    (27,'Nebraska','NE'),
    (28,'Nevada','NV'),
    (29,'New Hampshire','NH'),
    (30,'New Jersey','NJ'),
    (31,'New Mexico','NM'),
    (32,'New York','NY'),
    (33,'North Carolina','NC'),
    (34,'North Dakota','ND'),
    (35,'Ohio','OH'),
    (36,'Oklahoma','OK'),
    (37,'Oregon','OR'),
    (38,'Pennsylvania','PA'),
    (39,'Rhode Island','RI'),
    (40,'South Carolina','SC'),
    (41,'South Dakota','SD'),
    (42,'Tennessee','TN'),
    (43,'Texas','TX'),
    (44,'Utah','UT'),
    (45,'Vermont','VT'),
    (46,'Virginia','VA'),
    (47,'Washington','WA'),
    (48,'West Virginia','WV'),
    (49,'Wisconsin','WI'),
    (50,'Wyoming','WY');

CREATE TABLE metrics (
    huc10 INT NOT NULL REFERENCES hucs(huc10) ON DELETE CASCADE,
    state_id INT REFERENCES us_states(id) ON DELETE CASCADE,
    owner_id INT REFERENCES owners(id) ON DELETE CASCADE,
    flow_id INT REFERENCES flows(id) ON DELETE CASCADE,
    dgo_count REAL,
    dgo_area_acres REAL,
    dgo_length_miles REAL,
    active_area REAL,
    floodplain_access_area REAL,
    lui_zero_area REAL,
    hist_riparian_area REAL,

    PRIMARY KEY(huc10, state_id, owner_id, flow_id)
);

CREATE INDEX owner_id ON metrics(owner_id);
CREATE INDEX flow_id ON metrics(flow_id);

CREATE VIEW vw_metrics AS
SELECT
    huc10,
    s.where_clause AS us_state,
    o.name AS ownership,
    f.name AS fcode,
    dgo_count,
    dgo_area_acres,
    dgo_length_miles,
    active_area,
    floodplain_access_area,
    lui_zero_area,
    hist_riparian_area
FROM
    metrics m
    INNER JOIN flows f ON m.flow_id = f.id
    INNER JOIN us_states s ON m.state_id = s.id
    LEFT JOIN owners o ON m.owner_id = o.id;