CREATE TABLE Ecoregions
(
    EcoregionID INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT UNIQUE NOT NULL
);

CREATE TABLE Watersheds
(
    WatershedID TEXT PRIMARY KEY NOT NULL, 
    Name TEXT NOT NULL, 
    AreaSqKm REAL CONSTRAINT CHK_HUCs_Area CHECK (AreaSqKm >= 0), 
    States TEXT,
    QLow TEXT, 
    Q2 TEXT, 
    MaxDrainage REAL CHECK (MaxDrainage >= 0),
    EcoregionID INTEGER REFERENCES Ecoregions (EcoregionID),
    Notes TEXT,
    Metadata TEXT
);

CREATE TABLE WatershedHydroParams
(
    WatershedID TEXT, -- REFERENCES Watersheds (WatershedID) ON DELETE CASCADE NOT NULL, 
    ParamID INTEGER REFERENCES HydroParams (ParamID) NOT NULL, 
    Value REAL NOT NULL, 

    PRIMARY KEY (WatershedID, ParamID)
);

CREATE TABLE HydroParams
(
    ParamID INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT UNIQUE NOT NULL, 
    Description TEXT NOT NULL,
    Aliases TEXT, 
    DataUnits TEXT NOT NULL, 
    EquationUnits TEXT, 
    Conversion REAL NOT NULL DEFAULT (1), 
    Definition TEXT
);

CREATE TABLE FCodes
(
    FCode INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT NOT NULL, 
    DisplayName TEXT, 
    Description TEXT NOT NULL
);

CREATE TABLE ReachAttributes
(
    ReachID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER REFERENCES FCodes (FCode),
    NHDPlusID INTEGER,
    StreamName TEXT,
    level_path TEXT,
    ownership TEXT,
    divergence INTEGER,
    stream_order INTEGER,
    us_state TEXT,
    ecoregion_iii TEXT,
    ecoregion_iv TEXT,
    WatershedID TEXT,
    ElevMax REAL,
    ElevMin REAL,
    Length_m REAL,
    Slope REAL,
    DrainArea REAL,
    QLow REAL,
    Q2 REAL,
    SPLow REAL,
    SP2 REAL
);

CREATE TABLE DGOAttributes
(
    DGOID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER REFERENCES FCodes (FCode),
    level_path TEXT,
    seg_distance REAL,
    centerline_length REAL,
    segment_area REAL,
    WatershedID TEXT,
    ElevMax REAL,
    ElevMin REAL,
    Length_m REAL,
    Slope REAL,
    DrainArea REAL,
    QLow REAL,
    Q2 REAL,
    SPLow REAL,
    SP2 REAL
);

CREATE INDEX ix_dgoattributes_fcode ON DGOAttributes (FCode);
CREATE INDEX ix_dgoattributes_level_path ON DGOAttributes (level_path, seg_distance);

CREATE TABLE IGOAttributes
(
    IGOID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER REFERENCES FCodes (FCode),
    level_path TEXT,
    seg_distance REAL,
    stream_size INTEGER,
    WatershedID TEXT,
    ElevMax REAL,
    ElevMin REAL,
    Length_m REAL,
    Slope REAL,
    DrainArea REAL,
    QLow REAL,
    Q2 REAL,
    SPLow REAL,
    SP2 REAL
);

CREATE INDEX ix_igoattributes_fcode ON IGOAttributes (FCode);
CREATE INDEX ix_igoattributes_level_path ON IGOAttributes (level_path, seg_distance);


CREATE TABLE MetaData
(
    KeyInfo TEXT PRIMARY KEY NOT NULL, 
    ValueInfo TEXT
);

-- indexes

-- The main views
CREATE VIEW vwReaches AS SELECT R.*, G.geom
FROM ReachAttributes R
    INNER JOIN ReachGeometry G ON R.ReachID = G.ReachID;

CREATE VIEW vwDGOs AS SELECT D.*, G.geom
FROM DGOAttributes D
    INNER JOIN DGOGeometry G ON D.DGOID = G.DGOID;

CREATE VIEW vwIGOs AS SELECT I.*, G.geom
FROM IGOAttributes I
    INNER JOIN IGOGeometry G ON I.IGOID = G.IGOID;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Watersheds', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('WatershedHydroParams', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('HydroParams', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('FCodes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('IGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('MetaData', 'attributes')