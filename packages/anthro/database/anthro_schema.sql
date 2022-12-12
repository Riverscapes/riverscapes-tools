CREATE TABLE Agencies (
    AgencyID INTEGER PRIMARY KEY NOT NULL UNIQUE, 
    Name TEXT NOT NULL UNIQUE, 
    Abbreviation TEXT NOT NULL UNIQUE);

CREATE TABLE VegetationTypes (
    VegetationID INTEGER PRIMARY KEY NOT NULL,  
    Name TEXT NOT NULL, 
    LandUseID INTEGER REFERENCES LandUses (LandUseID), 
    Physiognomy TEXT, 
    Notes TEXT);

CREATE TABLE ReachCodes (
    ReachCode INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT NOT NULL, 
    DisplayName TEXT, 
    Description TEXT NOT NULL);

CREATE TABLE ReachVegetation (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL, 
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,  
    Area REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Area CHECK (Area > 0), 
    CellCount REAL NOT NULL CONSTRAINT CHK_ReachVegetation_CellCount CHECK (CellCount > 0), 
    PRIMARY KEY (ReachID, VegetationID));

CREATE TABLE IGOVegetation (
    IGOID INTEGER REFERENCES IGOAttributes ON DELETE CASCADE NOT NULL,
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,
    Area REAL NOT NULL CONSTRAINT CHK_IGOVegetation_Area CHECK (Area > 0),
    CellCount REAL NOT NULL CONSTRAINT CHK_IGOVegetation_CellCount CHECK (CellCount > 0),
    PRIMARY KEY (IGOID, VegetationID)
);

CREATE TABLE MetaData (KeyInfo TEXT PRIMARY KEY NOT NULL, ValueInfo TEXT);

CREATE TABLE LandUses (
    LandUseID INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT UNIQUE NOT NULL, 
    Intensity REAL NOT NULL CONSTRAINT CHK_LandUses_Intensity CHECK (Intensity >= 0 AND Intensity <= 1) DEFAULT (0));

CREATE TABLE LandUseIntensities (
    IntensityID INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT UNIQUE NOT NULL, 
    MaxIntensity REAL NOT NULL UNIQUE, 
    TargetCol TEXT UNIQUE NOT NULL);

CREATE TABLE Watersheds (
    WatershedID TEXT PRIMARY KEY NOT NULL UNIQUE, 
    Name TEXT NOT NULL, 
    AreaSqKm REAL CONSTRAINT CHK_HUCs_Area CHECK (AreaSqKm >= 0), 
    States TEXT, 
    Geometry STRING, 
    QLow TEXT, 
    Q2 TEXT, 
    MaxDrainage REAL CHECK (MaxDrainage >= 0), 
    Metadata TEXT, 
    Notes TEXT);

CREATE TABLE IGOAttributes (
    IGOID INTEGER PRIMARY KEY NOT NULL,
    LevelPathI, REAL,
    seg_distance REAL,
    stream_size INTEGER,
    LUI REAL,
    Road_len REAL,
    Road_dens REAL,
    Rail_len REAL,
    Rail_dens REAL,
    Canal_len REAL,
    Canal_dens REAL,
    RoadX_ct INTEGER,
    RoadX_dens REAL,
    DivPts_ct INTEGER,
    DivPts_dens REAL);

CREATE TABLE ReachAttributes (
    ReachID INTEGER PRIMARY KEY NOT NULL,
    ReachCode INTEGER REFERENCES ReachCodes (ReachCode),
    WatershedID TEXT REFERENCES Watersheds (WatershedID) ON DELETE CASCADE,
    StreamName TEXT,
    iPC_Road REAL CONSTRAINT CHK_Reaches_RoadDist CHECK (iPC_Road >= 0),
    iPC_RoadX REAL CONSTRAINT CHK_Reaches_RoadCrossDists CHECK (iPC_RoadX >= 0),
    iPC_RoadVB REAL CONSTRAINT CHK_Reaches_RoadVBDist CHECK (iPC_RoadVB >= 0),
    iPC_Rail REAL CONSTRAINT CHK_Reaches_RailDist CHECK (iPC_Rail >= 0),
    iPC_RailVB REAL CONSTRAINT CHK_Reaches_RailVBDist CHECK (iPC_RailVB >= 0),
    iPC_DivPts REAL,
    iPC_Privat REAL,
    iPC_Canal REAL,
    iPC_LU REAL,
    iPC_VLowLU REAL,
    iPC_LowLU REAL,
    iPC_ModLU REAL,
    iPC_HighLU REAL,
    oPC_Dist REAL,
    AgencyID INTEGER REFERENCES Agencies (AgencyID));

-- indexes


-- Non-spatial view of Anthro results with joins to the relevant tables
CREATE VIEW vwReachAttributes AS
SELECT R.*,
       A.Name Agency
FROM ReachAttributes R
    INNER JOIN Agencies A ON R.AgencyID = A.AgencyID;

-- The main views 
CREATE VIEW vwReaches AS SELECT R.*, G.geom
FROM vwReachAttributes R
    INNER JOIN anthro_lines_geom G ON R.ReachID = G.ReachID;

CREATE VIEW vwIgos AS SELECT I.*, G.geom
FROM IGOAttributes I
    INNER JOIN anthro_igo_geom G ON I.IGOID = G.IGOID;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Agencies', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachCodes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('MetaData', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('LandUses', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('LandUseIntensities', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Watersheds', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('IGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vwReachAttributes', 'attributes');
