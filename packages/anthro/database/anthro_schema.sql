CREATE TABLE VegetationTypes (
    VegetationID INTEGER PRIMARY KEY NOT NULL,  
    Name TEXT NOT NULL, 
    LandUseID INTEGER REFERENCES LandUses (LandUseID), 
    Physiognomy TEXT, 
    Notes TEXT);

CREATE TABLE FCodes (
    FCode INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT NOT NULL, 
    DisplayName TEXT, 
    Description TEXT NOT NULL);

CREATE TABLE ReachVegetation (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL, 
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,  
    Area REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Area CHECK (Area > 0), 
    CellCount REAL NOT NULL CONSTRAINT CHK_ReachVegetation_CellCount CHECK (CellCount > 0), 
    PRIMARY KEY (ReachID, VegetationID));

CREATE TABLE DGOVegetation (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,
    Area REAL NOT NULL CONSTRAINT CHK_DGOVegetation_Area CHECK (Area > 0),
    CellCount REAL NOT NULL CONSTRAINT CHK_DGOVegetation_CellCount CHECK (CellCount > 0),
    PRIMARY KEY (DGOID, VegetationID)
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
    FCode INTEGER,
    level_path REAL,
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
    DivPts_dens REAL,
    Road_prim_len REAL,
    Road_prim_dens REAL,
    Road_sec_len REAL,
    Road_sec_dens REAL,
    Road_4wd_len REAL,
    Road_4wd_dens REAL);

CREATE TABLE DGOAttributes (
    DGOID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER,
    level_path REAL,
    seg_distance REAL,
    centerline_length REAL,
    segment_area REAL,
    LUI REAL,
    Road_len REAL,
    Rail_len REAL,
    Canal_len REAL,
    RoadX_ct INTEGER,
    DivPts_ct INTEGER,
    Road_prim_len REAL,
    Road_sec_len REAL,
    Road_4wd_len REAL);

CREATE TABLE ReachAttributes (
    ReachID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER REFERENCES FCodes (FCode),
    ReachCode TEXT,
    NHDPlusID INTEGER,
    StreamName TEXT,
    level_path REAL,
    TotDASqKm REAL,
    DivDASqKm REAL,
    ownership TEXT,
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
    oPC_Dist REAL);

-- indexes

-- The main views 
CREATE VIEW vwReaches AS SELECT R.*, G.geom
FROM ReachAttributes R
    INNER JOIN ReachGeometry G ON R.ReachID = G.ReachID;

CREATE VIEW vwIgos AS SELECT I.*, G.geom
FROM IGOAttributes I
    INNER JOIN IGOGeometry G ON I.IGOID = G.IGOID;

CREATE VIEW vwDgos AS SELECT D.*, G.geom
FROM DGOAttributes D
    INNER JOIN DGOGeometry G ON D.DGOID = G.DGOID;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('FCodes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('MetaData', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('LandUses', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('LandUseIntensities', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Watersheds', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('IGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachAttributes', 'attributes');
