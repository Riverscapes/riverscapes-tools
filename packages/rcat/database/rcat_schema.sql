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

CREATE TABLE ConversionVals (
    ConversionID INTEGER PRIMARY KEY NOT NULL UNIQUE,
    ConversionValue INTEGER,
    ConversionName TEXT
);

CREATE TABLE ConversionLevels (
    LevelID INTEGER PRIMARY KEY NOT NULL UNIQUE,
    LevelValue INTEGER
);

CREATE TABLE Conversions (
    ConversionID INTEGER,
    LevelID INTEGER,
    ConversionCode INTEGER,
    ConversionType TEXT,

    CONSTRAINT fk_conversions_convid FOREIGN KEY (ConversionID) REFERENCES ConversionVals (ConvserionID),
    CONSTRAINT fk_conversions_levelid FOREIGN KEY (LevelID) REFERENCES ConversionLevels (LevelID),
    PRIMARY KEY (ConversionID, LevelID)
);

CREATE TABLE Departure (
    RiparianDepartureID INTEGER,
    Departure TEXT
);

CREATE TABLE Epochs
(
    EpochID  INTEGER PRIMARY KEY NOT NULL,
    Name     TEXT                NOT NULL UNIQUE,
    Metadata TEXT,
    Notes    TEXT
);

CREATE TABLE VegetationTypes
(
    VegetationID INTEGER PRIMARY KEY NOT NULL,
    EpochID      INTEGER             NOT NULL,
    Name         TEXT                NOT NULL,
    Physiognomy  TEXT,
    LandUseGroup TEXT,
    Notes        TEXT,

    CONSTRAINT fk_VegetationTypes_EpochID FOREIGN KEY (EpochID) REFERENCES Epochs (EpochID)
);

CREATE TABLE VegClassification (
    Physiognomy TEXT,
    EpochID INTEGER NOT NULL,
    ConversionVal INTEGER,
    Riparian INTEGER,
    Vegetated INTEGER,

    CONSTRAINT fk_VegClassification_EpochID FOREIGN KEY (EpochID) REFERENCES Epochs (EpochID),
    PRIMARY KEY (Physiognomy, EpochID)
);

CREATE TABLE DGOVegetation (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,
    Area REAL NOT NULL CONSTRAINT CHK_Area CHECK (Area > 0),
    CellCount REAL NOT NULL CONSTRAINT CHK_CellCount CHECK (CellCount > 0)
);

CREATE TABLE DGOExRiparian (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    ExRipVal INTEGER,
    ExRipArea REAL NOT NULL CONSTRAINT CHK_ExRip_Area CHECK (ExRipArea > 0),
    ExRipCellCount REAL NOT NULL CONSTRAINT CHK_ExRip_CellCount CHECK (ExRipCellCount > 0)
);

CREATE TABLE DGOHRiparian (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    HRipVal INTEGER,
    HRipArea REAL NOT NULL CONSTRAINT CHK_HRip_Area CHECK (HRipArea > 0),
    HRipCellCount REAL NOT NULL CONSTRAINT CHK_HRip_CellCount CHECK (HRipCellCount > 0)
);

CREATE TABLE DGOExVeg (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    ExVegVal INTEGER,
    ExVegArea REAL NOT NULL CONSTRAINT CHK_ExVeg_Area CHECK (ExVegArea > 0),
    ExVegCellCount REAL NOT NULL CONSTRAINT CHK_ExVeg_CellCount CHECK (ExVegCellCount > 0)
);

CREATE TABLE DGOHVeg (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    HVegVal INTEGER,
    HVegArea REAL NOT NULL CONSTRAINT CHK_HVeg_Area CHECK (HVegArea > 0),
    HVegCellCount REAL NOT NULL CONSTRAINT CHK_HVeg_CellCount CHECK (HVegCellCount > 0)
);

CREATE TABLE DGOConv (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    ConvVal INTEGER,
    ConvArea REAL NOT NULL CONSTRAINT CHK_Conv_Area CHECK (ConvArea > 0),
    ConvCellCount REAL NOT NULL CONSTRAINT CHK_Conv_CellCount CHECK (ConvCellCount > 0)
);

CREATE TABLE DGOFPAccess(
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    AccessVal INTEGER,
    CellArea REAL,
    CellCount INTEGER
);

CREATE TABLE ReachVegetation (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL, 
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,  
    Area REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Area CHECK (Area > 0), 
    CellCount REAL NOT NULL CONSTRAINT CHK_ReachVegetation_CellCount CHECK (CellCount > 0)
);

CREATE TABLE ReachExRiparian (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL,
    ExRipVal INTEGER,
    ExRipArea REAL NOT NULL CONSTRAINT CHK_ExRip_Area CHECK (ExRipArea > 0),
    ExRipCellCount REAL NOT NULL CONSTRAINT CHK_ExRip_CellCount CHECK (ExRipCellCount > 0)
);

CREATE TABLE ReachHRiparian (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL,
    HRipVal INTEGER,
    HRipArea REAL NOT NULL CONSTRAINT CHK_HRip_Area CHECK (HRipArea > 0),
    HRipCellCount REAL NOT NULL CONSTRAINT CHK_HRip_CellCount CHECK (HRipCellCount > 0)
);

CREATE TABLE ReachExVeg (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL,
    ExVegVal INTEGER,
    ExVegArea REAL NOT NULL CONSTRAINT CHK_ExVeg_Area CHECK (ExVegArea > 0),
    ExVegCellCount REAL NOT NULL CONSTRAINT CHK_ExVeg_CellCount CHECK (ExVegCellCount > 0)
);

CREATE TABLE ReachHVeg (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL,
    HVegVal INTEGER,
    HVegArea REAL NOT NULL CONSTRAINT CHK_HVeg_Area CHECK (HVegArea > 0),
    HVegCellCount REAL NOT NULL CONSTRAINT CHK_HVeg_CellCount CHECK (HVegCellCount > 0)
);

CREATE TABLE ReachConv (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL,
    ConvVal INTEGER,
    ConvArea REAL NOT NULL CONSTRAINT CHK_Conv_Area CHECK (ConvArea > 0),
    ConvCellCount REAL NOT NULL CONSTRAINT CHK_Conv_CellCount CHECK (ConvCellCount > 0)
);

CREATE TABLE ReachFPAccess (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL,
    AccessVal INTEGER,
    CellArea, REAL,
    CellCount INTEGER
);

CREATE TABLE IGOAttributes (
    IGOID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER,
    level_path REAL,
    seg_distance REAL,
    stream_size INTEGER,
    LUI REAL,
    FloodplainAccess REAL,
    FromConifer REAL,
    FromDevegetated REAL,
    FromGrassShrubland REAL,
    NoChange REAL,
    GrassShrubland REAL,
    Devegetation REAL,
    Conifer REAL,
    Invasive REAL,
    Development REAL,
    Agriculture REAL,
    NonRiparian REAL,
    ConversionID INTEGER,
    LevelID INTEGER,
    ExistingRiparianMean REAL,
    HistoricRiparianMean REAL,
    RiparianDeparture REAL,
    RiparianDepartureID INTEGER,
    Condition REAL

);

CREATE TABLE DGOAttributes (
    DGOID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER,
    level_path REAL,
    seg_distance REAL,
    centerline_length REAL,
    segment_area REAL,
    LUI REAL,
    FloodplainAccess REAL,
    FromConifer REAL,
    FromDevegetated REAL,
    FromGrassShrubland REAL,
    NoChange REAL,
    GrassShrubland REAL,
    Devegetation REAL,
    Conifer REAL,
    Invasive REAL,
    Development REAL,
    Agriculture REAL,
    NonRiparian REAL,
    ExistingRiparianMean REAL,
    HistoricRiparianMean REAL,
    RiparianDeparture REAL,
    Condition REAL

);

CREATE TABLE ReachAttributes (
    ReachID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER,
    ReachCode TEXT,
    NHDPlusID INTEGER,
    WatershedID TEXT,
    StreamName TEXT,
    level_path REAL,
    TotDASqKm REAL,
    DivDASqKm REAL,
    ownership TEXT,
    divergence INTEGER,
    stream_order INTEGER,
    us_state TEXT,
    ecoregion_iii TEXT,
    ecoregion_iv TEXT,
    iPC_LU REAL,
    FloodplainAccess REAL,
    FromConifer REAL,
    FromDevegetated REAL,
    FromGrassShrubland REAL,
    NoChange REAL,
    GrassShrubland REAL,
    Devegetation REAL,
    Conifer REAL,
    Invasive REAL,
    Development REAL,
    Agriculture REAL,
    NonRiparian REAL,
    ConversionID INTEGER,
    LevelID INTEGER,
    ExistingRiparianMean REAL,
    HistoricRiparianMean REAL,
    RiparianDeparture REAL,
    RiparianDepartureID INTEGER,
    Condition REAL,

    CONSTRAINT fk_ReachAttributes_ReachID FOREIGN KEY (ReachID) REFERENCES ReachGeometry (ReachID) ON DELETE CASCADE
);

CREATE TABLE MetaData
(
    KeyInfo   TEXT PRIMARY KEY NOT NULL,
    ValueInfo TEXT
);

-- The main views 
CREATE VIEW vwReaches AS SELECT R.*, G.geom, C.ConversionCode, C.ConversionType, D.Departure
FROM ReachAttributes R
    INNER JOIN ReachGeometry G ON R.ReachID = G.ReachID
    INNER JOIN Conversions C ON R.ConversionID = C.ConversionID AND R.LevelID = C.LevelID
    INNER JOIN Departure D ON R.RiparianDepartureID = D.RiparianDepartureID;

CREATE VIEW vwIgos AS SELECT I.*, G.geom, C.ConversionCode, C.ConversionType, D.Departure
FROM IGOAttributes I
    INNER JOIN IGOGeometry G ON I.IGOID = G.IGOID
    INNER JOIN Conversions C ON I.ConversionID = C.ConversionID AND I.LevelID = C.LevelID
    INNER JOIN Departure D ON I.RiparianDepartureID = D.RiparianDepartureID;

CREATE VIEW vwDgos AS SELECT DA.*, G.geom
FROM DGOAttributes DA
    INNER JOIN DGOGeometry G ON DA.DGOID = G.DGOID;


INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Watersheds', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Epochs', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ConversionVals', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ConversionLevels', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Conversions', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('Departure', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegClassification', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOExRiparian', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOHRiparian', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOExVeg', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOHVeg', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOConv', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOFPAccess', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachExRiparian', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachHRiparian', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachExVeg', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachHVeg', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachConv', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachFPAccess', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('IGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('ReachAttributes', 'attributes')