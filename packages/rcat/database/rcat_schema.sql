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
    CoversionVal INTEGER,
    Riparian INTEGER,
    Vegetated INTEGER,

    CONSTRAINT fk_VegClassification_EpochID FOREIGN KEY (EpochID) REFERENCES Epochs (EpochID)
    PRIMARY KEY (Physiognomy, EpochID)
);

CREATE TABLE IGOVegetation (
    IGOID INTEGER REFERENCES IGOAttributes ON DELETE CASCADE NOT NULL,
    VegetationID INTEGER REFERENCES IGOAttributes ON DELETE CASCADE NOT NULL,
    Area REAL NOT NULL CONSTRAINT CHK_Area CHECK (Area > 0),
    CellCount REAL NOT NULL CONSTRAINT CHK_CellCount CHECK (CellCount > 0),
    ExRipVal INTEGER,
    ExRipArea REAL NOT NULL CONSTRAINT CHK_ExRip_Area CHECK (ExRipArea > 0),
    ExRipCellCount REAL NOT NULL CONSTRAINT CHK_ExRip_CellCount CHECK (ExRipCellCount > 0),
    HRipVal INTEGER,
    HRipArea REAL NOT NULL CONSTRAINT CHK_HRip_Area CHECK (HRipArea > 0),
    HRipCellCount REAL NOT NULL CONSTRAINT CHK_HRip_CellCount CHECK (HRipCellCount > 0),
    ExVegVal INTEGER,
    ExVegArea REAL NOT NULL CONSTRAINT CHK_ExVeg_Area CHECK (ExVegArea > 0),
    ExVegCellCount REAL NOT NULL CONSTRAINT CHK_ExVeg_CellCount CHECK (ExVegCellCount > 0),
    HVegVal INTEGER,
    HVegArea REAL NOT NULL CONSTRAINT CHK_HVeg_Area CHECK (HVegArea > 0),
    HVegCellCount REAL NOT NULL CONSTRAINT CHK_HVeg_CellCount CHECK (HVegCellCount > 0),
    ConvVal INTEGER,
    ConvArea REAL NOT NULL CONSTRAINT CHK_Conv_Area CHECK (ConvArea > 0),
    ConvCellCount REAL NOT NULL CONSTRAINT CHK_Conv_CellCount CHECK (ConvCellCount > 0),
);

CREATE TABLE ReachVegetation (
    ReachID INTEGER REFERENCES ReachAttributes ON DELETE CASCADE NOT NULL, 
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,  
    Area REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Area CHECK (Area > 0), 
    CellCount REAL NOT NULL CONSTRAINT CHK_ReachVegetation_CellCount CHECK (CellCount > 0), 
    PRIMARY KEY (ReachID, VegetationID)
);

CREATE TABLE IGOAttributes (
    IGOID INTEGER PRIMARY KEY NOT NULL,
    LevelPathI REAL,
    seg_distance REAL,
    stream_size INTEGER,
    LUI REAL,
    FloodplainAccess REAL,
    FromConifer REAL,
    FromDevegetated REAL,
    FromGrassShrubland REAL,
    FromDeciduous REAL,
    NoChange REAL,
    Deciduous REAL,
    GrassShrubland REAL,
    Devegetation REAL,
    Conifer REAL,
    Invasive REAL,
    Development REAL,
    Agriculture REAL,
    RiparianTotal REAL,
    ConversionID INTEGER,
    ExistingRiparianMean REAL,
    HistoricRiparianMean REAL,
    RiparianDeparture REAL,
    RiparianDepartureID INTEGER,
    ExistingNativeRiparianMean REAL,
    HistoricNativeRiparianMean REAL,
    NativeRiparianDeparture REAL

);

CREATE TABLE ReachAttributes (
    ReachID INTEGER PRIMARY KEY NOT NULL,
    ReachCode INTEGER,
    WatershedID TEXT,
    StreamName TEXT,
    iPC_LU REAL,
    FloodplainAccess REAL,
    FromConifer REAL,
    FromDevegetated REAL,
    FromGrassShrubland REAL,
    FromDeciduous REAL,
    NoChange REAL,
    Deciduous REAL,
    GrassShrubland REAL,
    Devegetation REAL,
    Conifer REAL,
    Invasive REAL,
    Development REAL,
    Agriculture REAL,
    RiparianTotal REAL,
    ConversionID INTEGER,
    ExistingRiparianMean REAL,
    HistoricRiparianMean REAL,
    RiparianDeparture REAL,
    RiparianDepartureID INTEGER,
    ExistingNativeRiparianMean REAL,
    HistoricNativeRiparianMean REAL,
    NativeRiparianDeparture REAL,

    CONSTRAINT fk_ReachAttributes_ReachID FOREIGN KEY (ReachID) REFERENCES ReachGeometry (ReachID) ON DELETE CASCADE,
    CONSTRAINT fk_ReachAttributes_RiparianDepartureID FOREIGN KEY (RiparianDepartureID) REFERENCES DepartureLevels (LevelID),
    CONSTRAINT fk_ReachAttributes_ConversionID FOREIGN KEY (ConversionID) REFERENCES Conversions (ConversionID)
)


CREATE VIEW vwClassifications AS SELECT V.*
FROM VegetationTypes V
    INNER JOIN VegClassification C ON V.Physiognomy = C.Physiognomy;

-- The main views 
CREATE VIEW vwReaches AS SELECT R.*, G.geom
FROM ReachAttributes R
    INNER JOIN ReachGeometry G ON R.ReachID = G.ReachID;

CREATE VIEW vwIgos AS SELECT I.*, G.geom
FROM IGOAttributes I
    INNER JOIN IGOGeometry G ON I.IGOID = G.IGOID;