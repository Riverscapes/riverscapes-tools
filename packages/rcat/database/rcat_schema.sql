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


-- The main views 
CREATE VIEW vwReaches AS SELECT R.*, G.geom
FROM ReachAttributes R
    INNER JOIN ReachGeometry G ON R.ReachID = G.ReachID;

CREATE VIEW vwIgos AS SELECT I.*, G.geom
FROM IGOAttributes I
    INNER JOIN IGOGeometry G ON I.IGOID = G.IGOID;