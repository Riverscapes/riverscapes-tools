
CREATE TABLE Agencies
(
    AgencyID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL,
    Abbreviation TEXT
);

CREATE TABLE DamCapacities
(
    CapacityID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL,
    MinCapacity REAL CHECK (MinCapacity >= 0),
    MaxCapacity REAL CHECK (MaxCapacity >= 0)
);

CREATE TABLE DamLimitations
(
    LimitationID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL
);

CREATE TABLE DamOpportunities
(
    OpportunityID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL
);

CREATE TABLE DamRisks
(
    RiskID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL
);

CREATE TABLE Ecoregions
(
    EcoregionID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL
);

CREATE TABLE Epochs
(
    EpochID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL,
    Metadata TEXT,
    Notes TEXT
);

CREATE TABLE HydroParams
(
    ParamID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL,
    Description TEXT,
    Aliases TEXT,
    DataUnits TEXT,
    EquationUnits TEXT,
    Conversion REAL,
    Definition TEXT
);

CREATE TABLE Watersheds
(
    WatershedID TEXT PRIMARY KEY,
    Name TEXT NOT NULL,
    AreaSqkm REAL CHECK (AreaSqkm > 0),
    States TEXT,
    Qlow TEXT,
    Q2 TEXT,
    MaxDrainage REAL CHECK (MaxDrainage > 0),
    EcoregionID INTEGER REFERENCES Ecoregions(EcoregionID),
    Notes TEXT,
    Metadata TEXT
);

CREATE TABLE LandUseIntensities
(
    IntensityID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL,
    MaxIntensity REAL CHECK(MaxIntensity >= 0 AND MaxIntensity <= 4),
    TargetCol TEXT UNIQUE NOT NULL
);

CREATE TABLE LandUses
(
    LandUseID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT UNIQUE NOT NULL,
    Intensity INT CHECK (Intensity >= 0 AND Intensity <= 4)
);

CREATE TABLE ReachCodes
(
    ReachCode TEXT PRIMARY KEY,
    Name TEXT NOT NULL,
    DisplayName TEXT,
    Description TEXT
);

CREATE TABLE VegetationTypes
(
    VegetationID INTEGER PRIMARY KEY AUTOINCREMENT,
    EpochID INTEGER NOT NULL REFERENCES Epochs(EpochID),
    Name TEXT NOT NULL,
    DefaultSuitability INTEGER NOT NULL CHECK(DefaultSuitability >= 0 AND DefaultSuitability <= 4),
    LandUseID INTEGER REFERENCES LandUses(LandUseID),
    Physiognomy TEXT,
    Notes TEXT
);

CREATE TABLE VegetationOverrides
(
    EcoregionID INTEGER NOT NULL REFERENCES Ecoregions(EcoregionID),
    VegetationID INTEGER NOT NULL REFERENCES VegetationTypes(VegetationID),
    OverrideSuitability INTEGER NOT NULL CHECK(OverrideSuitability >= 0 AND OverrideSuitability <= 4),
    Notes TEXT,

    PRIMARY KEY (EcoregionID, VegetationID)
);

CREATE TABLE WatershedHydroParams
(
    WatershedID TEXT NOT NULL REFERENCES Watersheds(WatershedID) ON DELETE CASCADE,
    ParamID INTEGER NOT NULL REFERENCES HydroParams(ParamID) ON DELETE CASCADE,
    Value REAL,

    PRIMARY KEY (WatershedID, ParamID)
);

CREATE INDEX fx_watershed_hydro_params_param_id ON WatershedHydroParams(ParamID);

-- VIEWS

create view vwWatershedHydroParams AS
SELECT WatershedID,
       p.Name,
       whp.Value,
       p.DataUnits,
       p.EquationUnits,
       p.Conversion
FROM HydroParams p
    JOIN WatershedHydroParams whp ON p.ParamID = whp.ParamID;

