CREATE TABLE Epochs (
    EpochID INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT NOT NULL UNIQUE, 
    Metadata TEXT, 
    Notes TEXT);

CREATE TABLE VegetationTypes (
    VegetationID INTEGER PRIMARY KEY NOT NULL, 
    EpochID INTEGER REFERENCES Epochs (EpochID) NOT NULL, 
    Name TEXT NOT NULL, 
    LandUseID INTEGER REFERENCES LandUses (LandUseID), 
    Physiognomy TEXT, 
    Notes TEXT);

CREATE TABLE LandUses (
    LandUseID INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT UNIQUE NOT NULL, 
    Intensity REAL NOT NULL CONSTRAINT CHK_LandUses_Itensity CHECK (Intensity >= 0 AND Intensity <= 1) DEFAULT (0));

CREATE TABLE LandUseIntensities (
    IntensityID INTEGER PRIMARY KEY NOT NULL, 
    Name TEXT UNIQUE NOT NULL, 
    MaxIntensity REAL NOT NULL UNIQUE, 
    TargetCol TEXT UNIQUE NOT NULL);

CREATE TABLE IGOAttributes (
    LUI REAL NOT NULL
)

-- indexes

-- 
