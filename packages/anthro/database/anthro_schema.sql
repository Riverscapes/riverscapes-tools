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
    LUI REAL
    Road_len REAL
    Road_dens REAL
    Rail_len REAL
    Rail_dens REAL
    Canal_len REAL
    Canal_dens REAL
    Xing_ct INTEGER
    Xing_dens REAL

CREATE TABLE ReachAttributes
    iPC_Road REAL CONSTRAINT CHK_Reaches_RoadDist CHECK (iPC_Road >= 0)
    iPC_RoadX REAL CONSTRAINT CHK_Reaches_RoadCrossDists CHECK (iPC_RoadX >= 0)
    iPC_RoadVB REAL CONSTRAINT CHK_Reaches_RoadVBDist CHECK (iPC_RoadVB >= 0)
    iPC_Rail REAL CONSTRAINT CHK_Reaches_RailDist CHECK (iPC_Rail >= 0)
    iPC_RailVB REAL CONSTRAINT CHK_Reaches_RailVBDist CHECK (iPC_RailVB >= 0)
    iPC_LU REAL
    iPC_VLowLU REAL
    iPC_LowLU REAL
    iPC_ModLU REAL
    iPC_HighLU REAL
)

-- indexes


-- Non-spatial view of Anthro results with joins to the relevant tables
CREATE VIEW vwIGOAttributes AS 