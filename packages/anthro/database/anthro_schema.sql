CREATE TABLE Agencies (
    AgencyID INTEGER PRIMARY KEY NOT NULL UNIQUE, 
    Name TEXT NOT NULL UNIQUE, 
    Abbreviation TEXT NOT NULL UNIQUE);

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
    IGOID INTEGER PRIMARY KEY NOT NULL,
    LUI REAL,
    Road_len REAL,
    Road_dens REAL,
    Rail_len REAL,
    Rail_dens REAL,
    Canal_len REAL,
    Canal_dens REAL,
    Xing_ct INTEGER,
    Xing_dens REAL);

CREATE TABLE ReachAttributes (
    ReachID INTEGER PRIMARY KEY NOT NULL,
    iPC_Road REAL CONSTRAINT CHK_Reaches_RoadDist CHECK (iPC_Road >= 0),
    iPC_RoadX REAL CONSTRAINT CHK_Reaches_RoadCrossDists CHECK (iPC_RoadX >= 0),
    iPC_RoadVB REAL CONSTRAINT CHK_Reaches_RoadVBDist CHECK (iPC_RoadVB >= 0),
    iPC_Rail REAL CONSTRAINT CHK_Reaches_RailDist CHECK (iPC_Rail >= 0),
    iPC_RailVB REAL CONSTRAINT CHK_Reaches_RailVBDist CHECK (iPC_RailVB >= 0),
    iPC_DivPts REAL CONSTRAINT CHK_Reaches_DivPtsDist CHECK (iPC_DivPts >= 0),
    iPC_Privat REAL CONSTRAINT CHK_Reaches_PrivatDist CHECK (iPC_Privat >= 0),
    iPC_LU REAL,
    iPC_VLowLU REAL,
    iPC_LowLU REAL,
    iPC_ModLU REAL,
    iPC_HighLU REAL,
    AgencyID INTEGER REFERENCES Agencies (AgencyID));

-- indexes


-- Non-spatial view of Anthro results with joins to the relevant tables
CREATE VIEW vwReachAttributes AS
SELECT R.*,
       A.Name Agency,
FROM ReachAttributes R
        INNER JOIN Agencies A ON R.AgencyID = A.AgencyID;

-- The main views 
CREATE VIEW vwReaches AS SELECT R.*, G.geom
FROM vwReachAttributes R
        INNER JOIN anthro_lines_geom G ON R.ReachID = G.ReachID;

CREATE VIEW vwIgos AS SELECT I.*, G.geom
FROM IGOAttributes I
        INNER JOIN anthro_igo_geom G ON I.IGOID = G.IGOID