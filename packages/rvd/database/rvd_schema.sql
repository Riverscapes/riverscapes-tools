-- The ReachGeometry table is created by OGR during RVD code execution
CREATE INDEX FX_ReachGeometry_FCode ON ReachGeometry (ReachCode);
CREATE INDEX FX_ReachGeometry_HUC ON ReachGeometry (WatershedID);

CREATE TABLE Epochs
(
    EpochID  INTEGER PRIMARY KEY NOT NULL,
    Name     TEXT                NOT NULL UNIQUE,
    Metadata TEXT,
    Notes    TEXT
);

CREATE TABLE ReachCodes
(
    ReachCode   INTEGER PRIMARY KEY NOT NULL,
    Name        TEXT                NOT NULL,
    DisplayName TEXT,
    Description TEXT                NOT NULL
);

CREATE TABLE ReachVegetation
(
    ReachID      INTEGER NOT NULL,
    VegetationID INTEGER NOT NULL,
    Area         REAL    NOT NULL,
    CellCount    REAL    NOT NULL,

    CONSTRAINT pk_ReachVegetation PRIMARY KEY (ReachID, VegetationID),
    CONSTRAINT fk_ReachVegetation_ReachID FOREIGN KEY (ReachID) REFERENCES ReachGeometry (ReachID) ON DELETE CASCADE,
    CONSTRAINT fk_ReachVegetation_VegetationID FOREIGN KEY (VegetationID) REFERENCES VegetationTypes (VegetationID),
    CONSTRAINT ck_ReachVegetation_Area CHECK (Area > 0),
    CONSTRAINT ck_ReachVegetation_CellCount CHECK (CellCount > 0)
);
CREATE INDEX FK_ReachVegetation_ReachID ON ReachVegetation (ReachID);
CREATE INDEX FK_ReachVegetation_VegetationID ON ReachVegetation (VegetationID);


CREATE TABLE MetaData
(
    KeyInfo   TEXT PRIMARY KEY NOT NULL,
    ValueInfo TEXT
);

CREATE TABLE Watersheds
(
    WatershedID TEXT PRIMARY KEY NOT NULL UNIQUE,
    Name        TEXT             NOT NULL,
    AreaSqKm    REAL,
    States      TEXT,
    Metadata    TEXT,
    Notes       TEXT,

    CONSTRAINT ck_Watersheds_Area CHECK (AreaSqKm >= 0)
);
CREATE INDEX IX_Watersheds_States ON Watersheds (States);

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
CREATE INDEX FK_VegetationTypes_EpochID ON VegetationTypes (EpochID);

CREATE TABLE ConversionTypes
(
    TypeID      INTEGER PRIMARY KEY,
    TypeValue   INTEGER NOT NULL,
    Name        TEXT UNIQUE NOT NULL,
    FieldName   TEXT UNIQUE,
    Description TEXT
);

CREATE TABLE ConversionLevels
(
    LevelID     INTEGER PRIMARY KEY,
    Name        TEXT UNIQUE NOT NULL,
    Description TEXT,
    MaxValue    REAL
);

CREATE TABLE Conversions
(
    ConversionID INTEGER PRIMARY KEY,
    TypeID       INTEGER NOT NULL,
    LevelID      INTEGER NOT NULL,
    DisplayCode  INTEGER NOT NULL,
    DisplayText  TEXT,

    CONSTRAINT fk_ConversionsTypeID FOREIGN KEY (TypeID) REFERENCES ConversionTypes (TypeID),
    CONSTRAINT fk_ConversionsLevelID FOREIGN KEY (LevelID) REFERENCES ConversionLevels (LevelID)
);
CREATE UNIQUE INDEX ux_Conversions_TypeID_Level_ID ON Conversions(TypeID, LevelID);

CREATE TABLE DepartureLevels
(
    LevelID     INTEGER PRIMARY KEY,
    Name        TEXT UNIQUE NOT NULL,
    MaxRVD      REAL,
    Description TEXT
);

CREATE TABLE ReachAttributes
(
    ReachID                    INTEGER PRIMARY KEY NOT NULL,
    FromConifer                REAL,
    FromDevegetated            REAL,
    FromGrassShrubland         REAL,
    FromDeciduous              REAL,
    NoChange                   REAL,
    Deciduous                  REAL,
    GrassShrubland             REAL,
    Devegetation               REAL,
    Conifer                    REAL,
    Invasive                   REAL,
    Development                REAL,
    Agriculture                REAL,
    RiparianTotal              REAL,
    ConversionID               INTEGER,
    ExistingRiparianMean       REAL,
    HistoricRiparianMean       REAL,
    RiparianDeparture          REAL,
    RiparianDepartureID        INTEGER,
    ExistingNativeRiparianMean REAL,
    HistoricNativeRiparianMean REAL,
    NativeRiparianDeparture    REAL,

    CONSTRAINT fk_ReachAttributes_ReachID FOREIGN KEY (ReachID) REFERENCES ReachGeometry (ReachID) ON DELETE CASCADE,
    CONSTRAINT fk_ReachAttributes_RiparianDepartureID FOREIGN KEY (RiparianDepartureID) REFERENCES DepartureLevels (LevelID),
    CONSTRAINT fk_ReachAttributes_ConversionID FOREIGN KEY (ConversionID) REFERENCES Conversions (ConversionID)
);
CREATE INDEX fx_ReachAttributes_RiparianDepartureID ON ReachAttributes (RiparianDepartureID);
CREATE INDEX fx_ReachAttributes_ConversionID ON ReachAttributes (ConversionID);

CREATE VIEW vwConversions AS
SELECT ConversionID,
        DisplayCode,
       T.TypeID,
       T.TypeValue, 
       T.Name AS ConversionTypeDisplay,
       T.FieldName As ConversionType,
       L.LevelID,
       L.Name AS ConversionLevel
FROM Conversions C
         INNER JOIN ConversionTypes T ON C.TypeID = T.TypeID
         INNER JOIN ConversionLevels L ON C.LevelID = L.LevelID
ORDER BY ConversionID ASC;

CREATE VIEW vwReaches AS
SELECT G.ReachID, G.Geom, G.ReachCode, A.*, C.DisplayCode, C.ConversionLevel || ' ' || C.ConversionType ConversionLabel
FROM ReachAttributes A
         INNER JOIN ReachGeometry G ON A.ReachID = G.ReachID
         LEFT JOIN DepartureLevels D ON A.RiparianDepartureID = D.LevelID
         LEFT JOIN
     (SELECT C.ConversionID,
             DisplayCode,
             T.TypeID,
             T.Name AS ConversionType,
             L.LevelID,
             L.Name AS ConversionLevel
      FROM Conversions C
               INNER JOIN ConversionTypes T ON C.TypeID = T.TypeID
               INNER JOIN ConversionLevels L ON C.LevelID = L.LevelID
     ) C
     ON A.ConversionID = C.ConversionID;

INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('Epochs', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('ReachCodes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('ReachVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('MetaData', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('Watersheds', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('ConversionTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('ConversionLevels', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('Conversions', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('ReachAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('DepartureLevels', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type)
VALUES ('vwReaches', 'attributes');
