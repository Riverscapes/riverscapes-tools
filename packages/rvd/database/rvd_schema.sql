CREATE TABLE Epochs (EpochID INTEGER PRIMARY KEY NOT NULL, Name TEXT NOT NULL UNIQUE, Metadata TEXT, Notes TEXT);
CREATE TABLE ReachCodes (ReachCode INTEGER PRIMARY KEY NOT NULL, Name TEXT NOT NULL, DisplayName TEXT, Description TEXT NOT NULL);
CREATE TABLE ReachVegetation (ReachID INTEGER REFERENCES Reaches ON DELETE CASCADE NOT NULL, VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL, Area REAL NOT NULL CONSTRAINT CHK_ReachVegetation_Area CHECK (Area > 0), CellCount REAL NOT NULL CONSTRAINT CHK_ReachVegetation_CellCount CHECK (CellCount > 0), PRIMARY KEY (ReachID, VegetationID));
CREATE TABLE MetaData (KeyInfo TEXT PRIMARY KEY NOT NULL, ValueInfo TEXT);
CREATE TABLE Watersheds (WatershedID TEXT PRIMARY KEY NOT NULL UNIQUE, Name TEXT NOT NULL, AreaSqKm REAL CONSTRAINT CHK_HUCs_Area CHECK (AreaSqKm >= 0), States TEXT, Metadata TEXT, Notes TEXT);
CREATE TABLE VegetationTypes (VegetationID INTEGER PRIMARY KEY NOT NULL, EpochID INTEGER REFERENCES Epochs (EpochID) NOT NULL, Name TEXT NOT NULL, Physiognomy TEXT, Notes TEXT);

CREATE INDEX FK_ReachVegetation_ReachID ON ReachVegetation (ReachID);
CREATE INDEX FK_ReachVegetation_VegetationID ON ReachVegetation (VegetationID);
CREATE INDEX IX_Watersheds_States ON Watersheds (States);
CREATE INDEX FK_VegetationTypes_EpochID ON VegetationTypes (EpochID);
CREATE INDEX FX_Reaches_FCode ON Reaches (ReachCode);
CREATE INDEX FX_Reaches_HUC ON Reaches (WatershedID);