CREATE TABLE VegetationTypes (
    VegetationID INTEGER PRIMARY KEY NOT NULL,
    Name TEXT NOT NULL,
    EVT_PHYS TEXT,
    DefaultSuitability REAL CONSTRAINT CHK_VegetationTypes_DefaultSuitability CHECK (DefaultSuitability >= 0 AND DefaultSuitability <= 4) NOT NULL DEFAULT(0)
);

CREATE TABLE VegetationOverrides (
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) ON DELETE CASCADE,
    OverrideSuitability REAL CONSTRAINT CHK_VegetationOverrides_Suitability CHECK (OverrideSuitability >= 0 AND OverrideSuitability <= 4) NOT NULL
);

CREATE TABLE FCodeTypes (
    FCode INTEGER PRIMARY KEY NOT NULL,
    Name TEXT NOT NULL,
    DisplayName TEXT,
    Description TEXT
);

CREATE TABLE DGOAttributes (
    DGOID INTEGER PRIMARY KEY NOT NULL REFERENCES dgo_geometry (DGOID) ON DELETE CASCADE,
    FCode INTEGER REFERENCES FCodeTypes (FCode) ON DELETE SET NULL,
    level_path TEXT,
    seg_distance REAL,
    centerline_length REAL,
    segment_area REAL,
    grazing_likelihood REAL
);

CREATE TABLE IGOAttributes (
    IGOID INTEGER PRIMARY KEY NOT NULL REFERENCES igo_geometry (IGOID) ON DELETE CASCADE,
    FCode INTEGER REFERENCES FCodeTypes (FCode) ON DELETE SET NULL,
    level_path TEXT,
    seg_distance REAL,
    stream_size INT,
    grazing_likelihood REAL
);

CREATE TABLE MetaData (
     KeyInfo TEXT PRIMARY KEY NOT NULL, 
     ValueInfo TEXT);


CREATE VIEW grazing_dgos AS SELECT D.*, G.geom
FROM DGOAttributes D 
INNER JOIN dgo_geometry G ON D.DGOID = G.DGOID;

CREATE VIEW grazing_igos AS SELECT I.*, G.geom
FROM IGOAttributes I
INNER JOIN igo_geometry G ON I.IGOID = G.IGOID;

CREATE VIEW vwVegetationSuitability AS SELECT VT.VegetationID,
       VT.Name,
       DefaultSuitability,
       OverrideSuitability,
       IFNULL(OverrideSuitability, DefaultSuitability) EffectiveSuitability
  FROM VegetationTypes VT LEFT JOIN VegetationOverrides OV ON VT.VegetationID = OV.VegetationID;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationOverrides', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('MetaData', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('FCodeTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('IGOAttributes', 'attributes');
