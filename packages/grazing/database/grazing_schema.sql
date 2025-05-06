CREATE TABLE VegetationTypes (
    VegetationID INTEGER PRIMARY KEY NOT NULL,
    Name TEXT NOT NULL,
    DefaultSuitability REAL CONSTRAINT CHECK (DefaultSuitability >= 0 AND DefaultSuitability <= 4) NOT NULL DEFAULT(0)
);

CREATE TABLE DGOVegetation (
    DGOID INTEGER REFERENCES DGOAttributes ON DELETE CASCADE NOT NULL,
    VegetationID INTEGER REFERENCES VegetationTypes (VegetationID) NOT NULL,
    Area REAL NOT NULL CONSTRAINT CHK_Area CHECK (Area > 0),
    CellCount REAL NOT NULL CONSTRAINT CHK_CellCount CHECK (CellCount > 0)
);

CREATE TABLE FCodeTypes (
    FCode INTEGER PRIMARY KEY NOT NULL,
    Name TEXT NOT NULL
);

CREATE TABLE DGOAttributes (
    DGOID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER,
    level_path TEXT,
    seg_distance REAL,
    centerline_length REAL,
    segment_area REAL,
    grazing_likelihood REAL

    CONSTRAINT FOREIGN KEY (FCode) REFERENCES FCodeTypes (FCode) ON DELETE SET NULL,
    CONSTRAINT FOREIGN KEY (DGOID) REFERENCES dgo (DGOID) ON DELETE CASCADE
);

CREATE TABLE IGOAttributes (
    IGOID INTEGER PRIMARY KEY NOT NULL,
    FCode INTEGER,
    level_path TEXT,
    seg_distance REAL,
    grazing_likelihood REAL,

    CONSTRAINT FOREIGN KEY (FCode) REFERENCES FCodeTypes (FCode) ON DELETE SET NULL,
    CONSTRAINT FOREIGN KEY (IGOID) REFERENCES igo (IGOID) ON DELETE CASCADE
);


CREATE VIEW grazing_dgos AS SELECT D.*, G.geom
FROM DGOAttributes D 
INNER JOIN dgo_geometry G ON D.DGOID = G.DGOID;

CREATE VIEW grazing_igos AS SELECT I.*, G.geom
FROM IGOAttributes I
INNER JOIN igo_geometry G ON I.IGOID = G.IGOID;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('VegetationTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOVegetation', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('FCodeTypes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('DGOAttributes', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('IGOAttributes', 'attributes');
