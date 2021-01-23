CREATE TABLE inputs (
    input_id    INTEGER PRIMARY KEY UNIQUE NOT NULL,
    name        TEXT UNIQUE NOT NULL,
    description TEXT,
    metadata    TEXT
);

CREATE TABLE transform_types (
    type_id     INTEGER PRIMARY KEY UNIQUE NOT NULL,
    name        TEXT UNIQUE NOT NULL,
    description TEXT,
    metadata    TEXT
);

CREATE TABLE transforms (
    transform_id    INTEGER PRIMARY KEY UNIQUE NOT NULL,
    type_id         INTEGER NOT NULL,
    input_id        INTEGER NOT NULL,
    name            TEXT UNIQUE NOT NULL,
    description     TEXT,
    metadata        TEXT,

    CONSTRAINT fk_transforms_type_id FOREIGN KEY (type_id) REFERENCES transform_types(type_id),
    CONSTRAINT fk_transforms_input_id FOREIGN KEY (input_id) REFERENCES inputs(input_id) ON DELETE CASCADE
);

CREATE TABLE inflections (
    inflection_id   INTEGER PRIMARY KEY UNIQUE NOT NULL,
    transform_id    INTEGER NOT NULL,
    input_value     REAL NOT NULL,
    output_value    REAL NOT NULL,

    CONSTRAINT fk_inflections_transform_id FOREIGN KEY (transform_id) REFERENCES transforms(transform_id) ON DELETE CASCADE,
    CONSTRAINT ck_inflections_output_value CHECK (output_value >= 0 AND output_value <= 1)
);

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('inputs', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('transform_types', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('transforms', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('inflections', 'attributes');
