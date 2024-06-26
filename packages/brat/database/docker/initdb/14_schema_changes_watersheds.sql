CREATE EXTENSION if not exists postgis;

-- Drop the existing text "fake" geometry column
ALTER TABLE watersheds
    DROP COLUMN geometry;

-- Create and index the watershed geometry column
ALTER TABLE watersheds
    ADD COLUMN geom GEOMETRY(MultiPolygon, 4326);
CREATE INDEX gx_watersheds ON watersheds USING GIST (geom);

CREATE TABLE hydro_regions
(
    hydro_region_id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    grid_name       varchar(12),
    state_reg       varchar(12),
    name            varchar(254),
    grid_code       varchar(254),
    other           varchar(12),
    q2              varchar(254),
    qlow            varchar(254),
    id              int,
    geom            GEOMETRY(MultiPolygon, 4326)
);
CREATE INDEX gx_hydro_regions ON hydro_regions USING GIST (geom);
CREATE INDEX ix_hydro_regions_name ON hydro_regions (name);

CREATE TABLE equations
(
    equation_id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    equation_type varchar(255),
    equation text,
    region_id int,
    region_name varchar(255),
    datasource_id int
);
CREATE INDEX ix_equations_region_name ON equations(region_name);
