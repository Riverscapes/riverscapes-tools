CREATE TABLE metrics (
    metric_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT,
    machine_code TEXT,
    description TEXT,
    method TEXT,
    small REAL,
    medium REAL,
    large REAL,
    metric_group_id INTEGER,
    is_active BOOLEAN,
    docs_url TEXT
);

CREATE TABLE metric_values (
    point_id INTEGER NOT NULL,
    metric_id INTEGER NOT NULL,
    metric_value REAL,
    metadata TEXT,
    qaqc_date TEXT,
    PRIMARY KEY (point_id, metric_id)

    CONSTRAINT fk_point_id FOREIGN KEY (point_id) REFERENCES points (fid),
    CONSTRAINT fk_metric_id FOREIGN KEY (metric_id) REFERENCES metrics (metric_id)
);

CREATE VIEW vw_point_metrics AS 
    SELECT 
        G.fid AS fid, 
        G.geom AS geom,
        G.LevelPathI as level_path,
        G.seg_distance as seg_distance,
        G.stream_size as stream_size,
        CAST((CASE WHEN M.metric_id == 1 THEN M.metric_value END) AS REAL) AS stream_gradient,
        CAST((CASE WHEN M.metric_id == 2 THEN M.metric_value END) AS REAL) AS valley_gradient
    FROM metric_values M
    INNER JOIN points G ON M.point_id = G.fid;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metric_values', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('metrics', 'attributes');
-- INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vw_point_metrics', 'attributes');
