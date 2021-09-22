CREATE VIEW vbet_full_metrics AS
    SELECT  v.LevelPathI,
        v.valley_bottom_ha,
        ia.active_valley_bottom_ha,
        v.active_channel_ha,
        af.active_floodplain_ha,
        iaf.inactive_floodplain_ha,
        (ia.active_valley_bottom_ha/v.valley_bottom_ha) * 100 AS active_valley_bottom_pc,
        (v.active_channel_ha/v.valley_bottom_ha) * 100 AS active_channel_pc,
        (af.active_floodplain_ha/v.valley_bottom_ha) * 100 AS active_floodplain_pc,
        (iaf.inactive_floodplain_ha/v.valley_bottom_ha) * 100 AS inactive_floodplain_pc,
        af.cnt_active_fp_units,
        iaf.cnt_inactive_fp_units
    FROM (SELECT LevelPathI AS LevelPathI, SUM(area_ha) AS valley_bottom_ha, SUM(active_channel_ha) AS active_channel_ha FROM vbet_full GROUP BY LevelPathI) AS v
        LEFT JOIN (SELECT LevelPathI, SUM(area_ha) AS active_valley_bottom_ha FROM vbet_ia GROUP BY LevelPathI) AS ia ON v.LevelPathI=ia.LevelPathI
        LEFT JOIN (SELECT LevelPathI, SUM(area_ha) AS active_floodplain_ha, COUNT(LevelPathI) AS cnt_active_fp_units FROM active_floodplain GROUP BY LevelPathI) AS af ON v.LevelPathI=af.LevelPathI
        LEFT JOIN (SELECT LevelPathI, SUM(area_ha) AS inactive_floodplain_ha, COUNT(LevelPathI) AS cnt_inactive_fp_units FROM inactive_floodplain GROUP BY LevelPathI) AS iaf ON v.LevelPathI=iaf.LevelPathI;

CREATE VIEW active_floodplain_metrics AS
    SELECT active_floodplain.fid,
	   active_floodplain.LevelPathI, 
	   (area_ha/vbet_full_metrics.valley_bottom_ha) * 100 AS prop_valley_bottom_pc,
	   (area_ha/feats.level_path_area_sum) * 100 AS prop_active_pc
    FROM active_floodplain 
    LEFT JOIN vbet_full_metrics ON active_floodplain.LevelPathI=vbet_full_metrics.LevelPathI
    LEFT JOIN (SELECT LevelPathI, SUM(area_ha) AS level_path_area_sum FROM active_floodplain GROUP BY LevelPathI) AS feats ON active_floodplain.LevelPathI=feats.LevelPathI;

CREATE VIEW inactive_floodplain_metrics AS
    SELECT inactive_floodplain.fid,
	   inactive_floodplain.LevelPathI, 
	   (area_ha/vbet_full_metrics.valley_bottom_ha) * 100 AS prop_valley_bottom_pc,
	   (area_ha/feats.level_path_area_sum) * 100 AS prop_inactive_pc
    FROM inactive_floodplain 
    Left JOIN vbet_full_metrics ON inactive_floodplain.LevelPathI=vbet_full_metrics.LevelPathI
    LEFT JOIN (SELECT LevelPathI, SUM(area_ha) AS level_path_area_sum FROM inactive_floodplain GROUP BY LevelPathI) AS feats ON inactive_floodplain.LevelPathI=feats.LevelPathI;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vbet_full_metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('active_floodplain_metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('inactive_floodplain_metrics', 'attributes');


--ALTER TABLE active_floodplain
--ADD prop_valley_bottom_pc REAL;
ALTER TABLE active_floodplain
ADD prop_active_pc REAL;
