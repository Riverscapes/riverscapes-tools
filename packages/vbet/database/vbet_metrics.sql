CREATE VIEW vbet_full_metrics AS
    SELECT  v.vbet_level_path AS vbet_level_path,
        ROUND(v.valley_bottom_ha, 5) AS valley_bottom_ha,
        ROUND(ia.active_valley_bottom_ha,5) AS active_valley_bottom_ha,
        ROUND(v.active_channel_ha, 5) AS active_channel_ha,
        ROUND(af.active_floodplain_ha, 5) AS active_floodplain_ha,
        ROUND(iaf.inactive_floodplain_ha, 5) AS inactive_floodplain_ha,
        ROUND((ia.active_valley_bottom_ha/v.valley_bottom_ha) * 100, 2) AS active_valley_bottom_pc,
        ROUND((v.active_channel_ha/v.valley_bottom_ha) * 100, 2) AS active_channel_pc,
        ROUND((af.active_floodplain_ha/v.valley_bottom_ha) * 100, 2) AS active_floodplain_pc,
        ROUND((iaf.inactive_floodplain_ha/v.valley_bottom_ha) * 100, 2) AS inactive_floodplain_pc,
        af.cnt_active_fp_units,
        iaf.cnt_inactive_fp_units
    FROM (SELECT vbet_level_path AS vbet_level_path, SUM(area_ha) AS valley_bottom_ha, SUM(active_channel_ha) AS active_channel_ha FROM vbet_full GROUP BY vbet_level_path) AS v
        LEFT JOIN (SELECT vbet_level_path, SUM(area_ha) AS active_valley_bottom_ha FROM vbet_ia GROUP BY vbet_level_path) AS ia ON v.vbet_level_path=ia.vbet_level_path
        LEFT JOIN (SELECT vbet_level_path, SUM(area_ha) AS active_floodplain_ha, COUNT(vbet_level_path) AS cnt_active_fp_units FROM active_floodplain GROUP BY vbet_level_path) AS af ON v.vbet_level_path=af.vbet_level_path
        LEFT JOIN (SELECT vbet_level_path, SUM(area_ha) AS inactive_floodplain_ha, COUNT(vbet_level_path) AS cnt_inactive_fp_units FROM inactive_floodplain GROUP BY vbet_level_path) AS iaf ON v.vbet_level_path=iaf.vbet_level_path;

CREATE VIEW active_floodplain_metrics AS
    SELECT active_floodplain.fid,
	   active_floodplain.vbet_level_path, 
	   ROUND((area_ha/vbet_full_metrics.valley_bottom_ha) * 100, 2) AS prop_valley_bottom_pc,
	   ROUND((area_ha/feats.level_path_area_sum) * 100,2) AS prop_active_pc
    FROM active_floodplain 
    LEFT JOIN vbet_full_metrics ON active_floodplain.vbet_level_path=vbet_full_metrics.vbet_level_path
    LEFT JOIN (SELECT vbet_level_path, SUM(area_ha) AS level_path_area_sum FROM active_floodplain GROUP BY vbet_level_path) AS feats ON active_floodplain.vbet_level_path=feats.vbet_level_path;

CREATE VIEW inactive_floodplain_metrics AS
    SELECT inactive_floodplain.fid,
	   inactive_floodplain.vbet_level_path, 
	   ROUND((area_ha/vbet_full_metrics.valley_bottom_ha) * 100, 2) AS prop_valley_bottom_pc,
	   ROUND((area_ha/feats.level_path_area_sum) * 100, 2) AS prop_inactive_pc
    FROM inactive_floodplain 
    Left JOIN vbet_full_metrics ON inactive_floodplain.vbet_level_path=vbet_full_metrics.vbet_level_path
    LEFT JOIN (SELECT vbet_level_path, SUM(area_ha) AS level_path_area_sum FROM inactive_floodplain GROUP BY vbet_level_path) AS feats ON inactive_floodplain.vbet_level_path=feats.vbet_level_path;
    
CREATE VIEW vbet_channel_area_metrics AS
    SELECT vbet_channel_area.fid,
	   vbet_channel_area.vbet_level_path, 
	   ROUND((area_ha/vbet_full_metrics.valley_bottom_ha) * 100, 2) AS prop_valley_bottom_pc,
	   ROUND((area_ha/feats.level_path_area_sum) * 100, 2) AS prop_channel_pc
    FROM vbet_channel_area 
    Left JOIN vbet_full_metrics ON vbet_channel_area.vbet_level_path=vbet_full_metrics.vbet_level_path
    LEFT JOIN (SELECT vbet_level_path, SUM(area_ha) AS level_path_area_sum FROM vbet_channel_area GROUP BY vbet_level_path) AS feats ON vbet_channel_area.vbet_level_path=feats.vbet_level_path;

INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vbet_full_metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('active_floodplain_metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('inactive_floodplain_metrics', 'attributes');
INSERT INTO gpkg_contents (table_name, data_type) VALUES ('vbet_channel_area_metrics', 'attributes');

CREATE index fx_vbet_full_vbet_level_path on vbet_full(vbet_level_path);
CREATE index fx_vbet_active_floodplain_fid on active_floodplain(fid);
CREATE index fx_vbet_inactive_floodplain_fid on inactive_floodplain(fid);
CREATE index fx_vbet_channel_area_fid on vbet_channel_area(fid);
