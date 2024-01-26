import os
import sqlite3
from osgeo import ogr
from rscommons import ProgressBar, Logger, GeopackageLayer, get_shp_or_gpkg


def split_nhd_area(in_nhd_area: str, in_nhd_catchments: str, out_area_split: str):

    log = Logger('Split NHD Area')
    log.info('Splitting NHD Area by NHD Plus Catchments and attributing features with NHDPlusID')

    area_ids = {}
    area_feats = {}
    nhdplusids = {}

    with get_shp_or_gpkg(in_nhd_area) as lyr_area, \
            get_shp_or_gpkg(in_nhd_catchments) as lyr_catch:

        in_layer_def = lyr_area.ogr_layer_def
        spatial_ref = lyr_area.spatial_ref
        geom_type = lyr_area.ogr_geom_type

        for area_feat, *_ in lyr_area.iterate_features("NHD Flow Areas"):
            area_feat: ogr.Feature = area_feat
            area_geom: ogr.Geometry = area_feat.GetGeometryRef()
            area_id: int = area_feat.GetFID()
            if not area_geom.IsValid():
                area_geom = area_geom.MakeValid()
            area_feats[area_id] = area_feat
            area_outputs = {}
            for catch_feat, *_ in lyr_catch.iterate_features(clip_shape=area_geom):
                catch_feat: ogr.Feature = catch_feat
                catch_id: int = catch_feat.GetFID()
                catch_geom: ogr.Geometry = catch_feat.GetGeometryRef()
                if not catch_geom.IsValid():
                    catch_geom = catch_geom.MakeValid()
                if area_geom.Intersects(catch_geom):
                    out_geom: ogr.Geometry = area_geom.Intersection(catch_geom)
                    # make valid
                    if not out_geom.IsValid():
                        out_geom = out_geom.MakeValid()
                    if catch_id not in nhdplusids.keys():
                        nhdplusids[catch_id] = catch_feat.GetField('NHDPlusID')
                    area_outputs[catch_id] = out_geom
            area_ids[area_id] = area_outputs


    driver: ogr.Driver = ogr.GetDriverByName('ESRI Shapefile')

    outdatasrc: ogr.DataSource = driver.CreateDataSource(out_area_split)
    outlyr: ogr.Layer = outdatasrc.CreateLayer(os.path.basename(out_area_split), spatial_ref, geom_type=geom_type)

    for i in range(0, in_layer_def.GetFieldCount()):
        field_def: ogr.FieldDefn = in_layer_def.GetFieldDefn(i)
        field_name: str = field_def.GetName()
        fieldTypeCode = field_def.GetType()
        if field_name.lower() == 'nhdplusid' and fieldTypeCode == ogr.OFTReal:
            field_def.SetWidth(32)
            field_def.SetPrecision(0)
        outlyr.CreateField(field_def)

    outlyr_def: ogr.FeatureDefn = outlyr.GetLayerDefn()
    progbar = ProgressBar(len(area_ids), 50, "Adding features to output layer")
    counter = 0
    for area_id, outputs in area_ids.items():
        counter += 1
        progbar.update(counter)
        for out_catch_id, out_geom in outputs.items():
            for i in range(out_geom.GetGeometryCount()):
                g = out_geom.GetGeometryRef(i)
                geom = ogr.ForceToPolygon(g)
                out_feature: ogr.Feature = ogr.Feature(outlyr_def)
                for j in range(outlyr_def.GetFieldCount()):
                    field_def = outlyr_def.GetFieldDefn(j)
                    field_name = field_def.GetName()
                    if field_name.lower() != 'nhdplusid':
                        out_feature.SetField(outlyr_def.GetFieldDefn(j).GetNameRef(), area_feats[area_id].GetField(j))
                    else:
                        out_feature.SetField(field_name, nhdplusids[out_catch_id])

                out_feature.SetGeometry(geom)
                outlyr.CreateFeature(out_feature)
                out_feature = None

    progbar.finish()

    outdatasrc = None

    return out_area_split


def nhd_area_level_paths(in_nhdarea_split: str, in_nhd_vaa: str):

    log = Logger('NHD Area Level Paths')
    log.info('Finding NHD Area Level Paths')

    with GeopackageLayer(in_nhdarea_split, write=True) as lyr_area, \
            sqlite3.connect(in_nhd_vaa) as conn:
        
        curs = conn.cursor()
        lp_field_def = ogr.FieldDefn('level_path', ogr.OFTReal)
        lyr_area.ogr_layer.CreateField(lp_field_def)

        for area_feat, *_ in lyr_area.iterate_features("NHD Flow Areas"):
            nhdplusid = area_feat.GetField('NHDPlusID')
            lp = curs.execute(f"SELECT LevelPathI FROM NHDPlusFlowlineVAA WHERE NHDPlusID = {nhdplusid}").fetchone()[0]
            area_feat.SetField('level_path', lp)
            lyr_area.ogr_layer.SetFeature(area_feat)

    return 
