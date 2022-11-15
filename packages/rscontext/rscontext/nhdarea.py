import os
from osgeo import ogr
from rscommons import ProgressBar, Logger


def split_nhd_area(in_nhd_area: str, in_nhd_catchments: str, out_area_split: str):

    log = Logger('Split NHD Area')
    log.info('Splitting NHD Area by NHD Plus Catchments and attributing features with NHDPlusID')

    driver = ogr.GetDriverByName('ESRI Shapefile')

    catch_ftrs = {}
    catch_geoms = {}
    outdfns = {}
    outids = {}

    fa_data_src = driver.Open(in_nhd_area)
    fa_lyr = fa_data_src.GetLayer()
    fa_ftrs = [f for f in fa_lyr]

    catch_data_src = driver.Open(in_nhd_catchments)
    catch_lyr = catch_data_src.GetLayer()
    for ftr in catch_lyr:
        catch_ftrs[ftr.GetField('NHDPlusID')] = ftr
        catch_geoms[ftr.GetField('NHDPlusID')] = ftr.GetGeometryRef()

    for id, geom in catch_geoms.items():
        for ftr in fa_ftrs:
            if geom.Intersects(ftr.GetGeometryRef()):
                outids[id] = geom.Intersection(ftr.GetGeometryRef())
                outdfns[id] = ftr

    outdatasrc = driver.CreateDataSource(out_area_split)
    outlyr = outdatasrc.CreateLayer(os.path.basename(out_area_split), fa_lyr.GetSpatialRef(), geom_type=fa_lyr.GetGeomType())

    in_layer_def = fa_lyr.GetLayerDefn()
    for i in range(0, in_layer_def.GetFieldCount()):
        field_def = in_layer_def.GetFieldDefn(i)
        field_name = field_def.GetName()
        fieldTypeCode = field_def.GetType()
        if field_name.lower() == 'nhdplusid' and fieldTypeCode == ogr.OFTReal:
            field_def.SetWidth(32)
            field_def.SetPrecision(0)
        outlyr.CreateField(field_def)

    outlyr_def = outlyr.GetLayerDefn()
    progbar = ProgressBar(len(outids), 50, "Adding features to output layer")
    counter = 0
    for id, feature in outids.items():
        counter += 1
        progbar.update(counter)
        out_feature = ogr.Feature(outlyr_def)
        geom = ogr.CreateGeometryFromWkt(feature.ExportToWkt())

        for i in range(0, outlyr_def.GetFieldCount()):
            field_def = outlyr_def.GetFieldDefn(i)
            field_name = field_def.GetName()
            if field_name.lower() != 'nhdplusid':
                out_feature.SetField(outlyr_def.GetFieldDefn(i).GetNameRef(), outdfns[id].GetField(i))
            else:
                out_feature.SetField(field_name, id)

        out_feature.SetGeometry(geom)
        outlyr.CreateFeature(out_feature)
        out_feature = None

    progbar.finish()

    fa_data_src = None
    catch_data_src = None
    outdatasrc = None

    return out_area_split
