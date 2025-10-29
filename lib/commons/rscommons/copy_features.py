import os
from osgeo import ogr
from rsxml import Logger
from rscommons import get_shp_or_gpkg, VectorBase


def copy_features_fields(in_path: str, out_path: str, attribute_filter=None, epsg=None, clip_shape=None):
    """Used to copy features within a feature class with only fields specified in an output field definition
    """

    log = Logger('Copy Features')
    log.info(f'Copying features from {os.path.basename(in_path)} with specified attributes into project')

    with get_shp_or_gpkg(in_path) as in_lyr, get_shp_or_gpkg(out_path, write=True) as out_lyr:
        if epsg is not None:
            out_spatial_ref, transform = VectorBase.get_transform_from_epsg(in_lyr.spatial_ref, epsg)
        else:
            transform = None

        out_lyr.ogr_layer.StartTransaction()
        for feature, _counter, _progbar in in_lyr.iterate_features("Processing points", attribute_filter=attribute_filter, clip_shape=clip_shape):
            geom = feature.GetGeometryRef()
            if transform is not None:
                geom.Transform(transform)

            out_feature = ogr.Feature(out_lyr.ogr_layer_def)

            for i in range(0, out_lyr.ogr_layer_def.GetFieldCount()):
                field_name = out_lyr.ogr_layer_def.GetFieldDefn(i).GetNameRef()
                output_field_index = feature.GetFieldIndex(field_name)
                if output_field_index >= 0:
                    out_feature.SetField(field_name, feature.GetField(output_field_index))

            out_feature.SetGeometry(geom)
            out_lyr.ogr_layer.CreateFeature(out_feature)
        out_lyr.ogr_layer.CommitTransaction()
