import os
from osgeo import ogr

from rscommons import initGDALOGRErrors, ModelConfig, Logger, RSLayer, RSProject, RSMeta, RSMetaTypes, get_shp_or_gpkg, GeopackageLayer
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions

from beaver_sign.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'lyr_descriptions.json')

LayerTypes = {
    'BEAVER_ACTIVITY': RSLayer('Beaver Activity', 'BeaverActivity', 'Geopackage', 'beaver_activity.gpkg', {
        'DAMS': RSLayer('Dams', 'Dams', 'Vector', 'dams'),
        # 'SIGN': RSLayer('Sign', 'Sign', 'Vector', 'sign')
    })
}


def beaver_activity(huc, proj_boundary, beaver_dams, beaver_sign, output_dir):

    log = Logger('Beaver Activity')

    augment_layermeta('beaver_activity', LYR_DESCRIPTIONS_JSON, LayerTypes)

    project_name = f'Beaver Activity for HUC {huc}'
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'beaver_activity', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/beaver_activity', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ])

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['BEAVER_ACTIVITY'])

    output_gpkg_path = os.path.join(output_dir, LayerTypes['BEAVER_ACTIVITY'].rel_path)

    log.info('Setting up output feature classes')
    with GeopackageLayer(output_gpkg_path, LayerTypes['BEAVER_ACTIVITY'].sub_layers['DAMS'].rel_path, delete_dataset=True) as out_lyr, \
            get_shp_or_gpkg(beaver_dams) as dams, get_shp_or_gpkg(proj_boundary) as boundary:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, fields={
            'dam_cer': ogr.OFTString,
            'dam_type': ogr.OFTString,
            'type_cer': ogr.OFTString
        })
        boundary_ftr = boundary.ogr_layer.GetNextFeature().GetGeometryRef()
        for ftr, *_ in dams.iterate_features(clip_shape=boundary_ftr):
            if ftr.GetGeometryRef().Intersects(boundary_ftr):
                dam_cer = ftr.GetField('dam_certai')  # are these always the same or should it be a list param
                dam_type = ftr.GetField('feature_ty')
                type_cer = ftr.GetField('feature__1')
                new_ftr = ogr.Feature(out_lyr.ogr_layer.GetLayerDefn())
                new_ftr.SetGeometry(ftr.GetGeometryRef())
                new_ftr.SetField('dam_cer', dam_cer)
                new_ftr.SetField('dam_type', dam_type)
                new_ftr.SetField('type_cer', type_cer)
                out_lyr.ogr_layer.CreateFeature(new_ftr)
                new_ftr = None

    if beaver_sign:
        with GeopackageLayer(output_gpkg_path, LayerTypes['BEAVER_ACTIVITY'].sub_layers['SIGN'].rel_path, delete_dataset=True) as out_lyr, \
                get_shp_or_gpkg(beaver_sign) as sign, get_shp_or_gpkg(proj_boundary) as boundary:
            out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, fields={
                'type': ogr.OFTString
            })
            boundary_ftr = boundary.ogr_layer.GetNextFeature().GetGeometryRef()
            for ftr, *_ in sign.iterate_features(clip_shape=boundary_ftr):
                if ftr.GetGeometryRef().Intersects(boundary_ftr):
                    dam_cer = ftr.GetField('sign_type')  # are these always the same or should it be a list param
                    new_ftr = ogr.Feature(out_lyr.ogr_layer.GetLayerDefn())
                    new_ftr.SetGeometry(ftr.GetGeometryRef())
                    new_ftr.SetField('type', dam_cer)
                    out_lyr.ogr_layer.CreateFeature(new_ftr)
                    new_ftr = None
        project.add_project_vector(proj_nodes['BEAVER_ACTIVITY'], RSLayer('Sign', 'Sign', 'Vector', 'sign'))
