import argparse
import os
import sys
import traceback
from osgeo import ogr

from rscommons import initGDALOGRErrors, ModelConfig, Logger, RSLayer, RSProject, RSMeta, RSMetaTypes, get_shp_or_gpkg, GeopackageLayer, dotenv
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions
from rscommons.vector_ops import copy_feature_class
from rscommons.database import SQLiteCon
from rscommons.moving_window import moving_window_dgo_ids

from beaver_sign.utils.dam_counts_to_dgos import dam_counts_to_dgos
from beaver_sign.utils.riverscape_counts import riverscapes_dam_counts

from beaver_sign.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'lyr_descriptions.json')

LayerTypes = {
    'BEAVER_ACTIVITY': RSLayer('Beaver Activity', 'BeaverActivity', 'Geopackage', 'beaver_activity.gpkg', {
        'DGOS': RSLayer('Discrete Geographic Objects', 'DGOS', 'Vector', 'dgos'),
        'IGOS': RSLayer('Integrated Geographic Objects', 'IGOS', 'Vector', 'igos'),
        'DAMS': RSLayer('Dams', 'Dams', 'Vector', 'dams'),
        # 'SIGN': RSLayer('Sign', 'Sign', 'Vector', 'sign')
    })
}


def beaver_activity(huc, proj_boundary, dgos, igos, beaver_dams, output_dir, beaver_sign=None):

    log = Logger('Beaver Activity')

    augment_layermeta('beaver_activity', LYR_DESCRIPTIONS_JSON, LayerTypes)

    project_name = f'Beaver Activity for HUC {huc}'
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'beaver_activity', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/beaver_activity', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True)
    ])

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Outputs'])

    output_gpkg_path = os.path.join(output_dir, LayerTypes['BEAVER_ACTIVITY'].rel_path)

    log.info('Setting up output feature classes')
    with GeopackageLayer(output_gpkg_path, LayerTypes['BEAVER_ACTIVITY'].sub_layers['DAMS'].rel_path, delete_dataset=True) as out_lyr, \
            get_shp_or_gpkg(beaver_dams) as dams, get_shp_or_gpkg(proj_boundary) as boundary:
        out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, fields={
            'dam_cer': ogr.OFTString,
            'dam_type': ogr.OFTString,
            'type_cer': ogr.OFTString
        })
        boundary_ftr = boundary.ogr_layer.GetNextFeature()
        bbox = boundary_ftr.GetGeometryRef().GetEnvelope()
        for ftr, *_ in dams.iterate_features(clip_rect=bbox):
            if ftr.GetGeometryRef().Intersects(boundary_ftr.GetGeometryRef()):
                dam_cer = ftr.GetField('dam_cer')  # are these always the same or should it be a list param
                dam_type = ftr.GetField('type')
                type_cer = ftr.GetField('type_cer')
                new_ftr = ogr.Feature(out_lyr.ogr_layer.GetLayerDefn())
                new_ftr.SetGeometry(ftr.GetGeometryRef())
                new_ftr.SetField('dam_cer', dam_cer)
                new_ftr.SetField('dam_type', dam_type)
                new_ftr.SetField('type_cer', type_cer)
                out_lyr.ogr_layer.CreateFeature(new_ftr)
                new_ftr = None

    out_gpkg_node, *_ = project.add_project_geopackage(proj_nodes['Outputs'], LayerTypes['BEAVER_ACTIVITY'])

    if beaver_sign:
        with GeopackageLayer(output_gpkg_path, LayerTypes['BEAVER_ACTIVITY'].sub_layers['SIGN'].rel_path, delete_dataset=True) as out_lyr, \
                get_shp_or_gpkg(beaver_sign) as sign, get_shp_or_gpkg(proj_boundary) as boundary:
            out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, fields={
                'type': ogr.OFTString
            })
            boundary_ftr = boundary.ogr_layer.GetNextFeature()
            bbox = boundary_ftr.GetGeometryRef().GetEnvelope()
            for ftr, *_ in sign.iterate_features(clip_rect=bbox):
                if ftr.GetGeometryRef().Intersects(boundary_ftr.GetGeometryRef()):
                    dam_cer = ftr.GetField('sign_type')  # are these always the same or should it be a list param
                    new_ftr = ogr.Feature(out_lyr.ogr_layer.GetLayerDefn())
                    new_ftr.SetGeometry(ftr.GetGeometryRef())
                    new_ftr.SetField('type', dam_cer)
                    out_lyr.ogr_layer.CreateFeature(new_ftr)
                    new_ftr = None
        project.add_project_vector(out_gpkg_node, RSLayer('Sign', 'Sign', 'Vector', 'sign'))

    # set window distances for different stream sizes
    distancein = {
        '0': 200,
        '1': 400,
        '2': 1200,
        '3': 2000,
        '4': 8000
    }

    log.info('Copying valley bottom DGOs')
    dgos_out = os.path.join(output_gpkg_path, LayerTypes['BEAVER_ACTIVITY'].sub_layers['DGOS'].rel_path)
    igos_out = os.path.join(output_gpkg_path, LayerTypes['BEAVER_ACTIVITY'].sub_layers['IGOS'].rel_path)
    copy_feature_class(dgos, dgos_out, cfg.OUTPUT_EPSG)
    copy_feature_class(igos, igos_out, cfg.OUTPUT_EPSG)

    # list of level paths
    with SQLiteCon(output_gpkg_path) as db:
        db.conn.execute('CREATE INDEX ix_igo_levelpath on igos(level_path)')
        db.conn.execute('CREATE INDEX ix_igo_segdist on igos(seg_distance)')
        db.conn.execute('CREATE INDEX ix_igo_size on igos(stream_size)')
        db.conn.execute('CREATE INDEX ix_dgo_levelpath on dgos(level_path)')
        db.conn.execute('CREATE INDEX ix_dgo_segdist on dgos(seg_distance)')

        db.conn.commit()

        db.curs.execute('SELECT distinct level_path FROM dgos')
        levelps = db.curs.fetchall()
        levelpathsin = [lp['level_path'] for lp in levelps]

    dam_counts_to_dgos(os.path.join(output_gpkg_path, LayerTypes['BEAVER_ACTIVITY'].sub_layers['DAMS'].rel_path), dgos_out)

    windows = moving_window_dgo_ids(igos_out, dgos_out, levelpathsin, distancein)
    riverscapes_dam_counts(output_gpkg_path, windows)

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, LayerTypes)

    log.info('Project created successfully')


def main():

    parser = argparse.ArgumentParser(description='Beaver Activity')
    parser.add_argument('huc', type=int, help='Hydrologic Unit Code')
    parser.add_argument('proj_boundary', type=str, help='Path to watershed boundary feature class')
    parser.add_argument('dgos', type=str, help='Path to valley bottom DGOs')
    parser.add_argument('igos', type=str, help='Path to integrated geographic objects')
    parser.add_argument('beaver_dams', type=str, help='Path to beaver dams shapefile')
    parser.add_argument('output_dir', type=str, help='Output directory')
    parser.add_argument('--beaver_sign', type=str, help='Path to beaver sign shapefile')
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about thigs like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    log = Logger('Beaver Activity')
    log.setup(logPath=os.path.join(args.output_dir, 'beaver_activity.log'), verbose=args.verbose)
    log.title(f'Beaver Activity for HUC: {args.huc}')

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'mem_usage.log')
            retcode, max_obj = ThreadRun(beaver_activity, memfile, args.huc, args.proj_boundary, args.dgos, args.igos, args.beaver_dams, args.output_dir, args.beaver_sign)
            log.debug(f'Return code: {retcode} [Max process usage] {max_obj}')
        else:
            beaver_activity(args.huc, args.proj_boundary, args.dgos, args.igos, args.beaver_dams, args.output_dir, args.beaver_sign)
    except Exception as e:
        log.error(f'Error: {e}')
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
