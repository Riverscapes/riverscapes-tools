import argparse
import os
import sys
import traceback
from osgeo import ogr

from rscommons import initGDALOGRErrors, ModelConfig, RSLayer, RSProject, RSMeta, RSMetaTypes, get_shp_or_gpkg, GeopackageLayer
from rsxml import Logger, dotenv
from rscommons.augment_lyr_meta import augment_layermeta, add_layer_descriptions
from rscommons.vector_ops import copy_feature_class
from rscommons.database import SQLiteCon
from rscommons.moving_window import moving_window_dgo_ids, moving_window_by_intersection
from rscommons.project_bounds import generate_project_extents_from_layer

from beaver_sign.utils.dam_counts_to_dgos import dam_counts_to_dgos
from beaver_sign.utils.riverscape_counts import riverscapes_dam_counts
from beaver_sign.utils.census_info import census_info

from beaver_sign.__version__ import __version__

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'lyr_descriptions.json')

# LayerTypes = {
#     'BEAVER_ACTIVITY': RSLayer('Beaver Activity', 'BeaverActivity', 'Geopackage', 'beaver_activity.gpkg', {
#         # 'DGOS': RSLayer('Discrete Geographic Objects', 'DGOS', 'Vector', 'dgos'),
#         # 'IGOS': RSLayer('Integrated Geographic Objects', 'IGOS', 'Vector', 'igos'),
#         # 'DAMS': RSLayer('Dams', 'Dams', 'Vector', 'dams'),
#         # 'SIGN': RSLayer('Sign', 'Sign', 'Vector', 'sign')
#     })
# }


def beaver_activity(huc, proj_boundary, dgos, igos, qris_path, output_dir, beaver_sign=None):

    log = Logger('Beaver Activity')

    # augment_layermeta('beaver_activity', LYR_DESCRIPTIONS_JSON, LayerTypes)

    project_name = f'Beaver Activity for HUC {huc}'
    project = RSProject(cfg, output_dir)
    project.create(project_name, 'beaver_activity', [
        RSMeta('Model Documentation', 'https://tools.riverscapes.net/beaver_activity', RSMetaTypes.URL, locked=True),
        RSMeta('HUC', str(huc), RSMetaTypes.HIDDEN, locked=True),
        RSMeta('Hydrologic Unit Code', str(huc), locked=True),
        RSMeta('Small Search Window', '200', RSMetaTypes.INT, locked=True),
        RSMeta('Medium Search Window', '400', RSMetaTypes.INT, locked=True),
        RSMeta('Large Search Window', '600', RSMetaTypes.INT, locked=True),
    ])

    # find the number of different dam feature classes and their descriptions
    census = census_info(qris_path)

    log.info('Setting up output feature classes')
    # realization_nodes = {}
    for i in range(len(census)):
        r_num = i + 1
        _realization, proj_nodes = project.add_realization(list(census.values())[i][1], f'REALIZATION{r_num}', cfg.version, data_nodes=['Inputs', 'Outputs'], meta=[
            RSMeta('Description', list(census.values())[i][0], locked=True),
        ])
        # realization_nodes[i] = proj_nodes
        beaver_dams = os.path.join(qris_path, f'qris.gpkg/vw_beaver_dam_{r_num}')

        rz_layers = {
            'BEAVER_ACTIVITY_INPUTS': RSLayer('Beaver Activity Inputs', 'BeaverActivityInputs', 'Geopackage', f'inputs_{r_num}.gpkg', {
                'DGOS_IN': RSLayer('Discrete Geographic Objects', 'DGOS', 'Vector', 'dgos'),
                'IGOS_IN': RSLayer('Integrated Geographic Objects', 'IGOS', 'Vector', 'igos'),
            }),
            'BEAVER_ACTIVITY': RSLayer('Beaver Activity', 'BeaverActivity', 'Geopackage', f'beaver_activity_{r_num}.gpkg', {
                'DGOS': RSLayer('Discrete Geographic Objects', 'DGOS', 'Vector', 'beaver_activity_dgos'),
                'IGOS': RSLayer('Integrated Geographic Objects', 'IGOS', 'Vector', 'beaver_activity_igos'),
                'DAMS': RSLayer('Dams', 'Dams', 'Vector', 'dams'),
                # 'SIGN': RSLayer('Sign', 'Sign', 'Vector', 'sign')
            })
        }
        output_gpkg_path = os.path.join(output_dir, rz_layers['BEAVER_ACTIVITY'].rel_path)
        input_gpkg_path = os.path.join(output_dir, rz_layers['BEAVER_ACTIVITY_INPUTS'].rel_path)

        with GeopackageLayer(output_gpkg_path, rz_layers['BEAVER_ACTIVITY'].sub_layers['DAMS'].rel_path, write=True) as out_lyr, \
                GeopackageLayer(beaver_dams) as dams, get_shp_or_gpkg(proj_boundary) as boundary:
            out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, fields={
                'dam_cer': ogr.OFTString,
                'dam_type': ogr.OFTString,
                'type_cer': ogr.OFTString
            })
            boundary_ftr = boundary.ogr_layer.GetNextFeature()
            bbox = boundary_ftr.GetGeometryRef().GetEnvelope()
            for ftr, *_ in dams.iterate_features(clip_rect=bbox):
                if ftr.GetGeometryRef().Intersects(boundary_ftr.GetGeometryRef()):
                    dam_cer = ftr.GetField('Dam CER')  # are these always the same or should it be a list param
                    dam_type = ftr.GetField('Dam Type')
                    type_cer = ftr.GetField('Type CER')
                    new_ftr = ogr.Feature(out_lyr.ogr_layer.GetLayerDefn())
                    new_ftr.SetGeometry(ftr.GetGeometryRef())
                    new_ftr.SetField('dam_cer', dam_cer)
                    new_ftr.SetField('dam_type', dam_type)
                    new_ftr.SetField('type_cer', type_cer)
                    out_lyr.ogr_layer.CreateFeature(new_ftr)
                    new_ftr = None

            out_gpkg_node, *_ = project.add_project_geopackage(proj_nodes['Outputs'], rz_layers['BEAVER_ACTIVITY'])
            project.add_project_geopackage(proj_nodes['Inputs'], rz_layers['BEAVER_ACTIVITY_INPUTS'])

        if beaver_sign:
            with GeopackageLayer(output_gpkg_path, rz_layers['BEAVER_ACTIVITY'].sub_layers['SIGN'].rel_path, delete_dataset=True) as out_lyr, \
                    get_shp_or_gpkg(beaver_sign) as sign, get_shp_or_gpkg(proj_boundary) as boundary:
                out_lyr.create_layer(ogr.wkbMultiPoint, epsg=cfg.OUTPUT_EPSG, fields={
                    'type': ogr.OFTString
                })
                boundary_ftr = boundary.ogr_layer.GetNextFeature()
                bbox = boundary_ftr.GetGeometryRef().GetEnvelope()
                for ftr, *_ in sign.iterate_features(clip_rect=bbox):
                    if ftr.GetGeometryRef().Intersects(boundary_ftr.GetGeometryRef()):
                        sign = ftr.GetField('sign_type')  # are these always the same or should it be a list param
                        new_ftr = ogr.Feature(out_lyr.ogr_layer.GetLayerDefn())
                        new_ftr.SetGeometry(ftr.GetGeometryRef())
                        new_ftr.SetField('type', sign)
                        out_lyr.ogr_layer.CreateFeature(new_ftr)
                        new_ftr = None
            project.add_project_vector(out_gpkg_node, RSLayer('Sign', 'Sign', 'Vector', 'sign'))

    log.info('Copying valley bottom DGOs')
    windows = None
    for i in range(len(census)):
        output_gpkg_path = os.path.join(output_dir, f'beaver_activity_{i+1}.gpkg')
        input_gpkg_path = os.path.join(output_dir, f'inputs_{i+1}.gpkg')
        dgos_out = os.path.join(output_gpkg_path, 'beaver_activity_dgos')
        igos_out = os.path.join(output_gpkg_path, 'beaver_activity_igos')
        dgos_in = os.path.join(input_gpkg_path, 'dgos')
        igos_in = os.path.join(input_gpkg_path, 'igos')
        copy_feature_class(dgos, dgos_out, cfg.OUTPUT_EPSG)
        copy_feature_class(igos, igos_out, cfg.OUTPUT_EPSG)
        copy_feature_class(dgos, dgos_in, cfg.OUTPUT_EPSG)
        copy_feature_class(igos, igos_in, cfg.OUTPUT_EPSG)

        # list of level paths
        with SQLiteCon(output_gpkg_path) as db:
            db.conn.execute('CREATE INDEX ix_igo_levelpath on beaver_activity_igos(level_path)')
            db.conn.execute('CREATE INDEX ix_igo_segdist on beaver_activity_igos(seg_distance)')
            db.conn.execute('CREATE INDEX ix_igo_size on beaver_activity_igos(stream_size)')
            db.conn.execute('CREATE INDEX ix_dgo_levelpath on beaver_activity_dgos(level_path)')
            db.conn.execute('CREATE INDEX ix_dgo_segdist on beaver_activity_dgos(seg_distance)')

            db.conn.commit()

            db.curs.execute('SELECT distinct level_path FROM beaver_activity_dgos')
            levelps = db.curs.fetchall()
            levelpathsin = [lp['level_path'] for lp in levelps]

        if windows is None:
            windows = moving_window_by_intersection(igos_out, dgos_out, levelpathsin)

        dams_in = os.path.join(output_gpkg_path, 'dams')
        dam_counts_to_dgos(dams_in, dgos_out)

        riverscapes_dam_counts(output_gpkg_path, windows)

    # add project extents
    # extents_path = os.path.join(os.path.dirname(output_gpkg_path), 'project_bounds.geojson')
    # extents = generate_project_extents_from_layer(proj_boundary, extents_path)
    # project.add_project_extent(extents_path, extents['CENTROID'], extents['BBOX'])

    add_layer_descriptions(project, LYR_DESCRIPTIONS_JSON, rz_layers)

    log.info('Project created successfully')


def main():

    parser = argparse.ArgumentParser(description='Beaver Activity')
    parser.add_argument('huc', type=str, help='Hydrologic Unit Code')
    parser.add_argument('proj_boundary', type=str, help='Path to watershed boundary feature class')
    parser.add_argument('dgos', type=str, help='Path to valley bottom DGOs')
    parser.add_argument('igos', type=str, help='Path to integrated geographic objects')
    parser.add_argument('qris_path', type=str, help='Path to QRIS riverscapes project that contains dam census')
    parser.add_argument('output_dir', type=str, help='Output directory')
    parser.add_argument('--beaver_sign', type=str, help='Path to beaver sign shapefile')
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    parser.add_argument('--debug', help='(optional) more output about thigs like memory usage. There is a performance cost', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    log = Logger('Beaver Activity')
    log.setup(log_path=os.path.join(args.output_dir, 'beaver_activity.log'), verbose=args.verbose)
    log.title(f'Beaver Activity for HUC: {args.huc}')

    try:
        if args.debug is True:
            from rscommons.debug import ThreadRun
            memfile = os.path.join(args.output_dir, 'mem_usage.log')
            retcode, max_obj = ThreadRun(beaver_activity, memfile, args.huc, args.proj_boundary, args.dgos, args.igos, args.qris_path, args.output_dir, args.beaver_sign)
            log.debug(f'Return code: {retcode} [Max process usage] {max_obj}')
        else:
            beaver_activity(args.huc, args.proj_boundary, args.dgos, args.igos, args.qris_path, args.output_dir, args.beaver_sign)
    except Exception as e:
        log.error(f'Error: {e}')
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
