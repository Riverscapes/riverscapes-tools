import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from rsxml import safe_makedirs
from osgeo import ogr
from rscommons import GeopackageLayer, VectorBase
from rscommons.copy_features import copy_features_fields

from riverscapes import RiverscapesAPI, RiverscapesSearchParams


def split_beaver_activity(huc_list):
    # Create an API object
    api = RiverscapesAPI(stage='PRODUCTION')
    api.refresh_token()

    ba_dir = '/workspaces/data/beaver_activity'
    if not os.path.exists(ba_dir):
        safe_makedirs(ba_dir)
    rsc_dir = '/workspaces/data/rs_context'
    if not os.path.exists(rsc_dir):
        safe_makedirs(rsc_dir)

    for huc in huc_list:
        # Create a search parameters object
        search_params = RiverscapesSearchParams(
            {
                "projectTypeId": "beaver_activity",
                "meta": {
                    "HUC": str(huc)
                }
            }
        )

        # Get the results
        for project, _stats, search_total, _prg in api.search(search_params):
            # get the huc
            huc = project.project_meta['HUC']

            # download the project
            huc_dir = os.path.join(ba_dir, str(huc))
            api.download_files(project.id, huc_dir)

            # download the rscontexts
            for rscproj, _rscstats, _rsctotal, _rscprg in api.search(RiverscapesSearchParams({
                    "projectTypeId": "RScontext",
                    "tags": ["2024CONUS"],
                    "meta": {
                        "HUC": str(huc)
                    }})):

                huc10 = rscproj.project_meta['HUC']
                rsc10_dir = os.path.join(rsc_dir, str(huc10))
                api.download_files(rscproj.id, rsc10_dir, re_filter=['hydrology', 'project_bounds', 'project.rs.xml', 'beaver_activity.log'])

                new_badir = os.path.join('/workspaces/data/beaver_activity', huc10)

                # clip beaver_activity gpkg layers using rscontext huc10 boundary
                gpkg_lyrs = ['dams', 'dgos', 'igos']
                dam_ct = 0
                for gpkg_lyr in gpkg_lyrs:
                    if gpkg_lyr != 'dams' and dam_ct == 0:
                        if os.path.exists(rsc10_dir):
                            shutil.rmtree(rsc10_dir)
                            shutil.rmtree(new_badir)
                        continue
                    with GeopackageLayer(os.path.join(huc_dir, 'beaver_activity.gpkg'), gpkg_lyr) as ba_lyr, \
                            GeopackageLayer(os.path.join(rsc10_dir, 'hydrology', 'nhdplushr.gpkg'), 'WBDHU10') as wbd_lyr, \
                            GeopackageLayer(os.path.join(new_badir, 'beaver_activity.gpkg'), layer_name=gpkg_lyr, write=True) as out_lyr:
                        wbd_feat = wbd_lyr.ogr_layer.GetNextFeature()
                        # out_lyr.ogr_layer.StartTransaction()
                        out_lyr.create_layer(ba_lyr.ogr_layer.GetGeomType(), epsg=4326)
                        for field in ba_lyr.ogr_layer.schema:
                            out_lyr.ogr_layer.CreateField(field)
                        for feature, _counter, _progbar in ba_lyr.iterate_features("Processing points", clip_shape=wbd_feat):
                            if gpkg_lyr == 'dams':
                                dam_ct += 1
                            geom = feature.GetGeometryRef()
                            if geom.Intersects(wbd_feat.GetGeometryRef()):
                                out_feature = ogr.Feature(ba_lyr.ogr_layer_def)
                                for i in range(0, ba_lyr.ogr_layer_def.GetFieldCount()):
                                    field_name = ba_lyr.ogr_layer_def.GetFieldDefn(i).GetNameRef()
                                    output_field_index = feature.GetFieldIndex(field_name)
                                    if output_field_index >= 0:
                                        out_feature.SetField(field_name, feature.GetField(output_field_index))
                                out_feature.SetGeometry(geom)
                                out_lyr.ogr_layer.CreateFeature(out_feature)
                        # out_lyr.ogr_layer.CommitTransaction()

                if os.path.exists(rsc10_dir):
                    # copy the project bounds geojson
                    shutil.copy(os.path.join(rsc10_dir, 'project_bounds.geojson'), new_badir)
                    shutil.copy(os.path.join(huc_dir, 'project.rs.xml'), new_badir)
                    shutil.copy(os.path.join(huc_dir, 'beaver_activity.log'), new_badir)

                    # get info from rs context xml
                    tree = ET.parse(os.path.join(rsc10_dir, 'project.rs.xml'))
                    root = tree.getroot()
                    for meta in root.findall('.//Meta'):
                        if meta.get('name') == 'Watershed':
                            watershed_name = meta.text
                            break

                    pb = root.find('ProjectBounds')
                    if pb is not None:
                        centroid = pb.find('Centroid')
                        if centroid is not None:
                            centroid_lat = centroid.find('Lat').text
                            centroid_lon = centroid.find('Lng').text
                        bb = pb.find('BoundingBox')
                        if bb is not None:
                            min_lat = bb.find('MinLat').text
                            min_lon = bb.find('MinLng').text
                            max_lat = bb.find('MaxLat').text
                            max_lon = bb.find('MaxLng').text

                    # generate the project.rs.xml
                    tree = ET.parse(os.path.join(new_badir, 'project.rs.xml'))
                    root = tree.getroot()
                    root.remove(root.find('Warehouse'))
                    name = root.find('Name')
                    for meta in root.findall('.//Meta'):
                        if meta.get('name') in ('HUC', 'Hydrologic Unit Code'):
                            meta.text = huc10
                    if watershed_name:
                        name.text = f'Beaver Activity for {watershed_name}'
                        meta = root.find('MetaData')
                        new_meta_element = ET.SubElement(meta, 'Meta', name='Watershed', locked='true')
                        new_meta_element.text = watershed_name
                        root.find('Realizations').find('Realization').find('Name').text = f'Beaver Activity for {watershed_name}'
                    else:
                        name.text = f'Beaver Activity for HUC {huc10}'
                        root.find('Realizations').find('Realization').find('Name').text = f'Beaver Activity for HUC {huc10}'

                    pb = root.find('ProjectBounds')
                    if pb is not None:
                        centroid = pb.find('Centroid')
                        if centroid is not None:
                            if centroid_lat and centroid_lon:
                                centroid.find('Lat').text = centroid_lat
                                centroid.find('Lng').text = centroid_lon
                        bb = pb.find('BoundingBox')
                        if bb is not None:
                            if min_lat and min_lon and max_lat and max_lon:
                                bb.find('MinLat').text = min_lat
                                bb.find('MinLng').text = min_lon
                                bb.find('MaxLat').text = max_lat
                                bb.find('MaxLng').text = max_lon

                    tree.write(os.path.join(new_badir, 'project.rs.xml'))

                    # upload the project
                    subprocess.run(['rscli', 'upload', new_badir, '--org', '06439423-ee19-4040-9ebd-01c6e481a763', '--visibility', 'PRIVATE', '--tags', 'MT_Dam_Census', '--no-input', '--no-ui', '--verbose'])

                if os.path.exists(rsc10_dir):
                    shutil.rmtree(rsc10_dir)

            if os.path.exists(huc_dir):
                shutil.rmtree(huc_dir)


hucs = ['10020004', '10020003', '10020002', '10020001', '09040002', '09040001']
split_beaver_activity(hucs)
