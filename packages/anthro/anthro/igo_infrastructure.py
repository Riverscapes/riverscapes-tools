"""Functions to attribute IGO points with attributes related to the presence of infrastructure within the riverscape.

Jordan Gilbert

Dec 2022
"""
import sqlite3
from rscommons import get_shp_or_gpkg
from rscommons.classes.vector_base import VectorBase, get_utm_zone_epsg
from rscommons.vector_ops import get_geometry_unary_union


def infrastructure_attributes(igo: str, windows: str, road: str, rail: str, canal: str, crossings: str, diversions: str,
                              out_gpkg_path: str):

    in_data = {
        road: 'Road',
        rail: 'Rail',
        canal: 'Canal',
        crossings: 'RoadX',
        diversions: 'DivPts'
    }

    # get epsg_proj
    with get_shp_or_gpkg(igo) as inref:
        ftr = inref.ogr_layer.GetFeature(1)
        long = ftr.GetGeometryRef().GetEnvelope()[0]
        epsg_proj = get_utm_zone_epsg(long)

    conn = sqlite3.connect(out_gpkg_path)

    for dataset, label in in_data.items():
        # ds = get_geometry_unary_union(dataset)
        # sref, transform = ds.get_transform_from_epsg(ds.spatial_ref, epsg_proj)
        print(f'Summarizing metrics for dataset: {dataset}')

        counter = 1
        for igoid, window in windows.items():
            # print(f'summarizing on igo {counter} of {len(windows)} for dataset {label}')

            ftr_length = 0.0
            ftr_count = 0

            if igoid in [1110, 1442]:
                print(igoid)

            with get_shp_or_gpkg(dataset) as layer:
                sref, transform = layer.get_transform_from_epsg(layer.spatial_ref, epsg_proj)
                for geom in window[0].geoms:
                    layer.ogr_layer.SetSpatialFilter(VectorBase.shapely2ogr(geom))
                    if layer.ogr_layer.GetFeatureCount() == 0:
                        continue
                    else:
                        for feature in layer.ogr_layer:
                            lyr_cl = geom.intersection(VectorBase.ogr2shapely(feature))

                            # leave null if layer is empty?
                            if lyr_cl.is_empty is True:
                                continue
                            else:
                                if lyr_cl.type in ['MultiLineString', 'LineString']:
                                    ogrlyr = VectorBase.shapely2ogr(lyr_cl)
                                    lyr_clipped = VectorBase.ogr2shapely(ogrlyr, transform=transform)
                                    ftr_length += lyr_clipped.length
                                if lyr_cl.type in ['MultiPoint']:
                                    ftr_count += len(lyr_cl.geoms)
                                if lyr_cl.type in ['Point']:
                                    if lyr_cl.is_empty is False:
                                        ftr_count += 1

                if layer.ogr_geom_type in layer.LINE_TYPES:
                    lb1 = label + '_len'
                    lb2 = label + '_dens'
                    conn.execute(f'UPDATE IGOAttributes SET {lb1} = {ftr_length} WHERE IGOID = {igoid}')
                    if window[2] != 0.0:
                        conn.execute(f'UPDATE IGOAttributes SET {lb2} = {ftr_length / window[2]} WHERE IGOID = {igoid}')

                if layer.ogr_geom_type in layer.POINT_TYPES:
                    lb1 = label + '_ct'
                    lb2 = label + '_dens'
                    conn.execute(f'UPDATE IGOAttributes SET {lb1} = {ftr_count} WHERE IGOID = {igoid}')
                    if window[2] != 0.0:
                        conn.execute(f'UPDATE IGOAttributes SET {lb2} = {ftr_count / window[2]} WHERE IGOID = {igoid}')

                conn.commit()

                counter += 1
