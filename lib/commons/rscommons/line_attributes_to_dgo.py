from rsxml import Logger
from rscommons import get_shp_or_gpkg, VectorBase
from rscommons.database import SQLiteCon


def line_attributes_to_dgo(line_ftrs: str, dgo_ftrs: str, field_map: dict, method: str = 'lwa', update_dgo_ftrs: bool = False, dgo_table: str = None):
    """Copy attributes from a line feature class to a DGO table by either taking the value from the line segment
    with the longest lenght that intersects the DGO or by taking the length weighted average of the line segments that
    intersect the DGO.

    Args:
        line_ftrs (str): Path to the line feature class
        dgo_ftrs (str): Path to the DGO feature class
        field_map (dict): A dictionary with the keys being the field names in the line feature class and the values being
            the field names in the DGO feature class
        method (str): The method to use to transfer attributes. Can be either 'lwa' for length weighted average or 'lsl'
            for longest segment length. Defaults to 'lwa'."""

    log = Logger('Transfer attributes from line to DGO')
    log.info(f'Transferring attributes from {line_ftrs} to DGO features')

    # check that fields from dict exist in both feature classes
    with get_shp_or_gpkg(line_ftrs) as line_lyr, get_shp_or_gpkg(dgo_ftrs) as dgo_lyr:
        for line_field, dgo_field in field_map.items():
            if line_field not in line_lyr.get_fields():
                log.error(f'Field {line_field} not found in {line_ftrs}')
                raise ValueError(f'Field {line_field} not found in {line_ftrs}')
            if update_dgo_ftrs:
                if dgo_field not in dgo_lyr.get_fields():
                    log.error(f'Field {dgo_field} not found in {dgo_ftrs}')
                    raise ValueError(f'Field {dgo_field} not found in {dgo_ftrs}')
    if dgo_table is not None:
        with SQLiteCon(dgo_table) as db:
            db.curs.execute("""PRAGMA table_info(DGOAttributes)""")
            dgo_fields = [field['name'] for field in db.curs.fetchall()]
            for dgo_field in field_map.values():
                if dgo_field not in dgo_fields:
                    log.error(f'Field {dgo_field} not found in {dgo_table}')
                    raise ValueError(f'Field {dgo_field} not found in {dgo_table}')

    with get_shp_or_gpkg(dgo_ftrs, write=True) as dgo_lyr:
        if update_dgo_ftrs:
            dgo_lyr.ogr_layer.StartTransaction()
        for dgo_feature, _counter, _progbar in dgo_lyr.iterate_features("Processing DGO features"):
            dgoid = dgo_feature.GetFID()
            dgo_geom = dgo_feature.GetGeometryRef()

            # get the line segments that intersect the DGO
            intersecting_lines = {field: {'val': [], 'length': []} for field in field_map.keys()}
            with get_shp_or_gpkg(line_ftrs) as line_lyr:
                for line_feature, _counter, _progbar in line_lyr.iterate_features(clip_shape=dgo_geom):
                    line_geom = line_feature.GetGeometryRef()
                    intersect_geom = line_geom.Intersection(dgo_geom)
                    if intersect_geom is not None:
                        for field in field_map.keys():
                            intersecting_lines[field]['val'].append(line_feature.GetField(field))
                            intersecting_lines[field]['length'].append(intersect_geom.Length())

            # calculate the length weighted average or longest segment length
            for field in field_map.keys():
                vals_dict = {val: length for val, length in zip(intersecting_lines[field]['val'], intersecting_lines[field]['length']) if val is not None}
                if len(vals_dict) == 0:
                    continue
                if method == 'lwa':
                    total_length = sum(vals_dict.values())
                    if update_dgo_ftrs:
                        dgo_feature.SetField(field_map[field], sum([val * length / total_length for val, length in vals_dict.items()]))
                        dgo_lyr.ogr_layer.SetFeature(dgo_feature)
                    if dgo_table is not None:
                        with SQLiteCon(dgo_table) as db:
                            db.curs.execute(f'UPDATE DGOAttributes SET {field_map[field]} = ? WHERE dgoid = ?', (sum([val * length / total_length for val, length in vals_dict.items()]), dgoid))
                            db.conn.commit()
                elif method == 'lsl':
                    if update_dgo_ftrs:
                        dgo_feature.SetField(field_map[field], max(vals_dict, key=vals_dict.get))
                        dgo_lyr.ogr_layer.SetFeature(dgo_feature)
                    if dgo_table is not None:
                        with SQLiteCon(dgo_table) as db:
                            db.curs.execute(f'UPDATE DGOAttributes SET {field_map[field]} = ? WHERE dgoid = ?', (max(vals_dict, key=vals_dict.get), dgoid))
                            db.conn.commit()
                else:
                    log.error(f'Method {method} not recognised')
                    raise ValueError(f'Method {method} not recognised')

        if update_dgo_ftrs:
            dgo_lyr.ogr_layer.CommitTransaction()
