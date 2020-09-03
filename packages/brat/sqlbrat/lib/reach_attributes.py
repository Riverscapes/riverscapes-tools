from sqlbrat.lib.database import load_attributes
from rscommons.shapefile import write_attributes
from osgeo import ogr


def write_reach_attributes(feature_class, database, original_fields, field_aliases):
    """ Write reach values to a ShapeFile with some preprocessing to convert database
    fields to ShapeFile fields

    Arguments:
        feature_class {str} -- Path to output ShapeFile
        output_values {dict} -- dictionary. Key is ReachID and values is dictionary of values keyed by database field name
        original_fields {dict} -- OGR datatype keyed to list of database field names
        field_aliases {dict} -- Field names used in out_values dictionary (i.e. databaes field names) keyed 
            to ShapeFile field names that will be written to ShapeFile
        null_value {var} -- ShapeFiles can't store NULL values, so this value is substituted for any NULLs
            in database
    """

    for data_type, fields in original_fields.items():
        shp_fields = list(fields)

        values = load_attributes(database, fields)

        # ShapeFiles can't store Nulls.
        null_value = None
        if data_type == ogr.OFTInteger:
            null_value = -1
        elif data_type == ogr.OFTReal:
            null_value = -1.0

        # duplicate any values that are stored in a different ShapeFile field than in the database
        for original, alias in field_aliases.items():
            if original not in fields:
                continue

            # Ensure the fields list now contains the ShapeFile field name instead of the database field name
            shp_fields[shp_fields.index(original)] = alias
            for valdict in values.values():
                valdict[alias] = valdict[original]

        write_attributes(feature_class, values, 'ReachID', shp_fields, data_type, null_value)
