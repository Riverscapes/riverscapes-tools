# Name:     Vector Operations Methods
#
# Purpose:  Miscellaneous routines for common tasks related to Vector  files
#           All methods should take and return VectorLayer derivative objects
#
# Author:   Matt Reimer
#
# Date:     Nov 16, 2020
# -------------------------------------------------------------------------------
from copy import copy
from functools import reduce
from osgeo import ogr, gdal
from shapely.wkb import loads as wkbload
from shapely.ops import unary_union
from shapely.geometry.base import BaseGeometry
from shapely.geometry import mapping, Point, MultiPoint, LineString, MultiLineString, GeometryCollection, Polygon, MultiPolygon
from rscommons import Logger, ProgressBar
from rscommons.util import sizeof_fmt, get_obj_size
from rscommons.classes.vector_base import VectorLayer, VectorLayerException


def print_geom_size(logger, geom_obj):
    try:
        size_str = sizeof_fmt(get_obj_size(geom_obj.wkb))
        logger.debug('Byte Size of output object: {} Type: {} IsValid: {} Length: {} Area: {}'.format(size_str, geom_obj.type, geom_obj.is_valid, geom_obj.length, geom_obj.area))
    except Exception as e:
        logger.debug(e)
        logger.debug('Byte Size of output object could not be determined')


def get_geometry_union(in_layer: VectorLayer, epsg: int = None, attribute_filter: str = None, clip_shape: BaseGeometry = None) -> BaseGeometry:
    """
    TODO: Depprecated
    Load all features from a ShapeFile and union them together into a single geometry
    :param inpath: Path to a ShapeFile
    :param epsg: Desired output spatial reference
    :return: Single Shapely geometry of all unioned features
    """

    log = Logger('get_geometry_union')

    transform = None
    if epsg:
        _outref, transform = in_layer.get_transform_from_epsg(epsg)

    geom = None

    for feature, _counter, progbar in in_layer.iterate_features("Getting geometry union", attribute_filter=attribute_filter, clip_shape=clip_shape):
        new_geom = feature.GetGeometryRef()

        if new_geom is None:
            progbar.erase()  # get around the progressbar
            log.warning('Feature with FID={} has no geometry. Skipping'.format(feature.GetFID()))
            continue

        if transform is not None:
            new_geom.Transform(transform)

        new_shape = wkbload(new_geom.ExportToWkb())
        try:
            geom = geom.union(new_shape) if geom else new_shape
        except Exception:
            progbar.erase()  # get around the progressbar
            log.warning('Union failed for shape with FID={} and will be ignored'.format(feature.GetFID()))

    return geom


def get_geometry_unary_union(in_layer: VectorLayer, epsg: int = None, attribute_filter: str = None, clip_shape: BaseGeometry = None) -> BaseGeometry:
    """
    Load all features from a ShapeFile and union them together into a single geometry
    :param inpath: Path to a ShapeFile
    :param epsg: Desired output spatial reference
    :return: Single Shapely geometry of all unioned features
    """

    log = Logger('get_geometry_unary_union')

    transform = None
    if epsg:
        _outref, transform = in_layer.get_transform_from_epsg(epsg)

    def unionize(wkb_lst):
        return unary_union([wkbload(g) for g in wkb_lst]).wkb

    geom_list = []

    for feature, _counter, progbar in in_layer.iterate_features("Unary Unioning features", attribute_filter=attribute_filter, clip_shape=clip_shape):
        new_geom = feature.GetGeometryRef()
        geo_type = new_geom.GetGeometryType()

        # We can't union non-valid shapes but sometimes a buffer by 0 can help
        if not new_geom.IsValid():
            progbar.erase()  # get around the progressbar
            log.warning('Invalid shape with FID={} trying the Buffer0 technique...'.format(feature.GetFID()))
            try:
                new_geom = new_geom.Buffer(0)
                if not new_geom.IsValid():
                    progbar.erase()  # get around the progressbar
                    log.warning('   Still invalid. Skipping this geometry')
                    continue
            except Exception:
                progbar.erase()  # get around the progressbar
                log.warning('Exception raised during buffer 0 technique. skipping this file')
                continue

        if new_geom is None:
            progbar.erase()  # get around the progressbar
            log.warning('Feature with FID={} has no geoemtry. Skipping'.format(feature.GetFID()))
        # Filter out zero-length lines
        elif geo_type in VectorLayer.LINE_TYPES and new_geom.Length() == 0:
            progbar.erase()  # get around the progressbar
            log.warning('Zero Length for shape with FID={}'.format(feature.GetFID()))
        # Filter out zero-area polys
        elif geo_type in VectorLayer.POLY_TYPES and new_geom.Area() == 0:
            progbar.erase()  # get around the progressbar
            log.warning('Zero Area for shape with FID={}'.format(feature.GetFID()))
        else:
            if transform is not None:
                new_geom.Transform(transform)
            geom_list.append(new_geom.ExportToWkb())

            # IF we get past a certain size then run the union
            if len(geom_list) >= 500:
                geom_list = [unionize(geom_list)]
        new_geom = None

    log.debug('finished iterating with list of size: {}'.format(len(geom_list)))

    if len(geom_list) > 1:
        log.debug('Starting final union of geom_list of size: {}'.format(len(geom_list)))
        # Do a final union to clean up anything that might still be in the list
        geom_union = wkbload(unionize(geom_list))
    elif len(geom_list) == 0:
        log.warning('No geometry found to union')
        return None
    else:
        log.debug('FINAL Unioning geom_list of size {}'.format(len(geom_list)))
        geom_union = wkbload(geom_list[0])
        log.debug('   done')

    print_geom_size(log, geom_union)
    log.debug('Complete')
    return geom_union


def copy_feature_class(in_layer: VectorLayer, out_layer: VectorLayer, epsg: int = None, clip_shape: BaseGeometry = None, attribute_filter: str = None):
    """Copy a Shapefile from one location to another

    This method is capable of reprojecting the geometries as they are copied.
    It is also possible to filter features by both attributes and also clip the
    features to another geometryNone

    Arguments:
        inpath {str} -- File path to input Shapefile that will be copied.
        epsg {int} -- Output coordinate system
        outpath {str} -- File path where the output Shapefile will be generated.

    Keyword Arguments:

    Args:
        in_layer (VectorLayer): Input layer of abstract type VectorLayer
        epsg ([type]): EPSG Code to use for the transformation
        out_layer (VectorLayer): Output layer of abstract type VectorLayer
        clip_shape {shape} -- Shapely polygon geometry in the output EPSG used to clip the input geometries (default: {None})
        attribute_filter {str} -- Attribute filter used to limit the input features that will be copied. (default: {None})
    """

    log = Logger('copy_feature_class')

    # Add input Layer Fields to the output Layer if it is the one we want
    out_layer.create_layer_from_ref(in_layer, epsg=epsg)

    transform = in_layer.get_transform(out_layer)

    # This is the callback method that will be run on each feature
    for feature, _counter, progbar in in_layer.iterate_features("Copying features", clip_shape=clip_shape, attribute_filter=attribute_filter):
        geom = feature.GetGeometryRef()

        if geom is None:
            progbar.erase()  # get around the progressbar
            log.warning('Feature with FID={} has no geometry. Skipping'.format(feature.GetFID()))
            continue

        geom.Transform(transform)

        # Create output Feature
        out_feature = ogr.Feature(out_layer.ogr_layer_def)
        out_feature.SetGeometry(geom)

        # Add field values from input Layer
        for i in range(0, out_layer.ogr_layer_def.GetFieldCount()):
            out_feature.SetField(out_layer.ogr_layer_def.GetFieldDefn(i).GetNameRef(), feature.GetField(i))

        out_layer.ogr_layer.CreateFeature(out_feature)
        out_feature = None


def merge_feature_classes(feature_classes: list, boundary: BaseGeometry, out_layer: VectorLayer, epsg: int = None):
    """[summary]

    Args:
        feature_classes ([type]): [description]
        boundary (BaseGeometry): [description]
        out_layer (VectorLayer): [description]
    """
    log = Logger('Shapefile')
    log.info('Merging {} feature classes.'.format(len(feature_classes)))

    fccount = 0
    for in_layer in feature_classes:
        fccount += 1
        log.info("Merging feature class {}/{}".format(fccount, len(feature_classes)))

        in_layer.SetSpatialFilter(ogr.CreateGeometryFromWkb(boundary.wkb))

        # First input spatial ref sets the SRS for the output file
        transform = in_layer.get_transform(out_layer)

        for i in range(in_layer.ogr_layer_def.GetFieldCount()):
            inFieldDef = in_layer.ogr_layer_def.GetFieldDefn(i)
            # Only create fields if we really don't have them
            # NOTE: THIS ASSUMES ALL FIELDS OF THE SAME NAME HAVE THE SAME TYPE
            if out_layer.ogr_layer_def.GetFieldIndex(inFieldDef.GetName()) == -1:
                out_layer.ogr_layer.CreateField(inFieldDef)

        log.info('Processing feature: {}/{}'.format(fccount, len(feature_classes)))

        def merge_feature_class(feature, counter, progbar):
            geom = feature.GetGeometryRef()

            if geom is None:
                progbar.erase()  # get around the progressbar
                log.warning('Feature with FID={} has no geometry. Skipping'.format(feature.GetFID()))
                return

            geom.Transform(transform)
            outFeature = ogr.Feature(out_layer.ogr_layer_def)

            for i in range(in_layer.ogr_layer_def.GetFieldCount()):
                outFeature.SetField(out_layer.ogr_layer_def.GetFieldDefn(i).GetNameRef(), feature.GetField(i))

            outFeature.SetGeometry(geom)
            out_layer.ogr_layer.CreateFeature(outFeature)

    log.info('Merge complete.')
    return fccount,


def load_attributes(in_layer: VectorLayer, id_field: str, fields: list) -> dict:
    """
    Load ShapeFile attributes fields into a dictionary keyed by the id_field
    :param network: Full, absolute path to a ShapeFile
    :param id_field: Field that uniquely identifies each feature
    :param fields: List of fields to load into the dictionary
    :return: Dictionary with id_field as key and each feature as dictionary of values keyed by the field name
    """

    # Verify that all the fields are present or throw an exception
    [in_layer.verify_field(field) for field in fields]

    # Only calculate the combined FIS where all the inputs exist
    # [networkLr.SetAttributeFilter('{} is not null'.format(field)) for field in [veg_field, drain_field, hydq2_field, hydlow_field, length_field, slope_field]]
    # layer.SetAttributeFilter("iGeo_Slope > 0 and iGeo_DA > 0")

    print('{:,} features in polygon ShapeFile {}'.format(in_layer.ogr_layer.GetFeatureCount(), in_layer.filepath))

    feature_values = {}

    for feature, _counter, _progbar in in_layer.iterate_features("loading attributes"):
        reach = feature.GetField(id_field)
        feature_values[reach] = {}

        for field in fields:
            feature_values[reach][field] = feature.GetField(field)

    return feature_values


def load_geometries(in_layer: VectorLayer, id_field: str, epsg=None) -> dict:
    log = Logger('Shapefile')

    # Determine the transformation if user provides an EPSG
    transform = None
    if epsg:
        _outref, transform = in_layer.get_transform_from_epsg(epsg)

    features = {}

    for feature, _counter, progbar in in_layer.iterate_features("Loading features"):

        reach = feature.GetField(id_field)
        geom = feature.GetGeometryRef()

        # Optional coordinate transformation
        if transform:
            geom.Transform(transform)

        new_geom = wkbload(geom.ExportToWkb())
        geo_type = geom.GetGeometryType()

        if new_geom.is_empty:
            progbar.erase()  # get around the progressbar
            log.warning('Empty feature with FID={} cannot be unioned and will be ignored'.format(feature.GetFID()))
        elif not new_geom.is_valid:
            progbar.erase()  # get around the progressbar
            log.warning('Invalid feature with FID={} cannot be unioned and will be ignored'.format(feature.GetFID()))
        # Filter out zero-length lines
        elif geo_type in VectorLayer.LINE_TYPES and new_geom.length == 0:
            progbar.erase()  # get around the progressbar
            log.warning('Zero Length for feature with FID={}'.format(feature.GetFID()))
        # Filter out zero-area polys
        elif geo_type in VectorLayer.POLY_TYPES and new_geom.area == 0:
            progbar.erase()  # get around the progressbar
            log.warning('Zero Area for feature with FID={}'.format(feature.GetFID()))
        else:
            features[reach] = new_geom

    return features


def write_attributes(in_layer: VectorLayer, output_values, id_field: str, fields, field_type=ogr.OFTReal, null_values=None):
    """
    Write field values to a ShapeFile feature class
    :param feature_class: Path to feature class
    :param output_values: Dictionary of values keyed by id_field. Each feature is dictionary keyed by field names
    :param id_field: Unique key identifying each feature in both feature class and output_values dictionary
    :param fields: List of fields in output_values to write to ShapeFile
    :return: None
    """

    log = Logger('ShapeFile')

    # Create each field and store the name and index in a list of tuples
    field_indices = [(field, in_layer.create_field(field, field_type)) for field in fields]

    for feature, _counter, _progbar in in_layer.iterate_features("Writing Attributes"):
        reach = feature.GetField(id_field)
        if reach not in output_values:
            continue

        # Set all the field values and then store the feature
        for field, _idx in field_indices:
            if field in output_values[reach]:
                if not output_values[reach][field]:
                    if null_values:
                        feature.SetField(field, null_values)
                    else:
                        log.warning('Unhandled ShapeFile value for None type')
                        feature.SetField(field, None)
                else:
                    feature.SetField(field, output_values[reach][field])
        in_layer.ogr_layer.SetFeature(feature)


def network_statistics(label: str, vector_layer: VectorLayer):

    log = Logger('network_statistics')
    log.info('Network ShapeFile Summary: {}'.format(vector_layer.filename))

    results = {}
    total_length = 0.0
    min_length = None
    max_length = None
    invalid_features = 0
    no_geometry = 0

    # Delete output column from network ShapeFile if it exists and then recreate it
    for fieldidx in range(0, vector_layer.ogr_layer_def.GetFieldCount()):
        results[vector_layer.ogr_layer_def.GetFieldDefn(fieldidx).GetName()] = 0

    for feature, _counter, _progbar in vector_layer.iterate_features("Calculating Stats"):
        geom = feature.GetGeometryRef()

        if geom is None:
            no_geometry += 1
            return

        shapely_obj = wkbload(geom.ExportToWkb())
        length = shapely_obj.length

        if shapely_obj.is_empty or shapely_obj.is_valid is False:
            invalid_features += 1

        total_length += length
        min_length = length if not min_length or min_length > length else min_length
        max_length = length if not max_length or max_length < length else max_length

        for fieldidx in range(0, vector_layer.ogr_layer_def.GetFieldCount()):
            field = vector_layer.ogr_layer_def.GetFieldDefn(fieldidx).GetName()
            if field not in results:
                results[field] = 0

            results[field] += 0 if feature.GetField(field) else 1

    features = vector_layer.ogr_layer.GetFeatureCount()
    results['Feature Count'] = features
    results['Invalid Features'] = invalid_features
    results['Features without geometry'] = no_geometry
    results['Min Length'] = min_length
    results['Max Length'] = max_length
    results['Avg Length'] = (total_length / features) if features > 0 and total_length != 0 else 0.0
    results['Total Length'] = total_length

    for key, value in results.items():
        if value > 0:
            log.info('{}, {} with {:,} NULL values'.format(label, key, value))

    return results


def feature_class_bounds(shapefile_path):

    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(shapefile_path, 0)
    layer = data_source.GetLayer()
    return layer.GetExtent()


def intersect_feature_classes(feature_class1: VectorLayer, feature_class2: VectorLayer, epsg: int, out_layer: VectorLayer, output_geom_type):

    union = get_geometry_unary_union(feature_class1, epsg)
    intersect_geometry_with_feature_class(union, feature_class2, epsg, out_layer, output_geom_type)


def intersect_geometry_with_feature_class(geometry: BaseGeometry, in_layer: VectorLayer, epsg: int, out_layer: VectorLayer, output_geom_type):

    if output_geom_type not in [ogr.wkbMultiPoint, ogr.wkbMultiLineString]:
        raise VectorLayerException('Unsupported ogr type for geometry intersection: "{}"'.format(output_geom_type))

    geom_union = get_geometry_unary_union(in_layer, epsg)

    # Nothing to do if there were no features in the feature class
    if not geom_union:
        return

    geom_inter = geometry.intersection(geom_union)

    # Nothing to do if the intersection is empty
    if geom_inter.is_empty:
        return

    # Single features and collections need to be converted into Multi-features
    if output_geom_type == ogr.wkbMultiPoint and not isinstance(geom_inter, MultiPoint):
        if isinstance(geom_inter, Point):
            geom_inter = MultiPoint([(geom_inter)])

        elif isinstance(geom_inter, LineString):
            # Break this linestring down into vertices as points
            geom_inter = MultiPoint(list(geom_inter.coords))

        elif isinstance(geom_inter, MultiLineString):
            # Break this linestring down into vertices as points
            geom_inter = MultiPoint(reduce(lambda acc, ls: acc + list(ls.coords), list(geom_inter.geoms), []))
            reduce
        elif isinstance(geom_inter, GeometryCollection):
            geom_inter = MultiPoint([geom for geom in geom_inter.geoms if isinstance(geom, Point)])

    elif output_geom_type == ogr.wkbMultiLineString and not isinstance(geom_inter, MultiLineString):
        if isinstance(geom_inter, LineString):
            geom_inter = MultiLineString([(geom_inter)])
        else:
            raise VectorLayerException('Unsupported ogr type: "{}" does not match shapely type of "{}"'.format(output_geom_type, geom_inter.type))

    feature = ogr.Feature(out_layer.ogr_layer_def)
    feature.SetGeometry(ogr.CreateGeometryFromWkb(geom_inter.wkb))
    out_layer.ogr_layer.CreateFeature(feature)


def buffer_by_field(in_layer: VectorLayer, field: str, epsg: int = None, min_buffer=None) -> BaseGeometry:
    """generate buffered polygons by value in field

    Args:
        flowlines (str): feature class of line features to buffer
        field (str): field with buffer value
        epsg (int): output srs
        min_buffer: use this buffer value for field values that are less than this

    Returns:
        geometry: unioned polygon geometry of buffered lines
    """

    _out_spatial_ref, transform = in_layer.get_transform_from_epsg(epsg)
    conversion = in_layer.rough_convert_metres_to_shapefile_units(1)

    outpolys = []
    for feature, _counter, _progbar in in_layer.iterate_features('Buffering'):
        geom = feature.GetGeometryRef()
        bufferDist = feature.GetField(field) * conversion
        geom_buffer = geom.Buffer(bufferDist if bufferDist > min_buffer else min_buffer)
        geom_buffer.Transform(transform)
        outpolys.append(wkbload(geom_buffer.ExportToWkb()))

    # unary union
    outpoly = unary_union(outpolys)
    return outpoly


def polygonize(raster_path: str, band: int, out_layer: VectorLayer, epsg: int = None):
    # mapping between gdal type and ogr field type
    type_mapping = {
        gdal.GDT_Byte: ogr.OFTInteger,
        gdal.GDT_UInt16: ogr.OFTInteger,
        gdal.GDT_Int16: ogr.OFTInteger,
        gdal.GDT_UInt32: ogr.OFTInteger,
        gdal.GDT_Int32: ogr.OFTInteger,
        gdal.GDT_Float32: ogr.OFTReal,
        gdal.GDT_Float64: ogr.OFTReal,
        gdal.GDT_CInt16: ogr.OFTInteger,
        gdal.GDT_CInt32: ogr.OFTInteger,
        gdal.GDT_CFloat32: ogr.OFTReal,
        gdal.GDT_CFloat64: ogr.OFTReal
    }

    out_layer.create_layer(ogr.wkbPolygon, epsg=epsg)

    src_ds = gdal.Open(raster_path)
    src_band = src_ds.GetRasterBand(band)

    out_layer.create_field('id', type_mapping[src_band.DataType])

    progbar = ProgressBar(100, 50, "Polygonizing raster")

    def poly_progress(progress, _msg, data_):
        # double dfProgress, char const * pszMessage=None, void * pData=None
        progbar.update(int(progress * 100))

    gdal.Polygonize(src_band, src_ds.GetRasterBand(band), out_layer.ogr_layer, 0, [], callback=poly_progress)
    progbar.finish()

    src_ds = None


def remove_holes(geom: BaseGeometry, min_hole_area: float) -> BaseGeometry:
    """[summary]

    Arguments:
        geom {[type]} -- shapely geometry of either Polygon or Multipolygon
        min_hole_area {[type]} -- (optional) Minimum area of holes to keep. If ommitted, all holes will be removed

    Raises:
        Exception: [description]

    Returns:
        [type] -- [description]
    """

    def _simpl(geo):
        return Polygon(geo.exterior, list(filter(lambda x: Polygon(x).area > min_hole_area, geo.interiors)))

    if type(geom) == Polygon:
        if min_hole_area is None:
            # Remove all holes if we don't specify a min area
            return Polygon(geom.exterior)
        else:
            return _simpl(geom)
    elif type(geom) == MultiPolygon:
        if min_hole_area is None:
            # Remove all holes if we don't specify a min area
            return MultiPolygon([Polygon(mgeo.exterior) for mgeo in geom.geoms])
        else:
            return MultiPolygon([_simpl(mgeo) for mgeo in geom.geoms])
    else:
        raise VectorLayerException('Invalid geometry type used for "remove_holes": {}'.format(type(geom)))


def get_num_pts(geom: BaseGeometry) -> int:
    """Helper function for counting points

    Arguments:
        geom {[Shapely Shape]} -- [description]

    Returns:
        [int] -- Number
    """
    try:
        if type(geom) == Polygon:
            return len(geom.exterior.coords)
        elif type(geom) == MultiPolygon:
            return reduce(lambda x, y: x + y, [len(g.exterior.coords) for g in geom.geoms])
    except Exception as e:
        pass
    return None


def get_num_rings(geom: BaseGeometry) -> int:
    """Helper function for counting rings

    Arguments:
        geom {[Shapely Shape]} -- [description]

    Returns:
        [int] -- Number
    """
    try:
        if type(geom) == Polygon:
            return len(geom.interiors)
        elif type(geom) == MultiPolygon:
            return reduce(lambda x, y: x + y, [len(g.interiors) for g in geom.geoms])
    except Exception as e:
        pass
    return None


def export_geojson(geom: BaseGeometry, props=None):
    new_props = copy(props) if props is not None else {}
    the_dict = {
        "type": "FeatureCollection",
        "features": [
        ]
    }
    if geom is not None:
        the_dict["features"].append({
            "type": "Feature",
            "properties": new_props,
            "geometry": mapping(geom),
        })

    return the_dict
