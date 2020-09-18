import csv
import statistics
from osgeo import ogr
from shapely.wkt import loads as wkt_load
from shapely.geometry import mapping, shape, Polygon, MultiPolygon
from shapely.ops import unary_union
from rscommons.shapefile import get_transform_from_epsg


def load_mean_annual_data(gpkg, input_csv, attribute_name):
    """
    Loads PRISM attribute csv file into PRISM geopackage as mean annual value

    Args:
        gpkg (str): Geopackage of PRISM Locations
        input_csv (str): name of PRISM csv attribute file to load
        attribute_name (str): Name of new field to add to geopackage
    """

    # Load GPKG
    driver = ogr.GetDriverByName("GPKG")
    source = driver.Open(gpkg, 1)
    layer = source.GetLayerByName("PRISM_Data")

    # Read through CSV Lines and write to layer features
    layer.StartTransaction()
    with open(input_csv, 'r') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            # Retrieve just the valid integer values
            valint = [int(val) for val in row[1:] if val.isdigit()]
            # Calculate how many years these data points represent
            years = float(len(valint)) / 12.0
            # Convert values into annual precip in mm
            tot = (float(sum(valint)) / 100.0) / years
            feature = layer.GetFeature(int(row[0]))
            feature.SetField(attribute_name, tot)
            layer.SetFeature(feature)
            feature = None
    layer.CommitTransaction()
    layer = None
    source = None

    return


def mean_area_precip(polygon, gpkg):
    """Calculates the average mean annual precipitation from all PRISIM stations within polygon

    Args:
        polygon (str): HUC or polygon feature class to summarize PRISM data
        gpkg (str): PRISM geopackage with precipitation loaded

    Returns:
        float: preciptation (mm)
    """

    # Assumptions
    # One HUC Polygon, first feature in shp

    # Load Datasets
    driverPoly = ogr.GetDriverByName("ESRI Shapefile")
    driverGPKG = ogr.GetDriverByName("GPKG")
    sourcePoly = driverPoly.Open(polygon, 1)
    sourceGPKG = driverGPKG.Open(gpkg, 0)
    layerPoly = sourcePoly.GetLayer()
    layer = sourceGPKG.GetLayerByName("PRISM_Data")

    # Create Field
    field_precip = ogr.FieldDefn("Precip_mm", ogr.OFTReal)
    layerPoly.CreateField(field_precip)

    # Intersect gpkg with polygon (spatial Filter)
    feature = layerPoly.GetFeature(0)
    geom = feature.GetGeometryRef()
    layer.SetSpatialFilter(geom)
    # Average the precip measurements
    precip = statistics.mean([feat.GetField("Mean_Annual_Precip") for feat in layer])

    # Write to huc polygon precip field
    feature.SetField("Precip_mm", precip)
    layerPoly.SetFeature(feature)
    feature = None
    layer = None
    layerPoly = None
    sourceGPKG = None
    sourcePoly = None

    return precip


def calculate_bankfull_width(nhd_flowlines, precip):
    """calculate and add the bankfull width buffer attribute to nhd flowlines

    Args:
        nhd_flowlines (str): nhd flowlines feature class to add the bankfull width buffer
        precip (float): mean annual precipitation (mm) for the HUC
    """

    fieldname_bfbuffer = "BFwidth"
    p = precip / 10.0  # prism data in mm, need cm

    # Load Flowlines
    driver = ogr.GetDriverByName("ESRI Shapefile")
    source = driver.Open(nhd_flowlines, 1)
    layer = source.GetLayer()

    # Write Field BFbuffer_m
    bfField = ogr.FieldDefn(fieldname_bfbuffer, ogr.OFTReal)
    layer.CreateField(bfField)

    for feature in layer:
        a = feature.GetField("TotDASqKm")
        a = a if a is not None else 0.0
        w = 0.177 * (a ** 0.397) * (p ** 0.453)
        feature.SetField(fieldname_bfbuffer, w)
        layer.SetFeature(feature)
        feature = None

    layer = None
    source = None

    return


def buffer_by_field(flowlines, field, epsg, conversion_factor=1, min_buffer=None):
    """generate buffered polygons by value in field

    Args:
        flowlines (str): feature class of line features to buffer
        field (str): field with buffer value
        epsg (int): output srs
        conversion_factor (int, optional): apply a conversion value for the buffer value. Defaults to 1.
        min_buffer: use this buffer value for field values that are less than this (conversion factor will be applied)

    Returns:
        geometry: unioned polygon geometry of buffered lines
    """

    driver = ogr.GetDriverByName("ESRI Shapefile")
    source = driver.Open(flowlines, 0)
    layer = source.GetLayer()
    in_spatial_ref = layer.GetSpatialRef()

    out_spatial_ref, transform = get_transform_from_epsg(in_spatial_ref, epsg)

    outpolys = []
    for feature in layer:
        geom = feature.GetGeometryRef()
        bufferDist = feature.GetField(field) if feature.GetField(field) > min_buffer else min_buffer
        geom_buffer = geom.Buffer(bufferDist * conversion_factor)
        geom_buffer.Transform(transform)
        outpolys.append(wkt_load(geom_buffer.ExportToWkt()))

    # unary union
    outpoly = unary_union(outpolys)
    source = None

    return outpoly


def bankfull_width_buffer(flowlines, precip):
    """generate bankfull buffer polygon from precipitation value

    Args:
        flowlines (str): feature class of line features to buffer
        precip (str): mean annual precipitation (mm) for the HUC

    Returns:
        geometry: unioned polygon geometry of buffered lines 
    """

    p = precip / 10.0  # prism data in mm, need cm

    driver = ogr.GetDriverByName("ESRI Shapefile")
    source = driver.Open(flowlines, 1)
    layer = source.GetLayer()

    outpolys = []
    # Buffer width for each flow line feature
    for feature in layer:
        a = feature.GetField("TotDASqKm")
        a = a if a is not None else 0.0
        w = 0.177 * (a ** 0.397) * (p ** 0.453)
        geom = feature.GetGeometryRef()
        geom_buffer = geom.Buffer(w)
        outpolys.append(wkt_load(geom_buffer.ExportToWkt()))
        feature = None

    # unary union
    outpoly = unary_union(outpolys)
    source = None

    return outpoly
