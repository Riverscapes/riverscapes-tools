"""
Name:     Project Bounds

Purpose:  Support methods for generating project bounds for riverscapes
          projects

Author:   Kelly Whitehead

Date:     13 Jan 2022
"""
from typing import Dict

from osgeo import ogr
from osgeo.ogr import Geometry

from rscommons.vector_ops import collect_feature_class
from rscommons.classes.vector_classes import VectorBase

Path = str


def generate_project_extents_from_layer(bounding_layer: Path, output: Path, simplify_tolerance: float = 0.001) -> Dict[str, tuple]:
    """Generate a simplified polygon extent ( or convex hull for lines or points) from all geoms in the layer and return bounding box and centroid.

    Args:
        bounding_layer(Path): Layer to generate extents from. All geometries are merged first.
        output(Path): output path and filename for .geojson extent
        simplify_tolerance(float, optional): tolerance in decimal degrees. Defaults to 0.001.

    Returns:
        Dict[str, tuple]: {'CENTROID': (X, Y), "BBOX": (minX, maxX, minY, maxY)}
    """

    # this assumes bounding_layer is a simple type - fails for MULTIPOLYGON. returns ogr.Geometry. Consider changing.
    geom = collect_feature_class(bounding_layer)
    result = generate_project_extents_from_geom(geom, output, simplify_tolerance=simplify_tolerance)

    return result


def generate_project_extents_from_geom(geom: Geometry, output: Path, simplify_tolerance: float = 0.001) -> Dict[str, tuple]:
    """Generate a simplified polygon extent ( or convex hull for lines or points) from a geom and return bounding box and centroid.

    Args:
        geom(Geometry): Geometry to generate extents from
        output(Path): output path and filename for .geojson extent
        simplify_tolerance(float, optional): tolerance in decimal degrees. Defaults to 0.001.

    Returns:
        Dict[str, tuple]: {'CENTROID': (X, Y), "BBOX": (minX, maxX, minY, maxY)}
    """

    if geom.GetGeometryType() in VectorBase.POLY_TYPES:
        geom_extent = geom.SimplifyPreserveTopology(simplify_tolerance)
    else:
        # use the hull of the geometries
        geom_extent = geom.ConvexHull()

    extent = geom.GetEnvelope()

    geom_centroid = geom.Centroid()
    centroid = (geom_centroid.GetPoint(0)[0], geom_centroid.GetPoint(0)[1])

    save_as_geojson(geom_extent, output)

    return {'CENTROID': centroid, "BBOX": extent}


def save_as_geojson(geom: Geometry, output: Path, gtype=ogr.wkbPolygon) -> None:
    """ save geometry to geojson

    """
    # Save Extents Here
    outDriver = ogr.GetDriverByName('GeoJSON')
    # Create the output GeoJSON
    outDataSource = outDriver.CreateDataSource(output)
    outLayer = outDataSource.CreateLayer('Project Bounds', geom_type=gtype)
    featureDefn = outLayer.GetLayerDefn()
    outFeature = ogr.Feature(featureDefn)
    # Set new geometry
    outFeature.SetGeometry(geom)
    # Add new feature to output Layer
    outLayer.CreateFeature(outFeature)
    # dereference the feature
    outFeature = None
    # Save and close DataSources
    outDataSource = None


if __name__ == '__main__':
    # use argparse to run generate_project_extents_from_layer from the command line
    import argparse

    parser = argparse.ArgumentParser(description='Generate project extents from a layer')
    parser.add_argument('bounding_layer', type=str, help='Layer to generate extents from. All geometries are merged first.')
    parser.add_argument('output', type=str, help='output path and filename for .geojson extent')
    parser.add_argument('--simplify_tolerance', type=float, default=0.001, help='tolerance in decimal degrees. Defaults to 0.001.')
    args = parser.parse_args()

    generate_project_extents_from_layer(args.bounding_layer, args.output, args.simplify_tolerance)
