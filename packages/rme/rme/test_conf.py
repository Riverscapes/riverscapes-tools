import os

from osgeo import ogr

from shapely.ops import split, unary_union, linemerge, nearest_points
from shapely.geometry import LineString, MultiLineString, Point, MultiPoint

from rscommons.vector_ops import copy_feature_class, get_geometry_unary_union
from rscommons import GeopackageLayer, get_shp_or_gpkg, ProgressBar
from rscommons import VectorBase


def confinement(valley, dgos, channel_area, flowlines, output_folder, channel_buffer):

    # copy DGOS to output folder
    out_gpkg = os.path.join(output_folder, 'confinement.gpkg')
    intermediates_gpkg = os.path.join(output_folder, 'intermediates', 'confinement_intermediates.gpkg')
    out_dgos = os.path.join(out_gpkg, 'confinement_dgos')
    copy_feature_class(dgos, out_dgos)

    with get_shp_or_gpkg(flowlines) as flw_lyr:
        srs = flw_lyr.spatial_ref
        meter_conversion = flw_lyr.rough_convert_metres_to_vector_units(1)
        offset = flw_lyr.rough_convert_metres_to_vector_units(0.1)

    # create confining margins layer
    with GeopackageLayer(intermediates_gpkg, layer_name='confining_margins', write=True) as margins_lyr, \
            get_shp_or_gpkg(channel_area) as chan_lyr, \
            get_shp_or_gpkg(valley) as valley_lyr, \
            get_shp_or_gpkg(flowlines) as flowline_lyr:
        margins_lyr.create(ogr.wkbLineString, spatial_ref=srs)
        margins_lyr.ogr_layer.CreateField(ogr.FieldDefn('Side', ogr.OFTString))
        margins_lyr.ogr_layer.CreateField(ogr.FieldDefn('ApproxLeng', ogr.OFTReal))

        progbar = ProgressBar(chan_lyr.ogr_layer.GetFeatureCount(), 'Creating confining margins')
        counter = 0
        for chan_feat, _counter, _progbar in chan_lyr.iterate_features():
            progbar.update(counter)
            counter += 1
            chan_geom = VectorBase.ogr2shapely(chan_feat.GetGeometryRef()).buffer(channel_buffer * meter_conversion)
            flowline_lyr.ogr_layer.SetSpatialFilter(chan_feat.GetGeometryRef())
            segs = [VectorBase.ogr2shapely(ftr) for ftr in flowline_lyr.ogr_layer if ftr.GetGeometryRef().Within(chan_feat.GetGeometryRef())]
            if len(segs) == 0:
                print('skipping ftr, no flowline with channel area')
                continue
            elif len(segs) > 1:
                _flowline_clip = MultiLineString(segs)
                flowline_clip = linemerge(_flowline_clip)
            else:
                flowline_clip = segs[0]

            # split into left and right
            geom_coords = MultiPoint([coord for coord in chan_geom.exterior.coords])
            start = nearest_points(Point(flowline_clip.coords[0]), geom_coords)[1]
            end = nearest_points(Point(flowline_clip.coords[-1]), geom_coords)[1]
            geom_flowline_extended = LineString([start] + [pt for pt in flowline_clip.coords] + [end])
            channel_split = split(chan_geom, geom_flowline_extended)

            geom_offset = flowline_clip.parallel_offset(offset, 'left')
            geom_side_point = geom_offset.interpolate(0.5, normalized=True)
            print(len(channel_split))
            for geom in channel_split:
                if geom.contains(geom_side_point):
                    left_geom = geom
                else:
                    right_geom = geom

            # create left margin
            valley_lyr.ogr_layer.SetSpatialFilter(VectorBase.shapely2ogr(left_geom))
            for valley_feat, _counter, _progbar in valley_lyr.iterate_features():
                valley_geom = VectorBase.ogr2shapely(valley_feat.GetGeometryRef())
                dif_l = left_geom.difference(valley_geom)
                if not dif_l.is_empty:
                    if dif_l.type == 'MultiPolygon':
                        for dif_geom in dif_l:
                            coords = []
                            for coord in dif_geom.exterior.coords:
                                if coord not in valley_geom.exterior.coords:
                                    coords.append(coord)
                            if len(coords) > 1:
                                line = LineString(coords)
                                if line.is_valid:
                                    margins_lyr.create_feature(line, {'Side': 'left', 'ApproxLeng': line.length})
                    elif dif_l.type == 'Polygon':
                        coords = []
                        for coord in dif_l.exterior.coords:
                            if coord not in valley_geom.exterior.coords:
                                coords.append(coord)
                        if len(coords) > 1:
                            line = LineString(coords)
                            if line.is_valid:
                                margins_lyr.create_feature(line, {'Side': 'left', 'ApproxLeng': line.length})
                dif_r = right_geom.difference(valley_geom)
                if not dif_r.is_empty:
                    if dif_r.type == 'MultiPolygon':
                        for dif_geom in dif_r:
                            coords = []
                            for coord in dif_geom.exterior.coords:
                                if coord not in valley_geom.exterior.coords:
                                    coords.append(coord)
                            if len(coords) > 1:
                                line = LineString(coords)
                                if line.is_valid:
                                    margins_lyr.create_feature(line, {'Side': 'right', 'ApproxLeng': line.length})
                    elif dif_r.type == 'Polygon':
                        coords = []
                        for coord in dif_r.exterior.coords:
                            if coord not in valley_geom.exterior.coords:
                                coords.append(coord)
                        if len(coords) > 1:
                            line = LineString(coords)
                            if line.is_valid:
                                margins_lyr.create_feature(line, {'Side': 'right', 'ApproxLeng': line.length})

    print('something')


vb_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/vbet/1601020204/outputs/vbet.gpkg/vbet_full'
dgos_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/vbet/1601020204/intermediates/vbet_intermediates.gpkg/vbet_dgos'
channel_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/channel_area/1601020204/outputs/channel_area.gpkg/channel_area'
flowlines_in = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rs_context/1601020204/hydrology/nhdplushr.gpkg/NHDFlowline'
out_folder = '/mnt/c/Users/jordang/Documents/Riverscapes/data/confinement/test/'
chan_buf = 15

confinement(vb_in, dgos_in, channel_in, flowlines_in, out_folder, chan_buf)
