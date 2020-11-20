# The goal of this script is to validate the drainage area values used in Idaho BRAT with those of
# provided with the NHD Plus HR data

# import math
# import json
import argparse
import os
# import sys
# import traceback
# import json
# import time
from osgeo import ogr, osr
from rscommons import Logger, LoopTimer, dotenv, initGDALOGRErrors
from rscommons import plotting

initGDALOGRErrors()

expected_field = 'iGeo_DA'
nhdplushr_filed = 'TotDASqKm'


def drainage_area_validation(expected, nhdplushr):
    log = Logger("BRAT Drainage Area")
    TIMER_overall = LoopTimer("DA OVERALL")
    driver = ogr.GetDriverByName("ESRI Shapefile")

    # Load manually prepared drainage area values into a dictionary of "to nodes" as keys with DA as the values.
    data_expected = driver.Open(expected, 0)
    expectedLayer = data_expected.GetLayer()
    expectedSpatialRef = expectedLayer.GetSpatialRef()

    # Now load all the NHD Plus HR features and attempt to match up the coordinates
    chart_values = []
    data_source = driver.Open(nhdplushr, 0)
    layer = data_source.GetLayer()
    nhdSpatialRef = layer.GetSpatialRef()

    transform = osr.CoordinateTransformation(nhdSpatialRef, expectedSpatialRef)

    TIMER_compare = LoopTimer("Compare Expected", useMs=True)

    matchFound = 0
    totalPoints = 0

    for feature in layer:
        geom = feature.GetGeometryRef()
        geom.Transform(transform)

        pts = geom.GetPoints()
        # Slice off the elevation value for simplicity
        to_point = pts[-1][0:2]
        totalPoints += 1

        # Set a crude rectangle to filter by
        expectedLayer.SetSpatialFilterRect(to_point[0] - 5, to_point[1] - 5, to_point[0] + 5, to_point[1] + 5)

        #  If you are using an attribute filter ( SetAttributeFilter() ) or spatial filter ( SetSpatialFilter() or SetSpatialFilterRect() ) then you have to use GetNextFeature().
        expected_da = {}
        for expected_feat in expectedLayer:
            georef = expected_feat.GetGeometryRef()
            expectedPts = georef.GetPoints()
            expected_da[expectedPts[-1][0:2]] = expected_feat.GetField(expected_field)

        # Use the one with the least distance
        if len(expected_da) > 0:
            smallest_dist = ()
            for expected_point, da in expected_da.items():
                point1 = ogr.Geometry(ogr.wkbPoint)
                point1.AddPoint(*expected_point)

                point2 = ogr.Geometry(ogr.wkbPoint)
                point2.AddPoint(*to_point)

                dist = point2.Distance(point1)

                if dist < 0.1 and (len(smallest_dist) == 0 or smallest_dist[1] > dist):
                    smallest_dist = (da, dist)

            if smallest_dist:
                matchFound += 1
                feat_da = feature.GetField(nhdplushr_filed)
                if not type(da) is float:
                    log.error("Found invalid expected DA of type {}".format(type(da)))
                elif not type(feat_da) is float:
                    log.error("Found invalid nhdplus DA of type {}".format(type(feat_da)))
                else:
                    chart_values.append((da, feat_da, expected_point))

        TIMER_compare.progprint()
        TIMER_compare.tick()
        feature = None
    data_source = None

    TIMER_overall.print("Final")
    log.info("Points with match: {} / {}".format(matchFound, totalPoints))
    title = 'Idaho BRAT drainage area values against NHD Plus HR'
    plotting.validation_chart([x[0:2] for x in chart_values], title)

    """
    
    Let's write a fun little geojson file so we can compare these thigns spatially

    """

    # Create the output GeoJSON
    # geoJsonfName = title.replace(' ', '_').lower() + '.geojson'
    # geojsonPath = os.path.join('docs/assets/images/validation', geoJsonfName)

    # output = {
    #     "type": "FeatureCollection",
    #     "name": geoJsonfName,
    #     "features": []
    # }

    # for expectedDa, nhdDa, expected_point in chart_values:
    #     output["features"].append({
    #         "type": "Feature",
    #         "properties": {
    #             "left": expectedDa,
    #             "right": nhdDa,
    #             "diff": expectedDa - nhdDa,
    #             "diffPercent": math.fabs(expectedDa - nhdDa) / nhdDa if nhdDa > 0 else 0,
    #         },
    #         "geometry": {
    #             "type": "Point",
    #             "coordinates": expected_point
    #         }
    #     })

    # with open(geojsonPath, 'w') as outfile:
    #     json.dump(output, outfile)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('expected', help='Shapefile prepared by analysts possessing manually validated drainage area', type=str)
    parser.add_argument('nhdplushr', help='Shapefile with NHDPlusHR drainage area values', type=str)
    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '..', '..', '.env.validation'))

    log = Logger("BRAT Drainage Area")
    log.setup(verbose=True)

    drainage_area_validation(args.expected, args.nhdplushr)


if __name__ == '__main__':
    main()
