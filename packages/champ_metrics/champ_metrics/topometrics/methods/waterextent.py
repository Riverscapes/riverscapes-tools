import os
import sys
import numpy as np
from champ_metrics.lib.raster import Raster
from champ_metrics.lib.shapefileloader import Shapefile
from champ_metrics.lib.metrics import CHaMPMetric
from champ_metrics.lib.exception import MissingException

# Metrics
#   area = sum of area of all water extent polygon features
#   volume  = sum of all water extent polygon features in depth raster
#   integrated width = area / mainstem centerline length


class WaterExtentMetrics(CHaMPMetric):

    TEMPLATE = {
        'Area': None,
        'Volume': None,
        'Depth': {
            'Max': None,
            'Mean': None
        },
        'IntegratedWidth': None}

    def calc(self, channelName, shpWaterExtent, shpCenterline, rasDepth):

        if not os.path.isfile(shpCenterline):
            raise MissingException("Missing centerline shapefile")
        if not os.path.isfile(shpWaterExtent):
            raise MissingException("Missing water extent shapefile")
        if not os.path.isfile(rasDepth):
            raise MissingException("Missing depth raster")

        # Load the depth raster
        depthRaster = Raster(rasDepth)

        # Load the water extent ShapeFile and sum the area of all features
        shpExtent = Shapefile(shpWaterExtent)
        feats = shpExtent.featuresToShapely()

        arrMask = depthRaster.rasterMaskLayer(shpWaterExtent)
        self.metrics['Area'] = 0
        # TODO: See issue #79 and #15. We need to use a different strategy if we want volume for bankfull https://github.com/SouthForkResearch/CHaMP_Metrics/issues/79
        if channelName == "Bankfull":
            self.metrics['Volume'] = None
            for aFeat in feats:
                self.metrics['Area'] += aFeat['geometry'].area

        else:
            self.metrics['Volume'] = np.sum(depthRaster.array[arrMask > 0]) * (depthRaster.cellWidth ** 2)

            for aFeat in feats:
                self.metrics['Area'] += aFeat['geometry'].area
                dDepth = {}
                dDepth['Max'] = float(np.amax(depthRaster.array[arrMask > 0]))
                dDepth['Mean'] = np.mean(depthRaster.array[arrMask > 0])
                self.metrics['Depth'] = dDepth

        # Retrieve the centerline mainstem. If the ShapeFile possesses a 'Channel field then
        # find the 1 feature with a value of 'Main', otherwise this is simply the only ShapeFile feature
        mainstem = self._getMainstemGeometry(shpCenterline)

        self.metrics['IntegratedWidth'] = None
        if mainstem and mainstem.length > 0:
            self.metrics['IntegratedWidth'] = self.metrics['Area'] / mainstem.length

    def _getMainstemGeometry(self, shpCenterline):

        mainstem = None

        shp = Shapefile(shpCenterline)
        feats = shp.featuresToShapely()
        if 'Channel' in shp.fields:
            for feat in feats:
                if 'Channel' in feat['fields'] and \
                        feat['fields']['Channel'] is not None and \
                        feat['fields']['Channel'].lower() == 'main':
                    mainstem = feat['geometry']
                    break
        else:
            mainstem = feats[0]['geometry']

        return mainstem


if __name__ == "__main__":
    import argparse
    import logging

    logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
    dtfmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(filename='channel_units.log', level=logging.DEBUG, format=logfmt, datefmt=dtfmt)

    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('waterextent',
                        help='Path to the water extent shapefile',
                        type=argparse.FileType('r'))

    parser.add_argument('centerline',
                        help='Path to centerline shapefile',
                        type=argparse.FileType('r'))

    parser.add_argument('depth',
                        help='Path to depth raster',
                        type=argparse.FileType('r'))

    args = parser.parse_args()

    if not args.waterextent or not args.centerline or not args.depth:
        print("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    try:

        dMetrics = WaterExtentMetrics(args.waterextent.name, args.centerline.name, args.depth.name)

    except AssertionError as e:
        sys.exit(0)
    except Exception as e:
        raise
        sys.exit(0)
