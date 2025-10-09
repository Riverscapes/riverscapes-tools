import sys

from os import path
from champmetrics.lib.shapefileloader import Shapefile
from champmetrics.lib.metrics import CHaMPMetric

class IslandMetrics(CHaMPMetric):

    TEMPLATE = {'Count': None, 'Area': None, 'QualifyingCount': None, 'QualifyingArea': None}

    def calc(self, shpIslands):
        """

        :param shpIslands:
        :return:
        """
        self.metrics = {'Count': 0, 'Area': 0.0, 'QualifyingCount': 0, 'QualifyingArea': 0.0}

        if not path.isfile(shpIslands):
            return

        shp = Shapefile(shpIslands)
        feats = shp.featuresToShapely()
        if feats:
            for aFeat in feats:

                self.metrics['Count'] += 1
                self.metrics['Area'] += aFeat['geometry'].area

                if 'IsValid' in shp.fields:
                    if aFeat['fields']['IsValid'] == 1:
                        self.metrics['QualifyingCount'] += 1
                        self.metrics['QualifyingArea'] += aFeat['geometry'].area

    def _getMainstemGeometry(self, shpCenterline):
        """

        :param shpCenterline:
        :return:
        """
        mainstem = None

        shp = Shapefile(shpCenterline)
        feats = shp.featuresToShapely()
        if 'Channel' in shp.fields:
            for featureIndex in range(0, feats.count()):
                if shp.fields[featureIndex] == 'Main':
                    mainstem = shp.features[featureIndex]['geometry']
                    break
        else:
            mainstem = shp.features[0]['geometry'][0]

        return mainstem


if __name__ == "__main__":
    import argparse, logging
    logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
    dtfmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(filename='islands.log', level=logging.DEBUG, format=logfmt, datefmt=dtfmt)

    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('islands',
                        help='Path to the island shapefile',
                        type=argparse.FileType('r'))

    args = parser.parse_args()

    if not args.islands:
        print("ERROR: Missing arguments")
        parser.print_help()
        exit(0)

    try:
        dMetrics = IslandMetrics(args.islands.name)

    except AssertionError as e:
        sys.exit(0)
    except Exception as e:
        raise
        sys.exit(0)
