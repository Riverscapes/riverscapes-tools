import argparse
import sys
import os
import traceback
import rscommons.shapefile
from rsxml import Logger, ProgressBar, dotenv
from rscommons.util import safe_makedirs

from osgeo import ogr, osr


def cleaner(inpath, outpath):
    driver = ogr.GetDriverByName("ESRI Shapefile")
    inDataSource = driver.Open(inpath, 0)
    inLayer = inDataSource.GetLayer()

    geom_type = inLayer.GetGeomType()
    inSpatialRef = inLayer.GetSpatialRef()

    safe_makedirs(os.path.dirname(outpath))
    if os.path.exists(outpath) and os.path.isfile(outpath):
        driver.DeleteDataSource(outpath)

    # Create the output shapefile
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromWkt(inSpatialRef.ExportToWkt())
    outDataSource = driver.CreateDataSource(outpath)
    outLayer = outDataSource.CreateLayer('network', outSpatialRef, geom_type=geom_type)
    outLayerDefn = outLayer.GetLayerDefn()

    # Create new fields in the output shp and get a list of field names for feature creation
    fieldNames = []
    for i in range(inLayer.GetLayerDefn().GetFieldCount()):
        fieldDefn = inLayer.GetLayerDefn().GetFieldDefn(i)
        outLayer.CreateField(fieldDefn)
        fieldNames.append(fieldDefn.name)

    total = inLayer.GetFeatureCount()
    counter = 0
    nongeom = 0

    progbar = ProgressBar(inLayer.GetFeatureCount(), 50, "Unioning features")
    for feature in inLayer:
        progbar.update(counter)

        ingeom = feature.GetGeometryRef()
        fieldVals = []  # make list of field values for feature
        for f in fieldNames:
            fieldVals.append(feature.GetField(f))

        if ingeom:
            outFeature = ogr.Feature(outLayerDefn)
            sys.stdout.write("\nProcessing feature: {}/{}  {}: nongeometrics found. ".format(counter, total, nongeom))
            # sys.stdout.flush()
            # Buffer by 0 cleans everything up and makes it nice
            sys.stdout.write('  BUFFERING...')
            geomBuffer = ingeom.Buffer(0)
            sys.stdout.write('SETTING...')
            outFeature.SetGeometry(geomBuffer)
            sys.stdout.write('SETTING...')
            for v, val in enumerate(fieldVals):  # Set output feature attributes
                sys.stdout.write('FIELDS...')
                outFeature.SetField(fieldNames[v], val)
            sys.stdout.write('WRITING...')
            outLayer.CreateFeature(outFeature)

            outFeature = None
            sys.stdout.write('DONE!\n')

        else:
            # DISCARD features with no geometry
            nongeom += 1

        counter += 1

    progbar.finish()
    inDataSource = None
    outDataSource = None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='shapefile in', type=str)
    parser.add_argument('output', help='shapefile out', type=str)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    # log = Logger("BRAT Inputs")
    # log.setup(log_path=os.path.join(args.output, "rs_context.log"))

    try:
        cleaner(args.input, args.output)

    except Exception as e:
        # log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
