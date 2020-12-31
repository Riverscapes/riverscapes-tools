""" Calculates the average vegetation suitability for each reach. It takes
    the raw areas of each vegetation type within a buffer and converts
    these values into suitabilities using the lookup in the BRAT database.
    The average suitability is then written to the appropriate column in
    the reaches table.

    Philip Bailey
    17 Jan 2019
"""
import argparse
import os
import sys
import traceback
import rasterio
import numpy as np
from rscommons import Logger, ProgressBar, dotenv
from rscommons.database import write_attributes_NEW, SQLiteCon


def vegetation_suitability(gpkg_path: str, buffer: float, prefix: str, ecoregion: str):
    """Calculate vegetation suitability for each reach in a BRAT SQLite
    database

    Arguments:
        database {str} -- Path to BRAT SQLite database
        buffer {float} -- Distance to buffer reach polyline to obtain vegetation
        prefix {str} -- Either 'EX' for existing or 'HPE' for historic.
        ecoregion {int} -- Database ID of the ecoregion associated with the watershed
    """

    veg_col = 'iVeg{}{}{}'.format('_' if len(str(int(buffer))) < 3 else '', int(buffer), prefix)

    reaches = calculate_vegetation_suitability(gpkg_path, buffer, prefix, veg_col, ecoregion)
    write_attributes_NEW(gpkg_path, reaches, [veg_col])


def calculate_vegetation_suitability(gpkg_path: str, buffer: float, epoch: str, veg_col: str, ecoregion: str) -> dict:
    """ Calculation vegetation suitability

    Args:
        gpkg_path ([type]): [description]
        buffer ([type]): [description]
        epoch ([type]): [description]
        veg_col ([type]): [description]
        ecoregion ([type]): [description]

    Raises:
        Exception: [description]

    Returns:
        [type]: [description]
    """

    log = Logger('Veg Suitability')
    log.info('Buffer: {}'.format(buffer))
    log.info('Epoch: {}'.format(epoch))
    log.info('Veg Column: {}'.format(veg_col))

    with SQLiteCon(gpkg_path) as database:

        # Get the database epoch that has the prefix 'EX' or 'HPE' in the metadata
        database.curs.execute('SELECT EpochID FROM Epochs WHERE Metadata = ?', [epoch])
        epochid = database.curs.fetchone()['EpochID']
        if not epochid:
            raise Exception('Missing epoch in database with metadata value of "{}"'.format(epoch))

        database.curs.execute('SELECT R.ReachID, Round(SUM(CAST(IFNULL(OverrideSuitability, DefaultSuitability) AS REAL) * CAST(CellCount AS REAL) / CAST(TotalCells AS REAL)), 2) AS VegSuitability'
                              ' FROM vwReaches R'
                              ' INNER JOIN Watersheds W ON R.WatershedID = W.WatershedID'
                              ' INNER JOIN Ecoregions E ON W.EcoregionID = E.EcoregionID'
                              ' INNER JOIN ReachVegetation RV ON R.ReachID = RV.ReachID'
                              ' INNER JOIN VegetationTypes VT ON RV.VegetationID = VT.VegetationID'
                              ' INNER JOIN Epochs EP ON VT.EpochID = EP.EpochID'
                              ' INNER JOIN('
                              ' SELECT ReachID, SUM(CellCount) AS TotalCells'
                              ' FROM ReachVegetation RV'
                              ' INNER JOIN VegetationTypes VT ON RV.VegetationID = VT.VegetationID'
                              ' INNER JOIN Epochs E ON E.EpochID = VT.EpochID'
                              ' WHERE Buffer = ? AND E.Metadata = ?'
                              ' GROUP BY ReachID) AS RS ON R.ReachID = RS.ReachID'
                              ' LEFT JOIN VegetationOverrides VO ON E.EcoregionID = VO.EcoregionID AND VT.VegetationID = VO.VegetationID'
                              ' WHERE (Buffer = ?) AND (EP.Metadata = ?)  AND (E.EcoregionID = ? OR E.EcoregionID IS NULL)'
                              ' GROUP BY R.ReachID', [buffer, epoch, buffer, epoch, ecoregion])
        results = {row['ReachID']: {veg_col: row['VegSuitability']} for row in database.curs.fetchall()}

    log.info('Vegetation suitability complete')
    return results


def output_vegetation_raster(gpkg_path, raster_path, output_path, epoch, prefix, ecoregion):
    """Output a vegetation suitability raster. This has no direct use in the process
    but it's useful as a reference layer and visual aid.

    Arguments:
        database {str} -- Path to BRAT SQLite database
        raster_path {str} -- path to input raster
        output_path {str} -- path to output raster
        epoch {str} -- Label identifying either 'existing' or 'historic'. Used for log messages only.
        prefix {str} -- Either 'EX' for existing or 'HPE' for historic.
        ecoregion {int} -- Database ID of the ecoregion associated with the watershed
    """
    log = Logger('Veg Suitability Rasters')
    log.info('Epoch: {}'.format(epoch))

    with SQLiteCon(gpkg_path) as database:

        # Get the database epoch that has the prefix 'EX' or 'HPE' in the metadata
        database.curs.execute('SELECT EpochID FROM Epochs WHERE Metadata = ?', [prefix])
        epochid = database.curs.fetchone()['EpochID']
        if not epochid:
            raise Exception('Missing epoch in database with metadata value of "{}"'.format(epoch))

        database.curs.execute('SELECT VegetationID, EffectiveSuitability '
                              'FROM vwVegetationSuitability '
                              'WHERE EpochID = ? AND EcoregionID = ?', [epochid, ecoregion])
        results = {row['VegetationID']: row['EffectiveSuitability'] for row in database.curs.fetchall()}

    def translate_suit(in_val, in_nodata, out_nodata):
        if in_val == in_nodata:
            return out_nodata
        elif in_val in results:
            return results[in_val]
        log.warning('Could not find {} VegetationID={}'.format(prefix, in_val))
        return -1

    vector = np.vectorize(translate_suit)

    with rasterio.open(raster_path) as source_ds:
        out_meta = source_ds.meta
        out_meta['dtype'] = 'int16'
        out_meta['nodata'] = -9999
        out_meta['compress'] = 'deflate'

        with rasterio.open(output_path, "w", **out_meta) as dest_ds:
            progbar = ProgressBar(len(list(source_ds.block_windows(1))), 50, "Writing Vegetation Raster: {}".format(epoch))
            counter = 0
            for ji, window in dest_ds.block_windows(1):
                progbar.update(counter)
                counter += 1
                in_data = source_ds.read(1, window=window, masked=True)

                # Fill the masked values with the appropriate nodata vals
                # Unthresholded in the base band (mostly for debugging)
                out_data = vector(in_data, source_ds.meta['nodata'], out_meta['nodata'])
                dest_ds.write(np.int16(out_data), window=window, indexes=1)

            progbar.finish()


def main():
    """ Vegetation Suitability
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT database path', type=argparse.FileType('r'))
    parser.add_argument('buffer', help='buffer distance (metres)', type=float)
    parser.add_argument('epoch', help='Existing or Historic', type=str)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    logg = Logger('Veg Summary')
    logfile = os.path.join(os.path.dirname(args.database.name), 'vegetation_summary.log')
    logg.setup(logPath=logfile, verbose=args.verbose)

    try:
        vegetation_suitability(args.database.name, args.raster.name, args.buffer, args.table)

    except Exception as e:
        logg.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
