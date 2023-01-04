""" Name:     Valley Bottom Extraction Tool (VBET)
            https://github.com/Riverscapes/riverscapes-tools/issues/639
            2022-12-09 06:31:19,946 DEBUG    [raster_update  ] Timer: 0.1753990600045654
            2022-12-09 06:31:20,243 DEBUG    [raster_update  ] Timer: 0.29747750000387896
            2022-12-09 06:31:20,491 DEBUG    [raster_update  ] Timer: 0.24729560199921252
            2022-12-09 06:31:20,562 ERROR    [Debug          ] Error executing code: Read or write failed. /usr/local/data/output/intermediates/hand_composite.tif,
            band 1: IReadBlock failed at X offset 0, Y offset 2609: TIFFReadEncodedStrip() failed.

a = [7,1,4,3]
a.sort(reverse=True)
> [7, 4, 3, 1]

"""

import os
import csv
import shutil
from osgeo import ogr, gdal
from rscommons import ProgressBar, Logger, initGDALOGRErrors, TimerWaypoints, LoopTimer
from vbet.vbet_raster_ops import raster_logic_mask


initGDALOGRErrors()


BIG_FILE_DIR = os.environ.get('BIG_FILE_DIR')


def get_files(directory, pattern='*.tif'):
    """method to recursively get all files in a directory that match a glob pattern

    Args:
        directory (_type_): _description_
        pattern (str, optional): _description_. Defaults to '*.tif'.

    Returns:
        _type_: _description_
    """
    file_list = []
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(pattern):
                file_list.append(os.path.join(root, filename))
    return file_list


def main():
    log = Logger('VBET BIGFILE ISSUE')
    log.setup(verbose=True)
    log.title('VBET BIGFILE ISSUE')
    log.debug('starting')
    _tmr_wpts = TimerWaypoints()

    csv_file = os.path.join(BIG_FILE_DIR, 'level_path_debug.csv')
    temp_folder = os.path.join(BIG_FILE_DIR, 'temp')
    temp_raster_folder = os.path.join(temp_folder, 'rasters')

    dem_raster = os.path.join(BIG_FILE_DIR, 'inputs', 'dem.tif')
    out_hand_pre = os.path.join(BIG_FILE_DIR, 'DEBUG_hand_composite_PRE.tif')
    # Outputs
    out_hand = os.path.join(BIG_FILE_DIR, 'DEBUG_hand_composite_NOVRT.tif')
    vrt_path = os.path.join(BIG_FILE_DIR, 'DEBUG_hand_composite_VRT.vrt')
    vrt2raster_path = os.path.join(BIG_FILE_DIR, 'DEBUG_hand_composite_VRT.tif')

    # User dictionaryReader to load CSV file
    level_paths = []
    with open(csv_file, 'r', encoding='utf8') as csv_f:
        reader = csv.DictReader(csv_f)
        for row in reader:
            level_paths.append(row)

    # First we build the composite HAND raster. THis isn't what we're debuggng so skip it if the file exists
    if not os.path.exists(out_hand_pre):
        _prg1 = ProgressBar(len(level_paths), 50, 'Build Composite Hand')
        counter = 0
        _lp_tmr1 = LoopTimer('Build Composite Hand', log, True)
        for lpath_row in level_paths:
            counter += 1
            level_path = lpath_row['level_path'] if lpath_row['level_path'] else 'None'
            local_hand = os.path.join(temp_raster_folder, f'local_hand_{level_path}.tif')
            valley_bottom_raster = os.path.join(temp_folder, f'levelpath_{level_path}', f'valley_bottom_{level_path}.tif')
            # raster_logic_mask(local_hand, out_hand_pre, dem_raster, valley_bottom_raster)
            _lp_tmr1.tick()
            _prg1.update(counter)

        _prg1.finish()
        _lp_tmr1.print()
        log.debug()

    _tmr_wpts.timer_break('building the composite raster using raster_merge')

    # Make a copy of the original so we don't have to keep rebuilding it.
    # if os.path.exists(out_hand):
    #     os.remove(out_hand)
    # shutil.copy(out_hand_pre, out_hand)
    # _tmr_wpts.timer_break('copying the old raster')
    # # Then we update it
    # _prg2 = ProgressBar(len(level_paths), 50, 'Update Composite Hand')
    # counter2 = 0
    # _lp_tmr2 = LoopTimer('Update Composite Hand', log, True)
    # for lpath_row in level_paths:
    #     counter2 += 1
    #     level_path = lpath_row['level_path'] if lpath_row['level_path'] else 'None'
    #     try:
    #         local_hand = os.path.join(temp_raster_folder, f'local_hand_{level_path}.tif')
    #         raster_update(out_hand, local_hand)
    #     except Exception as err:
    #         log.error(f'Error applying to raster: {level_path}')
    #         log.error(err)
    #         raise err
    #     _lp_tmr2.tick()
    #     _prg2.update(counter2)

    # _prg2.finish()
    # _lp_tmr2.print()
    # _tmr_wpts.timer_break('raster_update for loop')

    # Recompress the raster
    # raster_recompress(out_hand)
    # _tmr_wpts.timer_break('raster recompress')

    # Now let's try to build a VRT
    # Clean up
    if os.path.exists(vrt2raster_path):
        os.remove(vrt2raster_path)
    if os.path.exists(vrt_path):
        os.remove(vrt_path)

    vrt_rasters = []
    for lpath_row in level_paths:
        level_path = lpath_row['level_path'] if lpath_row['level_path'] else 'None'
        local_hand = os.path.join(temp_raster_folder, f'local_hand_{level_path}.tif')
        # vrt_rasters.append(os.path.relpath(local_hand, os.path.dirname(vrt_path)))
        vrt_rasters.append(local_hand)
    vrt_rasters.reverse()
    vrt_rasters.append(out_hand_pre)
    gdal.BuildVRT(vrt_path, vrt_rasters)
    _tmr_wpts.timer_break('VRT Built')
    # vrt2raster(vrt_path, vrt2raster_path)
    _tmr_wpts.timer_break('vrt2raster')

    log.info(_tmr_wpts.toString())


if __name__ == "__main__":
    main()
