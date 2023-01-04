""" VBET Raster Operations

    Purpose:  Tools to support VBET raster operations
    Author:   North Arrow Research
    Date:     August 2022
"""

import os

from vbet.vbet_raster_ops import create_empty_raster, raster_update, raster_logic_mask, raster_recompress
from rscommons import LoopTimer, Logger

ROOT_FOLDER = '/data/vbet/compression'

log = Logger('COMPRESSION')
log.setup(verbose=True)

TEMPLATE_RASTER = os.path.join(ROOT_FOLDER, 'dem.tif')
COMPOSITE_HAND = os.path.join(ROOT_FOLDER, 'composite_hand.tif')
COMPOSITE_HAND2 = os.path.join(ROOT_FOLDER, 'composite_hand2.tif')

if os.path.isfile(COMPOSITE_HAND):
    os.remove(COMPOSITE_HAND)
if os.path.isfile(COMPOSITE_HAND2):
    os.remove(COMPOSITE_HAND2)

level_paths = []


for f in os.scandir(os.path.join(ROOT_FOLDER, 'temp')):
    if not os.path.isdir(f.path) or 'levelpath_' not in f.name:
        continue
    log.info(f.path)
    level_path = f.name.replace('levelpath_', '')
    local_hand = os.path.join(ROOT_FOLDER, 'temp', 'rasters', f'local_hand_{level_path}.tif')
    if not os.path.isfile(local_hand):
        continue
    level_paths.append((f.path, level_path, local_hand))


_lt = LoopTimer('raster_merge', log, True)
for level_path_dir, level_path, local_hand in level_paths:
    raster_logic_mask(local_hand, COMPOSITE_HAND, TEMPLATE_RASTER, os.path.join(level_path_dir, f'valley_bottom_{level_path}.tif'))
    _lt.tick()
_lt.print()
raster_recompress(COMPOSITE_HAND)


# _lt = LoopTimer('raster_merge_TMP', log, True)
# for level_path_dir, level_path, local_hand in level_paths:
#     raster_merge_TMP(local_hand, COMPOSITE_HAND2, TEMPLATE_RASTER, os.path.join(level_path_dir, f'valley_bottom_{level_path}.tif'))
#     _lt.tick()
# _lt.print()
