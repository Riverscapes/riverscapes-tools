from rscommons.hand import run_subprocess
import os

in_dem = '/mnt/c/Users/jordang/Documents/Riverscapes/data/anthro/test_data/dem_full.tif'

path_pitfill = os.path.join(os.path.dirname(in_dem), 'pitfill.tif')
pitfill_status = run_subprocess(os.path.dirname(in_dem), ["mpiexec", "-n", "2", "pitremove", "-z", in_dem, "-fel", path_pitfill])
if pitfill_status != 0 or not os.path.isfile(path_pitfill):
    raise Exception('TauDEM: pitfill failed')

path_d8fd = os.path.join(os.path.dirname(in_dem), 'flowdirection.tif')
path_d8slope = os.path.join(os.path.dirname(in_dem), 'slope.tif')
fd_status = run_subprocess(os.path.dirname(in_dem), ["mpiexec", "-n", "2", "d8flowdir", "-fel", path_pitfill, "-p", path_d8fd, "-sd8", path_d8slope])
if fd_status != 0 or not os.path.isfile(path_d8fd):
    raise Exception('Flow Direction failed')
