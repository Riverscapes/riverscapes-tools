from scipy import ndimage
import numpy as np

in_array = np.array(([1, 0, 0, 0, 0],
                     [0, 1, 0, 0, 0],
                     [0, 1, 0, 0, 0],
                     [0, 0, 1, 0, 0],
                     [0, 0, 0, 1, 0]))

dist = ndimage.distance_transform_edt(np.logical_not(in_array))
print(dist)
