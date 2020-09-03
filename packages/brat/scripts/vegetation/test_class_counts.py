# Philip Bailey
# 24 Oct 2019
# # Unit tests for how land use intensity math works
import os
import sys
import numpy as np
import numpy.ma as ma


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
land_use = __import__('land_use', fromlist=[''])


x = np.array([0.66, 0.33, 1.5, 0, -1.0, 0.7546, 0, 7000, 0.34, 0.23])
mx = ma.masked_array(x, mask=[0, 0, 1, 1, 0, 1, 0, 0, 0, 1])

print('Regular mean:', x.mean())
print('Masked mean:', mx.mean())

counts = land_use.get_class_counts(mx)
print(counts)
