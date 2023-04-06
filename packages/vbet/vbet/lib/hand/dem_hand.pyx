# cython: infer_types=True
import numpy as np
cimport cython
cimport numpy as cnp

@cython.boundscheck(False)
@cython.wraparound(False)
def calc_hand(cnp.ndarray[double, ndim=2] dem_array, cnp.ndarray[double, ndim=2] chan_array, double nodata):

    cdef Py_ssize_t j_max = dem_array.shape[0]
    cdef Py_ssize_t i_max = dem_array.shape[1]

    hand_array = np.full((j_max, i_max), nodata)
    cdef double[:, :] hand_array_view = hand_array

    row_vals = []
    col_vals = []

    cdef Py_ssize_t row, col

    for row in range(j_max):
        for col in range(i_max):
            if chan_array[row, col] == 1:
                row_vals.append(row)
                col_vals.append(col)
    row_vals = np.asarray(row_vals)
    col_vals = np.asarray(col_vals)

    nearest = {}

    cdef Py_ssize_t i, j

    for j in range(j_max):
        for i in range(i_max):
            if dem_array[j, i] == nodata:
                continue
            dist_arr = np.sqrt((j - row_vals)**2 + (i - col_vals)**2)
            min_ind = np.argmin(dist_arr)
            hand_val = max(0, dem_array[j, i] - dem_array[row_vals[min_ind], col_vals[min_ind]])

            hand_array_view[j, i] = hand_val

    return hand_array
