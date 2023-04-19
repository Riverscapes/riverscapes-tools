cimport cython
import numpy as np
cimport numpy as np

DTYPE = np.intc

@cython.boundscheck(False)
@cython.wraparound(False)
def access_algorithm(int[:, :] fdarray, int fd_nd, int[:, :] chan_a, int chan_nd, int[:, :] r_a, int road_nd,
                     int[:, :] rr_a, int rail_nd, int[:, :] c_a, int canal_nd, int[:, :] vb_a, int vb_nd):
    
    cdef list subprocessed

    cdef Py_ssize_t x_max = fdarray.shape[0]
    cdef Py_ssize_t y_max = fdarray.shape[1]

    cdef int row, col
    cdef int rowa, cola

    out_array = np.full((x_max, y_max), fd_nd, dtype=DTYPE)
    cdef int[:, :] out_view = out_array

    for row in range(x_max):
        for col in range(y_max):
            if vb_a[row, col] == vb_nd:
                continue
            if out_view[row, col] != fd_nd:
                continue
            else:
                subprocessed = [[row, col]]

                next_cell = fdarray[row, col]
                rowa, cola = row, col
                while next_cell is not None:
                    if next_cell == fd_nd:
                        # for coord in subprocessed:
                        #     if coord not in processed:
                        #         processed.append(coord)
                        next_cell = None
                    if out_view[rowa, cola] != fd_nd:
                        for coord in subprocessed:
                            out_view[coord[0], coord[1]] = out_view[rowa, cola]
                        next_cell = None
                    if chan_a[rowa, cola] != chan_nd:
                        for coord in subprocessed:
                            out_view[coord[0], coord[1]] = 1
                        next_cell = None
                    if r_a[rowa, cola] != road_nd:
                        for coord in subprocessed:
                            out_view[coord[0], coord[1]] = 0
                        next_cell = None
                    if rr_a[rowa, cola] != rail_nd:
                        for coord in subprocessed:
                            out_view[coord[0], coord[1]] = 0
                        next_cell = None
                    if c_a[rowa, cola] != canal_nd:
                        for coord in subprocessed:
                            out_view[coord[0], coord[1]] = 0
                        next_cell = None

                    if next_cell is not None:
                        if next_cell == 1:
                            rowa = rowa
                            cola = cola + 1
                        elif next_cell == 2:
                            rowa = rowa - 1
                            cola = cola + 1
                        elif next_cell == 3:
                            rowa = rowa - 1
                            cola = cola
                        elif next_cell == 4:
                            rowa = rowa - 1
                            cola = cola - 1
                        elif next_cell == 5:
                            rowa = rowa
                            cola = cola - 1
                        elif next_cell == 6:
                            rowa = rowa + 1
                            cola = cola - 1
                        elif next_cell == 7:
                            rowa = rowa + 1
                            cola = cola
                        elif next_cell == 8:
                            rowa = rowa + 1
                            cola = cola + 1
                        if [rowa, cola] in subprocessed:
                            print('circular flow path, could not resolve connectivity')
                            for coord in subprocessed:
                                out_view[coord[0], coord[1]] = 2
                            next_cell = None
                        else:
                            subprocessed.append([rowa, cola])
                            print(f'subprocessed {len(subprocessed)} cells')
                            next_cell = fdarray[rowa, cola]

    return out_array