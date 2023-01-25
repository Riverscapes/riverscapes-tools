cimport cython
import numpy as np

DTYPE = np.intc

@cython.boundscheck(False)
@cython.wraparound(False)
def access_algorithm(int[:, :] fdarray, int fd_nd, int[:, :] chan_a, int chan_nd, int[:, :] r_a, int road_nd,
                     int[:, :] rr_a, int rail_nd, int[:, :] c_a, int canal_nd, int[:, :] vb_a, int vb_nd):
    
    processed = []

    cdef Py_ssize_t x_max = fdarray.shape[0]
    cdef Py_ssize_t y_max = fdarray.shape[1]

    cdef Py_ssize_t row, col
    cdef Py_ssize_t rowa, cola

    out_array = np.zeros((x_max, y_max), dtype=DTYPE)
    cdef int[:, :] out_view = out_array

    for row in range(x_max):
        for col in range(y_max):
            if vb_a[row, col] == vb_nd:
                continue
            if [row, col] not in processed:

                subprocessed = [[row, col]]

                next_cell = fdarray[row, col]
                rowa, cola = row, col
                while next_cell is not None:
                    if next_cell == fd_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                    if [rowa, cola] in processed:
                        for coord in subprocessed:
                            if coord not in processed:
                                out_view[coord[0], coord[1]] = out_view[rowa, cola]
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    if chan_a[rowa, cola] != chan_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                out_view[coord[0], coord[1]] = 1
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    if r_a[rowa, cola] != road_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    if rr_a[rowa, cola] != rail_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')
                    if c_a[rowa, cola] != canal_nd:
                        for coord in subprocessed:
                            if coord not in processed:
                                processed.append(coord)
                        next_cell = None
                        print(f'{len(processed)} cells processed')

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
                                if coord not in processed:
                                    out_view[coord[0], coord[1]] = 2
                                    processed.append(coord)
                            next_cell = None
                        else:
                            subprocessed.append([rowa, cola])
                            print(f'subprocessed {len(subprocessed)} cells')
                            next_cell = fdarray[rowa, cola]

    return out_array