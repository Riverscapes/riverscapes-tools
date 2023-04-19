"""Find which windows intersect large rivers to determine if open water shoud be riparian or not
"""
import rasterio
from rasterio.mask import mask
import numpy as np
from rscommons.vector_ops import get_geometry_unary_union


def river_intersections(windows, ex_veg, hist_veg, flow_areas=None, waterbodies=None):

    out_dict = {'ex': {}, 'hist': {}}

    if flow_areas:
        geom_flow_areas = get_geometry_unary_union(flow_areas)

    if waterbodies:
        geom_waterbodies = get_geometry_unary_union(waterbodies)

    with rasterio.open(ex_veg) as src:
        for igoid, window in windows.items():
            if geom_flow_areas:
                if window[0].intersects(geom_flow_areas):
                    raw_raster = mask(src, window[0], crop=True)[0]
                    mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                    if 7292 in np.unique(mask_raster):
                        cell_count = np.count_nonzero(mask_raster == 7292)
                        if igoid in out_dict['ex'].keys():
                            out_dict['ex'][igoid] += cell_count
                        else:
                            out_dict['ex'][igoid] = cell_count
            if geom_waterbodies:
                if window[0].intersects(geom_waterbodies):
                    raw_raster = mask(src, window[0], crop=True)[0]
                    mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                    if 7292 in np.unique(mask_raster):
                        cell_count = np.count_nonzero(mask_raster == 7292)
                        if igoid in out_dict['ex'].keys():
                            out_dict['ex'][igoid] += cell_count
                        else:
                            out_dict['ex'][igoid] = cell_count

    with rasterio.open(hist_veg) as src:
        for igoid, window in windows.items():
            if geom_flow_areas:
                if window[0].intersects(geom_flow_areas):
                    raw_raster = mask(src, window[0], crop=True)[0]
                    mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                    if 11 in np.unique(mask_raster):
                        cell_count = np.count_nonzero(mask_raster == 11)
                        if igoid in out_dict['hist'].keys():
                            out_dict['hist'][igoid] += cell_count
                        else:
                            out_dict['hist'][igoid] = cell_count
            if geom_waterbodies:
                if window[0].intersects(geom_waterbodies):
                    raw_raster = mask(src, window[0], crop=True)[0]
                    mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                    if 11 in np.unique(mask_raster):
                        cell_count = np.count_nonzero(mask_raster == 11)
                        if igoid in out_dict['hist'].keys():
                            out_dict['hist'][igoid] += cell_count
                        else:
                            out_dict['hist'][igoid] = cell_count

    return out_dict
