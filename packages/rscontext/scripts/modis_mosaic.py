import os
import argparse
import glob
import arcpy


def main(modis_folder, out_folder):
    modis_files = glob.glob(os.path.join(modis_folder, "*.hdf"))

    list_scenes = list(set([modis_file.split(".")[1].lstrip("A") for modis_file in modis_files]))

    for scene in list_scenes:
        modis_process = [modis_file for modis_file in modis_files if modis_file.split(".")[1].lstrip("A") == scene]

        modis_process_string = ";".join(modis_process).strip(";")
        arcpy.MosaicToNewRaster_management(input_rasters=modis_process_string,
                                           output_location=out_folder,
                                           raster_dataset_name_with_extension="A{0}.tif".format(scene),
                                           coordinate_system_for_the_raster="",
                                           pixel_type="16_BIT_UNSIGNED",
                                           cellsize="",
                                           number_of_bands="1",
                                           mosaic_method="FIRST",
                                           mosaic_colormap_mode="FIRST")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('modis_folder', help='Top level data folder containing MODIS data', type=str)
    parser.add_argument('out_folder', help='output folder', type=str)
    args = parser.parse_args()

    main(args.modis_folder, args.out_folder)
