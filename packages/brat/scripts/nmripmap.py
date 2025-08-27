from rscommons import Raster, GeodatabaseLayer, GeopackageLayer, VectorBase, dotenv

from osgeo import gdal, ogr
import numpy as np
import rasterio
import os
import argparse
import traceback
import sys


veg_class_lookup = {
    "IA1": 10001,
    "IA2": 10002,
    "IA3": 10003,
    "IB1": 10004,
    "IB2": 10005,
    "IB3": 10006,
    "IB4": 10007,
    "IB5": 10008,
    "IB6": 10009,
    "IB7": 10010,
    "IB8": 10011,
    "IB9": 10012,
    "IC1": 10013,
    "IC2": 10014,
    "IC3": 10015,
    "IC4": 10016,
    "ID1": 10017,
    "IE1": 10018,
    "IIA1": 10019,
    "IIA2": 10020,
    "IIB1": 10021,
    "IIB2": 10022,
    "IIB3": 10023,
    "IIB4": 10024,
    "IIB5": 10025,
    "IIB6": 10026,
    "IIC1": 10027,
    "IIIA1": 10028,
    "IIIA2": 10029,
    "IIIB1": 10030,
    "IIIB2": 10031,
    "IIIB3": 10032,
    "IIIB4": 10033,
    "IIIC1": 10034,
    "IIID1": 10035,
    "IIID2": 10036,
    "IIIE1": 10037,
    "IIIE2": 10038,
    "IIIF1": 10039,
    "IVA1": 10040,
    "IVB1": 10041,
    "IVC1": 10042,
    "IVC2": 10043,
    "IVD1": 10044,
    "IVD2": 10045,
    "IVE1": 10046,
    "IVF1": 10047,
    "IVG1": 10048
}


def clip_ripmap(rip_map: str, clip_shp: str, out_polygon: str, lookup: dict):

    filename = os.path.basename(rip_map)  # Gets 'URG_Version2_0Plus.gdb'
    name_part = filename.split('_')[0]
    layer = f'{name_part}_Version2Plus_NMRipMap'
    with GeopackageLayer(clip_shp) as clip_layer, \
            GeodatabaseLayer(rip_map, layer_name=layer) as rip_layer, \
            GeopackageLayer(out_polygon, write=True) as out_layer:
        transform = clip_layer.get_transform_from_layer(rip_layer)
        transformback = rip_layer.get_transform_from_layer(clip_layer)

        out_layer.create_layer(ogr.wkbMultiPolygon, epsg=4326, fields={
            "L3_Code": ogr.OFTString,
            "L3_Name": ogr.OFTString,
            "RipMapID": ogr.OFTInteger
        })

        clip_ftr = clip_layer.ogr_layer.GetNextFeature()
        if clip_ftr is None:
            raise RuntimeError(f"Clip shapefile {clip_shp} is empty.")
        geom = clip_ftr.GetGeometryRef()
        # geom.Transform(transform)
        envelope = geom.GetEnvelope()
        min_x, max_x, min_y, max_y = envelope
        clip_geom = ogr.Geometry(ogr.wkbPolygon)
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(min_x, min_y)
        ring.AddPoint(max_x, min_y)
        ring.AddPoint(max_x, max_y)
        ring.AddPoint(min_x, max_y)
        ring.AddPoint(min_x, min_y)
        clip_geom.AddGeometry(ring)

        clip_geom.Transform(transform)
        if not clip_geom.IsValid():
            raise RuntimeError(f"Clip geometry is not valid: {clip_geom.ExportToWkt()}")

        for rip_ftr, *_ in rip_layer.iterate_features(clip_shape=clip_geom):
            rip_geom = rip_ftr.GetGeometryRef()
            geom_clone = rip_geom.Clone()
            geom_clone.Transform(transformback)
            feat = ogr.Feature(out_layer.ogr_layer.GetLayerDefn())
            feat.SetGeometry(geom_clone)

            rip_code = rip_ftr.GetField("L3_Code")
            if rip_code is None:
                raise RuntimeError(f"L3_Code not found in feature {rip_ftr.GetFID()}.")
            rip_name = rip_ftr.GetField("L3_Name")
            if rip_name is None:
                raise RuntimeError(f"L3_Name not found in feature {rip_ftr.GetFID()}.")
            rip_id = lookup[rip_code]
            if rip_id is None:
                raise Exception(f"RipMapID not found for L3_Code {rip_code}.")

            out_layer.create_feature(geom_clone, {
                "L3_Code": rip_code,
                "L3_Name": rip_name,
                "RipMapID": rip_id
            })


def rasterize_gpkg_layer(gpkg_path: str, extent_poly: str, attribute: str, output_raster: str, resolution: int):
    """
    Rasterize a file geodatabase layer based on an attribute value.

    Args:
        gdb_path (str): Path to the file geodatabase.
        layer_name (str): Name of the layer to rasterize.
        attribute (str): Attribute to use for rasterization.
        output_raster (str): Path to the output raster file.
        resolution (int): Resolution of the output raster.
    """
    # Open the file geodatabase
    with GeopackageLayer(gpkg_path) as layer, GeopackageLayer(extent_poly) as clip_layer:
        res_deg = VectorBase.rough_convert_metres_to_spatial_ref_units(layer.spatial_ref, layer.ogr_layer.GetExtent(), resolution)

        # Get the layer's extent
        extent = clip_layer.ogr_layer.GetExtent()
        x_min, x_max, y_min, y_max = extent

        # Calculate raster dimensions - ensure we cover the full extent
        x_res = int(np.ceil((x_max - x_min) / res_deg))
        y_res = int(np.ceil((y_max - y_min) / res_deg))

        # Adjust the max extents to match the raster dimensions exactly
        x_max = x_min + (x_res * res_deg)
        y_min = y_max - (y_res * res_deg)

        # Create the output raster
        target_ds = gdal.GetDriverByName("GTiff").Create(output_raster, x_res, y_res, 1, gdal.GDT_Int32)
        target_ds.SetGeoTransform((x_min, res_deg, 0, y_max, 0, -res_deg))
        target_ds.SetProjection(layer.ogr_layer.GetSpatialRef().ExportToWkt())
        target_ds.GetRasterBand(1).SetNoDataValue(-9999)
        target_ds.GetRasterBand(1).Fill(-9999)

        # Set the raster's spatial reference
        spatial_ref = layer.ogr_layer.GetSpatialRef()
        target_ds.SetProjection(spatial_ref.ExportToWkt())

        # Rasterize the layer
        gdal.RasterizeLayer(target_ds, [1], layer.ogr_layer, options=[f"ATTRIBUTE={attribute}", "COMPRESS=LZW"])

        # Close the datasets
        target_ds = None

        print(f"Rasterization complete. Output saved to {output_raster}")


def hybrid_raster(ripmap_raster: str, lf_raster: str, out_raster: str):

    with Raster(ripmap_raster) as ripmap, Raster(lf_raster) as lf:
        lf_res = os.path.join(os.path.dirname(lf_raster), os.path.basename(lf_raster).replace('.tif', '_res.tif'))

        gdal.Warp(lf_res, lf_raster, format='GTiff', width=ripmap.array.shape[1], height=ripmap.array.shape[0],
                  outputBounds=(ripmap.gt[0], ripmap.gt[3],
                  ripmap.gt[0] + ripmap.cellWidth * ripmap.array.shape[1],
                  ripmap.gt[3] + ripmap.cellHeight * ripmap.array.shape[0]),
                  dstSRS=ripmap.proj, resampleAlg=gdal.GRA_NearestNeighbour, srcNodata=lf.nodata, dstNodata=lf.nodata)

    with rasterio.open(lf_res) as lf_ras, rasterio.open(ripmap_raster) as ripmap:

        ripmap_array = ripmap.read(1)
        lf_array = lf_ras.read(1)

        ripmap_array = np.flipud(ripmap_array)  # Flip the array vertically

        out_array = np.where(ripmap_array == ripmap.nodata, lf_array, ripmap_array)

    with rasterio.open(lf_res) as src:
        meta = src.profile
        meta.update({})

    with rasterio.open(out_raster, 'w', **meta) as dst:
        dst.write(out_array, 1)

    print(f"Hybrid rasterization complete. Output saved to {out_raster}")


def main():
    parser = argparse.ArgumentParser(description="Clip RipMap and rasterize it.")
    parser.add_argument("huc", help="HUC code for the RipMap.")
    parser.add_argument("rip_map", help="Path to the RipMap geodatabase.")
    parser.add_argument("clip_shp", help="Path to the shapefile to clip the RipMap.")
    parser.add_argument("out_polygon", help="Path to the output polygon shapefile.")
    parser.add_argument("attribute", help="Attribute to use for rasterization.")
    parser.add_argument("output_raster", help="Path to the output raster file.")
    parser.add_argument("resolution", type=int, help="Resolution of the output raster.")
    parser.add_argument("landfire_raster", help="Path to the Landfire raster file.")

    args = dotenv.parse_args_env(parser)

    try:
        clip_ripmap(args.rip_map, args.clip_shp, args.out_polygon, veg_class_lookup)
        rasterize_gpkg_layer(args.out_polygon, args.clip_shp, args.attribute, args.output_raster, args.resolution)
        hybrid_raster(args.output_raster, args.landfire_raster, os.path.join(os.path.dirname(args.landfire_raster), "hybrid_" + os.path.basename(args.output_raster)))
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

# clip_ripmap('/workspaces/data/URG_Version2_0Plus.gdb', '/workspaces/data/rs_context/1302010110/hydrology/nhdplushr.gpkg/WBDHU10', '/workspaces/data/rs_context/1302010110/vegetation/ripmap.gpkg/ripmap', veg_class_lookup)
# rasterize_gpkg_layer('/workspaces/data/rs_context/1302010110/vegetation/ripmap.gpkg/ripmap', 'RipMapID', '/workspaces/data/rs_context/1302010110/vegetation/nmripmap.tif', 5)
# hybrid_raster('/workspaces/data/rs_context/1302010110/vegetation/nmripmap.tif', '/workspaces/data/rs_context/1302010110/vegetation/existing_veg.tif', '/workspaces/data/rs_context/1302010110/vegetation/hybrid_nmripmap.tif')
