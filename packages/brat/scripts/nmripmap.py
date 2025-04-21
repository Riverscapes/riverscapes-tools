from rscommons import Raster, GeodatabaseLayer, GeopackageLayer, VectorBase
from rscommons.classes.vector_base import get_utm_zone_epsg

from osgeo import gdal, ogr


veg_class_lookup = {
    "IA1": 0,
    "IA2": 1,
    "IA3": 2,
    "IB1": 3,
    "IB2": 4,
    "IB3": 5,
    "IB4": 6,
    "IB5": 7,
    "IB6": 8,
    "IB7": 9,
    "IB8": 10,
    "IB9": 11,
    "IC1": 12,
    "IC2": 13,
    "IC3": 14,
    "IC4": 15,
    "ID1": 16,
    "IE1": 17,
    "IIA1": 18,
    "IIA2": 19,
    "IIB1": 20,
    "IIB2": 21,
    "IIB3": 22,
    "IIB4": 23,
    "IIB5": 24,
    "IIB6": 25,
    "IIC1": 26,
    "IIIA1": 27,
    "IIIA2": 28,
    "IIIB1": 29,
    "IIIB2": 30,
    "IIIB3": 31,
    "IIIB4": 32,
    "IIIC1": 33,
    "IIID1": 34,
    "IIID2": 35,
    "IIIE1": 36,
    "IIIE2": 37,
    "IIIF1": 38,
    "IVA1": 39,
    "IVB1": 40,
    "IVC1": 41,
    "IVC2": 42,
    "IVD1": 43,
    "IVD2": 44,
    "IVE1": 45,
    "IVF1": 46,
    "IVG1": 47
}


def clip_ripmap(rip_map: str, clip_shp: str, out_polygon: str, lookup: dict):
    with GeopackageLayer(clip_shp) as clip_layer, GeodatabaseLayer(rip_map, layer_name='URG_Version2Plus_NMRipMap') as rip_layer, GeopackageLayer(out_polygon, write=True) as out_layer:
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
        clip_geom = clip_ftr.GetGeometryRef()
        clip_geom.Transform(transform)

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


def rasterize_gpkg_layer(gpkg_path: str, attribute: str, output_raster: str, resolution: int):
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
    with GeopackageLayer(gpkg_path) as layer:
        res_deg = VectorBase.rough_convert_metres_to_spatial_ref_units(layer.spatial_ref, layer.ogr_layer.GetExtent(), resolution)

        # Get the layer's extent
        extent = layer.ogr_layer.GetExtent()
        x_min, x_max, y_min, y_max = extent

        # Calculate raster dimensions
        x_res = int((x_max - x_min) / res_deg)
        y_res = int((y_max - y_min) / res_deg)

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
        gdal.RasterizeLayer(target_ds, [1], layer.ogr_layer, options=[f"ATTRIBUTE={attribute}"])

        # Close the datasets
        target_ds = None

        print(f"Rasterization complete. Output saved to {output_raster}")


# clip_ripmap('/workspaces/data/URG_Version2_0Plus.gdb', '/workspaces/data/rs_context/1302010213/hydrology/nhdplushr.gpkg/WBDHU10', '/workspaces/data/ripmap.gpkg/ripmap', veg_class_lookup)
rasterize_gpkg_layer('/workspaces/data/ripmap.gpkg/ripmap', 'RipMapID', '/workspaces/data/ripmap.tif', 1)
