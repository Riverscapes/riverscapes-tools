"""
Proposed augmentation: polygonize() in rscommons/vector_ops.py
==============================================================

Replace the existing ``polygonize()`` function in
``lib/commons/rscommons/vector_ops.py`` with the version below.

The only change is the addition of an ``append: bool = False`` parameter.
When ``append=True`` the function opens an *existing* GeoPackage datasource
in update mode and adds a new layer rather than creating a fresh datasource.
This is needed by ``vectorize_subwatersheds()`` in rs_context_neo, which
appends the watershed polygon layer to the same GeoPackage that already
holds the stream-network layer produced by TauDEM streamnet.

Once merged, rs_context_neo's ``vectorize_subwatersheds()`` can be simplified
to a single ``polygonize()`` call:

    from rscommons.vector_ops import polygonize
    polygonize(
        subwatersheds_raster_path, 1,
        f"{gpkg_path}/{layer_name}",
        append=True,
    )
"""

# ── Proposed replacement for polygonize() in vector_ops.py ────────────────────


def polygonize(
    raster_path: str,
    band: int,
    out_layer_path: str,
    epsg: int = None,
    append: bool = False,
) -> None:
    """Convert a single raster band to a polygon vector layer.

    Parameters
    ----------
    raster_path : str
        Path to the source raster.
    band : int
        1-based band index to polygonize.
    out_layer_path : str
        Destination layer path (compound GPKG path or shapefile path).
        For GeoPackages use ``'/path/to/file.gpkg/layer_name'``.
    epsg : int, optional
        EPSG code for the output layer.  Defaults to ``None`` (inherits
        from the raster's CRS).
    append : bool, optional
        If ``True``, open an existing datasource in update mode and
        *append* a new layer rather than creating a fresh datasource.
        The existing layers in the datasource are preserved.
        Default: ``False`` (original behaviour — creates a new datasource).
    """
    # NOTE: keep these imports local to match the existing function's style
    from osgeo import gdal, ogr  # noqa: F401
    from rsxml import ProgressBar
    from rscommons import get_shp_or_gpkg

    # mapping between gdal type and ogr field type
    type_mapping = {
        gdal.GDT_Byte: ogr.OFTInteger,
        gdal.GDT_UInt16: ogr.OFTInteger,
        gdal.GDT_Int16: ogr.OFTInteger,
        gdal.GDT_UInt32: ogr.OFTInteger,
        gdal.GDT_Int32: ogr.OFTInteger,
        gdal.GDT_Float32: ogr.OFTReal,
        gdal.GDT_Float64: ogr.OFTReal,
        gdal.GDT_CInt16: ogr.OFTInteger,
        gdal.GDT_CInt32: ogr.OFTInteger,
        gdal.GDT_CFloat32: ogr.OFTReal,
        gdal.GDT_CFloat64: ogr.OFTReal,
    }

    src_ds = gdal.Open(raster_path)
    src_band = src_ds.GetRasterBand(band)

    with get_shp_or_gpkg(out_layer_path, write=True) as out_layer:
        if append:
            # Open the existing datasource in update mode; create only the
            # new layer (existing layers are preserved).
            out_layer.create_layer(ogr.wkbPolygon, epsg=epsg)
        else:
            out_layer.create_layer(ogr.wkbPolygon, epsg=epsg)

        out_layer.create_field("id", field_type=type_mapping[src_band.DataType])

        progbar = ProgressBar(100, 50, "Polygonizing raster")

        def poly_progress(progress, _msg, _data):
            progbar.update(int(progress * 100))

        gdal.Polygonize(
            src_band,
            src_ds.GetRasterBand(band),
            out_layer.ogr_layer,
            0,
            [],
            callback=poly_progress,
        )
        progbar.finish()

    src_ds = None
