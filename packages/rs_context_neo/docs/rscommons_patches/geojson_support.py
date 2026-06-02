"""
Proposed augmentation: GeoJSON support in get_shp_or_gpkg()
===========================================================

File: lib/commons/rscommons/classes/vector_classes.py

Change the ``get_shp_or_gpkg()`` factory function to detect GeoJSON files
and open them with the OGR GeoJSON driver.  This eliminates the need for
the ``geojson_to_gpkg()`` workaround in rs_context_neo, and allows
``get_geometry_unary_union()`` in vector_ops.py to accept ``.geojson`` paths
directly.

Current code
------------

    def get_shp_or_gpkg(filepath: str, *args, **kwargs) -> VectorBase:
        if re.match(r'.*\\.shp', filepath) is not None:
            return ShapefileLayer(filepath, *args, **kwargs)
        else:
            return GeopackageLayer(filepath, *args, **kwargs)

Proposed replacement
--------------------

    def get_shp_or_gpkg(filepath: str, *args, **kwargs) -> VectorBase:
        if re.match(r'.*\\.shp', filepath) is not None:
            return ShapefileLayer(filepath, *args, **kwargs)
        elif re.match(r'.*\\.geojson', filepath, re.IGNORECASE) is not None:
            return GeoJSONLayer(filepath, *args, **kwargs)
        else:
            return GeopackageLayer(filepath, *args, **kwargs)

And add a new ``GeoJSONLayer`` class (read-only):

    class GeoJSONLayer(VectorBase):
        \"\"\"Read-only access to a GeoJSON file via OGR's GeoJSON driver.\"\"\"

        def __init__(self, filepath: str, write: bool = False, **kwargs):
            if write:
                raise VectorBaseException(
                    'GeoJSON layers are read-only.  Write to a GeoPackage instead.'
                )
            super().__init__(
                filepath,
                VectorBase.Drivers.GeoJSON,   # see note below
                layer_name=None,
                allow_write=False,
            )

NOTE: ``VectorBase.Drivers`` would also need a ``GeoJSON = 'GeoJSON'`` enum
member added.

Impact on rs_context_neo
------------------------
Once merged:
1. ``utils/gpkg.py``'s ``geojson_to_gpkg()`` can be removed.
2. ``fetch_dem_wcs.py``'s ``geojson_to_gpkg()`` call can be removed.
3. ``fetch_dem.py``'s ``geojson_to_gpkg()`` call can be removed.
4. ``utils/geom.py``'s ``load_geojson_geometry()`` can be replaced with
   ``from rscommons.vector_ops import get_geometry_unary_union`` and called
   as ``get_geometry_unary_union(bounds_geojson)``.
"""
