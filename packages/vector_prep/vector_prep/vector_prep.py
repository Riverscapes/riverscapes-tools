"""
Clean a vector layer by fixing invalid geometries and simplifying it. Ostensibly used for
preparing vector layers for use in the Riverscapes Reporting platform both as a picklist
layer, but also for storing in Athena for use in reports.

The input is a single ShapeFile for GeoPackage vector layer. It can be in any projection,
and any fields.

The output is always a GeoPackage layer with cleaned geometries, reprojected to EPSG:4326.

Philip Bailey
27 Nov 2025
"""
import argparse
import sys
import os
import traceback
import geopandas as gpd
from shapely.geometry.base import BaseGeometry
from shapely import make_valid  # shapely >=1.8
from rsxml import Logger, dotenv

# This script always produces the output in GeoPackage format
OUTPUT_DRIVER = "GPKG"


def vector_prep(input_dataset: str, output_dataset: str, layer_name: str, tolerance: float, epsg: int) ->None:

    log = Logger("Vector Prep")

    if not os.path.exists(input_dataset):
        raise Exception(f"Input file does not exist: {input_dataset}")

    log.info(f"Reading input dataset with GeoPandas: {input_dataset}")
    try:
        if layer_name:
            gdf = gpd.read_file(input_dataset, layer=layer_name)
        else:
            gdf = gpd.read_file(input_dataset)  # geopandas will choose default/first layer
    except Exception as e:
        log.error(f"GeoPandas failed to read input dataset: {e}")
        sys.exit(1)

    initial_count = len(gdf)
    log.info(f"Loaded {initial_count} features. CRS: {gdf.crs}")

    geom_types = gdf.geom_type.value_counts().to_dict()
    log.info(f"Geometry type of input: {geom_types}")

    # Drop features with null geometry right away (to reduce work)
    # We'll also handle empty/invalid ones in our cleaning step
    # Create a copy to preserve original until final write
    gdf_proc = gdf.copy()

    # Reproject to specified Cartesian CRS for processing
    if epsg:
        log.info(f"Reprojecting to EPSG:{epsg} for processing...")
        try:
            gdf_proc = gdf_proc.to_crs(epsg=epsg)
            log.info(f"Reprojection complete. New CRS: {gdf_proc.crs}")
        except Exception as e:
            raise Exception(f"Failed to reproject to EPSG:{epsg}: {e}")

    # Clean geometries (fix invalids, simplify)
    log.info(f"Cleaning geometries (tolerance={tolerance})...")
    cleaned_geom_series, stats = clean_geometries(gdf_proc.geometry, simplify_tolerance=tolerance)

    # assign cleaned geometries back
    gdf_proc["geometry"] = cleaned_geom_series

    # Drop rows where geometry is None or empty after cleaning
    before_drop = len(gdf_proc)
    gdf_proc = gdf_proc[~gdf_proc["geometry"].isna()]
    gdf_proc = gdf_proc[~gdf_proc["geometry"].is_empty]
    after_drop = len(gdf_proc)
    dropped = before_drop - after_drop

    log.info(f"Input features: {stats['input_count']}")
    log.info(f"Null/empty geometries found: {stats['null_or_empty']}")
    log.info(f"Invalid geometries fixed: {stats['invalid_fixed']}")
    log.info(f"Invalid geometries unfixed (dropped): {stats['invalid_unfixed']}")
    log.info(f"Features simplified: {stats['simplified_count']}")
    log.info(f"Dropped features after cleaning: {dropped}")
    log.info(f"Remaining features to write: {len(gdf_proc)}")

    # If nothing remains
    if len(gdf_proc) == 0:
        raise Exception("No valid geometries remain after cleaning. Aborting write.")
    
    # Loop over all string columns and ensure that empty strings are set to None (to avoid issues with some drivers)
    for col in gdf_proc.select_dtypes(include=['object']).columns:
        gdf_proc[col] = gdf_proc[col].apply(lambda x: x if x and str(x).strip() != "" else None)

    # Make sure there is a column called FID (some drivers require it)
    if "FID" not in gdf_proc.columns:
        gdf_proc = gdf_proc.reset_index(drop=True)
        gdf_proc["FID"] = gdf_proc.index.astype('int64')

    # If output exists and overwrite requested, remove it first (be careful with gpkg)
    if os.path.exists(output_dataset):
        try:
            if os.path.isdir(output_dataset):
                # shapefile's folder? be cautious
                pass
            os.remove(output_dataset)
            log.info(f"Overwrote existing file: {output_dataset}")
        except Exception:
            # for gpkg, removal may be different; try to proceed and fiona may overwrite if allowed
            log.debug("Could not remove existing file prior to write (continuing)...")

    # Reproject to EPSG for final output
    log.info("Reprojecting to EPSG 4326 for output")
    gdf_proc = gdf_proc.to_crs(epsg=4326)

    # Write output
    log.info(f"Writing cleaned layer to {output_dataset} (driver={OUTPUT_DRIVER})...")
    try:
        # For GeoPackage, preserve layer name if provided or derive from filename
        write_kwargs = {}
        if OUTPUT_DRIVER == "GPKG":
            # geopandas.to_file will write a layer named after filename (without ext) by default unless layer arg given
            layername = layer_name if layer_name else os.path.splitext(os.path.basename(output_dataset))[0]
            write_kwargs["layer"] = layername

        gdf_proc.to_file(output_dataset, driver=OUTPUT_DRIVER, **write_kwargs)
        log.info("Write complete.")
    except Exception as e:
        raise Exception("Failed to write output: %s", e)


def safe_make_valid(geom: BaseGeometry):
    """Try make_valid then fallback to buffer(0), or return None if can't fix."""

    if geom is None:
        return None
    try:
        valid = make_valid(geom)
    except Exception as e:
        log = Logger("Error")
        log.debug(f"make_valid/buffer(0) failed: {e}")
        try:
            return geom.buffer(0)
        except Exception as e2:
            log.debug(f"fallback buffer(0) failed too: {e2}")
            return None


def clean_geometries(gseries, simplify_tolerance):
    """
    Process a GeoSeries of geometries:
      - drop null/empty
      - attempt to fix invalid/self-intersecting geometries
      - apply topology-preserving simplify (Shapely's simplify with preserve_topology=True)
    Returns cleaned GeoSeries and diagnostics dict.
    """
    cleaned = []
    stats = {
        "input_count": len(gseries),
        "null_or_empty": 0,
        "invalid_fixed": 0,
        "invalid_unfixed": 0,
        "simplified_count": 0,
    }

    for idx, geom in enumerate(gseries):
        if geom is None:
            stats["null_or_empty"] += 1
            cleaned.append(None)
            continue
        # some drivers give empty geometries instead of None
        try:
            if geom.is_empty:
                stats["null_or_empty"] += 1
                cleaned.append(None)
                continue
        except Exception:
            # if .is_empty fails, we'll try to continue
            pass

        # If geometry invalid, try to fix
        try:
            is_valid = geom.is_valid
        except Exception:
            # some malformed geometries might raise; attempt fix
            is_valid = False

        if not is_valid:
            fixed = safe_make_valid(geom)
            if fixed is not None and not fixed.is_empty:
                geom = fixed
                stats["invalid_fixed"] += 1
            else:
                stats["invalid_unfixed"] += 1
                # keep as-is (or set to None) - we'll mark as None to drop later
                cleaned.append(None)
                continue

        # If simplify tolerance > 0, simplify while trying to preserve topology
        if simplify_tolerance is not None and simplify_tolerance > 0:
            try:
                simplified = geom.simplify(simplify_tolerance, preserve_topology=True)
                # ensure simplification didn't produce empty / invalid geometry
                if simplified is not None and not simplified.is_empty:
                    # if simplification creates invalid geometry, try to make valid again
                    if not simplified.is_valid:
                        simplified = safe_make_valid(simplified)
                    geom = simplified
                    stats["simplified_count"] += 1
            except Exception as e:
                log = Logger("Error")
                log.debug(f"simplify failed on feature {idx}: {e}")
                # keep original geom (already valid)
        cleaned.append(geom)

    return gpd.GeoSeries(cleaned, index=gseries.index, crs=gseries.crs), stats


def main():
    parser = argparse.ArgumentParser(description='Vector Prep: Clean and simplify vector datasets.')
    parser.add_argument("input", help="Input vector (shapefile, gpkg, etc.)")
    parser.add_argument("output", help="Output vector path")
    parser.add_argument("--layer", help="Layer name (for geopackage). If not provided and input is geopackage, first layer is used.", default=None)
    parser.add_argument("--tolerance", type=float, help="Simplify tolerance in METRES (0 to skip).", default=0.0)
    parser.add_argument("--epsg", type=int, help="Cartesian CRS EPSG code to reproject to before processing (optional). Default is 5070 (NAD83 / Conus Albers).", default=5070)
    parser.add_argument('--verbose', help='(optional) a little extra logging', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger("Vector Prep")
    log.setup(log_path=os.path.join(os.path.dirname(args.output), "vector_prep.log"), verbose=args.verbose)

    try:
        vector_prep(args.input, args.output, args.layer, float(args.tolerance), int(args.epsg))
    except Exception as e:
        log.error("Vector prep failed: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)
        

if __name__ == "__main__":
    main()