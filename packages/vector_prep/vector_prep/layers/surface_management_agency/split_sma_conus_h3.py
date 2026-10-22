#!/usr/bin/env python3
"""Split BLM SMA CONUS polygons by H3 cell and write to GeoPackage.

Workflow:
1) Extract CONUS-only features from the SMA FGDB layer (ADMIN_ST filter) into a temp GPKG.
2) Build an H3 grid (default resolution 3) over the extracted CONUS bounds.
3) Intersect SMA polygons with the H3 grid using ogr2ogr SQLite dialect.
4) Write split output to a GeoPackage layer.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import geopandas as gpd
import h3
import pyogrio
from rsxml import Logger
from shapely.geometry import Polygon

log = Logger("SMA H3 Split")
_CACHED_OGR_ENV: dict[str, str] | None = None


def _ogr_env() -> dict[str, str]:
    global _CACHED_OGR_ENV
    if _CACHED_OGR_ENV is not None:
        return _CACHED_OGR_ENV

    env = os.environ.copy()

    if not env.get("GDAL_DATA"):
        gdal_candidates = [
            Path(r"C:\OSGeo4W\share\gdal"),
        ]
        for candidate in gdal_candidates:
            if candidate.exists():
                env["GDAL_DATA"] = str(candidate)
                log.info(f"Using GDAL_DATA={candidate}")
                break
        else:
            log.warning(
                "GDAL_DATA is not set and no known GDAL data path was found; "
                "you may see non-fatal GDAL warnings."
            )

    if not env.get("PROJ_LIB"):
        proj_candidates = [
            Path(r"C:\OSGeo4W\share\proj"),
        ]
        for candidate in proj_candidates:
            if candidate.exists():
                env["PROJ_LIB"] = str(candidate)
                log.info(f"Using PROJ_LIB={candidate}")
                break

    _CACHED_OGR_ENV = env
    return _CACHED_OGR_ENV


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, env=_ogr_env())


def _cell_boundary_lonlat(cell: str) -> list[tuple[float, float]]:
    boundary = list(h3.cell_to_boundary(cell))
    if not boundary:
        raise RuntimeError(f"H3 returned empty boundary for cell {cell}")

    first = boundary[0]
    if len(first) < 2:
        raise RuntimeError(f"Invalid boundary coordinate for cell {cell}: {first}")

    return [(float(lon), float(lat)) for lat, lon in boundary]


def _create_spatial_index(
    gpkg_path: Path,
    layer_name: str,
    geom_col: str,
) -> None:
    sql = f"SELECT CreateSpatialIndex('{layer_name}', '{geom_col}')"
    log.info(
        f"Creating spatial index for layer '{layer_name}' on geometry column '{geom_col}'."
    )
    cmd = [
        "ogrinfo",
        str(gpkg_path),
        "-dialect",
        "SQLITE",
        "-sql",
        sql,
    ]
    _run(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract CONUS SMA polygons and split them by H3 cell."
    )
    parser.add_argument(
        "--input-gdb",
        required=True,
        help="Path to SMA file geodatabase (.gdb).",
    )
    parser.add_argument(
        "--output-gpkg",
        required=True,
        help="Output GeoPackage path.",
    )
    parser.add_argument(
        "--output-layer",
        default="surface_management_agency_conus_h3_l3",
        help="Output GeoPackage layer name.",
    )
    parser.add_argument(
        "--source-layer",
        default="SurfaceManagementAgency",
        help="Input layer name inside the FGDB.",
    )
    parser.add_argument(
        "--where",
        default="ADMIN_ST NOT IN ('AK', 'HI')",
        help="Attribute SQL filter used for CONUS extraction.",
    )
    parser.add_argument(
        "--h3-resolution",
        type=int,
        default=3,
        help="H3 resolution for split grid (default 3).",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Optional existing directory for temporary intermediates.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary files (useful for debugging).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_gdb = Path(args.input_gdb).expanduser().resolve()
    output_gpkg = Path(args.output_gpkg).expanduser().resolve()

    if not input_gdb.exists():
        raise FileNotFoundError(f"Input geodatabase not found: {input_gdb}")

    if args.work_dir:
        work_root = Path(args.work_dir).expanduser().resolve()
        work_root.mkdir(parents=True, exist_ok=True)
        log_path = work_root / "sma_h3_split.log"
    else:
        log_path = output_gpkg.parent / "sma_h3_split.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log.setup(log_path=str(log_path), verbose=True)
    log.info(f"Logging to {log_path}")

    if args.work_dir:
        work_root = Path(args.work_dir).expanduser().resolve()
        work_root.mkdir(parents=True, exist_ok=True)
        temp_ctx = tempfile.TemporaryDirectory(
            prefix="sma_h3_split_", dir=str(work_root)
        )
    else:
        temp_ctx = tempfile.TemporaryDirectory(prefix="sma_h3_split_")

    with temp_ctx as temp_dir:
        temp_dir_path = Path(temp_dir)
        temp_gpkg = temp_dir_path / "sma_conus_work.gpkg"

        log.info(f"Extracting CONUS features from {input_gdb}...")
        extract_cmd = [
            "ogr2ogr",
            "-f",
            "GPKG",
            str(temp_gpkg),
            str(input_gdb),
            args.source_layer,
            "-where",
            args.where,
            "-nln",
            "sma_conus",
            "-nlt",
            "MULTIPOLYGON",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-unsetFid",
            "-gt",
            "65536",
            "-makevalid",
        ]
        _run(extract_cmd)
        _create_spatial_index(temp_gpkg, "sma_conus", "geom")

        info = pyogrio.read_info(str(temp_gpkg), layer="sma_conus")
        sma_fields = [str(field_name) for field_name in info["fields"]]
        if not sma_fields:
            raise RuntimeError("No attribute fields found in sma_conus layer.")
        sma_attrs_sql = ",\n                    ".join(
            f's."{field_name}"' for field_name in sma_fields
        )
        bounds = info["total_bounds"]
        minx, miny, maxx, maxy = (
            float(bounds[0]),
            float(bounds[1]),
            float(bounds[2]),
            float(bounds[3]),
        )
        log.info(f"CONUS bounds (EPSG:3857): [{minx}, {miny}, {maxx}, {maxy}]")

        bounds_geom = gpd.GeoSeries(
            [Polygon([(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy)])],
            crs="EPSG:3857",
        ).to_crs("EPSG:4326")
        bounds_geojson = bounds_geom.iloc[0].__geo_interface__

        log.info(f"Generating H3 cells at resolution {args.h3_resolution}...")
        cells = set(h3.geo_to_cells(bounds_geojson, args.h3_resolution))
        cells_with_neighbors: set[str] = set(cells)
        for cell in cells:
            cells_with_neighbors.update(h3.grid_disk(cell, 1))
        cells = cells_with_neighbors
        log.info(f"H3 cell count: {len(cells)}")

        h3_rows = []
        for cell in sorted(cells):
            lonlat = _cell_boundary_lonlat(cell)
            h3_rows.append({"h3_index": cell, "geometry": Polygon(lonlat)})

        h3_grid = gpd.GeoDataFrame(
            h3_rows, geometry="geometry", crs="EPSG:4326"
        ).to_crs("EPSG:3857")
        h3_bounds = h3_grid.bounds
        h3_grid["minx"] = h3_bounds.minx
        h3_grid["miny"] = h3_bounds.miny
        h3_grid["maxx"] = h3_bounds.maxx
        h3_grid["maxy"] = h3_bounds.maxy
        pyogrio.write_dataframe(
            h3_grid,
            str(temp_gpkg),
            layer="h3_l3_grid",
            driver="GPKG",
            append=False,
        )
        h3_info = pyogrio.read_info(str(temp_gpkg), layer="h3_l3_grid")
        h3_geom_col = str(h3_info["geometry_name"])
        _create_spatial_index(temp_gpkg, "h3_l3_grid", h3_geom_col)

        output_gpkg.parent.mkdir(parents=True, exist_ok=True)
        sql = f"""
            SELECT * FROM (
                SELECT
                    {sma_attrs_sql},
                    h.h3_index AS h3geo,
                    ST_Multi(
                        ST_CollectionExtract(
                            ST_Intersection(
                                s.geom,
                                h."{h3_geom_col}"
                            ),
                            3
                        )
                    ) AS geom
                FROM h3_l3_grid h
                JOIN rtree_sma_conus_geom si
                    ON si.maxx >= h.minx
                    AND si.minx <= h.maxx
                    AND si.maxy >= h.miny
                    AND si.miny <= h.maxy
                JOIN sma_conus s
                    ON s.ROWID = si.id
                WHERE ST_Intersects(s.geom, h."{h3_geom_col}")
            )
            WHERE NOT ST_IsEmpty(geom) AND ST_Area(geom) > 0
        """
        log.info(
            f"Intersecting and writing output: {output_gpkg} ({args.output_layer})"
        )
        split_cmd = [
            "ogr2ogr",
            "-f",
            "GPKG",
            str(output_gpkg),
            str(temp_gpkg),
            "-dialect",
            "SQLITE",
            "-sql",
            " ".join(sql.split()),
            "-nln",
            args.output_layer,
            "-nlt",
            "MULTIPOLYGON",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-unsetFid",
            "-gt",
            "65536",
            "-overwrite",
        ]
        _run(split_cmd)

        if args.keep_temp:
            kept_path = output_gpkg.parent / f"{output_gpkg.stem}_temp_work"
            if kept_path.exists():
                shutil.rmtree(kept_path)
            shutil.copytree(temp_dir_path, kept_path)
            log.info(f"Kept temporary work files at: {kept_path}")

    log.info("Done.")


if __name__ == "__main__":
    main()
