"""
Add `rs_reports_rowid` (UUID) and `rs_reports_rowname` (user-supplied/calculated) columns to a new layer in a GeoPackage.
Aborts and reports if any rs_reports_rowname values are not unique or exceed MAXLENGTH characters.

Usage:
    python add_unique_id_and_name.py --input-gpkg input.gpkg --input-layer source_layer \
        --output-gpkg output.gpkg --output-layer new_layer \
        --rowname-template "{STATE_NAME} {NAMELSAD}"

Written mostly by copilot driven by Lorin 2025-12-12 
Tested and ran on a couple layers 2025-12-12
"""
import argparse
import os
import uuid
import sys
import geopandas as gpd
from rsxml import Logger

MAXLENGTH = 150


def add_unique_id_and_name(
    input_gpkg: str,
    input_layer: str,
    rowname_template: str,
    output_gpkg: str = "",
    output_layer: str = ""
) -> None:
    """Adds rs_reports_rowid (GUID) and rs_reports_rowname based on user-supplied rowname_template
    Validates that rowname values are unique and less than MAXLENGTH characters

    Args:
        input_gpkg (str): input geopackage
        input_layer (str): input layer
        rowname_template (str): python format string
        output_gpkg (str): output geopackage (defaults to input_gpkg)
        output_layer (str): new layer name (defaults to input_layer + '_rs_rpt_lyr')
    """
    log = Logger("Add Unique ID and Name")
    if not os.path.exists(input_gpkg):
        raise FileNotFoundError(f"Input file does not exist: {input_gpkg}")
    try:
        gdf = gpd.read_file(input_gpkg, layer=input_layer)
    except Exception as e:
        log.error(f"Failed to read input layer: {e}")
        raise
    log.info(f"Loaded {len(gdf)} features from {input_layer}")
    rowids = []
    rownames = []
    # list of characters to strip out
    replace_chars = ['\u00A0']  # default to nbsp
    for idx, row in gdf.iterrows():
        rowid = str(uuid.uuid4())
        # Preprocess fields: replace each char in replace_chars, then strip
        row_dict = {}
        for k, v in row.items():
            if isinstance(v, str):
                v_clean = v
                for ch in replace_chars:
                    v_clean = v_clean.replace(ch, ' ')
                v_clean = v_clean.strip()
                row_dict[k] = v_clean
            else:
                row_dict[k] = v
        try:
            rowname = rowname_template.format(**row_dict)
        except Exception as e:
            log.error(f"Row {idx}: Failed to compute rowname: {e}")
            raise
        if len(rowname) > MAXLENGTH:
            log.error(
                f"Row {idx}: rowname exceeds {MAXLENGTH} characters: {rowname[:MAXLENGTH+10]}")
        rowids.append(rowid)
        rownames.append(rowname)
    overlength = [i for i, n in enumerate(rownames) if len(n) > MAXLENGTH]
    duplicates = set([n for n in rownames if rownames.count(n) > 1])
    if overlength or duplicates:
        log.error("Aborting due to rowname violations.")
        if overlength:
            log.error(f"Rows with overlength rownames: {overlength}")
        if duplicates:
            log.error(f"Duplicate rownames found: {duplicates}")
        raise ValueError("Rowname validation failed. See log for details.")
    gdf["rs_reports_rowid"] = rowids
    gdf["rs_reports_rowname"] = rownames
    if not output_gpkg or len(output_gpkg) == 0:
        output_gpkg = input_gpkg
    if not output_layer or len(output_layer) == 0:
        output_layer = input_layer + '_rs_rpt_lyr'
    log.info(f"Writing new layer {output_layer} to {output_gpkg}")
    gdf.to_file(output_gpkg, layer=output_layer, driver="GPKG")
    log.info("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Add unique ID and name columns to a GeoPackage layer.")
    parser.add_argument("--input-gpkg", required=True,
                        help="Input GeoPackage path")
    parser.add_argument("--input-layer", required=True,
                        help="Input layer name")
    parser.add_argument("--rowname-template", required=True,
                        help="Python string template for rowname, e.g. '{STATE_NAME} {NAMELSAD}'")
    parser.add_argument("--output-gpkg", required=False,
                        help="Output GeoPackage path (defaults to same as input)")
    parser.add_argument("--output-layer", required=False,
                        help="Output layer name (defaults to input + _rs_rpt_lyr)")
    args = parser.parse_args()

    try:
        add_unique_id_and_name(
            input_gpkg=args.input_gpkg,
            input_layer=args.input_layer,
            output_gpkg=args.output_gpkg,
            output_layer=args.output_layer,
            rowname_template=args.rowname_template
        )
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
