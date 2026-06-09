#!/bin/bash
set -euo pipefail

DOWNLOAD_DIR="/Users/philipbailey/GISData/roads/ntd_download"
UNZIP_DIR="/Users/philipbailey/GISData/roads/ntd_unzip"
OUTPUT_DIR="/Users/philipbailey/GISData/roads"
ROADS_GPKG="$OUTPUT_DIR/national_roads.gpkg"
RAIL_GPKG="$OUTPUT_DIR/national_rail.gpkg"

# Remove existing output GeoPackages so we start fresh
rm -f "$ROADS_GPKG" "$RAIL_GPKG"

for ZIP in "$DOWNLOAD_DIR"/*.zip; do
    STATE=$(basename "$ZIP" .zip)
    echo "Processing $STATE..."

    # Unzip into its own subfolder
    unzip -q "$ZIP" -d "$UNZIP_DIR/$STATE"

    SHAPE_DIR="$UNZIP_DIR/$STATE/Shape"

    # Append Trans_RoadSegment (handles Trans_RoadSegment.shp, _0.shp, _1.shp, etc.)
    ROAD_SHPS=("$SHAPE_DIR"/Trans_RoadSegment*.shp)
    if [[ -f "${ROAD_SHPS[0]}" ]]; then
        for ROAD_SHP in "${ROAD_SHPS[@]}"; do
            if [[ -f "$ROADS_GPKG" ]]; then
                ogr2ogr -f GPKG -append -explodecollections "$ROADS_GPKG" "$ROAD_SHP" -nln Trans_RoadSegment
            else
                ogr2ogr -f GPKG -explodecollections "$ROADS_GPKG" "$ROAD_SHP" -nln Trans_RoadSegment
            fi
        done
    else
        echo "  WARNING: No Trans_RoadSegment*.shp found in $SHAPE_DIR, skipping."
    fi

    # Append Trans_RailFeature (handles Trans_RailFeature.shp, _0.shp, _1.shp, etc.)
    RAIL_SHPS=("$SHAPE_DIR"/Trans_RailFeature*.shp)
    if [[ -f "${RAIL_SHPS[0]}" ]]; then
        for RAIL_SHP in "${RAIL_SHPS[@]}"; do
            if [[ -f "$RAIL_GPKG" ]]; then
                ogr2ogr -f GPKG -append -explodecollections "$RAIL_GPKG" "$RAIL_SHP" -nln Trans_RailFeature
            else
                ogr2ogr -f GPKG -explodecollections "$RAIL_GPKG" "$RAIL_SHP" -nln Trans_RailFeature
            fi
        done
    else
        echo "  WARNING: No Trans_RailFeature*.shp found in $SHAPE_DIR, skipping."
    fi

    # Cleanup unzipped files
    rm -rf "$UNZIP_DIR/$STATE"
    echo "  Done."
done

echo "All states processed."
echo "Roads:  $ROADS_GPKG"
echo "Rail:   $RAIL_GPKG"
