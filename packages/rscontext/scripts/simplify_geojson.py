import geopandas as gpd
import os
import sys

def simplify_geojson(input_path, output_path=None, target_size_kb=200, start_tolerance=10, tolerance_increment=10):
    """
    Load a GeoJSON, check its size, and simplify iteratively until it is under the target size.
    Ensure projection is Cartesian during simplification, and final output is EPSG:4326.
    """
    
    if output_path is None:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_simplified{ext}"

    # Check initial file size
    if not os.path.exists(input_path):
        print(f"Error: File {input_path} not found.")
        return

    # Get file size in KB
    file_size_kb = os.path.getsize(input_path) / 1024
    print(f"Initial file size: {file_size_kb:.2f} KB")

    if file_size_kb <= target_size_kb:
        print("File is already under the target size. Enforcing standard format...")
        # Just creating a fresh copy to ensure consistent formatting if needed, or simple copy
        try:
            gdf = gpd.read_file(input_path)

            if not gdf.crs:
                print("Warning: Input CRS is missing. Assuming EPSG:4326.")
                gdf.set_crs(epsg=4326, inplace=True)
            elif gdf.crs.to_epsg() != 4326:
                print("Reprojecting to EPSG:4326...")
                gdf = gdf.to_crs(epsg=4326)

            gdf.to_file(output_path, driver='GeoJSON')
            print(f"Saved to {output_path}")
        except Exception as e:
            print(f"Error reading/writing file: {e}")
        return

    # Load data
    print("Loading GeoDF...")
    gdf = gpd.read_file(input_path)
    
    # Check if empty
    if gdf.empty:
        print("GeoJSON is empty.")
        return

    # Prepare for simplification
    # If the CRS is geographic (like 4326), we must project to a Cartesian system 
    # to use meters for tolerance.
    projected_gdf = gdf.copy()
    
    if projected_gdf.crs and projected_gdf.crs.is_geographic:
        try:
            # estimate_utm_crs is available in newer geopandas versions
            target_crs = projected_gdf.estimate_utm_crs()
            print(f"Projecting to {target_crs.name} for accurate simplification...")
            projected_gdf = projected_gdf.to_crs(target_crs)
        except AttributeError:
            # Fallback for older geopandas or if estimate fails
            print("Warning: Could not estimate UTM CRS. Falling back to EPSG:3857 (Web Mercator).")
            projected_gdf = projected_gdf.to_crs(epsg=3857)
        except Exception as e:
            print(f"Projection error: {e}")
            return
    elif projected_gdf.crs is None:
         # Assume it might be 4326 if missing, or user needs to fix input. 
         # But usually we assume input is valid. We'll warn.
         print("Warning: Input CRS is missing. Assuming EPSG:4326 and projecting to 3857.")
         projected_gdf.set_crs(epsg=4326, inplace=True)
         projected_gdf = projected_gdf.to_crs(epsg=3857)

    # We keep the "original" projected version to always simplify from scratch
    # so errors don't accumulate.
    original_projected_geometry = projected_gdf.geometry.copy()
    
    current_tolerance = start_tolerance
    
    while True:
        print(f"Simplifying with tolerance: {current_tolerance} meters...")
        
        # Simplify
        # preserve_topology=True is generally safer for polygons to avoid self-intersections
        projected_gdf.geometry = original_projected_geometry.simplify(current_tolerance, preserve_topology=True)
        
        # Reproject result to 4326
        final_gdf = projected_gdf.to_crs(epsg=4326)
        
        # Save to check size
        # We need to write to disk to know the exact file size (GeoJSON is text-based)
        if os.path.exists(output_path):
            os.remove(output_path)
            
        final_gdf.to_file(output_path, driver='GeoJSON')
        
        new_size_kb = os.path.getsize(output_path) / 1024
        print(f"  -> Resulting file size: {new_size_kb:.2f} KB")
        
        if new_size_kb < target_size_kb:
            print(f"Success! Final file size {new_size_kb:.2f} KB is under {target_size_kb} KB limit.")
            break
        
        # Increase tolerance for next pass
        current_tolerance += tolerance_increment
        
        # Safety break
        if current_tolerance > 5000:
            print("Aborting: Tolerance exceeded 5000m. The file cannot be reduced enough nicely.")
            break

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python simplify_geojson.py <input_geojson> [output_path]")
        sys.exit(1)
        
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    simplify_geojson(input_file, output_file)
