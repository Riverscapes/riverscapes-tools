# VBET

## [4.1.6] - 2025-SEP-2

### Fixed
- a lingering bug with block window size mismatch

## [4.1.4] - 2025-AUG-27

### Fixed
- a bug in 'raster_update_multipy' function that was causing failures

### Changed
- allowed multiple output regions to be retained for a single level path
- seg_distance does not reset for distinct centerline features of the same level path; instead, seg_distance is continuous even if the level path has a gap

## [0.8.6] - 2022-DEC-15

### Changed
- Clean up vbet raster donuts smaller than 20 pixels
- Small refactor for rectangle extent to geom
- Refactored parameter naming for igos/dgos

### Added
- Additional IGO metrics

### Fixed
- igo generation method uses better utm finding logic
- tiny bug affecting elapsed time meta

## [0.8.5] - 2022-DEC-13

### Changed
- vbet level path zone rasters renamed and added to projet file
- igo spacing changed to 100m
- igo metric windows changed to 200m, 500m and 1000m
- waterbodies processing extent changed to full dem extent

## [0.8.4] - 2022-DEC-09

### Changed
- change input parameter to use processing_extent layer instead of huc8 to match actual watershed boundary
- implement raster cleaning to remove "vbet ribbons"

## [0.8.3] - 2022-DEC-07

### Changed
- single transform per level path (based on max stream order)
- segmentation_points renamed to vbet_igos
- output fields store level path id as string instead of float

### Fixed
- collect linestring uses shapely instead of ogr to improve merging of level path reaches when generating centerline

## [0.8.2] - 2022-DEC-06

### Fixed
- Bug with level paths outside dem extent (i.e. Canadian Hucs)
- several small bug fixes around temp files and raster creation
- normalized twi layer name
- raster compression with 'r+' and window modes
- do not copy channel polygons with empty geometries

### Changed
- moved supporting files from scripts to vbet/lib
- evidence layer uses hand and slope when TWI is no-data
- force flowline endpoints on raster data for centerline generation

## [0.8.1] - 2022-DEC-02

### Added
- Changelog

### Fixed
- masking vbet evidence by hand raster data area per level path (remove artifact pixels/polygons)
- centerlines and igo/dgo clipping to inside vbet area
- raw centerlines added as temporary geopackage layer prior to clipping.

## [0.8.0] - 2022-DEC-01

### Changed
- apply appropriate windowing to vbet evidence rasters based on raster extents.
- Sort level path by decending drainage area