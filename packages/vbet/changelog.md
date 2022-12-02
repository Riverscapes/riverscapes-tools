# VBET

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