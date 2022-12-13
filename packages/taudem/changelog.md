# TAUDEM

## [1.2.1] - 2022-DEC-12

## Changed
- Use numpy to reclass slope raster instead of gdal (unstable on cybercastor)

## [1.2.0] - 2022-DEC-12

### Added
- d-infinity slope raster reclassed to remove zero slope in intermediates

### Changed
- twi uses reclassed dinfinity slope raster
- d-infinty slope uses -nc flag to reduce artifacts from edge effects.

