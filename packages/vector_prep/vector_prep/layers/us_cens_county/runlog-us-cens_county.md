Source: https://www2.census.gov/geo/tiger/TGRGPKG25/tlgpkg_db_2025_a_us_substategeo.gpkg.zip
Layer: County
Technical documentation: https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/tiger-geo-line.html
Downloaded: 2026-06-15
Preparation: Lorin Gaertner, NAR

This is the 'Line' version that is more detailed and not clipped to coastline.

What we need to do:

* run through vector prep to check for common errors
* add definitions for columns / create layer_definitions
* upload to athena (iceberg) using dex script
* add documentation to athena repo

* Decide about geographic simplification:
  * I think none for now. Philip showed that this can cause topology issues (introduce gaps), which I do not want.
  * The Cartographic boundary version is simplified, we can upload that later if needed.

* Clip/filter to CONUS - when?