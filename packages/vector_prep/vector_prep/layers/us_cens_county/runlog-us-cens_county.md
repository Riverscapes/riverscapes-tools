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

* Clip/filter to CONUS in going from ext_raw to ext_rpt

```sql
CREATE TABLE ext_rpt.us_cens_county
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  is_external = false,
  location = 's3://riverscapes-athena/ext-rpt/us_cens_county/'
) AS
WITH src AS (
  SELECT
    countyns,
    geoid,
    geoidfq,
    namelsad,
    classfp,
    funcstat,
    aland,
    awater,
    intptlat,
    intptlon,
    COALESCE(
      CASE
        WHEN regexp_like(CAST(geoid AS varchar), '^[0-9]{1,5}$')
          THEN lpad(CAST(geoid AS varchar), 5, '0')
      END,
      regexp_extract(CAST(geoidfq AS varchar), 'US([0-9]{5})', 1)
    ) AS geoid5
  FROM ext_raw.us_cens_county
)
SELECT
  countyns,
  geoid5 AS geoid,
  geoidfq,
  namelsad,
  classfp,
  funcstat,
  aland,
  awater,
  intptlat,
  intptlon,
  substr(geoid5, 1, 2) AS statefp
FROM src
WHERE geoid5 IS NOT NULL
  -- exclude AK, HI, territories; keep DC
  AND substr(geoid5, 1, 2) NOT IN ('02','15','60','66','69','72','78');
```