## Plan: Add Unique ID and Name Columns to New Vector Layer

The script will add `rs_reports_rowid` (UUID) and `rs_reports_rowname` (user-supplied/calculated) columns to a new layer in a GeoPackage. It will abort and report if any `rs_reports_rowname` values are not unique or exceed 150 characters.

### Steps
1. Accept input: GeoPackage path, source layer, new layer name, rowname expression/columns.
2. Open the source layer using `geopandas`.
3. Prepare new schema with added columns.
4. For each feature:
    - Generate UUID for `rs_reports_rowid`.
    - Compute `rs_reports_rowname` using the provided expression/columns.
5. Collect and check all `rs_reports_rowname` values for uniqueness and length.
6. If violations exist, report details and abort.
7. If valid, write features to the new layer in the GeoPackage.

### Further Considerations
1. Rowname expression option:
    - Python string templates (e.g., "{STATE_NAME} {NAMELSAD}").
2. Keep first version minimal. Report errors immediately rather than building in fallbacks. 

