# Vector Prep Tool

The purpose of the tool is to provide consistent and reproducible preparation of (usually public Government data) vector data for use in the Riverscapes ecosystem.

Example preparation includes:

* checking and fixing for geometry errors, null or invalid etc (TO BE BETTER DOCUMENTED)
* simplify as needed
* Documentation and Metadata enrichment - ingesting any existing metadata (eg. in an ISO xml format), source urls etc., making sure it is referenced/included and also translating it into the riverscapes metadata `.json` format, published at [xml.riverscapes.net](https://xml.riverscapes.net/riverscapes_metadata/schema/layer_definitions.schema.json) and documented on [docs.riverscapes.net](https://docs.riverscapes.net/standards/metadata) so it can be published to the central S3/Athena repository.

Planned, not yet implemented:

* Identify/Verify a primary key field that can be used for joins. Could be composite of existing fields.
* Identify/Verify a primary name field that can be used for labels. could be formulaic composite of multiple existing fields -- implemented in a view perhaps rather than materialized.
* Export to parquet in WGS84 for use in reporting
