# Mapping FGDC CSDGM to Riverscapes Layer Definitions Metadata

This note documents what the current code does today, where that maps well to Riverscapes, and where it should be improved.

## References

- Riverscapes metadata overview: https://docs.riverscapes.net/standards/metadata/
- Riverscapes layer definitions schema: https://xml.riverscapes.net/riverscapes_metadata/schema/layer_definitions.schema.json
- FGDC CSDGM standard: https://www.fgdc.gov/metadata/csdgm/

## Scope

- Current parser: [packages/vector_prep/vector_prep/fetch_xml_metadata.py](packages/vector_prep/vector_prep/fetch_xml_metadata.py)
- Current consumer: [packages/vector_prep/vector_prep/layers/blm_ownership/orchestrate-blm-ownership.py](packages/vector_prep/vector_prep/layers/blm_ownership/orchestrate-blm-ownership.py)

## Current Mapping Implemented

### Column-level metadata (strongest mapping)

Source section:
- FGDC attribute records under eainfo/detailed/attr

Mapped to Riverscapes layer columns:
- attrlabl -> columns[].name
- attrdef -> columns[].description (base text)
- attrdomv/edom entries -> appended to columns[].description as enumerated values
- attrdomv/udom -> appended to columns[].description when present

Notes:
- friendly_name is not currently derived from XML.
- dtype is currently hard-coded as STRING in XML-only output.

### Dataset-level metadata (partial, currently not strongly integrated)

Source examples parsed today:
- idinfo/citation/citeinfo/title or ISO-like resTitle
- idinfo/descript/abstract
- idinfo/descript/purpose
- publication date, originator, bbox, keywords
- entity name and entity description

Current state:
- Parsed into DatasetInfo, but only loosely consumed downstream.
- In blm_ownership orchestration, title/abstract/entity_description are used only when assembling a generated layer_definitions file.

## FGDC to Riverscapes Mapping Table (Recommended Canonical Mapping)

### Top-level layer_definitions document

- source_title <- title (fallback: entity_name)
- snapshot_id <- pub_date (optionally normalized to YYYY-MM-DD)
- source_url <- pipeline/config input (not reliable from FGDC alone)

### Per-layer metadata

- layers[].layer_name <- entity_name (fallback: source_title)
- layers[].description <- entity_description (fallback: abstract, then purpose)
- layers[].layer_id <- pipeline/config authored value
- layers[].layer_type <- pipeline/config or fixed per workflow

### Per-column metadata

- columns[].name <- attrlabl
- columns[].description <- attrdef + domain rendering
- columns[].friendly_name <- optional derived value (see improvements)
- columns[].dtype <- optional in schema; should be omitted unless known

## Gaps and Improvement Opportunities

### 1) Dtype strategy in XML-only mode

Issue:
- Assuming STRING for all XML fields is misleading.

Recommended behavior:
- If type is unknown from source, omit dtype instead of forcing STRING.
- If another source exists (FGDB or Feature Service), merge typed metadata into XML descriptions.

Practical rule:
- XML-only extraction: emit name, description, friendly_name (if derivable), no dtype.
- Typed source available: keep dtype from typed source, overlay XML descriptions.

### 2) DatasetInfo should map directly into layer_definitions structure

Issue:
- parse_dataset_info is useful but under-utilized.

Recommended behavior:
- Add a helper that builds the layer skeleton from DatasetInfo and explicit pipeline inputs.
- Keep config values authoritative where FGDC is ambiguous (layer_id, source_url, layer_type).

Suggested output helper contract:
- build_layer_def_from_dataset_info(dataset_info, layer_id, source_url, columns, ...)
- Returns one complete layer object ready to append under layers[].

### 3) Friendly name enrichment

Issue:
- XML parser does not currently populate friendly_name.

Recommended behavior:
- Use attr alias-like candidates when available in source profile; if absent, optionally derive a humanized fallback from field name.
- Keep this optional to avoid unintended naming churn.

### 4) Domain handling is currently flattened into description text

Issue:
- Domain values are useful but less machine-friendly when embedded only as prose.

Recommended behavior:
- Keep current rendered description for compatibility.
- Optionally emit a structured sidecar in run artifacts for QA/diff workflows.

### 5) Source input flexibility for local XML files

Issue:
- Existing fetch path is URL/item-ID centric.

Recommended behavior:
- Accept item ID, http/https URL, file URI, and local filesystem path.
- This is required for zip-extracted local XML workflows.

## Recommended Operational Modes

### Mode A: XML-only quick bootstrap

Use when only XML is available.

Output should include:
- columns with name + description (+ optional friendly_name)
- no dtype unless inferred with high confidence

### Mode B: Production-quality merged metadata

Use when FGDB or Feature Service schema is available.

Output should include:
- dtype and friendly_name from typed source
- description enriched from XML
- optional diff artifact to show disagreements

## Suggested Near-term Changes

1. Update XML extraction so unknown dtype is omitted instead of defaulting to STRING.
2. Add a small helper to map DatasetInfo into layer-level fields consistently.
3. Add local file path support for XML source resolution.
4. Keep merge_xml_descriptions_into_fgdb_columns as the preferred production path.

## Open Questions to Resolve in Code

- Should snapshot_id be pub_date as-is, or normalized?
- Should abstract or entity_description be preferred as layer description default?
- Should friendly_name fallback be enabled by default, or only behind a flag?
- Do we want a structured domain sidecar artifact for QA and comparisons?
