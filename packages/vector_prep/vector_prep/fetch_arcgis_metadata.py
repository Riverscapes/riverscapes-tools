"""
Fetch column (field) metadata from an ArcGIS Hub dataset page and produce a
``columns`` list conforming to the Riverscapes layer_definitions.json schema.

Background
----------
ArcGIS Hub pages (also called "Open Data" sites) are the public-facing
portals that organisations such as BLM publish data on.  The human-friendly
URL looks like::

    https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons/about

Behind every Hub page sits an ArcGIS Online (AGOL) *item* identified by a
stable GUID (the "item ID").  You can reach the AGOL Item Details page at::

    https://www.arcgis.com/home/item.html?id=<itemId>

There is a button "Open in ArcGIS Online" on the Hub page that navigates to
the Item Details page, but there is **no guaranteed back-link** from the Item
page to the Hub page, because an organisation may have many Hub sites or none.

This script uses the Hub URL as the starting point because it is the easier
URL for a human to find (the ``/about`` suffix shows "Full Details"; without
it the browser redirects to ``/explore?location=...`` which is hard to copy).

Two data sources are combined to build a complete column definition:

1. **Feature Service REST API** — provides the field *name*, *Esri field type*
   (mapped to the schema ``dtype`` enum), and ``alias`` (used as
   ``friendly_name``).  Reachable at ``{serviceUrl}/{layerIndex}?f=json``.

2. **FGDC metadata XML** — published alongside the AGOL item at
   ``https://www.arcgis.com/sharing/rest/content/items/{itemId}/info/metadata/metadata.xml?format=default``.
   Each ``<attr>`` element under ``<eainfo>/<detailed>`` contains
   ``<attrlabl>`` (field name) and ``<attrdef>`` (description).

The Hub API (``https://hub.arcgis.com/api/v3/datasets?filter[slug]=...``)
resolves the human-friendly slug to the item ID **and** the feature service
URL in a single call, so no manual ID wrangling is needed.

Esri system fields (``OBJECTID``, ``GlobalID``, ``Shape__Area``, etc.) are
excluded by default since they are auto-generated and not meaningful for
downstream schema documentation.

Usage
-----
As a CLI::

    python fetch_arcgis_metadata.py \\
        "https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons/about"

This prints a JSON array of column objects to stdout.

To write directly into an existing layer_definitions.json layer entry::

    python fetch_arcgis_metadata.py \\
        "https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons/about" \\
        --layer-definitions layer_definitions.json \\
        --layer-id blm_natl_grazing_allotments

As a library::

    from vector_prep.fetch_arcgis_metadata import fetch_columns_from_hub_url
    columns = fetch_columns_from_hub_url(
        "https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons/about"
    )
"""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import requests

# ---------------------------------------------------------------------------
# Esri field-type → layer_definitions.json dtype mapping
# ---------------------------------------------------------------------------
ESRI_TYPE_MAP: dict[str, str] = {
    "esriFieldTypeString": "STRING",
    "esriFieldTypeGUID": "STRING",
    "esriFieldTypeGlobalID": "STRING",
    "esriFieldTypeInteger": "INTEGER",
    "esriFieldTypeSmallInteger": "INTEGER",
    "esriFieldTypeOID": "INTEGER",
    "esriFieldTypeDouble": "FLOAT",
    "esriFieldTypeSingle": "FLOAT",
    "esriFieldTypeDate": "DATETIME",
    "esriFieldTypeGeometry": "GEOMETRY",
}

# Esri-generated fields that are rarely useful for schema documentation.
SYSTEM_FIELDS: set[str] = {
    "OBJECTID",
    "FID",
    "GlobalID",
    "Original_GlobalID",
    "Shape__Area",
    "Shape__Length",
    "Shape_Area",
    "Shape_Length",
    "SHAPE",
    "created_user",
    "created_date",
    "last_edited_user",
    "last_edited_date",
}


# ---------------------------------------------------------------------------
# Hub API helpers
# ---------------------------------------------------------------------------
def _extract_slug(hub_url: str) -> str:
    """Extract the ``Org::dataset-slug`` from a Hub URL.

    Handles URLs like:
      https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons/about
      https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons
    """
    path = urlparse(hub_url).path  # /datasets/BLM-EGIS::slug/about
    match = re.search(r"/datasets/([^/]+)", path)
    if not match:
        raise ValueError(f"Cannot extract dataset slug from URL: {hub_url}")
    return match.group(1)


def _resolve_hub_slug(slug: str) -> tuple[str, str, int]:
    """Call the Hub v3 API to resolve a slug to (item_id, service_url, layer_index).

    Returns:
        item_id:      The 32-char AGOL item GUID.
        service_url:  Full feature-service layer URL (includes layer index).
        layer_index:  Integer layer index within the service.
    """
    api_url = "https://hub.arcgis.com/api/v3/datasets"
    resp = requests.get(api_url, params={"filter[slug]": slug}, timeout=30)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        raise LookupError(f"Hub API returned no results for slug: {slug}")

    record = data[0]
    composite_id = record["id"]  # e.g. "0882acf7eada4b3bafee4dd673fbe8a0_1"
    service_url = record["attributes"]["url"]

    # composite_id = itemId_layerIndex
    parts = composite_id.rsplit("_", 1)
    item_id = parts[0]
    layer_index = int(parts[1]) if len(parts) > 1 else 0

    return item_id, service_url, layer_index


# ---------------------------------------------------------------------------
# Feature Service field fetch
# ---------------------------------------------------------------------------
def _fetch_service_fields(service_url: str) -> list[dict]:
    """GET the feature service layer JSON and return the ``fields`` array."""
    resp = requests.get(service_url, params={"f": "json"}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"Feature service error: {payload['error']}")
    return payload.get("fields", [])


# ---------------------------------------------------------------------------
# FGDC metadata XML fetch
# ---------------------------------------------------------------------------
def _fetch_fgdc_descriptions(item_id: str) -> dict[str, str]:
    """Return {field_name: description} parsed from FGDC metadata XML.

    The XML is fetched from the standard AGOL metadata endpoint.  Field
    descriptions live in ``eainfo/detailed/attr`` elements as::

        <attr>
            <attrlabl>ALLOT_NO</attrlabl>
            <attrdef>The identifying number ...</attrdef>
        </attr>
    """
    url = (
        f"https://www.arcgis.com/sharing/rest/content/items/{item_id}"
        "/info/metadata/metadata.xml?format=default"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    descs: dict[str, str] = {}

    for attr_el in root.iter("attr"):
        label_el = attr_el.find("attrlabl")
        def_el = attr_el.find("attrdef")
        if label_el is not None and label_el.text:
            name = label_el.text.strip()
            description = def_el.text.strip() if (def_el is not None and def_el.text) else ""
            descs[name] = description

    return descs


# ---------------------------------------------------------------------------
# Core: merge into columns list
# ---------------------------------------------------------------------------
def fetch_columns_from_hub_url(hub_url: str, include_system_fields: bool = False) -> list[dict]:
    """Fetch column definitions from an ArcGIS Hub dataset URL.

    Args:
        hub_url: A Hub dataset page URL, e.g.
            ``https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons/about``
        include_system_fields: If True, include Esri system fields such as
            ``OBJECTID``, ``GlobalID``, ``Shape__Area``, etc.

    Returns:
        A list of column dicts ready to insert into a layer_definitions.json
        ``columns`` array.
    """
    slug = _extract_slug(hub_url)
    item_id, service_url, _layer_index = _resolve_hub_slug(slug)

    service_fields = _fetch_service_fields(service_url)
    fgdc_descs = _fetch_fgdc_descriptions(item_id)

    columns: list[dict] = []
    for field in service_fields:
        name: str = field["name"]
        esri_type: str = field.get("type", "")

        if not include_system_fields and name in SYSTEM_FIELDS:
            continue

        dtype = ESRI_TYPE_MAP.get(esri_type)
        if dtype is None:
            # Skip geometry pseudo-fields or unknown types
            continue

        col: dict = {"name": name}

        # friendly_name from service alias (only if it differs from the raw name)
        alias = field.get("alias", "")
        if alias and alias != name:
            col["friendly_name"] = alias

        col["dtype"] = dtype

        # description from FGDC metadata
        desc = fgdc_descs.get(name, "")
        if desc:
            col["description"] = desc

        columns.append(col)

    return columns


# ---------------------------------------------------------------------------
# Optional: patch an existing layer_definitions.json
# ---------------------------------------------------------------------------
def update_layer_definitions(path: str, layer_id: str, columns: list[dict]) -> None:
    """Replace the ``columns`` array of a specific layer in a layer_definitions.json file."""
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)

    for layer in doc.get("layers", []):
        if layer.get("layer_id") == layer_id:
            layer["columns"] = columns
            break
    else:
        raise LookupError(f"layer_id '{layer_id}' not found in {path}")

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=4, ensure_ascii=False)
        fh.write("\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Fetch column metadata from an ArcGIS Hub dataset page."
    )
    parser.add_argument(
        "hub_url",
        help="ArcGIS Hub dataset URL, e.g. https://gbp-blm-egis.hub.arcgis.com/datasets/BLM-EGIS::blm-natl-grazing-allotment-polygons/about",
    )
    parser.add_argument(
        "--layer-definitions",
        help="Path to an existing layer_definitions.json to update in-place.",
        default=None,
    )
    parser.add_argument(
        "--layer-id",
        help="layer_id within the layer_definitions.json to update (required with --layer-definitions).",
        default=None,
    )
    parser.add_argument(
        "--include-system-fields",
        action="store_true",
        default=False,
        help="Include Esri system fields like OBJECTID, GlobalID, Shape__Area, etc.",
    )
    args = parser.parse_args()

    if args.layer_definitions and not args.layer_id:
        parser.error("--layer-id is required when --layer-definitions is specified.")

    columns = fetch_columns_from_hub_url(args.hub_url, include_system_fields=args.include_system_fields)

    if args.layer_definitions:
        update_layer_definitions(args.layer_definitions, args.layer_id, columns)
        print(f"Updated {args.layer_definitions} layer '{args.layer_id}' with {len(columns)} columns.", file=sys.stderr)
    else:
        json.dump(columns, sys.stdout, indent=4, ensure_ascii=False)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
