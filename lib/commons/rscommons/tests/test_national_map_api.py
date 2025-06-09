import pytest
from rscommons.national_map_api import TNM

def test_get_items_real_api():
    params = {
        "datasets": "National Hydrography Dataset (NHD) Flowline",
        "prodFormats": "GeoPackage",
        "max": 10  # small page size to test pagination
    }
    result = TNM.get_items(params)
    assert "items" in result
    assert isinstance(result["items"], list)
    assert result["total"] >= len(result["items"])
    # Optionally print for manual inspection
    print(f"Fetched {len(result['items'])} of {result['total']} items")