"""
HUC geometry fetching utilities.

Fetches HUC boundary GeoJSON files from the Riverscapes tile server.
Supports HUC8, HUC10, and HUC12 codes.
"""
import os
import urllib.request

from rsxml import Logger

_HUC_GEOM_URL = 'https://tiles.riverscapes.net/pmTiles/huc{level}/1.0/geom/{huc}.geojson'
_VALID_HUC_LENGTHS = {8, 10, 12}


def fetch_huc_geometry(huc: str, output_folder: str) -> str:
    """
    Fetch the HUC boundary GeoJSON from the Riverscapes tile server and save
    it to the output folder.

    Parameters:
        huc (str): HUC code — must be exactly 8, 10, or 12 digits.
        output_folder (str): Directory to save the downloaded GeoJSON file.

    Returns:
        str: Absolute path to the saved GeoJSON file.

    Raises:
        ValueError: If the HUC code is not 8, 10, or 12 digits.
        urllib.error.URLError: If the download fails.
    """
    log = Logger('HUC Geometry')

    if not huc.isdigit() or len(huc) not in _VALID_HUC_LENGTHS:
        raise ValueError(f'Invalid HUC code "{huc}". Must be exactly 8, 10, or 12 digits.')

    url = _HUC_GEOM_URL.format(level=len(huc), huc=huc)
    dest = os.path.join(output_folder, f'{huc}.geojson')

    log.info(f'Fetching HUC geometry from: {url}')
    urllib.request.urlretrieve(url, dest)
    log.info(f'HUC geometry saved to: {dest}')

    return dest
