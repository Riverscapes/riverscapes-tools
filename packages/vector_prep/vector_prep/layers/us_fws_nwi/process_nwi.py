"""Process the the Wetlands and Riparian layers FROM downloaded NWI zip files (one per HUC8)

Extract
- unzip the file geodatabase `HU8_01234567.gdb`
Transform
- save the HU8_01234567_Wetlands and HU8_01234567_Riparian layers to GeoPackage
- run vector prep on each layer
Load
- if no errors, SQL insert cleaned data on each layer to appropriate Athena table

Maintain ledger tables in a GeoPackage to track status of process.

See processing_runlog.md for more details.

"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

WORKING_DIR = Path(
    r"C:\nardata\localcode\riverscapes-tools\packages\vector_prep\vector_prep\layers\us_fws_nwi"
)
LEDGER_FILENAME = "nwi_processing_ledger.gpkg"

df = pd.read_csv(WORKING_DIR / "huc8.csv", header=0, names=["huc8"])

gdf = gpd.GeoDataFrame(df)

gdf.to_file(WORKING_DIR / LEDGER_FILENAME, layer="huc8list", driver="GPKG")
