"""

Constants
    - Here we have a file containing the constants we typically need to run BRAT.
    - These can all be overridden by a .env file

"""
import codecs
import re
import os
import logging 

class Config:
    GEOJSON_HEAD = """
    { 
        "type": "FeatureCollection",
        "name": "example",
        "crs": {
            "type": "name",
            "properties": {
            "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }
        },
        "features": [
    """
    GEOJSON_FOOT = "\n]}\n"


    def __init__(self):
        self.env = self.parse_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

        # Anything in the .env file will overwrite these values
        [setattr(self, k, v) for k, v in self.env.items()]
