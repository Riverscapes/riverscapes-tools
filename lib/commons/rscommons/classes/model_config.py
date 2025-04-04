"""

Constants
    - Here we have a file containing the constants we typically need to run BRAT.
    - These can all be overridden by a .env file

"""
from semver import VersionInfo

_INIT_PARAMS = {
    'PROJ_FILE': 'project.rs.xml',
    'OUTPUT_EPSG': 4326
}


class ModelConfig:

    def __init__(self, xsd_url, version):
        # This XSD is what we use to validate the XML. For writing project XMLs this is placed
        # In the top line. It is not used and only here for reference
        self.XSD_URL = xsd_url
        # Output coordinate system for riverscapes
        # https://en.wikipedia.org/wiki/World_Geodetic_System#WGS84
        # https://spatialreference.org/ref/epsg/4326/
        self.OUTPUT_EPSG = _INIT_PARAMS['OUTPUT_EPSG']

        # The name of the project XML file
        self.PROJ_XML_FILE = _INIT_PARAMS['PROJ_FILE']

        if type(version) is VersionInfo:
            self.version = str(version)
        elif type(version) is str:
            try:
                ver_str = VersionInfo.parse(version)
                self.version = str(ver_str)
            except ValueError as e:
                print(e)
                raise Exception('Version supplied: "{}" cannot be parsed using the semver conventions.'.format(version))
