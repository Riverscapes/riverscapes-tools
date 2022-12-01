""" Build an Anthropogenic Context project

Jordan Gilbert Dec 2022
"""

import argparse
import os
import time
from typing import Dict

from rscommons.classes.rs_project import RSMeta
from rscommons import Logger, initGDALOGRErrors, RSLayer, RSProject, ModelConfig, dotenv

from anthro.__version__ import __version__

Path = str

initGDALOGRErrors()

cfg = ModelConfig('https://xml.riverscapes.net/Projects/XSD/V2/RiverscapesProject.xsd', __version__)

LYR_DESCRIPTIONS_JSON = os.path.join(os.path.dirname(__file__), 'layer_descriptions.json')
LayerTypes = {
    'EXVEG': RSLayer('Existing Land Cover', 'EXVEG', 'Raster', 'inputs/existing_veg.tif'),
    'HILLSHADE': RSLayer('DEM Hillshade', 'HILLSHADE', 'Raster', 'inputs/dem_hillshade.tif'),
    'INPUTS': RSLayer('Anthropologic Inputs', 'INPUTS', 'Geopackage', 'inputs/inputs.gpkg', {
        'IGO': RSLayer('Integrated Geographic Objects', 'IGO', 'Vector', 'igo'),
        'DGO': RSLayer('Discrete Geographic Object', 'DGO', 'Vector', 'dgo'),
        'VALLEYBOTTOM': RSLayer('Valley Bottom', 'VALLEY', 'Vector', 'valley_bottom'),
        'CANALS': RSLayer('Canals', 'CANAL', 'Vector', 'canals'),
        'ROADS': RSLayer('Roads', 'ROADS', 'Vector', 'roads'),
        'RAILS': RSLayer('Railroads', 'RAIL', 'Vector', 'rails'),
        'LEVEES': RSLayer('Levees', 'LEVEE', 'Vector', 'levees')
    }),
    'OUTPUTS': RSLayer('Anthropologic Outputs', 'OUTPUTS', 'Geopackage', 'outputs/outputs.gpkg', {
        'ANTHRO_GEOMETRY': RSLayer('Anthropogenic Output Geometry', 'ANTHRO_GEOMETRY', 'Vector', 'igo')
    })
}


def anthro_context(huc: int, existing_veg: Path, hillshade: Path, igo: Path, dgo: Path,
                   valley_bottom: Path, canals: Path, roads: Path, railroads: Path,
                   levees: Path, output_folder: Path, meta: Dict[str, str]):
    """
    """

    log = Logger("Anthropogenic Context")
    log.info(f'HUC: {huc}')
    log.info(f'EPSG: {cfg.OUTPUT_EPSG}')

    start_time = time.time()

    project_name = f'Anthropogenic Context for HUC {huc}'
    project = RSProject(cfg, output_folder)
    project.create(project_name, 'Anthro', [
        RSMeta(f'HUC{len(huc)}', str(huc)),
        RSMeta('HUC', str(huc)),
        RSMeta('AnthroVersion', cfg.version),
        RSMeta('AnthroTimeStamp', str(int(time.time())))
    ], meta)

    _realization, proj_nodes = project.add_realization(project_name, 'REALIZATION1', cfg.version, data_nodes=['Inputs', 'Outputs'])
