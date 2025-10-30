import os
import json

from rscommons import Logger
from champ_metrics.lib.exception import MissingException
from champ_metrics.lib.metricxmloutput import writeMetricsToXML
from .metriclib.auxMetrics import calculateMetricsForVisit, calculateMetricsForChannelUnitSummary, calculateMetricsForTier1Summary, calculateMetricsForStructureSummary
# from .metriclib.fishMetrics import *


__version__ = "0.0.4"

# {key: urlslug} dict
MEASURE_KEYS = {
    "snorkelFish": "Snorkel Fish",
    "snorkelFishBinned": "Snorkel Fish Count Binned",
    "snorkelFishSteelheadBinned": "Snorkel Fish Count Steelhead Binned",
    "channelUnits": "Channel Unit",
    "largeWoodyPieces": "Large Woody Piece",
    "largeWoodyDebris": "Large Woody Debris",
    "woodyDebrisJams": "Woody Debris Jam",
    "jamHasChannelUnits": "Jam Has Channel Unit",
    "riparianStructures": "Riparian Structure",
    "pebbles": "Pebble",
    "pebbleCrossSections": "Pebble Cross-Section",
    "channelConstraints": "Channel Constraints",
    "channelConstraintMeasurements": "Channel Constraint Measurements",
    "bankfullWidths": "Bankfull Width",
    "driftInverts": "Drift Invertebrate Sample",
    "driftInvertResults": "Drift Invertebrate Sample Results",
    "sampleBiomasses": "Sample Biomasses",
    "undercutBanks": "Undercut Banks",
    "solarInputMeasurements": "Daily Solar Access Meas",
    "discharge": "Discharge",
    "waterChemistry": "Water Chemistry",
    "poolTailFines": "Pool Tail Fines",
}


def visit_aux_metrics(visit_id: int, visit_year: int, aux_data_folder: str, xmlfile: str) -> None:
    """Run the auxmetrics for a given visit and write them to an XML file."""

    log = Logger("Aux Metrics")
    log.info(f'Aux metrics for visit {visit_id}')

    if not os.path.isdir(aux_data_folder):
        raise Exception(f"Aux measurement folder does not exist: {aux_data_folder}")

    visitobj = {
        "visit_id": visit_id,
        # "visit": APIGet("visits/{}".format(visit_id)),
        "visit": {
            "VisitID": visit_id,
            "iterationID": visit_year - 2010,
            "protocol": 'champ',
        },
        "iteration": visit_year,
        "protocol": 'champ',
    }

    log.info("Visit " + str(visit_id) + " - " + visitobj['protocol'] + ": " + str(visitobj['iteration']))

    # Populate our measurements from the Aux JSON files.
    # This used to call the Sitka API to get this information.
    for key, url in MEASURE_KEYS.items():
        try:
            # visitobj[key] = APIGet("visits/{0}/measurements/{1}".format(visit_id, url))
            visitobj[key] = load_measurement_file(aux_data_folder, url)
        except MissingException:
            visitobj[key] = None

    # Metric calculations
    visitMetrics = calculateMetricsForVisit(visitobj)
    channelUnitMetrics = calculateMetricsForChannelUnitSummary(visitobj)
    tier1Metrics = calculateMetricsForTier1Summary(visitobj)
    structureMetrics = calculateMetricsForStructureSummary(visitobj)

    writeMetricsToXML({
        "VisitMetrics": visitMetrics,
        "ChannelUnitMetrics": channelUnitMetrics,
        "Tier1Metrics": tier1Metrics,
        "StructureMetrics": structureMetrics
    }, visit_id, "", xmlfile, "AuxMetrics", __version__)


def load_measurement_file(measure_folder: str, url_slug: str) -> dict:
    """
    Load a measurement JSON file from a given path.
    This replaces calls to the Sitka API for aux measurement data.
    """

    file_name = f"{url_slug.replace(' ', '_').lower()}.json"
    file_path = os.path.join(measure_folder, file_name)

    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    else:
        # raise MissingException(f"Measurement file not found: {file_path}")
        pass
