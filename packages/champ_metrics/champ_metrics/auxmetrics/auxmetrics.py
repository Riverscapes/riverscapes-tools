import os
import json
from rsxml import Logger
from champ_metrics.lib.exception import MissingException
from champ_metrics.lib.metricxmloutput import writeMetricsToXML
from .metriclib.auxMetrics import calculateMetricsForVisit, calculateMetricsForChannelUnitSummary, calculateMetricsForTier1Summary, calculateMetricsForStructureSummary


__version__ = "0.0.4"

# {key: urlslug} dict
# This dictionary maps old API endpoints to measurement keys.
# Instead of calling the API, we will load these from local measurement JSON files.
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

    # The following are not needed by aux, but were added because they are needed by topo+aux
    "channelSegments": "Channel Segment",
    "fishCover": "Fish Cover",
    "substrateCover": "Substrate Cover"
}


def visit_aux_metrics(visit_id: int, visit_year: int, aux_data_folder: str, xmlfile: str) -> dict:
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

    # Needed for topo+aux metrics
    visitMetrics['VisitYear'] = visit_year

    writeMetricsToXML({
        "VisitMetrics": visitMetrics,
        "ChannelUnitMetrics": channelUnitMetrics,
        "Tier1Metrics": tier1Metrics,
        "StructureMetrics": structureMetrics
    }, visit_id, "", xmlfile, "AuxMetrics", __version__)

    # Including aux measurements in the return so that topo+aux metrics can use them
    return {
        "VisitMetrics": visitMetrics,
        "ChannelUnitMetrics": channelUnitMetrics,
        "Tier1Metrics": tier1Metrics,
        "StructureMetrics": structureMetrics,
        "AuxMeasurements": visitobj
    }


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

            # The old API returned JSON with object keys with capitalization.
            # Some visits still have JSON files using this convention.
            #
            # In 2025 we regenerated JSON files from the aux measurement
            # Access database migrated to postgres. These new files use
            # lowercase keys. To support both formats, we duplicate
            # keys as needed.

            # Channel units
            duplicate_key_recursive(data, "channelunitnumber", "ChannelUnitNumber")
            duplicate_key_recursive(data, 'channelunitid', 'ChannelUnitID')
            duplicate_key_recursive(data, "tier1", "Tier1")
            duplicate_key_recursive(data, "tier2", "Tier2")
            duplicate_key_recursive(data, "channelsegmentid", "ChannelSegmentID")

            # Riparian structures
            duplicate_key_recursive_riparian(data, "canopybigtreesgt30cmdbh", "CanopyBigTrees")
            duplicate_key_recursive_riparian(data, 'canopysmalltreeslt30cmdbh', 'CanopySmallTrees')
            duplicate_key_recursive_riparian(data, 'totalgroundcover', 'GroundCover')
            duplicate_key_recursive_riparian(data, 'totalunderstorycover', 'UnderstoryCover')
            duplicate_key_recursive_riparian(data, 'groundcovernonwoodyshrubs', 'GroundcoverNonWoodyShrubs')
            duplicate_key_recursive_riparian(data, 'understorynonwoodyforbesgrasses', 'UnderstoryNonWoodyForbesGrasses')
            duplicate_key_recursive_riparian(data, 'groundcovernonwoodyforbesgrasses', 'GroundcoverNonWoodyForbesGrasses')
            duplicate_key_recursive_riparian(data, 'canopywoodyconiferous', 'CanopyWoodyConiferous')
            duplicate_key_recursive_riparian(data, 'understorywoodyconiferous', 'UnderstoryWoodyConiferous')

            # Pebbles
            duplicate_key_recursive(data, 'cobblepercentburied', 'CobbleEmbededPercent')
            duplicate_key_recursive(data, 'substratesizeclass', 'SubstrateSizeClass')
            duplicate_key_recursive(data, 'cobblepercentfines', 'CobblePercentFines')

            # Reach metrics
            duplicate_key_recursive(data, 'sitelength', 'SiteLength')
            duplicate_key_recursive(data, 'averagebfwidth', 'AverageBFWidth')
            duplicate_key_recursive(data, 'stationwidth', 'StationWidth')
            duplicate_key_recursive(data, 'depth', 'Depth')
            duplicate_key_recursive(data, 'velocity', 'Velocity')
            duplicate_key_recursive(data, 'conductivity', 'Conductivity')
            duplicate_key_recursive(data, 'totalalkalinity', 'TotalAlkalinity')

            # Undercut banks
            duplicate_key_recursive(data, 'estimatedundercutarea', 'EstimatedUndercutArea')
            duplicate_key_recursive(data, 'estimatedlength', 'EstimatedLength')
            duplicate_key_recursive(data, 'averagedepth', 'AverageDepth')
            duplicate_key_recursive(data, 'averagewidth', 'AverageWidth')
            duplicate_key_recursive(data, 'midpointwidth', 'MidpointWidth')
            duplicate_key_recursive(data, 'midpointdepth', 'MidpointDepth')
            duplicate_key_recursive(data, 'averagewidth', 'MidpointWidth')
            duplicate_key_recursive(data, 'averagedepth', 'MidpointDepth')

            # Pool tail fines
            duplicate_key_recursive(data, 'grid1lessthan2mm', 'Grid1Lessthan2mm')
            duplicate_key_recursive(data, 'grid1nonmeasureable', 'Grid1NonMeasureable')
            duplicate_key_recursive(data, 'grid2lessthan2mm', 'Grid2Lessthan2mm')
            duplicate_key_recursive(data, 'grid2nonmeasureable', 'Grid2NonMeasureable')
            duplicate_key_recursive(data, 'grid3lessthan2mm', 'Grid3Lessthan2mm')
            duplicate_key_recursive(data, 'grid3nonmeasureable', 'Grid3NonMeasureable')
            duplicate_key_recursive(data, 'grid3between2and6mm', 'Grid3Btwn2and6mm')
            duplicate_key_recursive(data, 'grid1between2and6mm', 'Grid1Btwn2and6mm')
            duplicate_key_recursive(data, 'grid2between2and6mm', 'Grid2Btwn2and6mm')
            duplicate_key_recursive(data, 'width25', 'Width25Percent')
            duplicate_key_recursive(data, 'width50', 'Width50Percent')
            duplicate_key_recursive(data, 'width75', 'Width75Percent')

            duplicate_key_recursive(data, 'bouldersgt256', 'Boulders')
            duplicate_key_recursive(data, 'cobbles65255', 'Cobbles')
            duplicate_key_recursive(data, 'coarsegravel1764', 'CourseGravel')
            duplicate_key_recursive(data, 'finegravel316', 'FineGravel')
            duplicate_key_recursive(data, 'fineslt006', 'Fines')
            duplicate_key_recursive(data, 'sand0062', 'Sand')

            duplicate_key_recursive(data, 'segmenttype', 'SegmentType')
            duplicate_key_recursive(data, 'sidechannellength', 'SideChannelLengthM')
            duplicate_key_recursive(data, 'sidechannelwidth', 'SideChannelWidthM')
            duplicate_key_recursive(data, 'woodydebrisfc', 'LWDFC')
            duplicate_key_recursive(data, 'aquaticvegetationfc', 'AquaticVegetationFC')
            duplicate_key_recursive(data, 'overhangingvegetationfc', 'VegetationFC')
            duplicate_key_recursive(data, 'undercutbanksfc', 'UndercutBanksFC')
            duplicate_key_recursive(data, 'artificialfc', 'ArtificialFC')
            duplicate_key_recursive(data, 'totalnofc', 'TotalNoFC')

            duplicate_key_recursive(data, 'largewoodtype', 'LargeWoodType')
            duplicate_key_recursive(data, 'sumjamcount', 'SumJamCount')

            replace_val_with_dict(data, 'LargeWoodyDebris')
            replace_val_with_dict(data, 'LargeWoodyPiece')

        return data
    else:
        # raise MissingException(f"Measurement file not found: {file_path}")
        pass


def duplicate_key_recursive_riparian(d: dict, target_key: str, new_key: str) -> None:
    """
    Riparian has both left bank and right bank versions of many keys.
    This function duplicates both versions of the target key with the new key.
    """

    for prefix in ['lb', 'rb']:
        full_target_key = prefix.lower() + target_key
        full_new_key = prefix.upper() + new_key
        duplicate_key_recursive(d, full_target_key, full_new_key)


def duplicate_key_recursive(d: dict, target_key: str, new_key: str) -> None:
    """
    Recursively walks through a nested dictionary and,
    whenever it finds `target_key`, it inserts a new
    entry with `new_key` and the same value.
    """
    if not isinstance(d, dict):
        return

    for key, value in list(d.items()):  # use list() to avoid runtime change issues
        # If we find the target key, add the new key with the same value
        if key == target_key:
            d[new_key] = value

        # Recurse into nested dictionaries
        if isinstance(value, dict):
            duplicate_key_recursive(value, target_key, new_key)
        elif isinstance(value, list):
            # Handle list of dictionaries, if present
            for item in value:
                if isinstance(item, dict):
                    duplicate_key_recursive(item, target_key, new_key)


def replace_val_with_dict(d: dict, target_key: str) -> None:
    """
    Recursively walks through a nested dictionary and,
    whenever it finds `target_key`, it inserts a new
    entry with `new_key` and the same value.
    """
    if not isinstance(d, dict):
        return

    for key, value in list(d.items()):  # use list() to avoid runtime change issues
        # If we find the target key, add the new key with the same value
        if key == target_key and value is None:
            d[key] = {}

        # Recurse into nested dictionaries
        if isinstance(value, dict):
            replace_val_with_dict(value, target_key)
        elif isinstance(value, list):
            # Handle list of dictionaries, if present
            for item in value:
                if isinstance(item, dict):
                    replace_val_with_dict(item, target_key)
