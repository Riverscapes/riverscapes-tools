import os
from rscommons import Logger
from champ_metrics.lib.metricxmloutput import writeMetricsToXML, integrateMetricDictionary, integrateMetricList
from champ_metrics.lib.channelunits import loadChannelUnitsFromJSON
from champ_metrics.__version__ import __version__

from .TopoData import TopoData
from .methods.thalweg import ThalwegMetrics
from .methods.centerline import CenterlineMetrics
from .methods.channelunit import ChannelUnitMetrics, dUnitDefs
from .methods.waterextent import WaterExtentMetrics
from .methods.crosssection import CrossSectionMetrics
from .methods.island import IslandMetrics
from .methods.raster import RasterMetrics
from .methods.bankfull import BankfullMetrics

# # PB 30 Oct 2025
# # This is not currently in use. I copied it here from the old
# # ChaMP Workbench SQLite database. Without this dictionary the
# # code only produces channel unit metrics for those channel unit types
# # that are present in the visit data.
# # Providing this dictionary means that you get metric output for ALL
# # channel unit types and not just those that are present in the visit data.
# #
# # That said, the code works without this dictionary. I'm leaving it here
# # in case we want to use it in the future.
# CHANNEL_UNIT_DEFS = {
#     'Tier1': [
#         'Slow/Pool',
#         'Fast-Turbulent',
#         'Fast-NonTurbulent/Glide',
#         'Small Side Channel'
#     ],
#     'Tier2': [
#         "Fast-NonTurbulent/Glide",
#         "Riffle",
#         "Off Channel",
#         "Scour Pool",
#         "Small Side Channel",
#         "Cascade",
#         "Dam Pool",
#         "Falls",
#         "Rapid",
#         "Plunge Pool",
#     ]
# }


def visit_topo_metrics(visit_id: int, topo_project_xml: str, metric_xml_path: str) -> dict:
    """Calculate all the topometrics for a given visit and write them to an XML file."""

    log = Logger('Topo Metrics')
    log.info(f'Topo metrics for visit {visit_id}')

    topo_data_folder = os.path.dirname(topo_project_xml)
    topo = TopoData(topo_project_xml, visit_id)
    topo.loadlayers()

    channel_units_file = os.path.join(topo_data_folder, 'aux_measurements', 'channel_unit.json')
    channelUnitInfo = loadChannelUnitsFromJSON(channel_units_file)

    # This is the dictionary for all topometrics to this visit. This will get written to XML when done.
    visitMetrics = {}

    # Loop over all the channels defined in the topo data (wetted and bankfull)
    for channelName, channel in topo.Channels.items():
        log.info(f'Processing topometrics for {channelName.lower()} channel')

        # Dictionary for the topometrics in this channel (wetted or bankfull)
        dChannelMetrics = {}

        metrics_cl = CenterlineMetrics(channel.Centerline)
        integrateMetricDictionary(dChannelMetrics, 'Centerline', metrics_cl.metrics)

        metrics_we = WaterExtentMetrics(channelName, channel.Extent, channel.Centerline, topo.Depth)
        integrateMetricDictionary(dChannelMetrics, 'WaterExtent', metrics_we.metrics)

        metrics_cs = CrossSectionMetrics(channel.CrossSections, topo.Channels[channelName].Extent, topo.DEM, 0.1)
        integrateMetricDictionary(dChannelMetrics, 'CrossSections', metrics_cs.metrics)

        metrics_i = IslandMetrics(channel.Islands)
        integrateMetricDictionary(dChannelMetrics, 'Islands', metrics_i.metrics)

        # Put topometrics for this channel into the visit metric dictionary keyed by the channel (wetted or bankfull)
        integrateMetricDictionary(visitMetrics, channelName, dChannelMetrics)

        log.info(f'{channelName} channel topometrics complete')

    metrics_thal = ThalwegMetrics(topo.Thalweg, topo.Depth, topo.WaterSurface, 0.1, visitMetrics)
    integrateMetricDictionary(visitMetrics, 'Thalweg', metrics_thal.metrics)

    # Channel units creates four groupings of topometrics that are returned as a Tuple
    # PB Note 30 Oct 2025. If Channel Unit Defintions are provided as last argument of ChannelUnitMetrics
    # then those definitions are used rather than the default ones built into the method.
    # I think providing the defs means that you get metric output for ALL channel unit types and
    # not just those that are present in the visit data.
    cuResults = ChannelUnitMetrics(topo.ChannelUnits, topo.Thalweg, topo.Depth, visitMetrics, channelUnitInfo, dUnitDefs)
    integrateMetricList(visitMetrics, 'ChannelUnits', 'Unit', cuResults.metrics['resultsCU'])
    integrateMetricDictionary(visitMetrics, 'ChannelUnitsTier1', cuResults.metrics['ResultsTier1'])
    integrateMetricDictionary(visitMetrics, 'ChannelUnitsTier2', cuResults.metrics['ResultsTier2'])
    integrateMetricDictionary(visitMetrics, 'ChannelUnitsSummary', cuResults.metrics['ResultsChannelSummary'])

    temp = RasterMetrics(topo.Depth)
    integrateMetricDictionary(visitMetrics, 'WaterDepth', temp)

    temp = RasterMetrics(topo.DEM)
    integrateMetricDictionary(visitMetrics, "DEM", temp)

    temp = RasterMetrics(topo.Detrended)
    integrateMetricDictionary(visitMetrics, "Detrended", temp)

    # special bankfull metrics appending to existing dictionary entry
    visitMetrics['Bankfull']['WaterExtent'].update(BankfullMetrics(topo.DEM, topo.Detrended, topo.TopoPoints))

    # Metric calculation complete. Write the topometrics to the XML file
    writeMetricsToXML(visitMetrics, visit_id, topo_data_folder, metric_xml_path, "TopoMetrics", __version__)

    log.info(f'Topo metric calculation complete for visit {visit_id}')
    return visitMetrics
