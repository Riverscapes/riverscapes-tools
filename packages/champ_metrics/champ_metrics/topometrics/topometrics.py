from rscommons import Logger
from champ_metrics.lib.metricxmloutput import writeMetricsToXML, integrateMetricDictionary, integrateMetricList
from champ_metrics.lib.channelunits import loadChannelUnitsFromAPI, loadChannelUnitsFromJSON, loadChannelUnitsFromSQLite
from champ_metrics.__version__ import __version__

from .TopoData import TopoData
from .methods.thalweg import ThalwegMetrics
from .methods.centerline import CenterlineMetrics
from .methods.channelunit import ChannelUnitMetrics
from .methods.waterextent import WaterExtentMetrics
from .methods.crosssection import CrossSectionMetrics
from .methods.island import IslandMetrics
from .methods.raster import RasterMetrics
from .methods.bankfull import BankfullMetrics


def visit_topo_metrics(visit_id: int, topo_project_xml: str, topo_data_folder: str, channel_units_file: str, workbench_db: str, channel_unit_defs: dict, metric_xml_path: str) -> dict:
    """Calculate all the topometrics for a given visit and write them to an XML file."""

    log = Logger('Metrics')
    log.info(f'Topo topometrics for visit {visit_id}')
    log.info(f'Loading topo data from {topo_data_folder}')

    topo = TopoData(topo_project_xml, visit_id)
    topo.loadlayers()

    # Load the channel unit information from the argument XML file
    if channel_units_file is not None:
        channelUnitInfo = loadChannelUnitsFromJSON(channel_units_file)
    elif workbench_db is not None:
        channelUnitInfo = loadChannelUnitsFromSQLite(visit_id, workbench_db)
    else:
        channelUnitInfo = loadChannelUnitsFromAPI(visit_id)

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
    cuResults = ChannelUnitMetrics(topo.ChannelUnits, topo.Thalweg, topo.Depth, visitMetrics, channelUnitInfo, channel_unit_defs)
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

    log.info(f'Metric calculation complete for visit {visit_id}')
    return visitMetrics
