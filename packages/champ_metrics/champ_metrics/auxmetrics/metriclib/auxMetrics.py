import json

from .metricUtil import populateDefaultColumns

from .reachMetrics import visitReachMetrics
from .pebbleMetrics import visitPebbleMetrics
from .coverMetrics import visitCoverMetrics
from .fishMetrics import visitFishCountMetrics, channelUnitFishCountMetrics, tier1FishCountMetrics, structureFishCountMetrics
from .woodMetrics import visitLWDMetrics, channelUnitLWDMetrics, tier1LWDMetrics
from champmetrics.lib.channelunits import getCleanTierName


def calculateMetricsForVisit(visitobj):
    visitMetrics = dict()
    visitid = visitobj["visit_id"]
    populateDefaultColumns(visitMetrics, visitid)

    visitFishCountMetrics(visitMetrics, visitobj)
    visitLWDMetrics(visitMetrics, visitobj)
    visitCoverMetrics(visitMetrics, visitobj)
    visitPebbleMetrics(visitMetrics, visitobj)
    visitReachMetrics(visitMetrics, visitobj)

    return visitMetrics


def calculateMetricsForChannelUnitSummary(visitobj):
    channelUnitMetrics = []
    visitid = visitobj["visit_id"]
    channelUnits = visitobj['channelUnits']
    # create the channel unit summary metric rows with the channel unit id

    if channelUnits is not None:
        for c in channelUnits["values"]:
            cu = dict()
            cu["ChannelUnitID"] = c["value"]["ChannelUnitID"]
            cu["ChannelUnitNumber"] = c["value"]["ChannelUnitNumber"]
            cu["Tier1"] = c["value"]["Tier1"]
            cu["Tier2"] = c["value"]["Tier2"]
            populateDefaultColumns(cu, visitid)
            channelUnitMetrics.append(cu)

        channelUnitFishCountMetrics(channelUnitMetrics, visitobj)
        channelUnitLWDMetrics(channelUnitMetrics, visitobj)

    return channelUnitMetrics


def calculateMetricsForTier1Summary(visitobj):
    visitid = visitobj["visit_id"]
    channelUnits = visitobj['channelUnits']
    tier1Metrics = []
    # create the tier 1 summary metric rows with the correct tier 1
    if channelUnits is not None:
        tier1s = [t["value"]["Tier1"] for t in channelUnits["values"]]
        tier1s = list(set(tier1s))  # this is a quick distinct

        for c in tier1s:
            t = dict()
            t["Tier1"] = c
            populateDefaultColumns(t, visitid)
            tier1Metrics.append(t)

        tier1FishCountMetrics(tier1Metrics, visitobj)
        tier1LWDMetrics(tier1Metrics, visitobj)

    results = {}
    for t1Metrics in tier1Metrics:
        rawt1Name = t1Metrics['Tier1']
        safet1Name = getCleanTierName(rawt1Name)
        results[safet1Name] = t1Metrics

    return results


def calculateMetricsForStructureSummary(visitobj):
    visitid = visitobj["visit_id"]
    snorkelFish = visitobj['snorkelFish']
    snorkelFishBinned = visitobj['snorkelFishBinned']
    snorkelFishSteelheadBinned = visitobj['snorkelFishSteelheadBinned']
    structureMetrics = []
    # create the correct structure type metric summaries
    structures = []

    if snorkelFish is not None:
        structures.extend([t["value"]["HabitatStructure"] for t in snorkelFish["values"]])
    if snorkelFishBinned is not None:
        structures.extend([t["value"]["HabitatStructure"] for t in snorkelFishBinned["values"]])
    if snorkelFishSteelheadBinned is not None:
        structures.extend([t["value"]["HabitatStructure"] for t in snorkelFishSteelheadBinned["values"]])

    st = list(set(structures))

    for c in st:
        t = dict()
        t["HabitatStructure"] = c
        populateDefaultColumns(t, visitid)
        structureMetrics.append(t)

    structureFishCountMetrics(structureMetrics, visitobj)

    return structureMetrics
