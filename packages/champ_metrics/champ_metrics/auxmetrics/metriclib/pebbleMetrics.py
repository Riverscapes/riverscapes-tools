import numpy as np
from rscommons import Logger


def visitPebbleMetrics(visitMetrics, visitobj):
    visit = visitobj['visit']
    pebbles = visitobj['pebbles']
    pebbleCrossSections = visitobj['pebbleCrossSections']
    channelUnits = visitobj['channelUnits']

    # ChampMetricVisitInformation.AvgFastWaterCobbleEmbeddedness
    fastWaterCobbleEmbeddednessAvg(visitMetrics, visit, pebbles)
    # ChampMetricVisitInformation.StdDeviationOfFastWaterCobbleEmbeddedness
    fastWaterCobbleEmbeddednessStdDev(visitMetrics, visit, pebbles)
    # ChampMetricVisitInformation.MeasurementOfD16
    measurementOfD16(visitMetrics, visit, pebbles, pebbleCrossSections, channelUnits)
    # ChampMetricVisitInformation.MeasurementOfD50
    measurementOfD50(visitMetrics, visit, pebbles, pebbleCrossSections, channelUnits)
    # ChampMetricVisitInformation.MeasurementOfD84
    measurementOfD84(visitMetrics, visit, pebbles, pebbleCrossSections, channelUnits)


def measurementOfD16(visitMetrics, visit, pebbles, pebbleCrossSections, channelUnits):
    visitMetrics["MeasurementOfD16"] = pebbleSubstrateAtPercentile(visit, 0.16, pebbles, pebbleCrossSections, channelUnits)


def measurementOfD50(visitMetrics, visit, pebbles, pebbleCrossSections, channelUnits):
    visitMetrics["MeasurementOfD50"] = pebbleSubstrateAtPercentile(visit, 0.50, pebbles, pebbleCrossSections, channelUnits)


def measurementOfD84(visitMetrics, visit, pebbles, pebbleCrossSections, channelUnits):
    visitMetrics["MeasurementOfD84"] = pebbleSubstrateAtPercentile(visit, 0.84, pebbles, pebbleCrossSections, channelUnits)


def pebbleSubstrateAtPercentile(visit, percent, pebbles, pebbleCrossSections, channelUnits):
    if visit["iterationID"] == 1:
        return pebbleSubstrateAtPercentile_2011(percent, pebbles)

    return pebbleSubstrateAtPercentile_2012(percent, pebbles, pebbleCrossSections, channelUnits)


def pebbleSubstrateAtPercentile_2011(percent, pebbles):
    records = sorted([p["value"]["Substrate"] for p in pebbles["values"] if p["value"]["Substrate"] is not None and p["value"]["Substrate"] > 0])

    len = records.__len__()
    if len == 0:
        return None

    index = int(len * percent)

    if len < index + 1:
        return records[len]

    return records[index - 1]


def pebbleSubstrateAtPercentile_2012(percent, pebbles, pebbleCrossSections, channelUnits):
    summaries = getPebbleSubstrateSummary(pebbles, pebbleCrossSections, channelUnits)

    if summaries.__len__() == 0:
        return None

    substrateLow = {}
    substrateHigh = {}

    for s in summaries:
        substrateLow = substrateHigh
        substrateHigh = s

        if substrateHigh["CumulativePercentOfPebbles"] >= percent:
            break

    result = None

    if substrateHigh is not None and substrateHigh["CumulativePercentOfPebbles"] == percent:
        result = substrateHigh["MaxDiameter"]
    elif substrateHigh is not None and substrateLow is None:
        ratio = percent/substrateHigh["CumulativePercentOfPebbles"]
        result = np.exp(ratio * substrateHigh["LogMaxDiameter"]) / 100
    elif substrateLow is not None and substrateHigh is not None and substrateHigh["CumulativePercentOfPebbles"] > percent:
        ratioN = (percent - substrateLow["CumulativePercentOfPebbles"])
        ratioD = (substrateHigh["CumulativePercentOfPebbles"] - substrateLow["CumulativePercentOfPebbles"])
        ratio = ratioN / ratioD

        result = np.exp((ratio * (substrateHigh["LogMaxDiameter"] - substrateLow["LogMaxDiameter"])) + substrateLow["LogMaxDiameter"]) / 100

    if result is None:
        return None

    if result > 1:
        return np.round(result)

    if result < 0.02:
        result = 0.02

    return result


maxDiameterDictionary = {
    "0.0002 - 0.06mm": 0.06,
    "0.06 - 2mm": 2,
    "2 - 4mm": 4,
    "4 - 5.7mm": 5.7,
    "5.7 - 8mm": 8,
    "8 - 11.3mm": 11.3,
    "11.3 - 16mm": 16,
    "16 - 22.5mm": 22.5,
    "22.5 - 32mm": 32,
    "32 - 45mm": 45,
    "45 - 64mm": 64,
    "64 - 90mm": 90,
    "90 - 128mm": 128,
    "128 - 180mm": 180,
    "180 - 256mm": 256,
    "256 - 362mm": 362,
    "362 - 512mm": 512
}


def getPebbleSubstrateSummary(pebbles, pebbleCrossSections, channelUnits):
    if channelUnits is None or pebbleCrossSections is None:
        return []

    channelUnitIDs = [c["value"]["ChannelUnitID"] for c in channelUnits["values"] if c["value"]["ChannelUnitID"] is not None and c["value"]["Tier1"] != "Slow/Pool"]
    crossSectionIDsInScope = [c["value"]["CrossSectionID"] for c in pebbleCrossSections["values"] if c["value"]["CrossSectionID"] is not None and c["value"]["ChannelUnitID"] is not None and c["value"]["ChannelUnitID"] in channelUnitIDs]

    pebblesInScope = [p for p in pebbles["values"] if p["value"]["CrossSectionID"] is not None and p["value"]["CrossSectionID"] in crossSectionIDsInScope]
    result = []

    for substrate in maxDiameterDictionary:
        if substrate not in [s["value"]["SubstrateSizeClass"] for s in pebblesInScope]:
            continue

        summary = dict()
        summary["SubstrateSizeClass"] = substrate
        summary["MaxDiameter"] = maxDiameterDictionary[substrate]
        summary["LogMaxDiameter"] = np.log(summary["MaxDiameter"] * 100)
        summary["CountPebbles"] = [p for p in pebblesInScope if p["value"]["SubstrateSizeClass"] == substrate].__len__()

        result.append(summary)

    for pebble in sorted([p for p in pebblesInScope if p["value"]["SubstrateSizeClass"] == "> 512mm" and p["value"]["Substrate"] is not None and p["value"]["Substrate"] > 512], key=sortSubstrate):
        summary = next((r for r in result if r["MaxDiameter"] == pebble["value"]["Substrate"]), None)  # FirstOrDefault

        if summary is None:
            summary = dict()
            summary["SubstrateSizeClass"] = "> 512mm"
            summary["MaxDiameter"] = pebble["value"]["Substrate"]
            summary["LogMaxDiameter"] = np.log(summary["MaxDiameter"] * 100)
            summary["CountPebbles"] = 0

            result.append(summary)

        summary["CountPebbles"] = summary["CountPebbles"] + 1

    if result.__len__() == 0:
        return result

    totalPebbles = np.sum([p["CountPebbles"] for p in result])
    cumulativePercent = 0.0

    result = sorted(result, key=sortMaxDiameter)

    for s in result:
        s["PercentOfPebbles"] = float(s["CountPebbles"]) / float(totalPebbles)
        cumulativePercent = cumulativePercent + s["PercentOfPebbles"]
        s["CumulativePercentOfPebbles"] = cumulativePercent

    return result


def sortSubstrate(a):
    return a["value"]["Substrate"]


def sortMaxDiameter(a):
    return a["MaxDiameter"]


def getCobbles(visit, pebbles):
    if visit["iterationID"] == 1:
        return [p for p in pebbles["values"] if p["value"]["CobbleEmbededPercent"] is not None and p["value"]["CobblePercentFines"] is not None and p["value"]["Substrate"] is not None and p["value"]["Substrate"] > 64 and p["value"]["Substrate"] <= 250]

    substrateSizeForCobbles = ["64 - 90mm", "90 - 128mm", "128 - 180mm", "180 - 256mm"]

    res = [p for p in pebbles["values"] if p["value"]["CobbleEmbededPercent"] is not None and p["value"]["SubstrateSizeClass"] is not None and p["value"]["SubstrateSizeClass"] in substrateSizeForCobbles]
    res = [p for p in res if (p["value"]["CobbleEmbededPercent"] > 0 and p["value"]["CobblePercentFines"] is not None) or (p["value"]["CobbleEmbededPercent"] == 0)]
    return res


def fastWaterCobbleEmbeddednessAvg(visitMetrics, visit, pebbles):
    visitMetrics["FastWaterCobbleEmbeddednessAvg"] = None

    if pebbles is not None:
        cobbles = getCobbles(visit, pebbles)

        if cobbles.__len__() > 0:
            embeddedness = [(c["value"]["CobbleEmbededPercent"] * (c["value"]["CobblePercentFines"] if c["value"]["CobblePercentFines"] is not None else 0)) for c in cobbles]
            visitMetrics["FastWaterCobbleEmbeddednessAvg"] = np.mean(embeddedness)/100


def fastWaterCobbleEmbeddednessStdDev(visitMetrics, visit, pebbles):
    visitMetrics["FastWaterCobbleEmbeddednessStdDev"] = None

    if pebbles is not None:
        cobbles = getCobbles(visit, pebbles)

        if cobbles.__len__() > 0:
            embeddedness = [(c["value"]["CobbleEmbededPercent"] * (c["value"]["CobblePercentFines"] if c["value"]["CobblePercentFines"] is not None else 0)) for c in cobbles]
            visitMetrics["FastWaterCobbleEmbeddednessStdDev"] = np.std(embeddedness, ddof=1)/100
