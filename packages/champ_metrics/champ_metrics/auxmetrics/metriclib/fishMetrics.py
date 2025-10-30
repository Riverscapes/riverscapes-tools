import logging
from champ_metrics.lib.exception import DataException

maxSizeToBeConsideredJuvinile = 250

otherSpecies = [
    "Chum",
    "Sucker",
    "Mountain White Fish",
    "Rainbow",
    "Sculpin",
    "Cyprinids",
    "Stickleback",
    "Other",
    "Unknown",
    "Dace Species",
    "Sunfish",
    "Unknown Salmon",
    "Tailed Frog",
    "Tailed Frog Tadpole",
    "Pacific Giant Salamander"
]

metricMapping = [
    ("Chinook", "CountOfChinook"),
    ("Coho", "CountOfCoho"),
    ("Sockeye", "CountOfSockeye"),
    ("O. mykiss", "CountOfOmykiss"),
    ("Pink", "CountOfPink"),
    ("Cutthroat", "CountOfCutthroat"),
    ("Bulltrout", "CountOfBulltrout"),
    ("Brooktrout", "CountOfBrooktrout"),
    ("Lamprey", "CountOfLamprey"),
    (otherSpecies, "CountOfOtherSpecies"),
]


def visitFishCountMetricsForSpecies(visitMetrics, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned, speciesNames, metricName):
    if snorkelFish is None and snorkelFishBinned is None and snorkelFishSteelheadBinned is None:
        visitMetrics[metricName] = None
        return

    snorkelFishJuvinileCount = 0
    snorkelFishBinnedJuvinileCount = 0
    snorkelFishSteelheadBinnedJuvinileCount = 0

    if not isinstance(speciesNames, list):
        speciesNames = [speciesNames]

    if snorkelFish is not None:
        snorkelFishForSpecies = [s for s in snorkelFish["values"] if s["value"]["FishSpecies"] in speciesNames]
        snorkelFishJuvinileCount = sum([int(s["value"]["FishCount"]) for s in snorkelFishForSpecies if int(s["value"]["SizeClass"].replace("mm", "").replace(">", "")) <= maxSizeToBeConsideredJuvinile])

    if snorkelFishBinned is not None:
        snorkelFishBinnedForSpecies = [s for s in snorkelFishBinned["values"] if s["value"]["FishSpecies"] in speciesNames]
        snorkelFishBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to69mm"]) + int(s["value"]["FishCount70to89mm"]) +
                                             int(s["value"]["FishCount90to99mm"]) + int(s["value"]["FishCountGT100mm"]) for s in snorkelFishBinnedForSpecies])

    if snorkelFishSteelheadBinned is not None:
        snorkelFishSteelheadBinnedForSpecies = [s for s in snorkelFishSteelheadBinned["values"] if s["value"]["FishSpecies"] in speciesNames]
        snorkelFishSteelheadBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to79mm"]) + int(s["value"]["FishCount80to129mm"]) +
                                                      int(s["value"]["FishCount130to199mm"]) + int(s["value"]["FishCount200to249mm"]) for s in snorkelFishSteelheadBinnedForSpecies])

    count = snorkelFishJuvinileCount + snorkelFishBinnedJuvinileCount + snorkelFishSteelheadBinnedJuvinileCount

    visitMetrics[metricName] = count


def visitFishCountMetrics(visitMetrics, visitobj):
    snorkelFish = visitobj['snorkelFish']
    snorkelFishBinned = visitobj['snorkelFishBinned']
    snorkelFishSteelheadBinned = visitobj['snorkelFishSteelheadBinned']

    for mItem in metricMapping:
        try:
            visitFishCountMetricsForSpecies(visitMetrics, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned,
                                            mItem[0], mItem[1])
        except AttributeError as e:
            raise DataException("visitFishCountMetricsForSpecies: Missing attribute for item: {}, {}".format(str(mItem[0]), str(mItem[1])))


def channelUnitFishCountMetricsForSpecies(channelUnitMetrics, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned, speciesNames, metricName):
    for c in channelUnitMetrics:
        channelUnitID = c["ChannelUnitID"]
        if snorkelFish is None and snorkelFishBinned is None and snorkelFishSteelheadBinned is None:
            c[metricName] = None
            continue

        snorkelFishJuvinileCount = 0
        snorkelFishBinnedJuvinileCount = 0
        snorkelFishSteelheadBinnedJuvinileCount = 0

        if not isinstance(speciesNames, list):
            speciesNames = [speciesNames]

        if snorkelFish is not None:
            snorkelFishForSpecies = [s for s in snorkelFish["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["ChannelUnitID"] == channelUnitID]
            snorkelFishJuvinileCount = sum([int(s["value"]["FishCount"]) for s in snorkelFishForSpecies if int(s["value"]["SizeClass"].replace("mm", "").replace(">", "")) <= maxSizeToBeConsideredJuvinile])

        if snorkelFishBinned is not None:
            snorkelFishBinnedForSpecies = [s for s in snorkelFishBinned["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["ChannelUnitID"] == channelUnitID]
            snorkelFishBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to69mm"]) + int(s["value"]["FishCount70to89mm"]) +
                                                 int(s["value"]["FishCount90to99mm"]) + int(s["value"]["FishCountGT100mm"]) for s in snorkelFishBinnedForSpecies])

        if snorkelFishSteelheadBinned is not None:
            snorkelFishSteelheadBinnedForSpecies = [s for s in snorkelFishSteelheadBinned["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["ChannelUnitID"] == channelUnitID]
            snorkelFishSteelheadBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to79mm"]) + int(s["value"]["FishCount80to129mm"]) +
                                                          int(s["value"]["FishCount130to199mm"]) + int(s["value"]["FishCount200to249mm"]) for s in snorkelFishSteelheadBinnedForSpecies])

        count = snorkelFishJuvinileCount + snorkelFishBinnedJuvinileCount + snorkelFishSteelheadBinnedJuvinileCount

        c[metricName] = count


def channelUnitFishCountMetrics(channelUnitMetrics, visitobj):
    snorkelFish = visitobj['snorkelFish']
    snorkelFishBinned = visitobj['snorkelFishBinned']
    snorkelFishSteelheadBinned = visitobj['snorkelFishSteelheadBinned']

    for mItem in metricMapping:
        try:
            channelUnitFishCountMetricsForSpecies(channelUnitMetrics, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned,
                                                  mItem[0], mItem[1])
        except AttributeError as e:
            raise DataException("channelUnitFishCountMetricsForSpecies: Missing attribute for item: {}, {}".format(str(mItem[0]), str(mItem[1])))


def tier1FishCountMetricsForSpecies(tier1Metrics, channelUnits, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned, speciesNames, metricName):
    for t in tier1Metrics:
        tier1 = t["Tier1"]
        if snorkelFish is None and snorkelFishBinned is None and snorkelFishSteelheadBinned is None:
            t[metricName] = None
            continue

        channelUnitIDsForTier = [c["value"]["ChannelUnitID"] for c in channelUnits["values"] if c["value"]["Tier1"] == tier1]

        snorkelFishJuvinileCount = 0
        snorkelFishBinnedJuvinileCount = 0
        snorkelFishSteelheadBinnedJuvinileCount = 0

        if not isinstance(speciesNames, list):
            speciesNames = [speciesNames]

        if snorkelFish is not None:
            snorkelFishForSpecies = [s for s in snorkelFish["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["ChannelUnitID"] in channelUnitIDsForTier]
            snorkelFishJuvinileCount = sum([int(s["value"]["FishCount"]) for s in snorkelFishForSpecies if int(s["value"]["SizeClass"].replace("mm", "").replace(">", "")) <= maxSizeToBeConsideredJuvinile])

        if snorkelFishBinned is not None:
            snorkelFishBinnedForSpecies = [s for s in snorkelFishBinned["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["ChannelUnitID"] in channelUnitIDsForTier]
            snorkelFishBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to69mm"]) + int(s["value"]["FishCount70to89mm"]) +
                                                 int(s["value"]["FishCount90to99mm"]) + int(s["value"]["FishCountGT100mm"]) for s in snorkelFishBinnedForSpecies])

        if snorkelFishSteelheadBinned is not None:
            snorkelFishSteelheadBinnedForSpecies = [s for s in snorkelFishSteelheadBinned["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["ChannelUnitID"] in channelUnitIDsForTier]
            snorkelFishSteelheadBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to79mm"]) + int(s["value"]["FishCount80to129mm"]) +
                                                          int(s["value"]["FishCount130to199mm"]) + int(s["value"]["FishCount200to249mm"]) for s in snorkelFishSteelheadBinnedForSpecies])

        count = snorkelFishJuvinileCount + snorkelFishBinnedJuvinileCount + snorkelFishSteelheadBinnedJuvinileCount

        t[metricName] = count


def tier1FishCountMetrics(tier1Metrics, visitobj):
    channelUnits = visitobj['channelUnits']
    snorkelFish = visitobj['snorkelFish']
    snorkelFishBinned = visitobj['snorkelFishBinned']
    snorkelFishSteelheadBinned = visitobj['snorkelFishSteelheadBinned']

    for mItem in metricMapping:
        try:
            tier1FishCountMetricsForSpecies(tier1Metrics, channelUnits, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned,
                                            mItem[0], mItem[1])
        except AttributeError as e:
            raise DataException("tier1FishCountMetrics: Missing attribute for item: {}, {}".format(str(mItem[0]), str(mItem[1])))


def structureFishCountMetricsForSpecies(structureMetrics, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned, speciesNames, metricName):
    for t in structureMetrics:
        structure = t["HabitatStructure"]
        if snorkelFish is None and snorkelFishBinned is None and snorkelFishSteelheadBinned is None:
            t[metricName] = None
            continue

        snorkelFishJuvinileCount = 0
        snorkelFishBinnedJuvinileCount = 0
        snorkelFishSteelheadBinnedJuvinileCount = 0

        if not isinstance(speciesNames, list):
            speciesNames = [speciesNames]

        if snorkelFish is not None:
            snorkelFishForSpecies = [s for s in snorkelFish["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["HabitatStructure"] == structure]
            snorkelFishJuvinileCount = sum([int(s["value"]["FishCount"]) for s in snorkelFishForSpecies if int(s["value"]["SizeClass"].replace("mm", "").replace(">", "")) <= maxSizeToBeConsideredJuvinile])

        if snorkelFishBinned is not None:
            snorkelFishBinnedForSpecies = [s for s in snorkelFishBinned["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["HabitatStructure"] == structure]
            snorkelFishBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to69mm"]) + int(s["value"]["FishCount70to89mm"]) +
                                                 int(s["value"]["FishCount90to99mm"]) + int(s["value"]["FishCountGT100mm"]) for s in snorkelFishBinnedForSpecies])

        if snorkelFishSteelheadBinned is not None:
            snorkelFishSteelheadBinnedForSpecies = [s for s in snorkelFishSteelheadBinned["values"] if s["value"]["FishSpecies"] in speciesNames and s["value"]["HabitatStructure"] == structure]
            snorkelFishSteelheadBinnedJuvinileCount = sum([int(s["value"]["FishCountLT50mm"]) + int(s["value"]["FishCount50to79mm"]) + int(s["value"]["FishCount80to129mm"]) +
                                                          int(s["value"]["FishCount130to199mm"]) + int(s["value"]["FishCount200to249mm"]) for s in snorkelFishSteelheadBinnedForSpecies])

        count = snorkelFishJuvinileCount + snorkelFishBinnedJuvinileCount + snorkelFishSteelheadBinnedJuvinileCount

        t[metricName] = count


def structureFishCountMetrics(structureMetrics, visitobj):
    snorkelFish = visitobj['snorkelFish']
    snorkelFishBinned = visitobj['snorkelFishBinned']
    snorkelFishSteelheadBinned = visitobj['snorkelFishSteelheadBinned']

    for mItem in metricMapping:
        try:
            structureFishCountMetricsForSpecies(structureMetrics, snorkelFish, snorkelFishBinned, snorkelFishSteelheadBinned,
                                                mItem[0], mItem[1])
        except AttributeError as e:
            raise DataException("structureFishCountMetrics: Missing attribute for item: {}, {}".format(str(mItem[0]), str(mItem[1])))
