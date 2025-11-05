import numpy as np
import math

largeWoodVolumeEstimatesDict = {
    "SmallSmall": 0.02035,
    "SmallMedium": 0.04878,
    "SmallLarge": 0.10758,
    "SmallMidLarge": 0.10470,
    "SmallExtraLarge": 0.23794,
    "MediumSmall": 0.05981,
    "MediumMedium": 0.15101,
    "MediumLarge": 0.40012,
    "MediumMidLarge": 0.33875,
    "MediumExtraLarge": 0.82393,
    "LargeSmall": 0.22887,
    "LargeMedium": 0.57739,
    "LargeLarge": 1.72582,
    "MidLargeSmall": 0.21187,
    "MidLargeMedium": 0.51680,
    "MidLargeMidLarge": 1.12232,
    "MidLargeExtraLarge": 2.71169,
    "ExtraLargeSmall": 0.8432,
    "ExtraLargeMedium": 1.8900,
    "ExtraLargeMidLarge": 3.82249,
    "ExtraLargeExtraLarge": 10.54683,
}

woodyDebrisJamVolumeEstimatesDict = {
    "SmallSmall": 0.02035,
    "SmallMedium": 0.04878,
    "SmallMidLarge": 0.10470,
    "SmallExtraLarge": 0.23794,
    "MediumSmall": 0.05981,
    "MediumMedium": 0.15101,
    "MediumMidLarge": 0.33875,
    "MediumExtraLarge": 0.82393,
    "MidLargeSmall": 0.21187,
    "MidLargeMedium": 0.51680,
    "MidLargeMidLarge": 1.12232,
    "MidLargeExtraLarge": 2.71169,
    "ExtraLargeSmall": 0.84320,
    "ExtraLargeMedium": 1.8900,
    "ExtraLargeMidLarge": 3.82249,
    "ExtraLargeExtraLarge": 10.54683
}


def visitLWDMetrics(visitMetrics, visitobj):
    visit = visitobj['visit']
    channelUnits = visitobj['channelUnits']
    largeWoodyPieces = visitobj['largeWoodyPieces']
    largeWoodyDebris = visitobj['largeWoodyDebris']
    woodyDebrisJams = visitobj['woodyDebrisJams']
    jamHasChannelUnits = visitobj['jamHasChannelUnits']

    visitLWDVolumeStdDev(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, None, "WettedLWDVolumeStdDev", True)
    visitLWDVolumeStdDev(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, None, "BankfullLWDVolumeStdDev", False)
    visitLargeWoodVolumeBySite(visitMetrics, visit, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "WettedLargeWoodVolumeBySite", True)
    visitLargeWoodVolumeBySite(visitMetrics, visit, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "BankfullLargeWoodVolumeBySite", False)
    visitLargeWoodVolumeForTier1AndWetted(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "Slow/Pool", "WettedLargeWoodVolumeInPools", True)
    visitLargeWoodVolumeForTier1AndWetted(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "Slow/Pool", "BankfullLargeWoodVolumeInPools", False)
    visitLargeWoodVolumeForTier1AndWetted(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "Fast-Turbulent", "WettedLargeWoodVolumeInFastTurbulent", True)
    visitLargeWoodVolumeForTier1AndWetted(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "Fast-Turbulent", "BankfullLargeWoodVolumeInFastTurbulent", False)
    visitLargeWoodVolumeForTier1AndWetted(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "Fast-NonTurbulent/Glide", "WettedLargeWoodVolumeInFastNonTurbulent", True)
    visitLargeWoodVolumeForTier1AndWetted(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, "Fast-NonTurbulent/Glide", "BankfullLargeWoodVolumeInFastNonTurbulent", False)

    if largeWoodyPieces is not None:
        visitMetrics["LargeWoodyPiecesCountPoolForming"] = [p for p in largeWoodyPieces["value"] if p["value"]["IsPoolForming"] is not None and p["value"]["IsPoolForming"] == "Yes"].__len__()
        visitMetrics["LargeWoodyPiecesCountIsKey"] = [p for p in largeWoodyPieces["value"] if p["value"]["IsKeyPiece"] is not None and p["value"]["IsKeyPiece"] == "Yes"].__len__()
        visitMetrics["LargeWoodyPiecesCountIsJam"] = [p for p in largeWoodyPieces["value"] if p["value"]["IsJam"] is not None and p["value"]["IsJam"] == "Yes"].__len__()
        visitMetrics["LargeWoodyPiecesCountRightBank"] = [p for p in largeWoodyPieces["value"] if p["value"]["PieceLocation"] is not None and p["value"]["PieceLocation"] == "Right"].__len__()
        visitMetrics["LargeWoodyPiecesCountLeftBank"] = [p for p in largeWoodyPieces["value"] if p["value"]["PieceLocation"] is not None and p["value"]["PieceLocation"] == "Left"].__len__()
        visitMetrics["LargeWoodyPiecesCountMidChannel"] = [p for p in largeWoodyPieces["value"] if p["value"]["PieceLocation"] is not None and p["value"]["PieceLocation"] == "Mid-Channel"].__len__()
        visitMetrics["LargeWoodyPiecesCount"] = [p for p in largeWoodyPieces["value"]].__len__()
    else:
        visitMetrics["LargeWoodyPiecesCountPoolForming"] = None
        visitMetrics["LargeWoodyPiecesCountIsKey"] = None
        visitMetrics["LargeWoodyPiecesCountIsJam"] = None
        visitMetrics["LargeWoodyPiecesCountRightBank"] = None
        visitMetrics["LargeWoodyPiecesCountLeftBank"] = None
        visitMetrics["LargeWoodyPiecesCountMidChannel"] = None
        visitMetrics["LargeWoodyPiecesCount"] = None


def visitLargeWoodVolumeForTier1AndWetted(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, tier1, metricName, isWet):

    channelUnitIDs = filterChannelUnitsToTier1Type(channelUnits, tier1, True)

    vols = getDebrisVolumeForVisitAndChannelUnits(visit, channelUnitIDs, isWet, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits)
    volume = np.sum(vols)

    visitMetrics[metricName] = volume


def visitLargeWoodVolumeBySite(visitMetrics, visit, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, metricName, isWet):

    vols = getDebrisVolumeForVisitAndChannelUnits(visit, None, isWet, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits)
    volume = np.sum(vols)

    visitMetrics[metricName] = volume


def visitLWDVolumeStdDev(visitMetrics, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, tier1, metricName, isWet):
    # get all channel units that are not OffChannel
    channelUnitIDs = filterChannelUnitsToTier1Type(channelUnits, tier1, True)

    if channelUnitIDs.__len__() == 0:
        visitMetrics[metricName] = None
        return

    # get all LWD volumes for those channel unit.
    vols = getDebrisVolumeForVisitAndChannelUnits(visit, channelUnitIDs, isWet, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits)
    if vols.__len__() > 1:
        # get standard dev and store in: WettedLWDVolumeStdDev
        stdDev = np.std(vols, ddof=1)
        visitMetrics[metricName] = stdDev
    elif vols.__len__() == 1:
        visitMetrics[metricName] = 0
    else:
        visitMetrics[metricName] = None


def filterChannelUnitsToTier1Type(channelUnits, tier1Type, excludeOffChannel):
    if channelUnits is None:
        return []

    # filter channel units without channel unit id set
    cus = [c for c in channelUnits["value"] if c["value"]["ChannelUnitID"] is not None]
    # filter out off channels
    if excludeOffChannel:
        cus = [c for c in cus if c["value"]["Tier2"] != "Off Channel"]

    if tier1Type is not None:
        cus = [c for c in cus if c["value"]["Tier1"] == tier1Type]

    return [c["value"]["ChannelUnitID"] for c in cus]


def getDebrisVolumeForVisitAndChannelUnits(visit, channelUnitIDs, isWet, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits):
    if visit["iterationID"] >= 4:
        return getDebrisVolumeForVisitAndChannelUnits_2014(channelUnitIDs, isWet, largeWoodyPieces)

    return getDebrisVolumeForVisitAndChannelUnits_2013Backwards(channelUnitIDs, isWet, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits)


def getDebrisVolumeForVisitAndChannelUnits_2014(channelUnitIDs, isWet, largeWoodyPieces):
    if largeWoodyPieces is None:
        return []

    # filter large woody piece to one's with cuIDs
    pieces = [p for p in largeWoodyPieces["value"] if p["value"]["ChannelUnitID"] is not None]
    # filter to id's that are passed in
    if channelUnitIDs is not None:
        pieces = [p for p in pieces if p["value"]["ChannelUnitID"] in channelUnitIDs]

    # filter to wet
    if isWet:
        pieces = [p for p in pieces if p["value"]["LargeWoodType"] == "Wet"]

    # calculate volume for each piece and return as list.
    return [np.pi * math.pow((p["value"]["DiameterM"] / 2.0), 2) * p["value"]["LengthM"] for p in pieces]


def getDebrisVolumeForVisitAndChannelUnits_2013Backwards(channelUnitIDs, isWet, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits):
    if largeWoodyDebris is None:
        return []

    # filter large woody debris to one's with cuIDs
    debris = [p for p in largeWoodyDebris["value"] if p["value"]["ChannelUnitID"] is not None]

    # filter to id's that are passed in
    if channelUnitIDs is not None:
        debris = [p for p in debris if p["value"]["ChannelUnitID"] in channelUnitIDs]

    # filter debris to wet
    if isWet:
        debris = [p for p in debris if p["value"]["IsDry"] is None or not p["value"]["IsDry"]]

    # get volumes for debris
    volumes = [volumeEstimates(p, largeWoodVolumeEstimatesDict) for p in debris]

    if woodyDebrisJams is None or jamHasChannelUnits is None:
        return volumes

    # filter jams to wet
    jams = [j for j in woodyDebrisJams["value"]]
    if isWet:
        jams = [j for j in jams if j["value"]["IsDry"] is None or not j["value"]["IsDry"]]

    # filter jam has channel units to passed in channel units
    jamUnits = [j for j in jamHasChannelUnits["value"] if j["value"]["ProportionOfJam"] is not None and j["value"]["ChannelUnitID"] is not None]
    if channelUnitIDs is not None:
        jamUnits = [j for j in jamUnits if j["value"]["ChannelUnitID"] in channelUnitIDs]

    # match jam to jam has channel unit and compute volume and add to volume array
    for ju in jamUnits:
        jam = next((j for j in jams if j["value"]["WoodyDebrisJamID"] == ju["value"]["WoodyDebrisJamID"]), None)  # FirstOrDefault
        volumes.append(volumeEstimates(jam, woodyDebrisJamVolumeEstimatesDict) * ju["value"]["ProportionOfJam"] / 100)

    return volumes


def volumeEstimates(measurement, volumeEstimatesDict):
    result = 0.0

    if measurement is not None:
        for attrib in volumeEstimatesDict:
            volEst = volumeEstimatesDict[attrib]
            measVal = measurement["value"][attrib]
            if measVal is None:
                continue

            result = result + volEst * measVal

    return result


def channelUnitLWDMetrics(channelUnitMetrics, visitobj):
    largeWoodyPieces = visitobj['largeWoodyPieces']
    largeWoodyDebris = visitobj['largeWoodyDebris']
    for c in channelUnitMetrics:
        channelUnitID = c["ChannelUnitID"]
        if largeWoodyPieces is not None:
            c["LargeWoodyPiecesCount"] = [p for p in largeWoodyPieces["value"] if
                                          p["value"]["ChannelUnitID"] is not None and p["value"][
                                              "ChannelUnitID"] == channelUnitID].__len__()
        elif largeWoodyDebris is not None:
            c["LargeWoodyPiecesCount"] = sum([p['value']['SumLWDCount'] for p in largeWoodyDebris["value"] if p["value"]["ChannelUnitID"] is not None and p["value"]["ChannelUnitID"] == channelUnitID])
        else:
            c["LargeWoodyPiecesCount"] = 0


def tier1LWDMetrics(tier1Metrics, visitobj):
    visit = visitobj['visit']
    channelUnits = visitobj['channelUnits']
    largeWoodyPieces = visitobj['largeWoodyPieces']
    largeWoodyDebris = visitobj['largeWoodyDebris']
    woodyDebrisJams = visitobj['woodyDebrisJams']
    jamHasChannelUnits = visitobj['jamHasChannelUnits']

    for t in tier1Metrics:
        tier1 = t["Tier1"]
        visitLargeWoodVolumeForTier1AndWetted(t, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, tier1, "BankfullLargeWoodVolumeByTier1", False)
        visitLargeWoodVolumeForTier1AndWetted(t, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, tier1, "WettedLargeWoodVolumeByTier1", True)

        channelUnitIDsForTier = [c["value"]["ChannelUnitID"] for c in channelUnits["value"] if c["value"]["Tier1"] == tier1]

        if largeWoodyPieces is not None:
            t["LargeWoodyPiecesCount"] = [p for p in largeWoodyPieces["value"] if p["value"]["ChannelUnitID"] in channelUnitIDsForTier].__len__()
            visitLWDVolumeStdDev(t, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, tier1, "WettedLWDVolumeStdDev", True)
            visitLWDVolumeStdDev(t, visit, channelUnits, largeWoodyPieces, largeWoodyDebris, woodyDebrisJams, jamHasChannelUnits, tier1, "BankfullLWDVolumeStdDev", False)
        else:
            t["LargeWoodyPiecesCount"] = 0
            t["WettedLWDVolumeStdDev"] = None
            t["BankfullLWDVolumeStdDev"] = None
