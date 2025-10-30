import datetime
import numpy as np
from rscommons import Logger


def visitReachMetrics(visitMetrics, visitobj):
    visit = visitobj['visit']
    channelConstraints = visitobj['channelConstraints']
    channelConstraintMeasurements = visitobj['channelConstraintMeasurements']
    bankfullWidths = visitobj['bankfullWidths']
    driftInverts = visitobj['driftInverts']
    driftInvertResults = visitobj['driftInvertResults']
    sampleBiomasses = visitobj['sampleBiomasses']
    undercutBanks = visitobj['undercutBanks']
    waterChemistry = visitobj['waterChemistry']
    solarInputMeasurements = visitobj['solarInputMeasurements']
    discharge = visitobj['discharge']
    poolTailFines = visitobj['poolTailFines']

    # ChampMetricVisitInformation.ConstrainingFeatureHeightAvg
    constrainingFeatureHeightAvg(visitMetrics, channelConstraints, channelConstraintMeasurements)
    # ChampMetricVisitInformation.FloodProneWidthMetersAverage
    floodProneWidthMetersAverage(visitMetrics, channelConstraintMeasurements)
    # ChampMetricVisitInformation.ValleyWidth
    valleyWidth(visitMetrics, channelConstraints)
    # ChampMetricVisitInformation.ConstrainingFeatureTypeListItemID
    constrainingFeatures(visitMetrics, channelConstraints)
    # ChampMetricVisitInformation.PercentConstrained
    percentConstrained(visitMetrics, channelConstraints)
    # ChampMetricVisitInformation.ChannelPattern
    channelPattern(visitMetrics, channelConstraints)
    # ChampMetricVisitInformation.SiteLength
    siteLength(visitMetrics, bankfullWidths)
    # ChampMetricVisitInformation.BankfullWidthAvg
    bankfullWidthAvg(visitMetrics, bankfullWidths)
    # ChampMetricVisitInformation.DriftBiomassDensity
    driftBiomassDensity(visitMetrics, driftInverts, driftInvertResults, sampleBiomasses)
    # ChampMetricVisitInformation.TotalUndercutArea
    totalUndercutArea(visitMetrics, visit, undercutBanks)
    # ChampMetricVisitInformation.TotalUndercutVolume
    totalUndercutVolume(visitMetrics, visit, undercutBanks)
    # ChampMetricVisitInformation.SolarAccessSummerAvg
    solarAccessSummerAvg(visitMetrics, visit, solarInputMeasurements)
    # ChampMetricVisitInformation.SiteDischarge
    siteDischarge(visitMetrics, discharge)
    # ChampMetricVisitInformation.SiteMeasurementOfConductivity
    siteMeasurementOfConductivity(visitMetrics, waterChemistry)
    # ChampMetricVisitInformation.SiteMeasurementOfAlkalinity
    siteMeasurementOfAlkalinity(visitMetrics, waterChemistry)
    # ChampMetricVisitInformation.PoolTailFinesPctObservationsLessThan2mm
    poolTailFinesPctObservationsLessThan2mm(visitMetrics, visit, poolTailFines)
    # ChampMetricVisitInformation.PoolTailFinesPctObservationsLessThan6mm
    poolTailFinesPctObservationsLessThan6mm(visitMetrics, visit, poolTailFines)


def constrainingFeatureHeightAvg(visitMetrics, channelConstraints, channelConstraintMeasurements):
    if channelConstraints is None or channelConstraintMeasurements is None:
        visitMetrics["ConstrainingFeatureHeightAvg"] = None
        return

    constraintMeas = next((c for c in channelConstraints["value"]
                           if c["value"]["ConstrainingType"] == "Natural"), None)
    if constraintMeas is None:
        constraintMeas = next((c for c in channelConstraints["value"]
                               if c["value"]["ConstrainingType"] == "Levee"), None)

    if constraintMeas is None or constraintMeas["value"]["ConstrainingFeatures"] == "No Constraining Features":
        visitMetrics["ConstrainingFeatureHeightAvg"] = None
        return

    constraintID = constraintMeas["value"]["ConstraintID"]

    measValues = [m["value"]["ConstraintHeightCM"] for m in channelConstraintMeasurements["value"]
                  if m["value"]["ConstraintID"] == constraintID and m["value"]["ConstraintHeightCM"] is not None]

    if measValues.__len__() == 0:
        visitMetrics["ConstrainingFeatureHeightAvg"] = None
    else:
        visitMetrics["ConstrainingFeatureHeightAvg"] = np.mean(measValues)


def floodProneWidthMetersAverage(visitMetrics, channelConstraintMeasurements):
    if channelConstraintMeasurements is None:
        visitMetrics["FloodProneWidthMetersAverage"] = None
        return

    floodProne = [c["value"]["FloodProneWidthM"] for c in channelConstraintMeasurements["value"]
                  if c["value"]["FloodProneWidthM"] is not None]
    if floodProne.__len__() > 0:
        visitMetrics["FloodProneWidthMetersAverage"] = np.mean(floodProne)
        return

    floodProneRemote = [c["value"]["FloodProneWidthRemoteM"] for c in channelConstraintMeasurements["value"]
                        if c["value"]["FloodProneWidthRemoteM"] is not None]
    if floodProneRemote.__len__() > 0:
        visitMetrics["FloodProneWidthMetersAverage"] = np.mean(floodProneRemote)
        return

    visitMetrics["FloodProneWidthMetersAverage"] = None


def valleyWidth(visitMetrics, channelConstraints):
    if channelConstraints is None:
        visitMetrics["ValleyWidth"] = None
        return

    naturalMeas = next((c for c in channelConstraints["value"] if c["value"]["ConstrainingType"] == "Natural"
                        and c["value"]["ValleyWidthM"] is not None), None)
    if naturalMeas is not None:
        visitMetrics["ValleyWidth"] = naturalMeas["value"]["ValleyWidthM"]
        return

    leveeMeas = next((c for c in channelConstraints["value"] if c["value"]["ConstrainingType"] == "Levee"
                      and c["value"]["ValleyWidthM"] is not None), None)
    if leveeMeas is not None:
        visitMetrics["ValleyWidth"] = leveeMeas["value"]["ValleyWidthM"]
        return

    visitMetrics["ValleyWidth"] = None


def constrainingFeatures(visitMetrics, channelConstraints):
    if channelConstraints is None:
        visitMetrics["ConstrainingFeatures"] = None
        return

    visitMeas = next((c for c in channelConstraints["value"]), None)

    if visitMeas is None:
        visitMetrics["ConstrainingFeatures"] = None
    else:
        visitMetrics["ConstrainingFeatures"] = visitMeas["value"]["ConstrainingFeatures"]


def percentConstrained(visitMetrics, channelConstraints):
    if channelConstraints is None:
        visitMetrics["PercentConstrained"] = None
        return

    constraintMeas = next((c for c in channelConstraints["value"] if c["value"]["ConstrainingType"] == "Natural"), None)
    if constraintMeas is None:
        constraintMeas = next((c for c in channelConstraints["value"] if c["value"]["ConstrainingType"] == "Levee"), None)

    if constraintMeas is None or constraintMeas["value"]["ConstrainingFeatures"] == "No Constraining Features":
        visitMetrics["PercentConstrained"] = None
        return

    visitMetrics["PercentConstrained"] = constraintMeas["value"]["ContactWithConstrainingFeature"]


def channelPattern(visitMetrics, channelConstraints):
    if channelConstraints is None:
        visitMetrics["ChannelPattern"] = None
        return

    constraintMeas = next((c for c in channelConstraints["value"] if c["value"]["ConstrainingType"] == "Natural"), None)
    if constraintMeas is None:
        constraintMeas = next((c for c in channelConstraints["value"] if c["value"]["ConstrainingType"] == "Levee"), None)

    if constraintMeas is None:
        visitMetrics["ChannelPattern"] = None
        return

    visitMetrics["ChannelPattern"] = constraintMeas["value"]["ChannelPattern"]


def siteLength(visitMetrics, bankfullWidths):
    if bankfullWidths is None:
        visitMetrics["SiteLength"] = None
        return

    visitMeas = next((w for w in bankfullWidths["value"]), None)

    if visitMeas is None:
        visitMetrics["SiteLength"] = None
    else:
        visitMetrics["SiteLength"] = visitMeas["value"]["SiteLength"]


def bankfullWidthAvg(visitMetrics, bankfullWidths):
    if bankfullWidths is None:
        visitMetrics["BankfullWidthAvg"] = None
        return

    visitMeas = next((w for w in bankfullWidths["value"]), None)

    if visitMeas is None:
        visitMetrics["BankfullWidthAvg"] = None
    else:
        visitMetrics["BankfullWidthAvg"] = visitMeas["value"]["AverageBFWidth"]


def driftBiomassDensity(visitMetrics, driftInverts, driftInvertResults, sampleBiomasses):
    log = Logger("driftBiomassDensity")
    if driftInverts is None or driftInverts["value"].__len__() == 0:
        visitMetrics["DriftBiomassDensity"] = None
        return

    if driftInvertResults is None or driftInvertResults["value"].__len__() == 0:
        visitMetrics["DriftBiomassDensity"] = None
        return

    if sampleBiomasses is None:
        visitMetrics["DriftBiomassDensity"] = None
        return

    volumes = [s["value"]["VolumeSampled"] for s in driftInverts["value"]]

    if any([v is None for v in volumes]):
        log.warning("VolumeSampled contains 'None'")

    sumVolumeSampled = np.sum([v for v in volumes if v is not None])
    sampleResult = next((i for i in driftInvertResults["value"]))
    sumSampleBiomass = np.sum([s["value"]["DryMassGrams"] / sampleResult["value"]["PortionOfSampleSorted"]
                               for s in sampleBiomasses["value"]])

    visitMetrics["DriftBiomassDensity"] = None

    if sumVolumeSampled > 0:
        visitMetrics["DriftBiomassDensity"] = sumSampleBiomass / sumVolumeSampled


def totalUndercutArea(visitMetrics, visit, undercutBanks):
    if visit["iterationID"] == 1 or undercutBanks is None:
        visitMetrics["TotalUndercutArea"] = None
        return

    areas = [(b["value"]["EstimatedUndercutArea"] if b["value"]["EstimatedUndercutArea"] is not None else 0)
             for b in undercutBanks["value"]]

    visitMetrics["TotalUndercutArea"] = np.sum(areas)


def totalUndercutVolume(visitMetrics, visit, undercutBanks):
    if visit["iterationID"] != 2 or undercutBanks is None:  # only 2012
        visitMetrics["TotalUndercutVolume"] = None
        return

    volumes = [(b["value"]["EstimatedLength"] if b["value"]["EstimatedLength"] is not None else 0)
               * (b["value"]["MidpointWidth"] if b["value"]["MidpointWidth"] is not None else 0)
               * (b["value"]["MidpointDepth"] if b["value"]["MidpointDepth"] is not None else 0)
               for b in undercutBanks["value"]]

    visitMetrics["TotalUndercutVolume"] = np.sum(volumes)


def siteMeasurementOfConductivity(visitMetrics, waterChemistry):
    if waterChemistry is None:
        visitMetrics["SiteMeasurementOfConductivity"] = None
        return

    visitMeas = next((w for w in waterChemistry["value"] if w["value"]["Conductivity"] is not None), None)

    if visitMeas is None:
        visitMetrics["SiteMeasurementOfConductivity"] = None
    else:
        visitMetrics["SiteMeasurementOfConductivity"] = visitMeas["value"]["Conductivity"]


def siteMeasurementOfAlkalinity(visitMetrics, waterChemistry):
    if waterChemistry is None:
        visitMetrics["SiteMeasurementOfAlkalinity"] = None
        return

    visitMeas = next((w for w in waterChemistry["value"] if w["value"]["TotalAlkalinity"] is not None), None)

    if visitMeas is None:
        visitMetrics["SiteMeasurementOfAlkalinity"] = None
    else:
        visitMetrics["SiteMeasurementOfAlkalinity"] = visitMeas["value"]["TotalAlkalinity"]


def solarAccessSummerAvg(visitMetrics, visit, solarInputMeasurements):
    if visit["iterationID"] == 1 or solarInputMeasurements is None:
        visitMetrics["SolarAccessSummerAvg"] = None
        return

    measWithValues = [m for m in solarInputMeasurements["value"] if m["value"]["SolarAccessDate"] is not None
                      and m["value"]["SolarAccessValue"] is not None]
    summerAccessValues = [m["value"]["SolarAccessValue"] for m in measWithValues
                          if datetime.datetime.strptime(m["value"]["SolarAccessDate"], "%Y-%m-%dT%H:%M:%S").month >= 7
                          and datetime.datetime.strptime(m["value"]["SolarAccessDate"], "%Y-%m-%dT%H:%M:%S").month <= 9]

    if summerAccessValues.__len__() == 0:
        visitMetrics["SolarAccessSummerAvg"] = None
    else:
        visitMetrics["SolarAccessSummerAvg"] = np.mean(summerAccessValues)


def siteDischarge(visitMetrics, discharge):
    if discharge is None:
        visitMetrics["SiteDischarge"] = None
        return

    discharges = [(d["value"]["StationWidth"] if d["value"]["StationWidth"] is not None else 0)
                  * (d["value"]["Depth"] if d["value"]["Depth"] is not None else 0)
                  * (d["value"]["Velocity"] if d["value"]["Velocity"] is not None else 0)
                  for d in discharge["value"]]

    if discharges.__len__() > 0:
        visitMetrics["SiteDischarge"] = np.sum(discharges)
    else:
        visitMetrics["SiteDischarge"] = None


def poolTailFinesPctObservationsLessThan2mm(visitMetrics, visit, poolTailFines):
    if poolTailFines is None:
        visitMetrics["PercentOfObservationsLessThan2mm"] = None
        return

    if visit["iterationID"] == 1:
        visitMetrics["PercentOfObservationsLessThan2mm"] = poolTailFinesPctObservationsLessThan2mm_2011(poolTailFines)
    else:
        visitMetrics["PercentOfObservationsLessThan2mm"] = poolTailFinesPctObservationsLessThan2mm_2012(poolTailFines)


def poolTailFinesPctObservationsLessThan2mm_2012(poolTailFines):
    nonSelected = -99
    magicNum = 50

    fineMeas1 = [f for f in poolTailFines["value"] if f["value"]["Grid1Lessthan2mm"] is not None
                 and f["value"]["Grid1NonMeasureable"] is not None
                 and (f["value"]["Grid1Lessthan2mm"] + f["value"]["Grid1NonMeasureable"]) <= magicNum]

    fineMeas2 = [f for f in poolTailFines["value"] if f["value"]["Grid2Lessthan2mm"] is not None
                 and f["value"]["Grid2NonMeasureable"] is not None
                 and (f["value"]["Grid2Lessthan2mm"] + f["value"]["Grid2NonMeasureable"]) <= magicNum]

    fineMeas3 = [f for f in poolTailFines["value"] if f["value"]["Grid3Lessthan2mm"] is not None
                 and f["value"]["Grid3NonMeasureable"] is not None
                 and (f["value"]["Grid3Lessthan2mm"] + f["value"]["Grid3NonMeasureable"]) <= magicNum]

    percents = []

    percents.extend([float(f["value"]["Grid1Lessthan2mm"]) / float(magicNum - f["value"]["Grid1NonMeasureable"])
                     for f in fineMeas1 if f["value"]["Grid1Lessthan2mm"] != nonSelected
                     and f["value"]["Grid1NonMeasureable"] != nonSelected
                     and f["value"]["Grid1NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid2Lessthan2mm"]) / float(magicNum - f["value"]["Grid2NonMeasureable"])
                     for f in fineMeas2 if f["value"]["Grid2Lessthan2mm"] != nonSelected
                     and f["value"]["Grid2NonMeasureable"] != nonSelected
                     and f["value"]["Grid2NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid3Lessthan2mm"]) / float(magicNum - f["value"]["Grid3NonMeasureable"])
                     for f in fineMeas3 if f["value"]["Grid3Lessthan2mm"] != nonSelected
                     and f["value"]["Grid3NonMeasureable"] != nonSelected
                     and f["value"]["Grid3NonMeasureable"] != magicNum])

    # 2 or more grids: calculate a value
    if len([len(x) > 0 for x in [fineMeas1, fineMeas2, fineMeas3]]) > 1:
        return np.mean(percents) * 100
    return None


def poolTailFinesPctObservationsLessThan2mm_2011(poolTailFines):
    nonSelected = -99
    magicNum = 50

    fineMeas1 = [f for f in poolTailFines["value"] if f["value"]["Grid1Lessthan2mm"] is not None
                 and f["value"]["Grid1Lessthan6mm"] is not None
                 and f["value"]["Grid1NonMeasureable"] is not None
                 and f["value"]["Grid1Lessthan2mm"] <= f["value"]["Grid1Lessthan6mm"]
                 and (f["value"]["Grid1Lessthan2mm"] + f["value"]["Grid1NonMeasureable"]) <= magicNum]

    fineMeas2 = [f for f in poolTailFines["value"] if f["value"]["Grid2Lessthan2mm"] is not None
                 and f["value"]["Grid2Lessthan6mm"] is not None
                 and f["value"]["Grid2NonMeasureable"] is not None
                 and f["value"]["Grid2Lessthan2mm"] <= f["value"]["Grid2Lessthan6mm"]
                 and (f["value"]["Grid2Lessthan2mm"] + f["value"]["Grid2NonMeasureable"]) <= magicNum]

    fineMeas3 = [f for f in poolTailFines["value"] if f["value"]["Grid3Lessthan2mm"] is not None
                 and f["value"]["Grid3Lessthan6mm"] is not None
                 and f["value"]["Grid3NonMeasureable"] is not None
                 and f["value"]["Grid3Lessthan2mm"] <= f["value"]["Grid3Lessthan6mm"]
                 and (f["value"]["Grid3Lessthan2mm"] + f["value"]["Grid3NonMeasureable"]) <= magicNum]

    percents = []

    percents.extend([float(f["value"]["Grid1Lessthan2mm"]) / float(magicNum - f["value"]["Grid1NonMeasureable"])
                     for f in fineMeas1 if f["value"]["Grid1Lessthan2mm"] != nonSelected
                     and f["value"]["Grid1NonMeasureable"] != nonSelected
                     and f["value"]["Grid1NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid2Lessthan2mm"]) / float(magicNum - f["value"]["Grid2NonMeasureable"])
                     for f in fineMeas2 if f["value"]["Grid2Lessthan2mm"] != nonSelected
                     and f["value"]["Grid2NonMeasureable"] != nonSelected
                     and f["value"]["Grid2NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid3Lessthan2mm"]) / float(magicNum - f["value"]["Grid3NonMeasureable"])
                     for f in fineMeas3 if f["value"]["Grid3Lessthan2mm"] != nonSelected
                     and f["value"]["Grid3NonMeasureable"] != nonSelected
                     and f["value"]["Grid3NonMeasureable"] != magicNum])

    # 2 or more grids: calculate a value
    if len([len(x) > 0 for x in [fineMeas1, fineMeas2, fineMeas3]]) > 1:
        return np.mean(percents) * 100
    return None


def poolTailFinesPctObservationsLessThan6mm(visitMetrics, visit, poolTailFines):
    if poolTailFines is None:
        visitMetrics["PercentOfObservationsLessThan6mm"] = None
        return

    if visit["iterationID"] == 1:
        visitMetrics["PercentOfObservationsLessThan6mm"] = poolTailFinesPctObservationsLessThan6mm_2011(poolTailFines)
    else:
        visitMetrics["PercentOfObservationsLessThan6mm"] = poolTailFinesPctObservationsLessThan6mm_2012(poolTailFines)


def poolTailFinesPctObservationsLessThan6mm_2012(poolTailFines):
    magicNum = 50

    fineMeas1 = [f for f in poolTailFines["value"] if f["value"]["Grid1Lessthan2mm"] is not None
                 and f["value"]["Grid1NonMeasureable"] is not None
                 and f["value"]["Grid1Btwn2and6mm"] is not None]

    fineMeas2 = [f for f in poolTailFines["value"] if f["value"]["Grid2Lessthan2mm"] is not None
                 and f["value"]["Grid2NonMeasureable"] is not None
                 and f["value"]["Grid2Btwn2and6mm"] is not None]

    fineMeas3 = [f for f in poolTailFines["value"] if f["value"]["Grid3Lessthan2mm"] is not None
                 and f["value"]["Grid3NonMeasureable"] is not None
                 and f["value"]["Grid3Btwn2and6mm"] is not None]

    percents = []

    percents.extend([float(f["value"]["Grid1Lessthan2mm"] + f["value"]["Grid1Btwn2and6mm"])
                     / float(magicNum - f["value"]["Grid1NonMeasureable"])
                     for f in fineMeas1 if f["value"]["Grid1NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid2Lessthan2mm"] + f["value"]["Grid2Btwn2and6mm"])
                     / float(magicNum - f["value"]["Grid2NonMeasureable"])
                     for f in fineMeas2 if f["value"]["Grid2NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid3Lessthan2mm"] + f["value"]["Grid3Btwn2and6mm"])
                     / float(magicNum - f["value"]["Grid3NonMeasureable"])
                     for f in fineMeas3 if f["value"]["Grid3NonMeasureable"] != magicNum])

    # 2 or more grids: calculate a value
    if len([len(x) > 0 for x in [fineMeas1, fineMeas2, fineMeas3]]) > 1:
        return np.mean(percents) * 100
    return None


def poolTailFinesPctObservationsLessThan6mm_2011(poolTailFines):
    nonSelected = -99
    magicNum = 50

    fineMeas1 = [f for f in poolTailFines["value"] if f["value"]["Grid1Lessthan2mm"] is not None
                 and f["value"]["Grid1Lessthan6mm"] is not None
                 and f["value"]["Grid1NonMeasureable"] is not None
                 and f["value"]["Grid1Lessthan2mm"] <= f["value"]["Grid1Lessthan6mm"]
                 and (f["value"]["Grid1Lessthan6mm"] + f["value"]["Grid1NonMeasureable"]) <= magicNum]

    fineMeas2 = [f for f in poolTailFines["value"] if f["value"]["Grid2Lessthan2mm"] is not None
                 and f["value"]["Grid2Lessthan6mm"] is not None
                 and f["value"]["Grid2NonMeasureable"] is not None
                 and f["value"]["Grid2Lessthan2mm"] <= f["value"]["Grid2Lessthan6mm"]
                 and (f["value"]["Grid2Lessthan6mm"] + f["value"]["Grid2NonMeasureable"]) <= magicNum]

    fineMeas3 = [f for f in poolTailFines["value"] if f["value"]["Grid3Lessthan2mm"] is not None
                 and f["value"]["Grid3Lessthan6mm"] is not None
                 and f["value"]["Grid3NonMeasureable"] is not None
                 and f["value"]["Grid3Lessthan2mm"] <= f["value"]["Grid3Lessthan6mm"]
                 and (f["value"]["Grid3Lessthan6mm"] + f["value"]["Grid3NonMeasureable"]) <= magicNum]

    percents = []

    percents.extend([float(f["value"]["Grid1Lessthan6mm"]) / float(magicNum - f["value"]["Grid1NonMeasureable"])
                     for f in fineMeas1 if f["value"]["Grid1Lessthan6mm"] != nonSelected
                     and f["value"]["Grid1NonMeasureable"] != nonSelected
                     and f["value"]["Grid1NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid2Lessthan6mm"]) / float(magicNum - f["value"]["Grid2NonMeasureable"])
                     for f in fineMeas2 if f["value"]["Grid2Lessthan6mm"] != nonSelected
                     and f["value"]["Grid2NonMeasureable"] != nonSelected
                     and f["value"]["Grid2NonMeasureable"] != magicNum])

    percents.extend([float(f["value"]["Grid3Lessthan6mm"]) / float(magicNum - f["value"]["Grid3NonMeasureable"])
                     for f in fineMeas3 if f["value"]["Grid3Lessthan6mm"] != nonSelected
                     and f["value"]["Grid3NonMeasureable"] != nonSelected
                     and f["value"]["Grid3NonMeasureable"] != magicNum])

    # 2 or more grids: calculate a value
    if len([len(x) > 0 for x in [fineMeas1, fineMeas2, fineMeas3]]) > 1:
        return np.mean(percents) * 100
    return None
