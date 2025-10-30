import numpy as np
from champ_metrics.lib.channelunits import dUnitDefs
from champ_metrics.lib.channelunits import getCleanTierName


def emptiesByChannelUnit(metricDict: dict):
    """
    Instantiate an "Empty" object full of null values in case our calculation fails
    :param metricDict:
    :return:
    """
    cleantypes = [getCleanTierName(t1Type) for t1Type in dUnitDefs]

    cuMetrics = {t1type: {} for t1type in cleantypes}

    for _t1Name, t1Obj in cuMetrics.items():
        for metricName in metricDict.keys():
            # The Tier 1 types from the API need to be sanitized for the metric XML file
            t1Obj[metricName] = None
        t1Obj['Total'] = None

    visitMetrics = {metricName: None for metricName in metricDict.keys()}
    visitMetrics['Total'] = None

    dResults = {'VisitMetrics': visitMetrics, 'Tier1Metrics': cuMetrics}
    return dResults


def metricsByChannelUnit(metricDict, channelUnitMetrics, apiValues, channelUnitMeasurements):

    # Retrieve channel unit areas and tier 1 type from channel unit metrics
    dChannelUnits = {}
    for unit in channelUnitMetrics:
        unitNumber = int(unit['ChannelUnitNumber'])
        unitID = next(u['value']['ChannelUnitID'] for u in channelUnitMeasurements['value'] if u['value']['ChannelUnitNumber'] == unitNumber)
        dChannelUnits[unitNumber] = {}
        dChannelUnits[unitNumber]['Area'] = unit['Area']
        dChannelUnits[unitNumber]['Tier1'] = unit['Tier1']

        # Loop over each metric.
        # See the metric definitions dict in each calling function.
        # Some metrics group several measurement classes into one metric
        for metricName, subClasses in metricDict.items():
            dChannelUnits[unitNumber][metricName] = 0.0

            # Loop over the API measurements that are used by this metric
            for subClass in subClasses[0]:
                # Sum the proportions of the measurement for this class
                vals = [val[subClass] for val in apiValues if val['ChannelUnitID'] == unitID and val[subClass] != None]
                if len(vals) == 0:
                    dChannelUnits[unitNumber][metricName] = None
                else:
                    if dChannelUnits[unitNumber][metricName] is None:
                        dChannelUnits[unitNumber][metricName] = 0
                    dChannelUnits[unitNumber][metricName] += np.sum(vals)

    cuMetrics = {}
    for t1Type in dUnitDefs:
        # The Tier 1 types from the API need to be sanitized for the metric XML file
        safet1Type = getCleanTierName(t1Type)
        cuMetrics[safet1Type] = {}
        tier1Total = 0.0

        # Set a template where eveyrthing is zero
        cuMetrics[safet1Type]['Total'] = None
        for metricName, subClasses in metricDict.items():
            cuMetrics[safet1Type][metricName] = None

        t1AreaTot = np.sum([val['Area'] for val in dChannelUnits.values() if val['Tier1'] == t1Type])
        cuMetrics[safet1Type]['Area'] = t1AreaTot

        # Only do this if we have some area to work with. Otherwise we'll be dividing by zero
        if t1AreaTot > 0:
            for metricName, subClasses in metricDict.items():
                vals = [val[metricName] * val['Area'] for val in dChannelUnits.values() if val[metricName] is not None and val['Tier1'] == t1Type]
                if len(vals) > 0:
                    # Sum product of substrate class proportions for a specific tier 1 type
                    t1SumProd = np.sum(vals)

                    cuMetrics[safet1Type][metricName] = t1SumProd / t1AreaTot

                    if subClasses[1] and cuMetrics[safet1Type][metricName]:
                        tier1Total += cuMetrics[safet1Type][metricName]

                    cuMetrics[safet1Type]['Total'] = tier1Total

    visitMetrics = {}
    for metricName, subClasses in metricDict.items():
        visitMetrics[metricName] = None
    visitMetrics['Total'] = None

    # Throw areas into XML
    areaTot = 0
    for val in dChannelUnits.values():
        if val['Tier1'] != "Small Side Channel":
            areaTot += val['Area']
    visitMetrics['TotalArea'] = areaTot

    metricTotal = 0.0
    for metricName, subClasses in metricDict.items():
        # Sum product of metric value proportions and areas, divided by sum area for all channel units
        vals = [val[metricName] * val['Area'] for val in dChannelUnits.values()
                if val[metricName] is not None and val['Tier1'] != "Small Side Channel"]

        if len(vals) > 0:
            visitMetrics[metricName] = np.sum(vals) / areaTot

            # Only include the metric in the overall total if the argument tuple indicates that the metric should be included
            if subClasses[1]:
                metricTotal += visitMetrics[metricName]

            visitMetrics['Total'] = metricTotal

    dResults = {'VisitMetrics': visitMetrics, 'Tier1Metrics': cuMetrics}
    return dResults
