import numpy as np
from rscommons import Logger

dAlias = {
    'LBGroundcoverWoodyShrubs': 'LBGroundcoverWoodyShurbs',
    'RBGroundcoverNonWoodyShrubs': 'RBGroundcoverNonWoodyShurbs'
}


def calculate(apiData):
    """
    Calculate riparian structure metrics
    :param apiData: dictionary of API data. Key is API call name. Value is API data
    :return: metrics dictionary
    """

    raise Exception('TODO: Code abandoned after it was determined that this was not needed.')

    log = Logger('riparianCoverMetrics')
    log.info("Running RiparianCoverMetrics")

    # Retrieve the riparian structure API data
    riparianVals = [val['value'] for val in apiData['RiparianStructure']['values']]

    # calculate metrics
    return _calc(riparianVals)


def _calc(riparianVals):
    """
    Calculate riparian structure metrics
    :param riparianVals: dictionary of riparian structure API data
    :return: metrics dictionary
    """

    dResults = {}

    # Dictionary of riparian cover types that go into each metric.
    # Note that the actual lookup is performed on both the left
    # and right bank by prefixing these items with 'LB' and 'RB'
    dLookups = {
        # 'RipCovBigTree' : ['CanopyBigTrees'],
        # 'RipCovConif'   : ['CanopyWoodyConiferous', 'UnderstoryWoodyConiferous'],
        # 'RipCovGrnd'    : ['GroundcoverWoodyShrubs', 'GroundcoverNonWoodyShrubs'],
        # 'RipCovNonWood' : ['UnderstoryNonWoodyShrubs', 'GroundcoverNonWoodyShrubs'],
        # 'RipCovUstory'  : ['UnderstoryWoodyShrubs', 'UnderstoryNonWoodyShrubs'],
        'RipCovWood': ['CanopyBigTrees', 'CanopySmallTrees', 'UnderstoryWoodyShrubs', 'GroundcoverWoodyShrubs']
    }

    # Loop over each metric and get the mean of all the lookup values
    for metric, lookupList in dLookups.items():
        dResults[metric] = _getMeanLeftAndRightBank(riparianVals, lookupList)

    # Doesn't work for 2011 data.  No documentation on how this was calculated then.
    dResults['RipCovCanNone'] = 100 - _getMeanLeftAndRightBank(riparianVals,
                                                               ['CanopyWoodyConiferous', 'CanopyWoodyDeciduous', 'CanopyWoodyBroadleafEvergreen', 'CanopyStandingDeadVegetation'])

    return dResults


def _getMeanLeftAndRightBank(riparianVals, lookupList):
    """
    Retrieve the riparian cover values from the API for the items in the lookup list ON BOTH THE LEFT AND RIGHT BANK
    :param riparianVals: Riparian Structure API data
    :param lookupList: List of riparian structure measurements. Will be prefixed with 'LB' and 'RB'
    :return: list of riparian structure values for all the items in the list on both the left and right bank
    """

    values = []
    for bank in ['LB', 'RB']:
        bankVals = []
        for apiVal in riparianVals:
            typeSum = 0.0
            for lookup in lookupList:
                # Concatenate the bank with the riparian cover class we are looking for
                # and check if this is one of the mis-spelled items and use the bad spelling
                apiLookup = bank + lookup
                if apiLookup in dAlias:
                    apiLookup = dAlias[apiLookup]

                if apiVal[apiLookup]:
                    typeSum += apiVal[apiLookup]

            bankVals.append(typeSum)

        # Remove any None values but retain zeroes
        print(bankVals)
        bankValues = [val for val in bankVals if val != None]
        if len(bankValues) > 0:
            values.append(np.mean(bankValues))
        else:
            values.append(0)

    if len(values) < 1:
        return 0
    else:
        return np.mean(values)
