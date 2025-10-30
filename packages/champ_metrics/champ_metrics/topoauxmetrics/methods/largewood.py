import numpy as np
from champ_metrics.lib.exception import DataException
from champ_metrics.lib.sitkaAPI import latestMetricInstance
from champ_metrics.lib.metrics import CHaMPMetric
from champ_metrics.lib.exception import MissingException


class LargeWoodMetrics(CHaMPMetric):

    TEMPLATE = {}

    def calc(self, apiData):
        """
        Calculate large wood metrics
        :param apiData: dictionary of API data. Key is API call name. Value is API data
        :return: metrics dictionary
        """

        self.log.info("Running Large Wood Metrics")

        # Retrieve the site wetted length from the latest topo metrics
        metricInstance = apiData['TopoVisitMetrics']
        if metricInstance is None:
            raise MissingException('Missing topo visit metric instance')
        siteWettedLength = metricInstance['Wetted']['Centerline']['TotalChannelLength']

        if apiData['VisitYear'] < 2014:
            woodData = [val['value'] for val in apiData['LargeWoodyDebris']['value']]

            # Only 2011 and 2012 have separate wood jam data
            jamData = None
            if 'WoodyDebrisJam' in apiData and apiData['WoodyDebrisJam'] is not None:
                jamData = [val['value'] for val in apiData['WoodyDebrisJam']['value']]

            metrics = LargeWoodMetrics._calcFrequency2011to2013(woodData, jamData, siteWettedLength)
        else:
            if 'LargeWoodyPiece' not in apiData:
                raise DataException("LargeWoodyPiece needed and not found.")
            woodData = [val['value'] for val in apiData['LargeWoodyPiece']['value']]
            metrics = LargeWoodMetrics._calcFrequency2014On(woodData, siteWettedLength)

        self.metrics = {'VisitMetrics': {'Frequency': metrics}}

    @staticmethod
    def _calcFrequency2014On(woodData, siteWettedLength):

        LWFreqWetted = 0.0
        wetWoodCount = len([val for val in woodData if val['LargeWoodType'] == 'Wet'])
        if wetWoodCount != 0 and siteWettedLength is not None and siteWettedLength != 0.0:
            LWFreqWetted = 100 * wetWoodCount / siteWettedLength

        LWFreqBankfull = 0.0
        dryWoodCount = len([val for val in woodData if val['LargeWoodType'] == 'Dry'])
        bankfullWoodCount = dryWoodCount + wetWoodCount
        if bankfullWoodCount != 0.0 and siteWettedLength is not None and siteWettedLength != 0.0:
            LWFreqBankfull = 100 * bankfullWoodCount / siteWettedLength

        return {'Wetted': LWFreqWetted, 'Bankfull': LWFreqBankfull}

    @staticmethod
    def _calcFrequency2011to2013(woodData, jamData, siteWettedLength):

        # Wet wood includes the wet count plus any wood that has a null value for LargeWood Type
        wetWoodCount = np.sum([val['SumLWDCount'] for val in woodData if val['LargeWoodType'] is None or val['LargeWoodType'].startswith('Wet')])
        wetJamCount = 0
        if jamData:
            wetJamCount = np.sum([val['SumJamCount'] for val in jamData if val['LargeWoodType'] is None or val['LargeWoodType'].startswith('Wet')])
        wetTotalCount = wetWoodCount + wetJamCount

        LWFreqWetted = 0.0
        if wetTotalCount != 0 and siteWettedLength is not None and siteWettedLength != 0.0:
            LWFreqWetted = 100 * wetTotalCount / siteWettedLength

        # All dry wood should have a largeWoodType value that starts with 'Dry'
        dryWoodCount = np.sum([val['SumLWDCount'] for val in woodData if val['LargeWoodType'] is None or val['LargeWoodType'].startswith('Dry')])

        dryJamCount = 0
        if jamData:
            # All dry jams should have a largeWoodType value that starts with 'Dry'
            dryJamCount = np.sum([val['SumJamCount'] for val in jamData if val['LargeWoodType'] is not None and val['LargeWoodType'].startswith('Dry')])

        dryTotalCount = dryWoodCount + dryJamCount

        bankfullCount = dryTotalCount + wetTotalCount
        LWFreqBankfull = 0.0
        if bankfullCount != 0 and siteWettedLength is not None and siteWettedLength != 0.0:
            LWFreqBankfull = 100 * bankfullCount / siteWettedLength

        visitMetrics = {'Wetted': LWFreqWetted, 'Bankfull': LWFreqBankfull}
        return visitMetrics

# def _calcVol(sampleYear, woodData, channelUnitMeasurements):
#     """
#     Calculate large wood metrics
#     :param undercutVals: dictionary of undercut API data
#     :param visitTopoVals: dictionary of channel unit metrics
#     :return: metrics dictionary
#     """
#
#     for piece in woodData:
#         piece['Tier1'] = [val['Tier1'] for val in channelUnitMeasurements if val['ChannelUnitID'] == piece['ChannelUnitID']][0]
#
#     dResults = {}
#     # Loop over all the tier 1 types and then 'wet' and 'dry'
#     for t1Type, t1Alias in dTier1Aliases.items():
#         for wetdry in ['Wet', 'Dry']:
#             metricName = 'LWVol_{0}{1}'.format(wetdry, t1Alias)
#
#             # Filter to just the pieces of wood for this tier 1 type and wet/dry
#             filteredWood = filter(lambda x : x['Tier1'] == t1Type and x['LargeWoodType'] == wetdry, woodData)
#
#             # Calculate the total volume for all pieces of wood for this tier 1 type and wet or dry
#             if sampleYear < 2014:
#                 dResults[metricName] = _volume2011to2013(filteredWood)
#             else:
#                 dResults[metricName] = _volume2014Onward(filteredWood)
#
#         # The bankfull volume is the sum of wet and dry for this tier 1 type
#         bfMetric = 'LWVol_Bf{0}'.format(t1Alias)
#         dResults[bfMetric] = np.sum([dResults[key] for key in dResults if t1Alias in key])
#
#     # Repeat the analysis but just filtering for wet and dry (not tier 1 type)
#     for wetdry in ['Wet', 'Dry']:
#         metricName = 'LWVol_{0}'.format(wetdry)
#         filteredWood = filter(lambda x: x['LargeWoodType'] == wetdry, woodData)
#         # Calculate the total volume for all pieces of wood for this tier 1 type and wet or dry
#         if sampleYear < 2014:
#             dResults[metricName] = _volume2011to2013(filteredWood)
#         else:
#             dResults[metricName] = _volume2014Onward(filteredWood)
#
#     # The overall bankfull is the sum of wet and dry
#     dResults['LWVol_Bf'] = dResults['LWVol_Wet'] + dResults['LWVol_Dry']
#
#     return dResults
#
#
# def _volume2014Onward(filteredPieces):
#
#     volume = 3.14159 * np.sum([0.5 * pow(val['DiameterM'], 2) * val['LengthM'] for val in filteredPieces])
#     return volume
#
# def _volume2011to2013(filteredDebris):
#
#     woodSizes = {
#         'SmallSmall': 0.02035,
#         'SmallMedium': 0.04878,
#         'SmallLarge': 0.10758,
#         'MediumSmall': 0.05981,
#         'MediumMedium': 0.15101,
#         'MediumLarge' : 0.40012,
#         'LargeSmall': 0.22887,
#         'LargeMedium': 0.57739,
#         'LargeLarge': 1.72582,
#         'SmallMidLarge': 0.10470,
#         'SmallExtraLarge': 0.23794,
#         'MediumMidLarge': 0.33875,
#         'MediumExtraLarge': 0.82393,
#         'MidLargeSmall': 0.21187,
#         'MidLargeMedium': 0.51680,
#         'MidLargeMidLarge': 1.12232,
#         'MidLargeExtraLarge': 2.71169,
#         'ExtraLargeSmall': 0.84320,
#         'ExtraLargeMedium': 1.89000,
#         'ExtraLargeMidLarge': 3.82249,
#         'ExtraLargeExtraLarge': 10.54683
#     }
#
#     volume = 0.0
#     for wood in filteredDebris:
#         for sizeName, sizeConst in woodSizes.items():
#             if wood[sizeName] is not None:
#                 volume += wood[sizeName] * sizeConst
#
#     return volume
