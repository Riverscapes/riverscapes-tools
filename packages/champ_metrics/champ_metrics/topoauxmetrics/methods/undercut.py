from champ_metrics.lib.exception import DataException, MissingException
from champ_metrics.lib.metrics import CHaMPMetric


class UndercutMetrics(CHaMPMetric):

    TEMPLATE = {
        'VisitMetrics': {
            'Length': 0.0,
            'LengthPercent': 0.0,
            'Area':  0.0,
            'AreaPerecent': 0.0
        }
    }

    def calc(self, apiData):
        """
        Calculate undercut metrics
        :param apiData: dictionary of API data. Key is API call name. Value is API data
        :return: metrics dictionary
        """

        self.log.info("Running UndercutMetrics")

        if 'UndercutBanks' not in apiData:
            raise MissingException("UndercutBanks missing from apiData")

        # Retrieve the undercut API data
        undercutVals = [val['value'] for val in apiData['UndercutBanks']['value']] if apiData['UndercutBanks'] else []

        # Retrieve the latest topo metrics
        metricInstance = apiData['TopoVisitMetrics']
        if metricInstance is None:
            raise MissingException('Missing topo visit metric instance')

        # calculate metrics
        self.metrics = self._calc(undercutVals, metricInstance)

    @staticmethod
    def _calc(undercutVals, visitTopoVals):
        """
        Calculate undercut metrics
        :param undercutVals: dictionary of undercut API data
        :param visitTopoVals: dictionary of visit topo metrics
        :return: metrics dictionary
        """

        # initialize all metrics as zero
        dMetrics = {
            'Length': 0.0,
            'LengthPercent': 0.0,
            'Area':  0.0,
            'AreaPerecent': 0.0
        }

        if len(undercutVals) > 0:
            # Calculate the total undercut length and area
            for undercut in undercutVals:
                dMetrics['Length'] += undercut['EstimatedLength']
                try:
                    dMetrics['Area'] += undercut['EstimatedLength'] * (undercut['Width25Percent'] + undercut['Width50Percent'] + undercut['Width75Percent']) / 3.0
                except TypeError as e:
                    raise DataException("Undercut: Unhandled 'None' values during length calculation")

            # Calculate the percent length and area of the site that is undercut
            if visitTopoVals['Lgth_Wet'] is None:
                raise DataException("Lgth_Wet cannot be null")
            if visitTopoVals['Area_Wet'] is None:
                raise DataException("Area_Wet cannot be null")

            dMetrics['LengthPercent'] = dMetrics['Length'] / (visitTopoVals['Lgth_Wet'] * 100 / 2)
            dMetrics['AreaPerecent'] = dMetrics['Area'] / (visitTopoVals['Area_Wet'] + dMetrics['Area']) * 100

        dResults = {'VisitMetrics': dMetrics}

        return dResults
