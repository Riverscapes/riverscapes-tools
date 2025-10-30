from copy import deepcopy
from rscommons import Logger
from .exception import DataException, MissingException


class CHaMPMetric(object):
    """
    Base class for all CHaMP metrics
    """

    # This is the dictionary representation of the object
    # in its null state.
    TEMPLATE = {}

    def __init__(self, *args, **kwargs):
        # First initialize our logger and our empty metrics
        self.log = Logger(self.__module__)
        self.metrics = deepcopy(self.TEMPLATE)

        # Now try a calculation
        try:
            self.calc(*args, **kwargs)
        except (DataException, MissingException) as e:
            self.log.warning(f"Could not complete metric calculation: {e}")
            self.metrics = deepcopy(self.TEMPLATE)

    def calc(self, *args, **kwargs):
        """
        Needs to be overloaded in the children
        :return:
        """
        raise Exception("YOU MUST IMPLEMENT THIS")
