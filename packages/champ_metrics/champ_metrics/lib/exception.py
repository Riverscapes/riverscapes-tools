from .loghelper import Logger

class DataException(Exception):

    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(DataException, self).__init__(message)
        self.returncode = 2
        logg = Logger('DataException')
        logg.error(message)

class NetworkException(Exception):

    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(NetworkException, self).__init__(message)
        self.returncode = 3

        logg = Logger('NetworkException')
        logg.error(message)

class MissingException(Exception):

    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(MissingException, self).__init__(message)
        self.returncode = 4

        logg = Logger('MissingException')
        logg.error(message)
