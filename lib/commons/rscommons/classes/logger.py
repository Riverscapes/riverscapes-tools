import os
import datetime
import logging
import logging.handlers
from pprint import pformat
from termcolor import colored

# Set if this environment variable is set don't show any UI
NO_UI = os.environ.get('NO_UI') is not None

"""

This class exists to allow us to do logging the way we want and in a consistent way

"""
# logging.getLogger("botocore").setLevel(logging.ERROR)
# logging.getLogger("requests").setLevel(logging.ERROR)
# logging.getLogger("shapely").setLevel(logging.ERROR)
# logging.getLogger("urllib3").setLevel(logging.ERROR)
# logging.getLogger("pygeoprocessing").setLevel(logging.ERROR)
# logging.getLogger("rasterio").setLevel(logging.ERROR)


class _LoggerSingleton:
    instance = None

    class __Logger:

        def __init__(self):
            self.initialized = False
            self.verbose = False
            self.logpath = None

        def setup(self, logPath=None, verbose=False):
            self.initialized = True
            self.verbose = verbose

            loglevel = logging.INFO if not verbose else logging.DEBUG

            self.logger = logging.getLogger("NARLOGGER")
            self.logger.setLevel(loglevel)

            # Make sure we capture osgeo warnigns if we need to
            osgeoLogger = logging.getLogger("osgeo")
            osgeoLogger.setLevel(logging.WARNING)

            if logPath:
                self.logpath = logPath
                if not os.path.exists(os.path.dirname(logPath)):
                    os.makedirs(os.path.dirname(logPath))
                if not os.path.isdir(os.path.dirname(logPath)):
                    os.makedirs(os.path.dirname(logPath))

                self.handler = logging.FileHandler(logPath, mode='w')
                self.handler.setLevel(loglevel)
                # self.handler.
                self.handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s [%(curmethod)-15s] %(message)s'))
                self.handler.datefmt = '%Y-%m-%d %H:%M:%S'

                self.logger.addHandler(self.handler)

                osgeoLogger.addHandler(self.handler)

        def logprint(self, message, method="", severity="info", exception=None):
            """
            Logprint logs things 3 different ways: 1) stdout 2) log txt file 3) xml
            :param message:
            :param method:
            :param severity:
            :param exception:
            :return:
            """

            # Verbose logs don't get written until we ask for them
            if severity == 'debug' and not self.verbose:
                return

            # dateStr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
            msg_arr = []
            if exception is not None:
                txtmsg = '{0}  Exception: {1}'.format(message, str(exception))
                msg = '[{0}] [{1}] {2} : {3}'.format(severity, method, message, str(exception))
            elif severity == 'title':
                buffer = 15
                buffer_str = buffer * ' '
                bar = (len(message) + (buffer * 2)) * '='
                msg_arr = [
                    bar, '{}{}{}'.format(buffer_str, message, buffer_str), bar, ' '
                ]
                msg = '\n'.join(['[info] [{0}] {1}'.format(method, m) for m in msg_arr])
            else:
                txtmsg = message
                msg = '[{0}] [{1}] {2}'.format(severity, method, message)

            # Print to stdout
            if not NO_UI:
                if (severity == 'debug'):
                    msg = colored(msg, 'cyan')
                if (severity == 'warning'):
                    msg = colored(msg, 'yellow')
                if (severity == 'error'):
                    msg = colored(msg, 'red')
                if (severity == 'title'):
                    msg = colored(msg, 'magenta')

            print(msg)

            # If we haven't set up a logger then we're done here. Don't write to any files
            if not self.initialized:
                return

            # Write to log file
            if severity == 'info':
                self.logger.info(txtmsg, extra={'curmethod': method})
            elif severity == 'warning':
                self.logger.warning(txtmsg, extra={'curmethod': method})
            elif severity == 'error':
                self.logger.error(txtmsg, extra={'curmethod': method})
            elif severity == 'critical':
                self.logger.critical(txtmsg, extra={'curmethod': method})
            elif severity == 'debug':
                self.logger.debug(txtmsg, extra={'curmethod': method})
            elif severity == 'title':
                [self.logger.info(m, extra={'curmethod': method}) for m in msg_arr]

    def __init__(self, **kwargs):
        if not _LoggerSingleton.instance:
            _LoggerSingleton.instance = _LoggerSingleton.__Logger(**kwargs)

    def __getattr__(self, name):
        return getattr(self.instance, name)


class Logger():
    """
    Think of this class like a light interface
    """

    def __init__(self, method=""):
        """

        :rtype: object
        """
        self.instance = _LoggerSingleton()
        self.method = method

    def setup(self, **kwargs):
        self.instance.setup(**kwargs)

    def print_(self, message, **kwargs):
        self.instance.logprint(message, **kwargs)

    def debug(self, *args):
        """
        This works a little differently. You can basically throw anything you want into it.
        :param message:
        :return:
        """
        msgarr = []
        for arg in args:
            if type(arg) is str:
                msgarr.append(arg)
            else:
                msgarr.append(pformat(arg))
        finalmessage = '\n'.join(msgarr).replace('\n', '\n              ')
        self.instance.logprint(finalmessage, self.method, "debug")

    def destroy(self):
        self.instance = None
        self.method = None

    def info(self, message):
        self.instance.logprint(message, self.method, "info")

    def error(self, message, exception=None):
        self.instance.logprint(message, self.method, "error", exception)

    def warning(self, message, exception=None):
        self.instance.logprint(message, self.method, "warning", exception)

    def title(self, message):
        self.instance.logprint(message, self.method, "title")
