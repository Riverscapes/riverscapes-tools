import os, sys, xml, datetime, pytz, re
import logging, logging.handlers
from pprint import pformat

"""

This class exists to allow us to do logging the way we want and in a consistent way

"""
logging.getLogger("botocore").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)

class _LoggerSingleton:
    instance = None

    class __Logger:

        def __init__(self):
            self.initialized = False
            self.verbose = False

        def setup(self, logPath, verbose=False):
            self.initialized = True
            self.verbose = verbose

            if not os.path.exists(os.path.dirname(logPath)):
                os.makedirs(os.path.dirname(logPath))

            loglevel = logging.INFO if not verbose else logging.DEBUG

            self.logger = logging.getLogger()

            for hdlr in self.logger.handlers[:]:  # remove all old handlers
                self.logger.removeHandler(hdlr)

            logging.basicConfig(level=loglevel,
                                filemode='w',
                                format='%(asctime)s %(levelname)-8s [%(curmethod)-15s] %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                filename=logPath)

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

            dateStr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')

            if exception is not None:
                txtmsg = '{0}  Exception: {1}'.format(message, str(exception))
                msg = '[{0}] [{1}] {2} : {3}'.format(severity, method, message, str(exception))
            else:
                txtmsg = message
                msg = '[{0}] [{1}] {2}'.format(severity, method, message)

            # Print to stdout
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
            if severity == 'debug':
                self.logger.debug(txtmsg, extra={'curmethod': method})

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
        msgarr =  []
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
