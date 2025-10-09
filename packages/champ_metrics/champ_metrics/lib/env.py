# settings.py
import os
import re

def setEnvFromFile(batFilePath, verbose = False):
    """
    This is a convenience function to load environment variables from a file
    :param batFilePath:
    :param verbose:
    :return:
    """

    if os.path.isfile(batFilePath):
        SetEnvPattern = re.compile("^([^#][A-Za-z_-]+)=(.*)", re.MULTILINE)
        SetEnvFile = open(batFilePath, "r")
        SetEnvText = SetEnvFile.read().split('\n')
        for line in SetEnvText:
            rematch = re.match(SetEnvPattern, line)
            if rematch:
                name = rematch.group(1)
                val = rematch.group(2)
                if verbose:
                    print("%s=%s"%(name,val))
                os.environ[name]=val

setEnvFromFile(os.path.join(os.path.dirname(__file__), '..','.env'))
