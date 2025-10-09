import os
from rscommons import Logger
from champ_metrics.lib.exception import MissingException


def getAbsInsensitivePath(abs_insensitive_path, ignoreAbsent=False):
    """
    Will sanitize cases and return the correct case.
    :param abs_insensitive_path:
    :param ignoreAbsent: if true this will not throw an exception and just return the path
    :return:
    """
    log = Logger("getAbsInsensitivePath")

    if len(abs_insensitive_path) == 0:
        raise IOError("Zero-length path used: getAbsInsensitivePath()")

    if os.path.sep == "/":
        pathreplaced = abs_insensitive_path.replace("\\", os.path.sep)
    else:
        pathreplaced = abs_insensitive_path.replace("/", os.path.sep)

    parts = pathreplaced.split(os.path.sep)

    improved_parts = []

    for part in parts:
        if part == ".." or part == "." or part == "":
            improved_parts.append(part)
        else:
            improved_path = os.path.sep.join(improved_parts)
            if len(improved_path) == 0:
                improved_path = os.path.sep
            try:
                found = False
                for name in os.listdir(improved_path):
                    if part.lower() == name.lower():
                        improved_parts.append(name)
                        found = True
                if not found:
                    raise OSError("Not found")
            except OSError as e:
                if not ignoreAbsent:
                    raise MissingException("Could not find case-insensitive path: {}".format(abs_insensitive_path))
                else:
                    return abs_insensitive_path

    finalpath = os.path.sep.join(improved_parts)

    if (abs_insensitive_path != finalpath):
        log.warning("Paths do not match: `{}`  != `{}`".format(abs_insensitive_path, finalpath))

    return finalpath
