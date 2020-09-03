import sys
import time
import gc
import sys
import glob
import shutil
import os
from math import cos, sin, asin, sqrt, radians
from rscommons import Logger

# Set if this environment variable is set don't show any UI
NO_UI = os.environ.get('NO_UI') is not None


def batch(iterable, n=1):
    """Split a list into a list of regularly-sized lists of size n

    Args:
        iterable ([type]): [description]
        n (int, optional): [description]. Defaults to 1.

    Yields:
        [type]: [description]
    """
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx:min(ndx + n, length)]


def safe_makedirs(dir_create_path):
    """safely, recursively make a directory

    Arguments:
        dir_create_path {[type]} -- [description]
    """
    log = Logger("MakeDir")

    # Safety check on path lengths
    if len(dir_create_path) < 5 or len(dir_create_path.split(os.path.sep)) <= 2:
        raise Exception('Invalid path: {}'.format(dir_create_path))

    if os.path.exists(dir_create_path) and os.path.isfile(dir_create_path):
        raise Exception('Can\'t create directory if there is a file of the same name: {}'.format(dir_create_path))

    if not os.path.exists(dir_create_path):
        try:
            log.info('Folder not found. Creating: {}'.format(dir_create_path))
            os.makedirs(dir_create_path)
        except Exception as e:
            # Possible that something else made the folder while we were trying
            if not os.path.exists(dir_create_path):
                log.error('Could not create folder: {}'.format(dir_create_path))
                raise e


def sizeof_fmt(num, suffix='B'):
    """Format bytesize properly

    Arguments:
        num {[type]} -- [description]

    Keyword Arguments:
        suffix {str} -- [description] (default: {'B'})

    Returns:
        [type] -- [description]
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def get_obj_size(obj):
    """Generic function to get the byte-size of a variable

    Arguments:
        obj {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    marked = {id(obj)}
    obj_q = [obj]
    sz = 0

    while obj_q:
        sz += sum(map(sys.getsizeof, obj_q))

        # Lookup all the object referred to by the object in obj_q.
        # See: https://docs.python.org/3.7/library/gc.html#gc.get_referents
        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

        # Filter object that are already marked.
        # Using dict notation will prevent repeated objects.
        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

        # The new obj_q will be the ones that were not marked,
        # and we will update marked with their ids so we will
        # not traverse them again.
        obj_q = new_refr.values()
        marked.update(new_refr.keys())

    return sz


def parse_metadata(arg_string):

    meta = {}
    try:
        if arg_string:
            for kvp in arg_string.split(','):
                key_value = kvp.split('=')
                clean_key = key_value[0].strip()
                clean_value = key_value[1].strip()
                if len(clean_key) < 1:
                    raise Exception('Empty key')
                if len(clean_value) < 1:
                    raise Exception('Empty value')
                if clean_key in meta:
                    raise Exception('Duplicate metadata key')

                meta[clean_key] = clean_value
    except Exception as ex:
        print(ex)
        raise Exception('Error parsing command line metadata: {}'.format(arg_string))

    return meta
