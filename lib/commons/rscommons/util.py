import sys
import gc
import shutil
import hashlib
import os
from datetime import datetime
from math import floor
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


def safe_remove_file(file_path):
    """Remove a file without throwing an error

    Args:
        file_path ([type]): [description]
    """
    log = Logger("safe_remove_file")
    try:
        if not os.path.isfile(file_path):
            log.warning('File not found: {}'.format(file_path))
        os.remove(file_path)
        log.debug('File removed: {}'.format(file_path))
    except Exception as e:
        log.error(str(e))


def file_compare(file_a, file_b, md5=True):
    """Do a file comparison, starting with file size and finishing with md5

    Args:
        file_a ([type]): [description]
        file_b ([type]): [description]

    Returns:
        [type]: [description]
    """
    log = Logger("file_compare")
    log.debug('Comparing: {} {}'.format(file_a, file_b))
    try:
        # If the file sizes aren't the same then there's
        # no reason to do anything more
        a_stats = os.stat(file_a)
        b_stats = os.stat(file_b)
        if a_stats.st_size != b_stats.st_size:
            log.debug('Files are NOT the same size: {:,} vs. {:,}')
            return False

        # If we want this to be a quick-compare and not do MD5 then we just
        # do the file size and leave it at that
        if not md5:
            return True

        with open(file_a, 'rb') as afile:
            hasher1 = hashlib.md5()
            buf1 = afile.read()
            hasher1.update(buf1)
            md5_a = (str(hasher1.hexdigest()))

        with open(file_b, 'rb') as bfile:
            hasher2 = hashlib.md5()
            buf1 = bfile.read()
            hasher2.update(buf1)
            md5_b = (str(hasher2.hexdigest()))

        # Compare md5
        if(md5_a == md5_b):
            log.debug('File MD5 hashes match')
            return True
        else:
            log.debug('File MD5 hashes DO NOT match')
            return False
    except Exception as e:
        log.error('Error comparing files: {}', str(e))
        return False


def safe_remove_dir(dir_path):
    """Remove a directory without throwing an error

    Args:
        file_path ([type]): [description]
    """
    log = Logger("safe_remove_dir")
    try:
        shutil.rmtree(dir_path, ignore_errors=True)
        log.debug('Directory removed: {}'.format(dir_path))
    except Exception as e:
        log.error(str(e))


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


def pretty_date(time=False):
    """
    Get a datetime object or a int() Epoch timestamp and return a
    pretty string like 'an hour ago', 'Yesterday', '3 months ago',
    'just now', etc
    """
    now = datetime.now()
    if isinstance(time, int):
        diff = now - datetime.fromtimestamp(time)
    elif isinstance(time, datetime):
        diff = now - time
    elif not time:
        diff = now - now
    second_diff = diff.seconds
    day_diff = diff.days

    if day_diff < 0:
        return ''

    if day_diff == 0:
        if second_diff < 10:
            return "just now"
        if second_diff < 60:
            return str(second_diff) + " seconds ago"
        if second_diff < 120:
            return "a minute ago"
        if second_diff < 3600:
            return str(second_diff / 60) + " minutes ago"
        if second_diff < 7200:
            return "an hour ago"
        if second_diff < 86400:
            return str(second_diff / 3600) + " hours ago"
    if day_diff == 1:
        return "Yesterday"
    if day_diff < 7:
        return str(day_diff) + " days ago"
    if day_diff < 31:
        return str(day_diff / 7) + " weeks ago"
    if day_diff < 365:
        return str(day_diff / 30) + " months ago"
    return str(day_diff / 365) + " years ago"


def pretty_duration(time_s=False):
    """
    Get a datetime object or a int() Epoch timestamp and return a
    pretty string like 'an hour ago', 'Yesterday', '3 months ago',
    'just now', etc
    """
    if not time_s >= 0:
        return '???'
    seconds = time_s % 60
    minutes = floor(time_s / 60) % 60
    hours = floor(time_s / 3600) % 24
    if time_s < 60:
        return "{0:.1f} seconds".format(seconds)
    elif time_s < 3600:
        return "{}:{:02} minutes".format(minutes, seconds)
    elif time_s < 86400:
        return "{}:{:02} hours".format(hours, minutes)
    else:
        days = floor(time_s / 86400)
        return "{} days, {}:{:02} hours".format(days, hours, minutes)


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
