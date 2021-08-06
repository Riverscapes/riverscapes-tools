""" handling of paths across operating systems
"""
from pathlib import Path, PurePosixPath


def parse_rel_path(path: str) -> str:
    """ Path handling across platforms is gnarly. 
    This method returns the correct path for your operating system regardless of 
    whether the input is a windows path or a linux path

    Args:
        path ([type]): [description]

    Returns:
        [type]: [description]
    """
    new_path = Path(path.replace('\\', '/')).resolve()
    return str(new_path)


def parse_posix_path(path: str) -> str:
    """This method returns a posix path no matter if you pass it a windows or a linux path

    Args:
        path ([type]): [description]
    """
    new_path = PurePosixPath(path.replace('\\', '/'))
    return str(new_path)
