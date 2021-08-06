""" Testing for the vector ops

"""
import unittest
import os
import platform

from pathlib import Path, PurePosixPath, PureWindowsPath
from rscommons import rspaths

IS_WINDOWS = platform.system() == 'Windows'
"""

"""


class UtilTest(unittest.TestCase):
    """[summary]

    Args:
        unittest ([type]): [description]
    """

    def test_pretty_duration(self):
        """[summary]
        """

        # Find this path and make sure it exists as a baseline
        home_dir = Path(os.path.join(os.path.dirname(__file__), '..', '..', '..')).resolve()
        this_rel = Path(__file__).relative_to(home_dir)
        self.assertTrue(os.path.isfile(os.path.join(home_dir, str(this_rel))))

        # Create paths of two different types
        linux_path = str(this_rel).replace('\\', '/')
        windows_path = str(this_rel).replace('/', '\\')

        # Now joining these with our base directory is what we will test
        linux_path_parsed = rspaths.parse_rel_path(os.path.join(home_dir, linux_path))
        windows_path_parsed = rspaths.parse_rel_path(os.path.join(home_dir, windows_path))

        self.assertTrue(os.path.isfile(linux_path_parsed))
        self.assertTrue(os.path.isfile(windows_path_parsed))

    def test_posix_path(self):

        # Find this path and make sure it exists as a baseline
        home_dir = Path(os.path.join(os.path.dirname(__file__), '..', '..', '..')).resolve()
        this_rel = Path(__file__).relative_to(home_dir)
        self.assertTrue(os.path.isfile(os.path.join(home_dir, str(this_rel))))

        # Create paths of two different types
        linux_path = str(this_rel).replace('\\', '/')
        windows_path = str(this_rel).replace('/', '\\')

        self.assertEqual(rspaths.parse_posix_path(linux_path), 'lib/commons/test/test_paths.py')
        self.assertEqual(rspaths.parse_posix_path(windows_path), 'lib/commons/test/test_paths.py')
