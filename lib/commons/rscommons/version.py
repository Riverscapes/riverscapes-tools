#!/usr/bin/env python
from __future__ import annotations
from typing import Mapping
import os
import re
import argparse
import semver
from termcolor import colored
import questionary
from rscommons.dotenv import parse_args_env


def get_version(fname: str):
    """Get the version from your version.py file

    Args:
        fname ([type]): path to version file
    Returns:
        semver.VersionInfo:  parsed value with bump
    Raises:
        Exception: File must exist
    """
    if not os.path.isfile(fname):
        create_file = questionary.confirm(f'File not found. Create it? ({fname})').ask()
        if create_file:
            new_version = semver.VersionInfo.parse('0.0.1')
            write_version(fname, new_version)
            print(f'Version file created with initial version of "{new_version}"')
            exit(0)

    version = re.search(
        '^__version__\\s*=\\s*"(.*)"',
        open(fname, encoding='utf-8').read(),
        re.M
    ).group(1)
    return semver.VersionInfo.parse(version)


def write_version(fname, newver):
    verdir = os.path.dirname(fname)
    if not os.path.splitext(fname)[1] == '.py':
        raise Exception('File must have a .py suffix: {}'.format(fname))
    if not os.path.isdir(verdir):
        raise Exception('Could not find root folder: {}'.format(verdir))
        open(fname, 'w', encoding='utf-8').write('__version__ = "{}"\n'.format(newver))


def version_bump(version: semver.VersionInfo):
    """ Bump the version number

    Args:
        version (semver.VersionInfo): parsed value

    Returns:
        bumped (semver.VersionInfo):  parsed value with bump
    """
    choices = [
        questionary.Choice('Patch: {} ==> {}'.format(version, version.bump_patch()), value=version.bump_patch()),
        questionary.Choice('Minor: {} ==> {}'.format(version, version.bump_minor()), value=version.bump_minor()),
        questionary.Choice('Major: {} ==> {}'.format(version, version.bump_major()), value=version.bump_major()),
        questionary.Choice('Build: {} ==> {}'.format(version, version.bump_build()), value=version.bump_build()),
        questionary.Choice('Prerelease: {} ==> {}'.format(version, version.bump_prerelease()), value=version.bump_prerelease()),
        questionary.Separator(),
        questionary.Choice('Manual type version', value='manual'),
        questionary.Separator(),
        questionary.Choice('Quit', value='quit'),
    ]
    # PyInquirer has a mouse-click problem (yes you read that correctly)
    # https://github.com/CITGuru/PyInquirer/issues/41
    # Until that's fixed we need to handle menus carefully
    response = None
    while response is None:
        response = questionary.select(
            'What bump do you want?',
            choices=choices
        ).ask()

    if type(response) is str and response == 'manual':
        return get_manual()
    elif type(response) is str and response == 'quit':
        exit(0)
    else:
        return response


def get_manual():
    """Ask the user to type a version manually

    Returns:
        [type]: [description]
    """
    compliant = False
    newversion = ''
    while not compliant:
        newversion_str = questionary.text('Type the new version?').ask()
        try:
            newversion = semver.VersionInfo.parse(newversion_str)
            compliant = True
        except ValueError as e:
            print(colored(e, 'red'))

    return newversion


def bump_version_file(verfile_path: str):
    """The Main function really only applies to this package
    """
    version = get_version(verfile_path)
    bumped = version_bump(version)
    write_version(verfile_path, bumped)
    print('Patching done: {}  ==> {}'.format(version, bumped))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('version_file', help='Path to __version__.py file', type=str)
    args = parse_args_env(parser, os.path.join(os.path.dirname(__file__), '.env'))
    bump_version_file(args.version_file)
