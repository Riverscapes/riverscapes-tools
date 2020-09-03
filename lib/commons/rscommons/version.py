#!/usr/bin/env python
import semver
import os
import re
import argparse
import subprocess
from termcolor import colored
from PyInquirer import prompt, print_json, Separator
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
        answers = prompt([
            {
                'type': 'confirm',
                'name': 'create_file',
                'message': 'File not found. Create it? ({})'.format(fname),
            }
        ])
        if answers['create_file']:
            new_version = semver.VersionInfo.parse('0.0.1')
            write_version(fname, new_version)
            print('Version file created with initial version of "{}"'.format(new_version))
            exit(0)

    version = re.search(
        '^__version__\\s*=\\s*"(.*)"',
        open(fname).read(),
        re.M
    ).group(1)
    return semver.VersionInfo.parse(version)


def write_version(fname, newver):
    verdir = os.path.dirname(fname)
    if not os.path.splitext(fname)[1] == '.py':
        raise Exception('File must have a .py suffix: {}'.format(fname))
    if not os.path.isdir(verdir):
        raise Exception('Could not find root folder: {}'.format(verdir))
    open(fname, 'w').write('__version__ = "{}"\n'.format(newver))


def version_bump(version: semver.VersionInfo):
    """ Bump the version number

    Args:
        version (semver.VersionInfo): parsed value

    Returns:
        bumped (semver.VersionInfo):  parsed value with bump
    """
    choices = [
        {'name': 'Patch: {} ==> {}'.format(version, version.bump_patch()), 'value': version.bump_patch()},
        {'name': 'Minor: {} ==> {}'.format(version, version.bump_minor()), 'value': version.bump_minor()},
        {'name': 'Major: {} ==> {}'.format(version, version.bump_major()), 'value': version.bump_major()},
        {'name': 'Build: {} ==> {}'.format(version, version.bump_build()), 'value': version.bump_build()},
        {'name': 'Prerelease: {} ==> {}'.format(version, version.bump_prerelease()), 'value': version.bump_prerelease()},
        Separator(),
        {'name': 'Manual type version', 'value': 'manual'},
        Separator(),
        {'name': 'Quit', 'value': 'quit'},
    ]
    # PyInquirer has a mouse-click problem (yes you read that correctly)
    # https://github.com/CITGuru/PyInquirer/issues/41
    # Until that's fixed we need to handle menus carefully
    response = None
    while response is None:
        answers = prompt([
            {
                'type': 'list',
                'name': 'verbump',
                'message': 'What bump do you want?',
                'choices': choices
            }
        ])
        if 'verbump' in answers:
            response = answers['verbump']

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
        answers = prompt([
            {
                'type': 'input',
                'name': 'newversion',
                'message': 'Type the new version?'
            }
        ])
        try:
            newversion = semver.VersionInfo.parse(answers['newversion'])
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
