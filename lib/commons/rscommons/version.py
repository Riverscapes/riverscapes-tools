#!/usr/bin/env python
from __future__ import annotations
import os
import re
import argparse
import semver
from termcolor import colored
import questionary
from rsxml import dotenv


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
    """ Write the version
    """
    verdir = os.path.dirname(fname)
    if not os.path.splitext(fname)[1] == '.py':
        raise Exception(f'File must have a .py suffix: {fname}')
    if not os.path.isdir(verdir):
        raise Exception(f'Could not find root folder: {verdir}')
    # Previously the write was indented under the error branch and never executed
    with open(fname, 'w', encoding='utf-8') as f:
        f.write(f'__version__ = "{newver}"\n')


def version_bump(version: semver.VersionInfo, bump: str | None = None, set_version: str | None = None):
    """Return a bumped version, optionally non-interactively.

    Args:
        version: Current semantic version.
        bump: Optional bump keyword (patch|minor|major|build|prerelease).
        set_version: Optional explicit version string to set (overrides bump).

    Returns:
        semver.VersionInfo: New version.
    """

    # Non-interactive explicit version wins
    if set_version:
        return semver.VersionInfo.parse(set_version)

    # Non-interactive bump keywords
    if bump:
        bump = bump.lower().strip()
        if bump == 'patch':
            return version.bump_patch()
        if bump == 'minor':
            return version.bump_minor()
        if bump == 'major':
            return version.bump_major()
        if bump == 'build':
            return version.bump_build()
        if bump == 'prerelease':
            return version.bump_prerelease()
        raise ValueError(f"Unknown bump type: {bump}")

    # Interactive mode
    # Pre-compute bump values as strings to avoid semver comparison strictness inside questionary
    patch_v = str(version.bump_patch())
    minor_v = str(version.bump_minor())
    major_v = str(version.bump_major())
    build_v = str(version.bump_build())
    pre_v = str(version.bump_prerelease())
    choices = [
        questionary.Choice(f'Patch: {version} ==> {patch_v}', value=patch_v),
        questionary.Choice(f'Minor: {version} ==> {minor_v}', value=minor_v),
        questionary.Choice(f'Major: {version} ==> {major_v}', value=major_v),
        questionary.Choice(f'Build: {version} ==> {build_v}', value=build_v),
        questionary.Choice(f'Prerelease: {version} ==> {pre_v}', value=pre_v),
        questionary.Separator(),
        questionary.Choice('Manual type version', value='manual'),
        questionary.Separator(),
        questionary.Choice('Quit', value='quit'),
    ]

    default_choice = patch_v
    response = None
    while response is None:
        response = questionary.select(
            'What bump do you want?',
            choices=choices,
            default=default_choice
        ).ask()

    if isinstance(response, str):
        if response == 'manual':
            return get_manual()
        if response == 'quit':
            exit(0)
        return semver.VersionInfo.parse(response)
    return semver.VersionInfo.parse(str(response))


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


def bump_version_file(verfile_path: str, bump: str | None = None, set_version: str | None = None):
    """Bump (or set) the version stored in a version file.

    Args:
        verfile_path: Path to file containing __version__ = "x.y.z".
        bump: Optional bump keyword (patch|minor|major|build|prerelease) for non-interactive use.
        set_version: Optional explicit version string to set (overrides bump).
    """
    version = get_version(verfile_path)
    bumped = version_bump(version, bump=bump, set_version=set_version)
    write_version(verfile_path, bumped)
    print(f'Patching done: {version}  ==> {bumped}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('version_file', help='Path to __version__.py file', type=str)
    parser.add_argument('--bump', choices=['patch', 'minor', 'major', 'build', 'prerelease'], help='Non-interactive bump keyword')
    parser.add_argument('--set-version', dest='set_version', help='Explicit version to set (overrides --bump)')
    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '.env'))
    bump_version_file(args.version_file, bump=args.bump, set_version=args.set_version)
