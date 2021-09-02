import codecs
from typing import List, Dict
import re
import os
import argparse
from pathlib import Path


def parse_dotenv(dotenv_path):
    """Given a path to a dotenv file, return that file as a dictionary

    Args:
        dotenv_path ([type]): [description]

    Returns:
        [type]: [description]
    """
    results = {}
    # We fall back gracefully if there's no file there
    if not os.path.exists(dotenv_path):
        return results
    with open(dotenv_path) as f:
        for line in f:
            line = line.strip()
            # Ignore any line that starts with '#'
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)

            # Remove any leading and trailing spaces in key, value
            k, v = k.strip(), v.strip().encode('unicode-escape').decode('ascii')
            if len(v) > 0:
                quoted = v[0] == v[len(v) - 1] in ['"', "'"]

                if quoted:
                    v = codecs.getdecoder('unicode_escape')(v[1:-1])[0]
            results[k] = v
    return results


def parse_args_env(parser: argparse.ArgumentParser, env_path=None):
    """substitute environment variables for argparse parameters

    Arguments:
        args {dict} -- key: value pairs of environment variables
        args {argparse.parser} -- [description]
    """
    _env = parse_dotenv(env_path) if env_path is not None else {}
    args = parser.parse_args()
    pattern = r'{env:([^}]+)}'

    # Try and make substitutions for all the {env:ENVNAME} parameters we find
    for k, v in vars(args).items():
        new_val = replace_env_varts(pattern, v, os.environ)
        setattr(args, k, new_val)

    return args


def replace_env_varts(pattern: str, value_str: str, env: Dict[str, str]):
    if type(value_str) is str:
        new_str = value_str

        def replace(m):
            envname = m.group(1)
            if envname in env:
                sub = env[envname]
            elif envname in os.environ:
                sub = os.environ[envname]
            else:
                raise Exception('COULD NOT FIND ENVIRONMENT VARIABLE: {}'.format(envname))
            # Finally, make the substitution
            return sub.replace("\\", "/")

        new_str = str(Path(re.sub(pattern, replace, new_str)))

        return new_str
    else:
        return value_str
