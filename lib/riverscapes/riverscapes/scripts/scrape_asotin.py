"""[summary]
"""
from typing import Dict, List
import os
import re
import argparse
import codecs
from pathlib import Path
import psycopg
from riverscapes import RiverscapesAPI, RiverscapesProject


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


def scrape_asotin(riverscapes_api: RiverscapesAPI, curs) -> None:

    search_params = {
        "tags": ["AsotinIMW"]
    }

    changeable_projects: List[RiverscapesProject] = []
    total = 0
    for project, _stats, search_total, _prg in riverscapes_api.search(search_params, progress_bar=True):
        total = search_total

        project_type = project.project_type
        visit_id = project.project_metadata.get('Visit', None)
        site_name = project.project_metadata.get('Site', None)
        watershed = project.project_metadata.get('Watershed', None)
        year = project.project_metadata.get('Year', None)

        if visit_id is None:
            continue

        try:
            curs.execute('UPDATE projects SET guid = %s WHERE visit_id = %s AND project_type = %s', (project.guid, visit_id, project_type))
        except Exception as ex:
            print(ex)


def main():

    parser = argparse.ArgumentParser(description="Scrape Asotin County data")
    parser.add_argument("champ_db_host", help="Output directory")
    parser.add_argument("champ_db_port", help="Output directory")
    parser.add_argument("champ_db_database", help="Output directory")
    parser.add_argument("champ_db_user", help="Output directory")
    parser.add_argument("champ_db_password", help="Output directory")
    parser.add_argument("champ_root_cert", help="Output directory")
    parser.add_argument("champ_client_cert", help="Output directory")
    parser.add_argument("champ_client_key", help="Output directory")
    args = parse_args_env(parser)

    with RiverscapesAPI() as api:
        conn = psycopg.connect(
            dbname=args.champ_db_database,
            user=args.champ_db_user,
            password=args.champ_db_password,
            host=args.champ_db_host,
            port=args.champ_db_port,
            sslmode="require",
            sslrootcert=args.champ_root_cert,
            sslcert=args.champ_client_cert,
            sslkey=args.champ_client_key,
        )
        curs = conn.cursor()
        scrape_asotin(api, curs)


if __name__ == "__main__":
    main()
