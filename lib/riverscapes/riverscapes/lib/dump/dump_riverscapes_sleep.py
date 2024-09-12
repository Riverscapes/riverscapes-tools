import sys
import os
import traceback
import argparse
from time import sleep
from datetime import datetime, timezone
from rsxml import Logger, dotenv
from dump_riverscapes import dump_riverscapes
from riverscapes import RiverscapesAPI

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_db_path', help='The final resting place of the SQLite DB', type=str)
    parser.add_argument('stage', help='URL to the cybercastor API', type=str, default='production')
    parser.add_argument('sleep', help='Number of minutes to sleep between dumps', type=int, default=5)
    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    mainlog = Logger("SQLite Riverscapes Dump")
    mainlog.setup(log_path=os.path.join(os.path.dirname(args.output_db_path), "dump_riverscapes.log"), verbose=True)

    try:
        with RiverscapesAPI(args.stage) as api:
            while True:
                dump_riverscapes(api, args.output_db_path)

                mainlog.info(f'Sleeping for {args.sleep} minutes')
                sys.stdout.flush()
                sys.stderr.flush()
                os.fsync(sys.stdout.fileno())
                os.fsync(sys.stderr.fileno())
                datetime.now(timezone.utc).astimezone().replace(microsecond=0)
                sleep(int(args.sleep) * 60)

    except Exception as e:
        mainlog.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)
