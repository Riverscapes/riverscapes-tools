import os
import argparse
from rsxml import Logger, dotenv
# TODO: THIS NEEDS TO GO IN COMMON
from rscommons.clean_nhd_data import download_unzip


nhd_url = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/NHDPLUS_H_{}_HU4_GDB.zip'


def load_nhd(vpuids, data_folder, user_name=None, password=None):

    log = Logger('Load NHD')

    download_folder = os.path.join(data_folder, 'download')
    unzip_folder = os.path.join(download_folder, 'scratch')

    for vpuid in vpuids.split(','):
        log.info('Processing VPU {}'.format(vpuid))

        url = nhd_url.format(vpuid)
        log.info(url)

        nhd_download_folder = os.path.join(download_folder, 'nhd', vpuid)
        nhd_unzip_folder = os.path.join(unzip_folder, 'nhd', vpuid)
        _final_folder = download_unzip(url, nhd_download_folder, nhd_unzip_folder, False)

        log.info('VPU {} complete'.format(vpuid))

    log.info('Process complete')


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('vpuids', help='Comma separated list of VPUs to process', type=str)
    parser.add_argument('data_folder', help='Top level data folder containing riverscapes context projects', type=str)
    # parser.add_argument('user_name', help='Postgres user name', type=str)
    # parser.add_argument('password', help='Postgres password', type=str)
    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '.env'))

    # Initiate the log file
    log = Logger('Load NHD')
    log.setup(log_path=os.path.join(args.data_folder, 'load_nhd.log'), verbose=True)

    try:
        load_nhd(args.vpuids, args.data_folder)  # , args.user_name, args.password)
        log.info('Process completed successfully')
    except Exception as ex:
        log.error(ex)


if __name__ == '__main__':
    main()
