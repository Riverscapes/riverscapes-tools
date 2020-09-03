import os
import re
import time
import shutil
import zipfile
import requests
import tempfile
from rscommons.util import safe_makedirs
from rscommons import Logger, ProgressBar


def download_unzip(url, download_folder, unzip_folder=None, force_download=False, retries=3):
    """
    A wrapper for Download() and Unzip(). WE do these things together
    enough that it makes sense. Also there's the concept of retrying that
    needs to be handled in a centralized way

    Arguments:
        url {[type]} -- [description]
        download_folder {[type]} -- [description]

    Keyword Arguments:
        unzip_folder {[string]} -- (optional) specify the specific directory to extract files into (we still create a subfolder with the zip-file's name though)
        force_download {bool} -- [description] (default: {False})

    Returns:
        [type] -- [description]
    """
    log = Logger('Download')

    # If we specified an unzip path then use it, otherwise just unzip into the folder
    # with the same name as the file (minus the '.zip' extension)
    dl_retry = 0
    dl_success = False
    while not dl_success and dl_retry < 3:
        try:
            zipfilepath = download_file(url, download_folder, force_download)
            dl_success = True
        except Exception as e:
            log.debug(e)
            log.warning('download failed. retrying...')
            dl_retry += 1

    if (not dl_success):
        raise Exception('Downloading of file failed after {} attempts'.format(retries))

    final_unzip_folder = unzip_folder if unzip_folder is not None else os.path.splitext(zipfilepath)[0]

    try:
        unzip(zipfilepath, final_unzip_folder, force_download, retries)
    except Exception as e:
        log.debug(e)
        log.info('Waiting 5 seconds and Retrying once')
        time.sleep(5)
        unzip(zipfilepath, unzip_folder, force_download, retries)

    return final_unzip_folder


def get_unique_file_path(folder, file):
    """
    Ensure that the argument file name is unique within a given folder
    :param folder: Local folder path
    :param file: File name with extension
    :return: Full path to a unique file name
    """

    file_path = os.path.join(folder, file)
    i = 0

    new_path = file_path
    filename, ext = os.path.splitext(os.path.basename(file_path))

    while os.path.isfile(new_path):
        new_path = os.path.join(folder, '{}_{}{}'.format(filename, i, ext))
        i = i + 1

    return new_path


def download_file(s3_url, download_folder, force_download=False):
    """
    Download a file given a HTTPS URL that points to a file on S3
    :param s3_url: HTTPS URL for a file on S3
    :param download_folder: Folder where the file will be downloaded.
    :param force_download:
    :return: Local file path where the file was downloaded
    """

    log = Logger('Download')

    safe_makedirs(download_folder)

    # Retrieve the S3 bucket and path from the HTTPS URL
    result = re.match(r'https://([^.]+)[^/]+/(.*)', s3_url)

    # If file already exists and forcing download then ensure unique file name
    file_path = os.path.join(download_folder, os.path.basename(result.group(2)))

    if os.path.isfile(file_path) and force_download:
        os.remove(file_path)

    # Skip the download if the file exists
    if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
        log.info('Skipping download because file exists.')
    else:
        log.info('Downloading {}'.format(s3_url))

        try:
            dl = 0
            tmpfilepath = tempfile.mktemp(".temp")
            with requests.get(s3_url, stream=True) as r:
                r.raise_for_status()
                byte_total = int(r.headers.get('content-length'))
                progbar = ProgressBar(byte_total, 50, s3_url, byteFormat=True)

                with open(tmpfilepath, 'wb') as tempf:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            dl += len(chunk)
                            tempf.write(chunk)
                            progbar.update(dl)
                    # Close the temporary file. It will be removed
                if (not os.path.isfile(tmpfilepath)):
                    raise Exception('Error writing to temporary file: {}'.format(tmpfilepath))

            progbar.finish()

            shutil.copy(tmpfilepath, file_path)
            srcStats = os.stat(file_path)
            dstStats = os.stat(tmpfilepath)

            os.remove(tmpfilepath)
            if (srcStats.st_size != dstStats.st_size):
                # Make sure to clean up so the next process doesn't encounter a broken file
                os.remove(file_path)
                raise Exception('Error copying temporary download to final path')

        except Exception as e:
            print(e)
            raise Exception('Error downloading file from s3 {}'.format(s3_url))

    return file_path


def unzip(file_path, destination_folder, force_overwrite=False, retries=3):
    """
    Uncompress a zip archive into the specified destination folder
    :param file_path: Full path to an existing zip archive
    :param destination_folder: Path where the zip archive will be unzipped
    :return: None
    """
    log = Logger('Unzipper')

    if not os.path.isfile(file_path):
        raise Exception('Unzip error: file not found: {}'.format(file_path))

    try:
        log.info('Attempting unzip: {} ==> {}'.format(file_path, destination_folder))
        zip_ref = zipfile.ZipFile(file_path, 'r')

        # only unzip files we don't already have
        safe_makedirs(destination_folder)

        log.info('Extracting: {}'.format(file_path))

        # Only unzip things we haven't already unzipped
        for fitem in zip_ref.filelist:
            uz_success = False
            uz_retry = 0
            while not uz_success and uz_retry < retries:
                try:
                    outfile = os.path.join(destination_folder, fitem.filename)
                    if fitem.is_dir():
                        if not os.path.isdir(outfile):
                            zip_ref.extract(fitem, destination_folder)
                            log.debug('   (creating)  {}'.format(fitem.filename))
                        else:
                            log.debug('   (skipping)  {}'.format(fitem.filename))
                    else:
                        if force_overwrite or (fitem.file_size > 0 and not os.path.isfile(outfile)) or (os.path.getsize(outfile) / fitem.file_size) < 0.99999:
                            log.debug('   (unzipping) {}'.format(fitem.filename))
                            zip_ref.extract(fitem, destination_folder)
                        else:
                            log.debug('   (skipping)  {}'.format(fitem.filename))

                    uz_success = True
                except Exception as e:
                    log.debug(e)
                    log.warning('unzipping file failed. waiting 3 seconds and retrying...')
                    time.sleep(3)
                    uz_retry += 1

            if (not uz_success):
                raise Exception('Unzipping of file {} failed after {} attempts'.format(fitem.filename, retries))

        zip_ref.close()
        log.info('Done')

    except Exception as e:
        log.error('Error unzipping. Cleaning up file')
        if os.path.isfile(file_path):
            os.remove(file_path)
        raise Exception('Unzip error: file could not be unzipped')
