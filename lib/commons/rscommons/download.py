import os
import re
import time
import shutil
import zipfile
import requests
import tempfile
import datetime
from rscommons.util import safe_makedirs, safe_remove_dir, safe_remove_file, file_compare
from rscommons import Logger, ProgressBar, Timer

MAX_ATTEMPTS = 3  # Number of attempts for things like downloading and copying
PENDING_TIMEOUT = 60  # number of seconds before pending files are deemed stale


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

    unzip(zipfilepath, final_unzip_folder, force_download, retries)

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


def pending_check(file_path_pending, timeout):
    """returns true if we're still pending

    Args:
        file_path_pending ([type]): path to pending file
        file_path_pending ([itn]): in seconds
    """
    pending_exists = os.path.isfile(file_path_pending)
    if not pending_exists:
        return False

    pf_stats = os.stat(file_path_pending)
    # If the pending file is older than the timeout
    # then we need to delete the file and keep going
    if time.time() - pf_stats.st_mtime > timeout:
        safe_remove_file(file_path_pending)
        return False
    else:
        return True


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
    file_path_pending = os.path.join(download_folder, os.path.basename(result.group(2)) + '.pending')

    if os.path.isfile(file_path) and force_download:
        safe_remove_file(file_path)

    # If there is a pending path  and the pending path is fairly new
    # then wait for it.
    while pending_check(file_path_pending, PENDING_TIMEOUT):
        log.debug('Waiting for .pending file. Another process is working on this.')
        time.sleep(30)
    log.info('Waiting done. Proceeding.')

    # Skip the download if the file exists
    if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
        log.info('Skipping download because file exists.')
    else:
        tmpfilepath = tempfile.mktemp(".temp")

        # Write our pending file. No matter what we must clean this file up!!!
        def refresh_pending(init=False):
            with open(file_path_pending, 'w') as f:
                f.write(str(datetime.datetime.now()))

        # Cleaning up the commone areas is really important
        def download_cleanup():
            safe_remove_file(tmpfilepath)
            safe_remove_file(file_path_pending)

        refresh_pending()

        pending_timer = Timer()
        log.info('Downloading {}'.format(s3_url))

        # Actual file download
        for download_retries in range(MAX_ATTEMPTS):
            if download_retries > 0:
                log.warning('Download file retry: {}'.format(download_retries))
            try:
                dl = 0
                tmpfilepath = tempfile.mktemp(".temp")
                with requests.get(s3_url, stream=True) as r:
                    r.raise_for_status()
                    byte_total = int(r.headers.get('content-length'))
                    progbar = ProgressBar(byte_total, 50, s3_url, byteFormat=True)

                    # Binary write to file
                    with open(tmpfilepath, 'wb') as tempf:
                        for chunk in r.iter_content(chunk_size=8192):
                            # Periodically refreshing our .pending file
                            # so other processes will be aware we are still working on it.
                            if pending_timer.ellapsed() > 10:
                                refresh_pending()
                            if chunk:  # filter out keep-alive new chunks
                                dl += len(chunk)
                                tempf.write(chunk)
                                progbar.update(dl)
                    # Close the temporary file. It will be removed
                    if (not os.path.isfile(tmpfilepath)):
                        raise Exception('Error writing to temporary file: {}'.format(tmpfilepath))

                progbar.finish()
                break
            except Exception as e:
                log.debug('Error downloading file from s3 {}: \n{}'.format(s3_url, str(e)))
                # if this is our last chance then the function must fail [0,1,2]
                if download_retries == MAX_ATTEMPTS - 1:
                    download_cleanup()  # Always clean up
                    raise e

        # Now copy the temporary file (retry 3 times)
        for copy_retries in range(MAX_ATTEMPTS):
            if copy_retries > 0:
                log.warning('Copy file retry: {}'.format(copy_retries))
            try:
                shutil.copy(tmpfilepath, file_path)
                # Make sure to clean up so the next process doesn't encounter a broken file
                if not file_compare(file_path, tmpfilepath):
                    raise Exception('Error copying temporary download to final path')
                break

            except Exception as e:
                log.debug('Error copying file from temporary location {}: \n{}'.format(tmpfilepath, str(e)))
                # if this is our last chance then the function must fail [0,1,2]
                if copy_retries == MAX_ATTEMPTS - 1:
                    download_cleanup()  # Always clean up
                    raise e

        download_cleanup()  # Always clean up

    return file_path


def unzip(file_path, destination_folder, force_overwrite=False, retries=3):
    """[summary]

    Args:
        file_path: Full path to an existing zip archive
        destination_folder: Path where the zip archive will be unzipped
        force_overwrite (bool, optional): Force overwrite of a file if it's already there. Defaults to False.
        retries (int, optional): Number of retries on a single file. Defaults to 3.

    Raises:
        Exception: [description]
        Exception: [description]
        Exception: [description]
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

    except zipfile.BadZipFile as e:
        # If the zip file is bad then we have to remove it.
        log.error('BadZipFile. Cleaning up zip file and output folder')
        safe_remove_file(file_path)
        safe_remove_dir(destination_folder)
        raise Exception('Unzip error: BadZipFile')
    except Exception as e:
        log.error('Error unzipping. Cleaning up output folder')
        safe_remove_dir(destination_folder)
        raise Exception('Unzip error: file could not be unzipped')
