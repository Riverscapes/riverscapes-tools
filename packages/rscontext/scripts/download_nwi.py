"""
    Temporary, standalone script to downlaod National Wetland Inventory data.
    These data aren't available through the API, so we'll need to download them
    as zip files containing GeoPackages. It then uploads them to an S3 bucket.

    https://www.fws.gov/program/national-wetlands-inventory/download-state-wetlands-data

    Philip Bailey
    4 December 2024
    """

from typing import List
import argparse
import os
# NOTE: boto3 is not included in cybercastor so you will need to install it locally and use it only here
import boto3.s3
import boto3.s3.transfer
import requests
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# List of two-letter US state abbreviations
state_abbreviations = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# Base URL where the data are stored on the Fish Wildlife Service website
base_url = "https://documentst.ecosphere.fws.gov/wetlands/data/State-Downloads/"


def list_existing_files(s3_client, bucket: str, folder: str) -> List[str]:
    """
    List files in the specified S3 folder.

    :param bucket: S3 bucket name
    :param folder: Folder path within the bucket
    :return: Set of file names in the folder
    """
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        existing_files = set()

        for page in paginator.paginate(Bucket=bucket, Prefix=folder):
            for obj in page.get("Contents", []):
                existing_files.add(obj["Key"].replace(folder, "", 1))  # Remove folder prefix

        return existing_files
    except Exception as e:
        print(f"Error listing files in S3 folder {folder}: {e}")
        return set()


def upload_to_s3(s3_client, file_path: str, bucket: str, key: str) -> bool:
    """
    Upload a file to an S3 bucket.

    :param file_path: Local file path to upload
    :param bucket: S3 bucket name
    :param key: S3 object key
    """
    try:
        s3_client.upload_file(file_path, bucket, key)
        print(f"Uploaded {file_path} to s3://{bucket}/{key}")
        return True
    except (NoCredentialsError, PartialCredentialsError):
        print("AWS credentials not configured correctly.")
    except Exception as e:
        print(f"Failed to upload {file_path} to S3: {e}")

    return False


def download_nwi_data(s3_client, existing_files: List[str], download_dir: str, bucket: str, folder: str) -> None:
    """Loop over US states and download NWI data.

    Args:
        s3_client (_type_): _description_
        existing_files (List[str]): _description_
        download_dir (str): _description_
        bucket (str): _description_
        folder (str): _description_
    """

    os.makedirs(download_dir, exist_ok=True)
    for state in state_abbreviations:
        file_name = f"{state}_geopackage_wetlands.zip"
        url = f"{base_url}{file_name}"
        destination = os.path.join(download_dir, file_name)
        s3_key = f"{folder}/{file_name}"

        if s3_key in existing_files:
            print(f"Skipping {file_name} because it already exists in S3.")
            continue

        print(f"Downloading {url}")

        try:
            response = requests.get(url, stream=True, timeout=90)
            response.raise_for_status()  # Raise an error for bad status codes

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Downloaded {file_name} to {destination}.")

            status = upload_to_s3(s3_client, destination, bucket, s3_key)

            if status is True:
                os.remove(destination)
                print(f"Deleted {destination}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {file_name}: {e}")


def main():
    """Main entry point"""

    args = argparse.ArgumentParser(description="Download National Wetland Inventory data.")
    args.add_argument("download_folder", help="Local download folder")
    args.add_argument("bucket", help="S3 bucket name")
    args.add_argument("folder", help="S3 folder name")
    args.add_argument("skip_existing", help="Skip files that already exist in the S3 folder")
    args.add_argument("--region", default="us-west-2", help="AWS region")
    args = args.parse_args()

    s3_client = boto3.client("s3")

    existing = list_existing_files(s3_client, args.bucket, args.folder) if args.skip_existing else set()
    download_nwi_data(s3_client, existing, args.download_folder, args.bucket, args.folder)


if __name__ == "__main__":
    main()
