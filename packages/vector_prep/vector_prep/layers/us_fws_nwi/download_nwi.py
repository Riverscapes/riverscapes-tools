"""Download NWI HUC8 zip archives for CONUS with simple resume behavior.

The HUC8 list is expected to be pre-curated upstream (example SQL below), and
this script treats the presence of an expected zip file as completed work.

SELECT DISTINCT SUBSTR(huc10,1,8) AS huc8 FROM default.wbdhu10_cleaned
"""

import argparse
import os
import tempfile
from pathlib import Path

import requests
from rsxml import Logger

# Defaults
CSV_INPUT = "huc8.csv"
DESTINATION_FOLDER = r"F:\nardata\datadownload\fws\nwi"
RETRIES = 3
TIMEOUT = 120
NWI_BASE_URL = "https://documentst.ecosphere.fws.gov/wetlands/downloads/watershed/HU8_{}_Watershed.zip"


def read_huc8_list(csv_path: Path) -> list[str]:
    """Read HUC8 identifiers from a plain-text or CSV-like file."""
    huc8s: list[str] = []
    with csv_path.open("r", encoding="utf-8") as huc_file:
        for line in huc_file:
            raw = line.strip()
            if not raw:
                continue
            # Support plain one-column files and CSV rows by taking first field.
            huc8s.append(raw.split(",", 1)[0].strip())
    return huc8s


def download_zip(url: str, destination_zip: Path, timeout: int) -> None:
    """Download URL to destination via temp file, then atomically move to final path."""
    destination_zip.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_path = tempfile.mkstemp(
        prefix="nwi_", suffix=".part", dir=str(destination_zip.parent)
    )
    temp_file_path = Path(temp_path)
    # Close file descriptor from mkstemp before reopening as a Python file object.
    os.close(fd)

    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with temp_file_path.open("wb") as temp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)

        temp_file_path.replace(destination_zip)
    finally:
        if temp_file_path.exists():
            temp_file_path.unlink()


def run(
    csv_input: Path, destination_folder: Path, retries: int, timeout: int, force: bool
) -> int:
    """Run bulk HUC8 zip downloads.

    Returns process exit code: 0 on full success, 1 when one or more HUC8s fail.
    """
    log = Logger("NWI Downloader")
    destination_folder.mkdir(parents=True, exist_ok=True)

    huc8s = read_huc8_list(csv_input)
    total = len(huc8s)
    skipped = 0
    downloaded = 0
    failed = 0

    log.info(f"Starting NWI zip run for {total} HUC8 rows")
    log.info(f"Source list: {csv_input}")
    log.info(f"Destination: {destination_folder}")

    for index, huc8 in enumerate(huc8s, start=1):
        zip_name = f"HU8_{huc8}_Watershed.zip"
        destination_zip = destination_folder / zip_name
        url = NWI_BASE_URL.format(huc8)

        if destination_zip.is_file() and not force:
            skipped += 1
            log.info(f"[{index}/{total}] Skip existing zip: {zip_name}")
            continue

        if destination_zip.is_file() and force:
            destination_zip.unlink()

        success = False
        for attempt in range(1, retries + 1):
            try:
                log.info(
                    f"[{index}/{total}] Download {zip_name} (attempt {attempt}/{retries})"
                )
                download_zip(url, destination_zip, timeout)
                downloaded += 1
                success = True
                log.info(f"[{index}/{total}] Downloaded: {zip_name}")
                break
            except (requests.exceptions.RequestException, OSError) as exc:
                log.warning(
                    f"[{index}/{total}] Attempt {attempt} failed for {zip_name}: {exc}"
                )

        if not success:
            failed += 1
            log.error(f"[{index}/{total}] Failed after {retries} attempts: {zip_name}")

    log.info(
        f"Run complete. total={total}, skipped={skipped}, downloaded={downloaded}, failed={failed}"
    )

    return 1 if failed > 0 else 0


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download NWI HUC8 zip archives for CONUS."
    )
    default_csv = Path(__file__).resolve().parent / CSV_INPUT
    parser.add_argument(
        "--csv", type=Path, default=default_csv, help="Path to HUC8 input file."
    )
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path(DESTINATION_FOLDER),
        help="Destination folder for zip files.",
    )
    parser.add_argument(
        "--retries", type=int, default=RETRIES, help="Retry attempts per HUC8."
    )
    parser.add_argument(
        "--timeout", type=int, default=TIMEOUT, help="HTTP timeout in seconds."
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-download even when zip already exists."
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    log = Logger("Download NWI Main")
    log.setup(log_path=args.destination / "download_nwi.log")
    log.title("Download NWI")
    exit_code = run(args.csv, args.destination, args.retries, args.timeout, args.force)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
