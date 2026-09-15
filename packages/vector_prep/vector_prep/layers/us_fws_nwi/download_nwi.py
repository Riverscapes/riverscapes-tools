"""Download NWI HUC8 zip archives for CONUS with simple resume behavior.

The HUC8 list is expected to be pre-curated upstream (example SQL below), and
this script treats the presence of an expected zip file as completed work.

SELECT DISTINCT SUBSTR(huc10,1,8) AS huc8 FROM default.wbdhu10_cleaned
"""

import argparse
import getpass
import os
import sqlite3
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from rsxml import Logger

# Defaults
CSV_INPUT = "huc8.csv"
DESTINATION_FOLDER = r"F:\nardata\datadownload\fws\nwi"
RETRIES = 3
TIMEOUT = 120
NWI_BASE_URL = "https://documentst.ecosphere.fws.gov/wetlands/downloads/watershed/HU8_{}_Watershed.zip"
LEDGER_FILENAME = "nwi_processing_ledger.gpkg"


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Return True when a table exists in the SQLite database."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_ledger_layers(ledger_path: Path) -> None:
    """Create GeoPackage attribute layers for the ledger when missing."""
    ledger_path.parent.mkdir(parents=True, exist_ok=True)

    existing_tables: set[str] = set()
    if ledger_path.exists():
        with sqlite3.connect(ledger_path) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            existing_tables = {row[0] for row in rows}

    if "ledger_runs" not in existing_tables:
        runs_df = gpd.GeoDataFrame(
            pd.DataFrame(
                {
                    "run_id": pd.Series(dtype="str"),
                    "run_started_utc": pd.Series(dtype="str"),
                    "run_finished_utc": pd.Series(dtype="str"),
                    "operator": pd.Series(dtype="str"),
                    "notes": pd.Series(dtype="str"),
                }
            )
        )
        runs_df.to_file(
            ledger_path,
            layer="ledger_runs",
            driver="GPKG",
            mode=("a" if ledger_path.exists() else "w"),
        )

    if "ledger_huc8_status" not in existing_tables:
        status_df = gpd.GeoDataFrame(
            pd.DataFrame(
                {
                    "huc8": pd.Series(dtype="str"),
                    "zip_mtime_utc": pd.Series(dtype="str"),
                    "download_status": pd.Series(dtype="str"),
                    "download_last_checked_utc": pd.Series(dtype="str"),
                    "download_last_error": pd.Series(dtype="str"),
                }
            )
        )
        status_df.to_file(
            ledger_path,
            layer="ledger_huc8_status",
            driver="GPKG",
            mode=("a" if ledger_path.exists() else "w"),
        )


def utc_now_iso() -> str:
    """Return current UTC time as an ISO-8601 string without microseconds."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def file_mtime_utc(file_path: Path) -> str | None:
    """Return file modified time in UTC ISO-8601 form if file exists."""
    if not file_path.exists():
        return None
    return (
        datetime.fromtimestamp(file_path.stat().st_mtime, UTC)
        .replace(microsecond=0)
        .isoformat()
    )


def init_ledger(conn: sqlite3.Connection) -> None:
    """Create ledger tables and indexes if they do not already exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_runs (
            run_id TEXT PRIMARY KEY,
            run_started_utc TEXT NOT NULL,
            run_finished_utc TEXT,
            operator TEXT,
            notes TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_huc8_status (
            huc8 TEXT PRIMARY KEY,
            zip_mtime_utc TEXT,
            download_status TEXT,
            download_last_checked_utc TEXT,
            download_last_error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ledger_huc8_download_status
        ON ledger_huc8_status(download_status)
        """
    )

    conn.commit()


def upsert_download_status(
    conn: sqlite3.Connection,
    *,
    huc8: str,
    zip_mtime_utc: str | None,
    download_status: str,
    download_last_checked_utc: str,
    download_last_error: str | None,
) -> None:
    """Insert or update one HUC8 download status row in the ledger."""
    # GeoPandas-created attribute tables do not enforce huc8 as a unique key,
    # so use replace-by-delete to keep one current row per HUC8.
    conn.execute(
        """
        DELETE FROM ledger_huc8_status
        WHERE huc8 = ?
        """,
        (huc8,),
    )
    conn.execute(
        """
        INSERT INTO ledger_huc8_status (
            huc8,
            zip_mtime_utc,
            download_status,
            download_last_checked_utc,
            download_last_error
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            huc8,
            zip_mtime_utc,
            download_status,
            download_last_checked_utc,
            download_last_error,
        ),
    )
    conn.commit()


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
    csv_input: Path,
    destination_folder: Path,
    retries: int,
    timeout: int,
    force: bool,
    ledger_path: Path,
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
    run_id = str(uuid.uuid4())

    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_ledger_layers(ledger_path)
    conn = sqlite3.connect(ledger_path)
    init_ledger(conn)
    conn.execute(
        """
        INSERT INTO ledger_runs (run_id, run_started_utc, operator, notes)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, utc_now_iso(), getpass.getuser(), f"csv={csv_input}"),
    )
    conn.commit()

    log.info(f"Starting NWI zip run for {total} HUC8 rows")
    log.info(f"Source list: {csv_input}")
    log.info(f"Destination: {destination_folder}")
    log.info(f"Ledger: {ledger_path}")

    try:
        for index, huc8 in enumerate(huc8s, start=1):
            zip_name = f"HU8_{huc8}_Watershed.zip"
            destination_zip = destination_folder / zip_name
            url = NWI_BASE_URL.format(huc8)
            checked_utc = utc_now_iso()

            if destination_zip.is_file() and not force:
                skipped += 1
                upsert_download_status(
                    conn,
                    huc8=huc8,
                    zip_mtime_utc=file_mtime_utc(destination_zip),
                    download_status="downloaded",
                    download_last_checked_utc=checked_utc,
                    download_last_error=None,
                )
                log.info(f"[{index}/{total}] Skip existing zip: {zip_name}")
                continue

            if destination_zip.is_file() and force:
                destination_zip.unlink()

            success = False
            last_error: str | None = None
            for attempt in range(1, retries + 1):
                try:
                    log.info(
                        f"[{index}/{total}] Download {zip_name} (attempt {attempt}/{retries})"
                    )
                    download_zip(url, destination_zip, timeout)
                    downloaded += 1
                    success = True
                    checked_utc = utc_now_iso()
                    upsert_download_status(
                        conn,
                        huc8=huc8,
                        zip_mtime_utc=file_mtime_utc(destination_zip),
                        download_status="downloaded",
                        download_last_checked_utc=checked_utc,
                        download_last_error=None,
                    )
                    log.info(f"[{index}/{total}] Downloaded: {zip_name}")
                    break
                except (requests.exceptions.RequestException, OSError) as exc:
                    last_error = str(exc)
                    log.warning(
                        f"[{index}/{total}] Attempt {attempt} failed for {zip_name}: {exc}"
                    )

            if not success:
                failed += 1
                checked_utc = utc_now_iso()
                upsert_download_status(
                    conn,
                    huc8=huc8,
                    zip_mtime_utc=file_mtime_utc(destination_zip),
                    download_status="failed",
                    download_last_checked_utc=checked_utc,
                    download_last_error=(
                        last_error[:2000] if last_error else "unknown_error"
                    ),
                )
                log.error(
                    f"[{index}/{total}] Failed after {retries} attempts: {zip_name}"
                )

        summary = f"Run complete. total={total}, skipped={skipped}, downloaded={downloaded}, failed={failed}"
        log.info(summary)
        conn.execute(
            """
            UPDATE ledger_runs
            SET run_finished_utc = ?, notes = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), summary, run_id),
        )
        conn.commit()
    finally:
        conn.close()

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
        "--retries",
        type=int,
        default=RETRIES,
        help=f"Retry attempts per HUC8. (Default {RETRIES})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=TIMEOUT,
        help=f"HTTP timeout in seconds. (Default {TIMEOUT})",
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-download even when zip already exists."
    )
    default_ledger = Path(__file__).resolve().parent / LEDGER_FILENAME
    parser.add_argument(
        "--ledger",
        type=Path,
        default=default_ledger,
        help="GeoPackage path for download ledger tables.",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    log = Logger("Download NWI Main")
    log.setup(log_path=args.destination / "download_nwi.log")
    log.title("Download NWI")
    exit_code = run(
        args.csv,
        args.destination,
        args.retries,
        args.timeout,
        args.force,
        args.ledger,
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
