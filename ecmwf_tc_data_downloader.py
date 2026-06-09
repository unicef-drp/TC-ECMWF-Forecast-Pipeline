#!/usr/bin/env python3
"""
ECMWF Tropical Cyclone Track Data Downloader

Primary: ecmwf-opendata client (data.ecmwf.int, type="tf", stream="enfo").
Fallback: ECMWF DISS/Essential portal (essential.ecmwf.int) — used when the
  primary endpoint returns 404. DISS publishes per-storm BUFR4 files which are
  concatenated into a single output file so the extractor sees the same format.

Max forecast horizons by run time (per ECMWF documentation):
    00Z / 12Z → step=240 (10-day track)
    06Z / 18Z → step=144 (6-day track)

References:
    - ecmwf-opendata: https://github.com/ecmwf/ecmwf-opendata
    - ECMWF DISS: https://essential.ecmwf.int/
"""

import os
import re
import logging
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union

import requests
from ecmwf.opendata import Client

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = "tc_data"

# Maximum TC forecast step by run time (hours)
_MAX_STEP = {0: 240, 6: 144, 12: 240, 18: 144}


def _step_for_run_time(run_time: int) -> int:
    """Return the maximum TC track forecast step for a given run time."""
    return _MAX_STEP.get(run_time, 240)


def _output_filename(forecast_date: datetime, run_time: int) -> str:
    """Canonical filename for a TC track BUFR file."""
    return f"tc_tracks_{forecast_date.strftime('%Y-%m-%d')}_r{run_time:02d}.bufr4"


_DISS_BASE = "https://essential.ecmwf.int"
_TC_TRACK_RE = re.compile(r'tropical_cyclone_track.*bufr4\.bin$', re.IGNORECASE)


def _diss_dt_str(forecast_date: datetime, run_time: int) -> str:
    return forecast_date.strftime('%Y%m%d') + f'{run_time:02d}0000'


def _list_diss_tc_files(forecast_date: datetime, run_time: int) -> List[str]:
    """Return a list of TC track file URLs from essential.ecmwf.int for this run."""
    dt_str = _diss_dt_str(forecast_date, run_time)
    listing_url = f"{_DISS_BASE}/file/{dt_str}/"
    try:
        r = requests.get(listing_url, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
    except Exception as e:
        logger.debug(f"DISS listing failed ({listing_url}): {e}")
        return []

    urls = []
    for match in re.finditer(r'href="(/file/[^"]+)"', r.text):
        path = match.group(1)
        if _TC_TRACK_RE.search(path):
            urls.append(f"{_DISS_BASE}{path}")
    return urls


def _download_diss_and_combine(
    forecast_date: datetime, run_time: int, filepath: str
) -> bool:
    """
    Download all TC track BUFR4 files for this run from essential.ecmwf.int and
    concatenate them into a single output file. BUFR is a sequential record format
    so binary concatenation produces a valid multi-message BUFR4 file.
    Returns True if at least one file was downloaded successfully.
    """
    tc_urls = _list_diss_tc_files(forecast_date, run_time)
    if not tc_urls:
        logger.info("DISS: no TC track files found for this run (no active named storms)")
        return False

    logger.info(f"DISS: found {len(tc_urls)} TC track file(s) — downloading and combining")

    total_bytes = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        part_files = []
        for i, url in enumerate(tc_urls):
            fname = os.path.basename(url)
            part_path = os.path.join(tmpdir, fname)
            try:
                r = requests.get(url, stream=True, timeout=120)
                r.raise_for_status()
                with open(part_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
                if os.path.getsize(part_path) > 0:
                    part_files.append(part_path)
                    total_bytes += os.path.getsize(part_path)
            except Exception as e:
                logger.warning(f"DISS: failed to download {fname}: {e}")

        if not part_files:
            return False

        with open(filepath, 'wb') as out:
            for part in part_files:
                with open(part, 'rb') as inp:
                    out.write(inp.read())

    logger.info(
        f"DISS: combined {len(part_files)}/{len(tc_urls)} files "
        f"→ {filepath} ({total_bytes / 1024:.0f} KB)"
    )
    return True


def download_tc_data(
    limit: int = 1,
    date: Optional[str] = None,
    run_time: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    named_storms_only: bool = True,
    **_kwargs,
) -> Dict[str, int]:
    """
    Download TC track BUFR files from ECMWF Open Data.

    Each call downloads one combined BUFR4 file per forecast run containing
    tracks for all active storms and all 51 ensemble members.

    Args:
        limit: Number of latest forecasts to download when no date is given (default: 1).
        date: Specific forecast date in YYYYMMDD format. Requires run_time.
        run_time: Forecast run time: "00", "06", "12", or "18".
                  Required when date is specified.
        output_dir: Directory for downloaded files.
        named_storms_only: Passed through for compatibility; actual storm
                           filtering happens in the extractor after download.

    Returns:
        dict with 'downloaded' and 'failed' counts.
    """
    os.makedirs(output_dir, exist_ok=True)
    client = Client(source="ecmwf")

    # Build the list of (date, run_time_int) pairs to download
    targets = _resolve_targets(date, run_time, limit)

    if not targets:
        logger.error("No forecast targets resolved — nothing to download")
        return {'downloaded': 0, 'failed': 0}

    downloaded = 0
    failed = 0

    for forecast_date, rt in targets:
        step = _step_for_run_time(rt)
        filename = _output_filename(forecast_date, rt)
        filepath = os.path.join(output_dir, filename)

        if os.path.exists(filepath):
            logger.info(f"Skipping already downloaded: {filename}")
            downloaded += 1
            continue

        logger.info(f"Downloading TC tracks: {forecast_date.strftime('%Y-%m-%d')} {rt:02d}Z (step={step}h)")
        success = False
        try:
            client.retrieve(
                date=forecast_date,
                time=rt,
                stream="enfo",
                type="tf",
                step=step,
                target=filepath,
            )
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                size_kb = os.path.getsize(filepath) / 1024
                logger.info(f"Downloaded: {filename} ({size_kb:.0f} KB)")
                success = True
            else:
                logger.warning(f"data.ecmwf.int returned empty file for {filename}")
                if os.path.exists(filepath):
                    os.remove(filepath)
        except Exception as e:
            logger.warning(f"data.ecmwf.int failed for {filename}: {e}")
            if os.path.exists(filepath):
                os.remove(filepath)

        if not success:
            logger.info(f"Falling back to DISS (essential.ecmwf.int) for {filename}")
            if _download_diss_and_combine(forecast_date, rt, filepath):
                success = True
            else:
                logger.error(f"Download failed for {filename} (both data.ecmwf.int and essential.ecmwf.int)")
                if os.path.exists(filepath):
                    os.remove(filepath)

        if success:
            downloaded += 1
        else:
            failed += 1

    logger.info(f"TC download complete — downloaded: {downloaded}, failed: {failed}")
    return {'downloaded': downloaded, 'failed': failed}


def _resolve_targets(
    date: Optional[str],
    run_time: Optional[str],
    limit: int,
) -> List[tuple]:
    """
    Return a list of (forecast_date: datetime, run_time: int) pairs to download.

    If date is given, returns that specific date + run_time.
    Otherwise returns the most recently available run(s) based on current UTC time.
    """
    if date:
        if not run_time:
            raise ValueError("run_time is required when date is specified")
        forecast_date = datetime.strptime(date, '%Y%m%d')
        rt = int(run_time)
        return [(forecast_date, rt)]

    # "Latest N" mode: try recent dates/runs in reverse order
    # ECMWF data is typically available ~7h40m after each model run time.
    # Determine the most recently expected available run based on current UTC
    # time, then walk backwards so candidates are newest-first.
    #
    # Availability windows (UTC):
    #   18Z (prev day) → ready by ~01:40Z
    #   00Z            → ready by ~07:40Z
    #   06Z            → ready by ~13:40Z
    #   12Z            → ready by ~19:40Z
    DATA_READY_OFFSET_HOURS = 8  # conservative: available within 8h of run
    now_utc = datetime.now(timezone.utc)
    # Walk back through run times until we find one whose data should be ready
    run_times_desc = [18, 12, 6, 0]
    candidates = []

    # Generate candidates for the past 3 days, newest-first
    for day_offset in range(3):
        check_date = (now_utc - timedelta(days=day_offset)).date()
        for rt in run_times_desc:
            run_utc = datetime(check_date.year, check_date.month, check_date.day,
                               rt, 0, 0, tzinfo=timezone.utc)
            if now_utc >= run_utc + timedelta(hours=DATA_READY_OFFSET_HOURS):
                candidates.append((datetime.combine(check_date, datetime.min.time()), rt))

    # If run_time is specified without a date, filter to that run time only
    if run_time:
        rt_int = int(run_time)
        candidates = [(d, rt) for d, rt in candidates if rt == rt_int]

    return candidates[:limit]


def list_downloaded_files(output_dir: str = DEFAULT_OUTPUT_DIR) -> List[str]:
    """List all TC track BUFR files in the output directory."""
    output_path = Path(output_dir)
    if not output_path.exists():
        return []
    return sorted(str(f) for f in output_path.glob("tc_tracks_*.bufr4"))
