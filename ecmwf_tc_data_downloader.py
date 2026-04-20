#!/usr/bin/env python3
"""
ECMWF Tropical Cyclone Track Data Downloader

Downloads TC track BUFR files from ECMWF's Open Data service using the
official ecmwf-opendata Python client (type="tf", stream="enfo").

Each downloaded file contains tracks for ALL active storms and ALL ensemble
members (50 perturbed + 1 control) in BUFR4 format — one file per forecast run.

Max forecast horizons by run time (per ECMWF documentation):
    00Z / 12Z → step=240 (10-day track)
    06Z / 18Z → step=144 (6-day track)

References:
    - ecmwf-opendata: https://github.com/ecmwf/ecmwf-opendata
    - ECMWF Open Data: https://www.ecmwf.int/en/forecasts/datasets/open-data
    - TC tracks product: https://pypi.org/project/ecmwf-opendata/ (type="tf")
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union

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
                downloaded += 1
            else:
                logger.error(f"Download produced empty or missing file: {filename}")
                failed += 1

        except Exception as e:
            logger.error(f"Download failed for {filename}: {e}")
            if os.path.exists(filepath):
                os.remove(filepath)
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
