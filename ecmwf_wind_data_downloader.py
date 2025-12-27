#!/usr/bin/env python3
"""
ECMWF Ensemble Wind Data Downloader

This module provides functions to download ensemble wind forecast GRIB files
from ECMWF's Open Data service.

The downloader processes:
- Ensemble forecasts (50 perturbed members: 1-50, + control member 51)
- 10m wind components (u10 and v10)
- Multiple forecast lead times (every 3-6 hours up to 360 hours)
- Different forecast run times (00Z, 06Z, 12Z, 18Z)

References:
- ECMWF Open Data: https://www.ecmwf.int/en/forecasts/datasets/open-data
- ECMWF Python Client: https://github.com/ecmwf/ecmwf-opendata
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from ecmwf.opendata import Client

# Configuration
DEFAULT_OUTPUT_DIR = "wind_data"

# Forecast step configurations - simplified to 0-144h with 6h steps for all run times
FORECAST_STEPS = {
    0: list(range(0, 145, 6)),
    # 0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144
    6: list(range(0, 145, 6)),  # Same as 00Z
    12: list(range(0, 145, 6)),  # Same as 00Z
    18: list(range(0, 145, 6))  # Same as 00Z
}


def get_forecast_steps(run_time: int) -> List[int]:
    """
    Get available forecast steps for a given run time.

    Simplified configuration for all run times:
    - All run times: 0 to 144 hours by 6-hour steps
    - This provides 25 forecast steps: 0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144

    Args:
        run_time (int): Model run time (0, 6, 12, or 18)

    Returns:
        List[int]: List of available forecast steps in hours

    Reference:
        ECMWF Open Data: https://www.ecmwf.int/en/forecasts/datasets/open-data
    """
    if run_time not in FORECAST_STEPS:
        raise ValueError(f"Invalid run_time: {run_time}. Must be 0, 6, 12, or 18")

    return FORECAST_STEPS[run_time]


def _download_single_step_pf(client: Client, forecast_date: datetime, run_time: int,
                             step: int, output_dir: str, verbose: bool = False) -> Dict[str, Union[str, int, bool]]:
    """
    Download a single forecast step PF (perturbed) file. Helper function for concurrent downloads.

    Args:
        client: ECMWF client instance
        forecast_date: Forecast date
        run_time: Model run time (0, 6, 12, or 18 UTC)
        step: Forecast step in hours
        output_dir: Output directory
        verbose: Whether to print progress

    Returns:
        dict: Download result with 'success', 'step', 'filepath', 'file_size', 'type'
    """
    filename = f"wind_ens_{forecast_date.strftime('%Y-%m-%d')}_r{run_time:02d}_f{step:03d}h_pf.grib2"
    filepath = os.path.join(output_dir, filename)

    # Skip if already downloaded
    if os.path.exists(filepath):
        file_size = os.path.getsize(filepath)
        if verbose:
            print(f"  [SKIP] +{step:3d}h PF: Already exists ({file_size / 1024 / 1024:.1f} MB)")
        return {
            'success': True,
            'step': step,
            'type': 'pf',
            'filepath': filepath,
            'file_size': file_size,
            'skipped': True
        }

    try:
        if verbose:
            print(f"  [DOWN] +{step:3d}h PF: Downloading...", end='', flush=True)

        # Download PF (perturbed forecast) data
        client.retrieve(
            date=forecast_date,
            time=run_time,
            stream="enfo",
            type="pf",  # Perturbed forecast (members 1-50)
            step=step,
            param=["10u", "10v"],
            target=filepath
        )

        # Verify file was created
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            if verbose:
                print(f" ({file_size / 1024 / 1024:.1f} MB)")
            return {
                'success': True,
                'step': step,
                'type': 'pf',
                'filepath': filepath,
                'file_size': file_size,
                'skipped': False
            }
        else:
            if verbose:
                print(" File not created")
            return {
                'success': False,
                'step': step,
                'type': 'pf',
                'filepath': None,
                'error': 'File not created',
                'skipped': False
            }

    except Exception as e:
        if verbose:
            print(f" Error: {e}")
        return {
            'success': False,
            'step': step,
            'type': 'pf',
            'filepath': None,
            'error': str(e),
            'skipped': False
        }


def _download_single_step_cf(client: Client, forecast_date: datetime, run_time: int,
                             step: int, output_dir: str, verbose: bool = False) -> Dict[str, Union[str, int, bool]]:
    """
    Download a single forecast step CF (control) file. Helper function for concurrent downloads.

    Args:
        client: ECMWF client instance
        forecast_date: Forecast date
        run_time: Model run time (0, 6, 12, or 18 UTC)
        step: Forecast step in hours
        output_dir: Output directory
        verbose: Whether to print progress

    Returns:
        dict: Download result with 'success', 'step', 'filepath', 'file_size', 'type'
    """
    filename = f"wind_ens_{forecast_date.strftime('%Y-%m-%d')}_r{run_time:02d}_f{step:03d}h_cf.grib2"
    filepath = os.path.join(output_dir, filename)

    # Skip if already downloaded
    if os.path.exists(filepath):
        file_size = os.path.getsize(filepath)
        if verbose:
            print(f"  [SKIP] +{step:3d}h CF: Already exists ({file_size / 1024 / 1024:.1f} MB)")
        return {
            'success': True,
            'step': step,
            'type': 'cf',
            'filepath': filepath,
            'file_size': file_size,
            'skipped': True
        }

    try:
        if verbose:
            print(f"  [DOWN] +{step:3d}h CF: Downloading...", end='', flush=True)

        # Download CF (control forecast) data for member 51
        client.retrieve(
            date=forecast_date,
            time=run_time,
            stream="enfo",
            type="cf",  # Control forecast (member 51)
            step=step,
            param=["10u", "10v"],
            target=filepath
        )

        # Verify file was created
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            if verbose:
                print(f" ({file_size / 1024 / 1024:.1f} MB)")
            return {
                'success': True,
                'step': step,
                'type': 'cf',
                'filepath': filepath,
                'file_size': file_size,
                'skipped': False
            }
        else:
            if verbose:
                print(" File not created")
            return {
                'success': False,
                'step': step,
                'type': 'cf',
                'filepath': None,
                'error': 'File not created',
                'skipped': False
            }

    except Exception as e:
        if verbose:
            print(f" Error: {e}")
        return {
            'success': False,
            'step': step,
            'type': 'cf',
            'filepath': None,
            'error': str(e),
            'skipped': False
        }


def download_ensemble_wind(
        date: Union[str, datetime],
        run_time: int,
        forecast_hours: Optional[List[int]] = None,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        verbose: bool = True,
        max_workers: int = 4
) -> Dict[str, Union[str, int, bool]]:
    """
    Download ensemble 10m wind forecasts from ECMWF.

    Downloads u10 and v10 wind components for both perturbed ensemble members (1-50, PF files)
    and control forecast (member 51, CF files) at specified forecast lead times.
    Supports concurrent downloads for improved performance.

    Args:
        date (str or datetime): Forecast date in 'YYYY-MM-DD' format or datetime object
        run_time (int): Model run time (0, 6, 12, or 18 UTC)
        forecast_hours (List[int], optional): Specific forecast hours to download.
            If None, downloads all available hours for the run_time.
        output_dir (str): Output directory for downloaded files
        verbose (bool): Whether to print detailed progress information
        max_workers (int): Maximum number of concurrent downloads (default: 4).
                          Set to 1 for sequential downloads (original behavior).

    Returns:
        dict: Summary with 'success', 'files_downloaded', 'files_failed'
    """
    # Parse date
    if isinstance(date, str):
        forecast_date = datetime.strptime(date, '%Y-%m-%d')
    else:
        forecast_date = date

    # Validate run time
    if run_time not in [0, 6, 12, 18]:
        raise ValueError(f"Invalid run_time: {run_time}. Must be 0, 6, 12, or 18")

    # Get forecast hours
    if forecast_hours is None:
        forecast_hours = get_forecast_steps(run_time)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Initialize ECMWF client
    client = Client(source="ecmwf")

    if verbose:
        print("=" * 70)
        if max_workers > 1:
            print("ECMWF ENSEMBLE WIND FORECAST DOWNLOAD (CONCURRENT)")
        else:
            print("ECMWF ENSEMBLE WIND FORECAST DOWNLOAD")
        print("=" * 70)
        print(f"Date: {forecast_date.strftime('%Y-%m-%d')}")
        print(f"Run: {run_time:02d}Z")
        print(f"Forecast hours: {len(forecast_hours)} steps ({min(forecast_hours)}-{max(forecast_hours)}h)")
        if max_workers > 1:
            print(f"Concurrent workers: {max_workers}")
        print(f"Output: {output_dir}/")
        print("=" * 70)

    downloaded_files = []
    failed_downloads = 0

    # Use concurrent downloads if max_workers > 1, otherwise sequential
    if max_workers > 1:
        # Concurrent download mode
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all PF and CF download tasks
            futures = []
            for step in forecast_hours:
                futures.append(
                    executor.submit(_download_single_step_pf, client, forecast_date, run_time, step, output_dir,
                                    verbose))
                futures.append(
                    executor.submit(_download_single_step_cf, client, forecast_date, run_time, step, output_dir,
                                    verbose))

            # Process results as they complete
            for future in as_completed(futures):
                result = future.result()
                if result['success']:
                    if result['filepath']:
                        downloaded_files.append(result['filepath'])
                else:
                    failed_downloads += 1
    else:
        # Sequential download mode (original behavior)
        for step in forecast_hours:
            # Download PF file
            result_pf = _download_single_step_pf(client, forecast_date, run_time, step, output_dir, verbose)
            if result_pf['success'] and result_pf['filepath']:
                downloaded_files.append(result_pf['filepath'])
            elif not result_pf['success']:
                failed_downloads += 1

            # Download CF file
            result_cf = _download_single_step_cf(client, forecast_date, run_time, step, output_dir, verbose)
            if result_cf['success'] and result_cf['filepath']:
                downloaded_files.append(result_cf['filepath'])
            elif not result_cf['success']:
                failed_downloads += 1

    # Summary
    if verbose:
        print("=" * 70)
        print("DOWNLOAD SUMMARY")
        print("=" * 70)
        print(f"Successfully downloaded: {len(downloaded_files)} files")
        print(f"Failed downloads: {failed_downloads} files")
        print(f"Files saved to: {output_dir}/")
        print("=" * 70)

    return {
        'success': failed_downloads == 0,
        'files_downloaded': len(downloaded_files),
        'files_failed': failed_downloads,
        'output_dir': output_dir,
        'downloaded_files': downloaded_files
    }


def download_multiple_forecasts(
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        run_times: Optional[List[int]] = None,
        forecast_hours: Optional[List[int]] = None,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        verbose: bool = True,
        max_workers: int = 4
) -> Dict[str, int]:
    """
    Download multiple ensemble wind forecasts.

    Args:
        start_date (str or datetime): Start date in 'YYYY-MM-DD' format
        end_date (str or datetime, optional): End date. If None, only download start_date
        run_times (List[int], optional): Run times to download (default: [0, 12])
        forecast_hours (List[int], optional): Specific forecast hours to download
        output_dir (str): Output directory for downloaded files
        verbose (bool): Whether to print detailed progress information
        max_workers (int): Maximum number of concurrent downloads (default: 4).
                          Set to 1 for sequential downloads (original behavior).

    Returns:
        dict: Summary with 'total_downloaded', 'total_failed'
    """
    # Parse dates
    if isinstance(start_date, str):
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    else:
        start_dt = start_date

    if end_date is None:
        end_dt = start_dt
    elif isinstance(end_date, str):
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        end_dt = end_date

    # Default run times
    if run_times is None:
        run_times = [0, 12]  # Main runs with longest forecast horizon

    # Generate list of dates
    date_list = []
    current_date = start_dt
    while current_date <= end_dt:
        date_list.append(current_date)
        current_date += timedelta(days=1)

    if verbose:
        print(f"Downloading forecasts for {len(date_list)} date(s) and {len(run_times)} run time(s)")

    total_downloaded = 0
    total_failed = 0

    for forecast_date in date_list:
        for run_time in run_times:
            result = download_ensemble_wind(
                date=forecast_date,
                run_time=run_time,
                forecast_hours=forecast_hours,
                output_dir=output_dir,
                verbose=verbose,
                max_workers=max_workers
            )

            total_downloaded += result['files_downloaded']
            total_failed += result['files_failed']

    if verbose:
        print("\n" + "=" * 70)
        print("OVERALL SUMMARY")
        print("=" * 70)
        print(f"Total files downloaded: {total_downloaded}")
        print(f"Total files failed: {total_failed}")
        print("=" * 70)

    return {
        'total_downloaded': total_downloaded,
        'total_failed': total_failed
    }


def list_downloaded_files(output_dir: str = DEFAULT_OUTPUT_DIR) -> List[str]:
    """
    List all downloaded wind GRIB files in the output directory.

    Args:
        output_dir (str): Directory containing downloaded files

    Returns:
        List[str]: List of downloaded file paths
    """
    output_path = Path(output_dir)

    if not output_path.exists():
        return []

    grib_files = sorted(output_path.glob("wind_ens_*.grib2"))
    return [str(f) for f in grib_files]


def get_file_info(filepath: str) -> Dict[str, Union[str, int]]:
    """
    Extract information from wind GRIB filename.

    Expected format: wind_ens_YYYY-MM-DD_rHH_fHHHh.grib2
    Example: wind_ens_2025-10-12_r00_f024h.grib2

    Args:
        filepath (str): Path to wind GRIB file

    Returns:
        dict: File information with keys:
            - date: Forecast date
            - run_time: Run time (0, 6, 12, 18)
            - forecast_hour: Forecast lead time in hours
            - filename: Original filename
    """
    import re

    filename = os.path.basename(filepath)

    # Parse filename
    pattern = r'wind_ens_(\d{4}-\d{2}-\d{2})_r(\d{2})_f(\d{3})h\.grib2'
    match = re.match(pattern, filename)

    if match:
        date_str, run_str, hour_str = match.groups()
        return {
            'date': date_str,
            'run_time': int(run_str),
            'forecast_hour': int(hour_str),
            'filename': filename,
            'filepath': filepath
        }

    return {}