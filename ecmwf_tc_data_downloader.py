#!/usr/bin/env python3
"""
ECMWF Tropical Cyclone Track Data Downloader

This module provides functions to download tropical cyclone track BUFR files 
from ECMWF's Dissemination (DISS) system.

References:
- ECMWF Dissemination System: https://essential.ecmwf.int/
- ECMWF File Naming Convention: https://confluence.ecmwf.int/display/DAC/File+naming+convention+and+format+for+real-time+data
- BUFR Format Documentation: https://confluence.ecmwf.int/display/ECC/BUFR+examples
"""

import os
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from typing import List, Optional, Dict

# Configuration
BASE_URL = "https://essential.ecmwf.int/"
DEFAULT_OUTPUT_DIR = "tc_data"

# Global session for connection pooling
_session = None


def get_session(pool_size: int = 10):
    """
    Get or create a requests session with proper connection pooling.
    
    This improves performance by reusing HTTP connections and automatically
    retrying failed requests with exponential backoff.
    
    Args:
        pool_size: Maximum number of connections in the pool (default: 10)
        
    Returns:
        requests.Session: Configured session with connection pooling and retry strategy
    """
    global _session
    
    if _session is None or pool_size > 10:
        # Create new session with larger pool if needed
        _session = requests.Session()
        
        # Configure retry strategy for transient failures
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on rate limit and server errors
        )
        
        # Configure HTTP adapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=retry_strategy
        )
        
        _session.mount("http://", adapter)
        _session.mount("https://", adapter)
    
    return _session


def get_available_dates(limit: Optional[int] = None, pool_size: int = 10) -> List[str]:
    """
    Get available forecast dates from ECMWF DISS system.

    Args:
        limit (int, optional): Maximum number of dates to return
        pool_size (int): Connection pool size for HTTP requests (default: 10)

    Returns:
        List[str]: List of available forecast dates in YYYYMMDDHHMMSS format

    Reference:
        ECMWF Dissemination System: https://essential.ecmwf.int/
    """

    try:
        session = get_session(pool_size)
        response = session.get(BASE_URL, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)

        # Extract date-like directories (14 digits: YYYYMMDDHHMMSS)
        date_pattern = re.compile(r'/file/(\d{14})/?$')
        available_dates = []

        for link in links:
            href = link.get('href', '')
            match = date_pattern.match(href)
            if match:
                available_dates.append(match.group(1))

        available_dates.sort(reverse=True)  # Most recent first

        if limit:
            available_dates = available_dates[:limit]

        print(f"Found {len(available_dates)} forecast dates")
        return available_dates

    except requests.RequestException as e:
        print(f"Error discovering dates: {e}")
        return []


def get_tc_files(forecast_time: str, storm_name: Optional[str] = None, named_storms_only: bool = True, pool_size: int = 10) -> List[str]:
    """
    Get tropical cyclone track files for a specific forecast time.

    Args:
        forecast_time (str): Forecast time in YYYYMMDDHHMMSS format
        storm_name (str, optional): Specific storm name to filter
        named_storms_only (bool): Only include storms with proper names (default: True)
        pool_size (int): Connection pool size for HTTP requests (default: 10)

    Returns:
        List[str]: List of tropical cyclone track filenames

    Reference:
        ECMWF DISS file naming convention for tropical cyclone tracks:
        A_JSXX02ECEP{MM}{DD}{HH}00_C_ECMP_{YYYYMMDDHHMMSS}_tropical_cyclone_track_{STORM}_{LON}_{LAT}_bufr4.bin
        https://confluence.ecmwf.int/display/DAC/File+naming+convention+and+format+for+real-time+data
    """
    forecast_url = urljoin(BASE_URL, f"/file/{forecast_time}/")

    try:
        session = get_session(pool_size)
        response = session.get(forecast_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)

        # Extract .bin files (BUFR format)
        bin_files = []
        for link in links:
            text = link.get_text().strip()
            if text and text.endswith('.bin'):
                bin_files.append(text)

        # Filter for tropical cyclone tracks
        tc_files_raw = [f for f in bin_files if 'tropical_cyclone_track' in f.lower()]

        tc_files = []
        for tc_file in tc_files_raw:
            if tc_file[10:12] == 'EP':
                tc_files.append(tc_file)

        # Filter by named storms only if requested
        if named_storms_only:
            named_tc_files = []
            for tc_file in tc_files:
                # Extract storm name from filename using regex
                # Format: ...tropical_cyclone_track_{STORM}_{LON}_{LAT}_bufr4.bin
                import re
                match = re.search(r'tropical_cyclone_track_([^_]+)_', tc_file)
                if match:
                    storm_part = match.group(1)
                    # Check if it's a proper name (not just numbers/letters like "70U", "73E")
                    if len(storm_part) > 2 and not (storm_part[:2].isdigit() and len(storm_part) == 3):
                        named_tc_files.append(tc_file)
            tc_files = named_tc_files

        # Filter by storm name if specified
        if storm_name:
            tc_files = [f for f in tc_files if storm_name.upper() in f.upper()]

        print(f"   Found {len(tc_files)} tropical cyclone track files")
        return tc_files

    except requests.RequestException as e:
        print(f"   Error getting files: {e}")
        return []


def download_file(filename: str, forecast_time: str, output_dir: str, pool_size: int = 10) -> bool:
    """
    Download a single tropical cyclone track file.

    Args:
        filename (str): Name of the file to download
        forecast_time (str): Forecast time directory
        output_dir (str): Local directory to save the file
        pool_size (int): Connection pool size for HTTP requests (default: 10)

    Returns:
        bool: True if download successful, False otherwise
    """
    file_url = urljoin(BASE_URL, f"/file/{forecast_time}/{filename}")
    local_path = os.path.join(output_dir, filename)

    try:
        session = get_session(pool_size)
        response = session.get(file_url, timeout=60)
        response.raise_for_status()

        # Save file
        with open(local_path, 'wb') as f:
            f.write(response.content)

        # Validate BUFR file
        with open(local_path, 'rb') as f:
            first_bytes = f.read(4)
            is_valid_bufr = first_bytes == b'BUFR'

        if is_valid_bufr:
            print(f"   Downloaded: {filename} ({len(response.content):,} bytes)")
            return True
        else:
            print(f"   Warning: {filename} (invalid BUFR format)")
            return False

    except requests.RequestException as e:
        print(f"   Error: {filename}: {e}")
        return False


def parse_filename(filename: str) -> Optional[Dict[str, str]]:
    """
    Parse filename to extract storm information.

    Args:
        filename (str): ECMWF BUFR filename

    Returns:
        dict: Parsed metadata or None if parsing fails

    Reference:
        ECMWF DISS filename format:
        A_JSXX02ECEP{MM}{DD}{HH}00_C_ECMP_{YYYYMMDDHHMMSS}_tropical_cyclone_track_{STORM}_{LON}_{LAT}_bufr4.bin
        https://confluence.ecmwf.int/display/DAC/File+naming+convention+and+format+for+real-time+data
    """
    pattern = r'A_JSXX\d+ECEP\d+_C_ECMP_(\d{14})_tropical_cyclone_track_([A-Z0-9-]+)_(?:([+-]?\d+(?:p\d+)?deg[EW])_(\d+p\d+deg[NS])_)?bufr4\.bin'

    match = re.match(pattern, filename)

    if match:
        forecast_time, storm_name, lon, lat = match.groups()
        return {
            'forecast_time': forecast_time,
            'storm_name': storm_name,
            'longitude': lon,
            'latitude': lat
        }
    return None


def download_tc_data(limit: int = 1,
                     storm_name: Optional[str] = None,
                     date: Optional[str] = None,
                     run_time: Optional[str] = None,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None,
                     output_dir: str = DEFAULT_OUTPUT_DIR,
                     named_storms_only: bool = True,
                     max_workers: int = 4) -> Dict[str, int]:
    """
    Download tropical cyclone track data from ECMWF DISS system.

    Args:
        limit (int): Number of latest forecasts to download (default: 1)
        storm_name (str, optional): Specific storm name to filter
        date (str, optional): Specific date to download (YYYYMMDD format)
        run_time (str, optional): Specific run time to download (00, 06, 12, 18)
        start_date (str, optional): Start date for range (YYYYMMDD format)
        end_date (str, optional): End date for range (YYYYMMDD format)
        output_dir (str): Output directory for downloaded files
        named_storms_only (bool): Only download storms with proper names (default: True)
        max_workers (int): Maximum concurrent connections for connection pooling (default: 4).
                          Used to size the HTTP connection pool. Higher values can improve
                          performance when downloading multiple files, but may be limited by
                          server rate limits.

    Returns:
        dict: Summary with 'downloaded' and 'failed' counts

    Example:
        # Download latest 1 forecast (named storms only)
        result = download_tc_data()

        # Download all storms including numbered ones
        result = download_tc_data(named_storms_only=False)

        # Download only KIKO storm data
        result = download_tc_data(storm_name="KIKO")

        # Download specific date and run time
        result = download_tc_data(date="20251015", run_time="12")

        # Download specific date range
        result = download_tc_data(start_date="20250909", end_date="20250910")
        
        # Download with optimized connection pooling
        result = download_tc_data(max_workers=10)
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Use max_workers to size the connection pool (capped at 10 to avoid overwhelming servers)
    pool_size = min(max_workers, 10)

    # Determine which dates to process
    if date:
        # Single specific date - find forecast times for that date
        target_date = date
        all_dates = get_available_dates(pool_size=pool_size)
        target_dates = [d for d in all_dates if d.startswith(target_date)]

        # Filter by run time if specified
        if run_time:
            # ECMWF format: YYYYMMDDHHMMSS, where HH is the run time
            run_time_hour = run_time.zfill(2)  # Ensure 2 digits
            target_dates = [d for d in target_dates if d[8:10] == run_time_hour]
            print(f"Filtering for run time: {run_time_hour}Z")

        if not target_dates:
            if run_time:
                print(f"Error: No forecasts found for date {target_date} at run time {run_time}Z")
            else:
                print(f"Error: No forecasts found for date {target_date}")
            return {'downloaded': 0, 'failed': 0}
    elif start_date and end_date:
        # Date range
        all_dates = get_available_dates(pool_size=pool_size)
        target_dates = [d for d in all_dates if start_date <= d[:8] <= end_date]
        if not target_dates:
            print(f"Error: No forecasts found in date range {start_date} to {end_date}")
            return {'downloaded': 0, 'failed': 0}
    else:
        # Latest N forecasts (default)
        target_dates = get_available_dates(limit, pool_size=pool_size)
        if not target_dates:
            print("Error: No forecast dates found")
            return {'downloaded': 0, 'failed': 0}

    print(f"Processing {len(target_dates)} forecast(s)")
    if storm_name:
        print(f"Filtering for storm: {storm_name}")
    print()

    # Download files
    total_downloaded = 0
    total_failed = 0

    for forecast_time in target_dates:
        tc_files = get_tc_files(forecast_time, storm_name, named_storms_only, pool_size=pool_size)

        if not tc_files:
            print(f"   Warning: No tropical cyclone files found for {forecast_time}")
            continue

        # Show storm information for first few files
        for i, filename in enumerate(tc_files[:3]):
            storm_info = parse_filename(filename)
            if storm_info:
                print(f"   Storm: {storm_info['storm_name']} at {storm_info['latitude']}, {storm_info['longitude']}")

        if len(tc_files) > 3:
            print(f"   ... and {len(tc_files) - 3} more storms")

        # Download files
        for filename in tc_files:
            if download_file(filename, forecast_time, output_dir, pool_size=pool_size):
                total_downloaded += 1
            else:
                total_failed += 1

        print()

    # Summary
    print("=" * 50)
    print(f"Summary:")
    print(f"   Successfully downloaded: {total_downloaded} files")
    print(f"   Failed downloads: {total_failed} files")
    print(f"   Files saved to: {output_dir}/")

    return {'downloaded': total_downloaded, 'failed': total_failed}


def list_available_storms(limit: int = 5) -> List[Dict[str, str]]:
    """
    List available storms from recent forecasts.

    Args:
        limit (int): Number of recent forecasts to check

    Returns:
        List[dict]: List of storm information dictionaries
    """

    available_dates = get_available_dates(limit)
    storms = []

    for forecast_time in available_dates:
        tc_files = get_tc_files(forecast_time, named_storms_only=True)

        for filename in tc_files:
            storm_info = parse_filename(filename)
            if storm_info:
                storm_info['forecast_time'] = forecast_time
                storms.append(storm_info)

    # Remove duplicates based on storm name
    unique_storms = []
    seen_storms = set()

    for storm in storms:
        if storm['storm_name'] not in seen_storms:
            unique_storms.append(storm)
            seen_storms.add(storm['storm_name'])

    print(f"Found {len(unique_storms)} unique storms")
    return unique_storms
