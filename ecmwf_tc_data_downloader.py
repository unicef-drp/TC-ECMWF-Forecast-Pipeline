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
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from typing import List, Optional, Dict

# Configuration
BASE_URL = "https://essential.ecmwf.int/"
DEFAULT_OUTPUT_DIR = "tc_data"

def get_available_dates(limit: Optional[int] = None) -> List[str]:
    """
    Get available forecast dates from ECMWF DISS system.
    
    Args:
        limit (int, optional): Maximum number of dates to return
        
    Returns:
        List[str]: List of available forecast dates in YYYYMMDDHHMMSS format
        
    Reference:
        ECMWF Dissemination System: https://essential.ecmwf.int/
    """
    
    try:
        response = requests.get(BASE_URL, timeout=30)
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

def get_tc_files(forecast_time: str, storm_name: Optional[str] = None) -> List[str]:
    """
    Get tropical cyclone track files for a specific forecast time.
    
    Args:
        forecast_time (str): Forecast time in YYYYMMDDHHMMSS format
        storm_name (str, optional): Specific storm name to filter
        
    Returns:
        List[str]: List of tropical cyclone track filenames
        
    Reference:
        ECMWF DISS file naming convention for tropical cyclone tracks:
        A_JSXX02ECEP{MM}{DD}{HH}00_C_ECMP_{YYYYMMDDHHMMSS}_tropical_cyclone_track_{STORM}_{LON}_{LAT}_bufr4.bin
        https://confluence.ecmwf.int/display/DAC/File+naming+convention+and+format+for+real-time+data
    """
    forecast_url = urljoin(BASE_URL, f"/file/{forecast_time}/")
    
    try:
        response = requests.get(forecast_url, timeout=30)
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
        
        # Filter by storm name if specified
        if storm_name:
            tc_files = [f for f in tc_files if storm_name.upper() in f.upper()]
        
        print(f"   Found {len(tc_files)} tropical cyclone track files")
        return tc_files
        
    except requests.RequestException as e:
        print(f"   Error getting files: {e}")
        return []

def download_file(filename: str, forecast_time: str, output_dir: str) -> bool:
    """
    Download a single tropical cyclone track file.
    
    Args:
        filename (str): Name of the file to download
        forecast_time (str): Forecast time directory
        output_dir (str): Local directory to save the file
        
    Returns:
        bool: True if download successful, False otherwise
    """
    file_url = urljoin(BASE_URL, f"/file/{forecast_time}/{filename}")
    local_path = os.path.join(output_dir, filename)
    
    try:
        response = requests.get(file_url, timeout=60)
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
    pattern = r'A_JSXX\d+ECEP\d+_C_ECMP_(\d{14})_tropical_cyclone_track_([A-Z0-9]+)_(?:([+-]?\d+(?:p\d+)?deg[EW])_(\d+p\d+deg[NS])_)?bufr4\.bin'
    
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
                    start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    output_dir: str = DEFAULT_OUTPUT_DIR) -> Dict[str, int]:
    """
    Download tropical cyclone track data from ECMWF DISS system.
    
    Args:
        limit (int): Number of latest forecasts to download (default: 1)
        storm_name (str, optional): Specific storm name to filter
        date (str, optional): Specific date to download (YYYYMMDD format)
        start_date (str, optional): Start date for range (YYYYMMDD format)
        end_date (str, optional): End date for range (YYYYMMDD format)
        output_dir (str): Output directory for downloaded files
        
    Returns:
        dict: Summary with 'downloaded' and 'failed' counts
        
    Example:
        # Download latest 1 forecast
        result = download_tc_data()
        
        # Download only KIKO storm data
        result = download_tc_data(storm_name="KIKO")
        
        # Download specific date range
        result = download_tc_data(start_date="20250909", end_date="20250910")
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Determine which dates to process
    if date:
        # Single specific date - find all forecast times for that date
        target_date = date
        all_dates = get_available_dates()
        target_dates = [d for d in all_dates if d.startswith(target_date)]
        if not target_dates:
            print(f"Error: No forecasts found for date {target_date}")
            return {'downloaded': 0, 'failed': 0}
    elif start_date and end_date:
        # Date range
        all_dates = get_available_dates()
        target_dates = [d for d in all_dates if start_date <= d[:8] <= end_date]
        if not target_dates:
            print(f"Error: No forecasts found in date range {start_date} to {end_date}")
            return {'downloaded': 0, 'failed': 0}
    else:
        # Latest N forecasts (default)
        target_dates = get_available_dates(limit)
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
        tc_files = get_tc_files(forecast_time, storm_name)
        
        if not tc_files:
            print(f"   Warning: No tropical cyclone files found for {forecast_time}")
            continue
        
        # Show storm information for first few files
        for i, filename in enumerate(tc_files[:3]):
            storm_info = parse_filename(filename)
            if storm_info:
                print(f"   Storm: {storm_info['storm_name']} at {storm_info['latitude']}, {storm_info['longitude']}")
        
        if len(tc_files) > 3:
            print(f"   ... and {len(tc_files)-3} more storms")
        
        # Download files
        for filename in tc_files:
            if download_file(filename, forecast_time, output_dir):
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
        tc_files = get_tc_files(forecast_time)
        
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
