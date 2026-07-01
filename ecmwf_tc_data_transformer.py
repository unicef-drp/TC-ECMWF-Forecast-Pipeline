#!/usr/bin/env python3
"""
ECMWF Tropical Cyclone Track Data Transformer

This module provides functions to transform raw tropical cyclone track data 
extracted from ECMWF BUFR files into standardized format.

The transformer processes:
- Raw CSV output from ecmwf_tc_data_extractor module
- Converts from long format (many rows) to wide format (one row per forecast point)
- Standardizes column names and units
- Adds data quality checks and validation
- Handles wind radii data conversion
- Creates individual wind field polygons

Data Structure:
- Input: Raw CSV with duplicated rows for wind radii data
- Output: One CSV file:
  - <base>_transformed.csv: Individual forecast points with wind field polygons
- Wind radii converted from long to wide format (quadrants as columns)
- Units standardized (km, knots, hPa)

References:
- ECMWF BUFR Format: https://confluence.ecmwf.int/display/ECC/BUFR+examples
- Tropical Cyclone Data: https://essential.ecmwf.int
"""

import math
import os
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime
import re

import numpy as np
import pandas as pd
from shapely.geometry import Polygon, Point

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configuration
DEFAULT_OUTPUT_DIR = "tc_data"
DEFAULT_CSV_SUFFIX = "_transformed.csv"


def validate_raw_data(df: pd.DataFrame) -> bool:
    """
    Validate raw extractor output has expected columns.

    Args:
        df (pd.DataFrame): Raw dataframe from extractor

    Returns:
        bool: True if valid, raises ValueError if not
    """
    required_columns = [
        'storm_id', 'ensemble_member', 'step', 'datetime',
        'latitude', 'longitude', 'pressure', 'wlatitude', 'wlongitude', 'wind'
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Check for data quality issues
    if df.empty:
        raise ValueError("Input dataframe is empty")

    if df['latitude'].isna().all():
        raise ValueError("All latitude values are missing")

    if df['longitude'].isna().all():
        raise ValueError("All longitude values are missing")

    # Check wind location data quality (needed for RMW calculation)
    if df['wlatitude'].isna().all():
        raise ValueError("All wind latitude values are missing")

    if df['wlongitude'].isna().all():
        raise ValueError("All wind longitude values are missing")

    print(f" Validation passed: {len(df)} rows, {len(df.columns)} columns")
    return True


def calculate_forecast_time(df: pd.DataFrame) -> pd.Timestamp:
    """
    Calculate forecast issuance time from the first timestep (step=0).

    The forecast_time is when the forecast was made, which equals
    the valid_time of the analysis (step=0). This includes both date AND time
    since ECMWF issues 4 forecasts per day (00Z, 06Z, 12Z, 18Z).

    Args:
        df (pd.DataFrame): Raw dataframe from extractor

    Returns:
        pd.Timestamp: Forecast issuance time
    """
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Get the earliest valid time (should be step=0)
    # This preserves the full timestamp including hours
    forecast_time = df[df['step'] == 0]['datetime'].min()

    if pd.isna(forecast_time):
        # Fallback: calculate from minimum valid time and step
        first_row = df.iloc[0]
        forecast_time = first_row['datetime'] - pd.Timedelta(hours=int(first_row['step']))

    print(f"  Forecast issued at: {forecast_time}")
    return forecast_time


def convert_wind_radii_wide(wind_radii_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert wind radii from long format to wide format.

    Input (long format):
        storm_id, member, step, wind_threshold, quadrant, radius
        STORM,    0,      0,    18,             1,        45000
        STORM,    0,      0,    18,             2,        38000
        STORM,    0,      0,    18,             3,        42000
        STORM,    0,      0,    18,             4,        40000

    Output (wide format):
        storm_id, member, step, radius_34_knot_winds_ne_km, radius_34_knot_winds_se_km, ...
        STORM,    0,      0,    45.0,                       38.0,                        ...

    Quadrant mapping:
        1 = NE (0-90 degrees)
        2 = SE (90-180 degrees)
        3 = SW (180-270 degrees)
        4 = NW (270-360 degrees)

    Wind threshold mapping (m/s to knots):
        18 m/s = 34 knots
        26 m/s = 50 knots
        33 m/s = 64 knots

    Args:
        wind_radii_df (pd.DataFrame): Wind radii data in long format

    Returns:
        pd.DataFrame: Wind radii data in wide format
    """
    if wind_radii_df.empty:
        print("  No wind radii data to convert")
        return pd.DataFrame()

    # Map wind thresholds (m/s) to knots
    threshold_map = {
        18: 34,
        26: 50,
        33: 64
    }

    # Map quadrants to direction abbreviations
    quadrant_map = {
        1: 'ne',
        2: 'se',
        3: 'sw',
        4: 'nw'
    }

    # Create wide format columns
    wind_radii_df['threshold_knots'] = wind_radii_df['wind_threshold'].map(threshold_map)
    wind_radii_df['direction'] = wind_radii_df['quadrant'].map(quadrant_map)
    wind_radii_df['column_name'] = (
            'radius_' +
            wind_radii_df['threshold_knots'].astype(str) +
            '_knot_winds_' +
            wind_radii_df['direction'] +
            '_km'
    )

    # Convert radius from meters to kilometers
    wind_radii_df['radius_km'] = wind_radii_df['wind_radius'] / 1000.0

    # Pivot to wide format
    wide_df = wind_radii_df.pivot_table(
        index=['storm_id', 'ensemble_member', 'step'],
        columns='column_name',
        values='radius_km',
        aggfunc='first'  # Should only be one value per combination
    ).reset_index()

    # Ensure all expected columns exist (fill missing with NaN)
    expected_columns = []
    for threshold in [34, 50, 64]:
        for direction in ['ne', 'se', 'sw', 'nw']:
            col = f'radius_{threshold}_knot_winds_{direction}_km'
            expected_columns.append(col)
            if col not in wide_df.columns:
                wide_df[col] = np.nan

    return wide_df


def wind_quadrant_polygon(lat: float, lon: float, r_ne: float, r_se: float, r_sw: float, r_nw: float) -> Optional[Polygon]:
    """
    Create a rectangular wind region polygon from quadrant radii (in km).
    Returns a Shapely Polygon in lon/lat degrees.

    Args:
        lat: Hurricane center latitude
        lon: Hurricane center longitude
        r_ne: Northeast quadrant radius in km
        r_se: Southeast quadrant radius in km
        r_sw: Southwest quadrant radius in km
        r_nw: Northwest quadrant radius in km

    Returns:
        Shapely Polygon or None if invalid
    """
    # Check if any radius is non-zero
    if not any([r_ne, r_se, r_sw, r_nw]):
        return None

    # Convert km to degrees — latitude scaling is constant; longitude shrinks with cos(lat)
    lat_deg_per_km = 1.0 / 111.0
    lon_deg_per_km = 1.0 / (111.0 * math.cos(math.radians(lat))) if lat != 90.0 else lat_deg_per_km

    # Calculate polygon bounds
    lat_min = lat - max(r_sw, r_se) * lat_deg_per_km
    lat_max = lat + max(r_ne, r_nw) * lat_deg_per_km
    lon_min = lon - max(r_sw, r_nw) * lon_deg_per_km
    lon_max = lon + max(r_ne, r_se) * lon_deg_per_km

    # Create rectangular polygon (ensure it's properly closed)
    polygon_coords = [
        (lon_min, lat_min),  # SW corner
        (lon_max, lat_min),  # SE corner
        (lon_max, lat_max),  # NE corner
        (lon_min, lat_max),  # NW corner
        (lon_min, lat_min)  # Close polygon
    ]

    # Ensure the polygon is valid
    try:
        polygon = Polygon(polygon_coords)
        if not polygon.is_valid:
            return None
    except Exception:
        return None

    return polygon if polygon.is_valid else None


def create_wind_field_polygons(forecasts_df: pd.DataFrame, wind_threshold: int = 64) -> pd.DataFrame:
    """
    Create wind field polygons for each forecast point.

    Args:
        forecasts_df: DataFrame with forecast data and wind radii
        wind_threshold: Wind speed threshold in knots (34, 50, 64)

    Returns:
        DataFrame with added wind field polygon columns
    """
    df = forecasts_df.copy()

    # Get wind radius columns for the specified threshold
    radius_cols = [
        f'radius_{wind_threshold}_knot_winds_ne_km',
        f'radius_{wind_threshold}_knot_winds_se_km',
        f'radius_{wind_threshold}_knot_winds_sw_km',
        f'radius_{wind_threshold}_knot_winds_nw_km'
    ]

    # Check if wind radius data exists
    if not all(col in df.columns for col in radius_cols):
        print(f"  Warning: Wind radius data for {wind_threshold} kt not found")
        df[f'wind_field_polygon_{wind_threshold}kt'] = None
        return df

    # Create wind field polygons using apply (avoids iterrows overhead)
    def _make_polygon(row):
        _z = lambda v: 0.0 if pd.isna(v) else v
        r_ne = _z(row[radius_cols[0]])
        r_se = _z(row[radius_cols[1]])
        r_sw = _z(row[radius_cols[2]])
        r_nw = _z(row[radius_cols[3]])
        if any([r_ne, r_se, r_sw, r_nw]):
            return wind_quadrant_polygon(row['latitude'], row['longitude'], r_ne, r_se, r_sw, r_nw)
        return None

    df[f'wind_field_polygon_{wind_threshold}kt'] = df.apply(_make_polygon, axis=1)

    return df


def polygon_to_wkt(polygon) -> Optional[str]:
    """
    Convert Shapely polygon to WKT format for database storage.

    Args:
        polygon: Shapely Polygon or None

    Returns:
        WKT string or None
    """
    if polygon is None:
        return None

    try:
        if hasattr(polygon, 'wkt'):
            return polygon.wkt
        else:
            return None
    except Exception:
        return None


def convert_polygons_to_wkt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all polygon columns to WKT format for database storage.

    Args:
        df: DataFrame with polygon columns

    Returns:
        DataFrame with WKT string columns
    """
    df = df.copy()

    # Find all polygon columns
    polygon_cols = [col for col in df.columns if 'polygon' in col.lower() or 'region' in col.lower()]

    for col in polygon_cols:
        if col in df.columns:
            # Convert polygons to WKT strings
            df[col] = df[col].apply(polygon_to_wkt)

    return df


def calculate_rmw(forecasts_df: pd.DataFrame) -> pd.Series:
    """
    Calculate radius of maximum winds from storm center to max wind location.

    Uses vectorized Haversine formula to calculate great circle distance between:
    - Storm center: (latitude, longitude)
    - Maximum wind location: (wlatitude, wlongitude)

    Args:
        forecasts_df (pd.DataFrame): DataFrame with lat/lon and wlat/wlon columns

    Returns:
        pd.Series: Radius of maximum winds in kilometers
    """
    R = 6371.0

    lat1 = np.radians(forecasts_df['latitude'].values.astype(float))
    lon1 = np.radians(forecasts_df['longitude'].values.astype(float))
    lat2 = np.radians(forecasts_df['wlatitude'].values.astype(float))
    lon2 = np.radians(forecasts_df['wlongitude'].values.astype(float))

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    distances = R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    # Propagate NaN for any row where inputs were NaN
    nan_mask = (
        forecasts_df['latitude'].isna() | forecasts_df['longitude'].isna() |
        forecasts_df['wlatitude'].isna() | forecasts_df['wlongitude'].isna()
    )
    distances = np.where(nan_mask, np.nan, distances)

    return pd.Series(distances, index=forecasts_df.index)


def transform_tc_data(raw_csv_path: str,
                      output_base_path: Optional[str] = None,
                      storm_name: Optional[str] = None,
                      verbose: bool = True) -> Dict[str, Union[str, int, bool]]:
    """
    Transform raw tropical cyclone data to standardized format.

    This is the main function for processing raw CSV data from the extractor.
    It handles the complete transformation pipeline from raw data to standardized format.

    Converts raw ECMWF extractor output to standardized format:
    - One row per forecast point
    - Clear column names
    - Wind radii in wide format
    - Proper units (km, knots, hPa)
    - Quality checks
    - Individual wind field polygons

    Args:
        raw_csv_path (str): Path to raw CSV from extractor
        output_base_path (str, optional): Base path for output file (without suffix)
            Will create: <base>_transformed.csv
            Example: output_base_path='tc_data/HUMBERTO' creates:
                - tc_data/HUMBERTO_transformed.csv
        storm_name (str, optional): Override storm ID with specific storm name
        verbose (bool): Whether to print detailed progress information

    Returns:
        dict: Summary dictionary with keys:
            - success (bool): Whether transformation was successful
            - csv_file (str): Path to saved main CSV file (None if failed)
            - records (int): Total number of forecast records transformed
    """
    if verbose:
        print(f"Transforming: {raw_csv_path}")

    try:
        # Read raw data
        raw_df = pd.read_csv(raw_csv_path)
        if verbose:
            print(f"  Loaded {len(raw_df)} rows")

        # Validate
        validate_raw_data(raw_df)

        # Calculate forecast time
        forecast_time = calculate_forecast_time(raw_df)

        # Split into forecast data and wind radii data
        # Core forecast columns (appear once per forecast point)
        forecast_cols = [
            'storm_id', 'ensemble_member', 'step', 'datetime',
            'latitude', 'longitude', 'pressure', 'wlatitude', 'wlongitude', 'wind'
        ]

        # Get unique forecast points (deduplicate)
        forecasts_df = raw_df[forecast_cols].drop_duplicates()
        if verbose:
            print(f"  Found {len(forecasts_df)} unique forecast points")

        # Convert wind radii to wide format
        if 'wind_radius' in raw_df.columns:
            wind_cols = [
                'storm_id', 'ensemble_member', 'step',
                'wind_threshold', 'quadrant', 'wind_radius'
            ]
            # Include rows with wind_radius data (including 0.0 values, but exclude NaN)
            wind_radii_df = raw_df[wind_cols].dropna(subset=['wind_radius'])

            if not wind_radii_df.empty:
                if verbose:
                    print(f"  Converting {len(wind_radii_df)} wind radii records to wide format")
                wind_wide_df = convert_wind_radii_wide(wind_radii_df)

                # Merge wind radii with forecasts
                forecasts_df = forecasts_df.merge(
                    wind_wide_df,
                    on=['storm_id', 'ensemble_member', 'step'],
                    how='left'
                )

        # Rename columns to standard format
        forecasts_df = forecasts_df.rename(columns={
            'storm_id': 'track_id',
            'step': 'lead_time',
            'datetime': 'valid_time',
            'pressure': 'pressure_hpa',  # Assuming hPa from ECMWF
            'wind': 'wind_speed_ms'
        })

        # Optionally overwrite track_id with extracted name
        if storm_name:
            if verbose:
                print(f"  Overriding track_id with extracted storm name: {storm_name}")
            forecasts_df['track_id'] = storm_name

        # Add forecast_time column
        forecasts_df['forecast_time'] = forecast_time

        # Ensure forecast_time is properly formatted as datetime
        forecasts_df['forecast_time'] = pd.to_datetime(forecasts_df['forecast_time'])

        # Convert units
        # Pressure: Pa to hPa (if needed)
        if 'pressure_hpa' in forecasts_df.columns:
            # Check if values are in Pa (>10000) or hPa (<2000)
            if forecasts_df['pressure_hpa'].mean() > 10000:
                forecasts_df['pressure_hpa'] = forecasts_df['pressure_hpa'] / 100.0
                if verbose:
                    print("  Converted pressure from Pa to hPa")

        # Wind speed: m/s to knots
        if 'wind_speed_ms' in forecasts_df.columns:
            forecasts_df['wind_speed_knots'] = forecasts_df['wind_speed_ms'] * 1.944
            if verbose:
                print("  Converted wind speed from m/s to knots")

        # Calculate radius of maximum winds using Haversine formula
        if verbose:
            print("  Calculating radius of maximum winds")
        forecasts_df['radius_of_maximum_winds_km'] = calculate_rmw(forecasts_df)

        # Create wind field polygons for available thresholds
        available_thresholds = [34, 50, 64]  # Common hurricane wind thresholds
        for threshold in available_thresholds:
            # Check if wind radius data exists for this threshold
            threshold_cols = [
                f'radius_{threshold}_knot_winds_ne_km',
                f'radius_{threshold}_knot_winds_se_km',
                f'radius_{threshold}_knot_winds_sw_km',
                f'radius_{threshold}_knot_winds_nw_km'
            ]

            if all(col in forecasts_df.columns for col in threshold_cols):
                if verbose:
                    print(f"  Creating wind field polygons for {threshold} kt winds")

                # Create individual wind field polygons
                forecasts_df = create_wind_field_polygons(forecasts_df, threshold)

            else:
                if verbose:
                    print(f"  Skipping {threshold} kt winds - no radius data available")

        # Convert polygons to WKT format for database storage
        if verbose:
            print("  Converting polygons to WKT format")
        forecasts_df = convert_polygons_to_wkt(forecasts_df)

        # Reorder columns to match standard format
        standard_columns = [
            'forecast_time',
            'track_id',
            'ensemble_member',
            'valid_time',
            'lead_time',
            'latitude',
            'longitude',
            'pressure_hpa',
            'wind_speed_knots',
            'radius_of_maximum_winds_km',
            # Wind radii columns (34, 50, 64 knots × 4 quadrants)
            'radius_34_knot_winds_ne_km',
            'radius_34_knot_winds_se_km',
            'radius_34_knot_winds_sw_km',
            'radius_34_knot_winds_nw_km',
            'radius_50_knot_winds_ne_km',
            'radius_50_knot_winds_se_km',
            'radius_50_knot_winds_sw_km',
            'radius_50_knot_winds_nw_km',
            'radius_64_knot_winds_ne_km',
            'radius_64_knot_winds_se_km',
            'radius_64_knot_winds_sw_km',
            'radius_64_knot_winds_nw_km',
            # Wind field polygon columns
            'wind_field_polygon_34kt',
            'wind_field_polygon_50kt',
            'wind_field_polygon_64kt'
        ]

        # Select columns (add missing ones as NaN)
        for col in standard_columns:
            if col not in forecasts_df.columns:
                forecasts_df[col] = np.nan

        result_df = forecasts_df[standard_columns]

        # Save file (if output path provided)
        main_csv = None

        if output_base_path:
            # Generate output filename
            main_csv = f"{output_base_path}_transformed.csv"

            # Save main forecast CSV
            result_df.to_csv(main_csv, index=False, date_format='%Y-%m-%d %H:%M:%S')
            if verbose:
                print(f"  Saved {len(result_df)} forecast records to {main_csv}")

        # Data quality summary
        if verbose:
            print(f"\n  Transformation Summary:")
            print(f"    Input rows:  {len(raw_df)}")
            print(f"    Output rows: {len(result_df)} (deduplication: {len(raw_df) - len(result_df)} rows removed)")
            print(f"    Storms: {result_df['track_id'].nunique()}")
            print(f"    Ensemble members: {result_df['ensemble_member'].nunique()}")
            print(f"    Time steps: {result_df['lead_time'].nunique()}")
            print(f"    Date range: {result_df['valid_time'].min()} to {result_df['valid_time'].max()}")

        # Check for data quality issues
        null_counts = result_df.isnull().sum()
        critical_nulls = null_counts[['latitude', 'longitude', 'wind_speed_knots', 'pressure_hpa']]
        if critical_nulls.any():
            print(f"Missing critical data:")
            for col, count in critical_nulls[critical_nulls > 0].items():
                print(f"    {col}: {count} missing values")

        return {
            'success': True,
            'csv_file': main_csv,
            'records': len(result_df)
        }

    except Exception as e:
        if verbose:
            print(f"Error transforming {raw_csv_path}: {e}")
        return {
            'success': False,
            'csv_file': None,
            'records': 0
        }


def transform_all_in_directory(input_dir: str,
                               output_dir: str = "tc_data_transformed",
                               verbose: bool = True) -> Dict[str, int]:
    """
    Transform all CSV files in a directory.

    Args:
        input_dir (str): Directory containing raw CSVs (required)
        output_dir (str): Directory to save transformed CSVs (default: "tc_data_transformed")
        verbose (bool): Whether to print detailed progress information (default: True)

    Returns:
        dict: Summary with 'transformed' and 'failed' counts
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Find all CSV files
    csv_files = [f for f in input_path.glob("*.csv") if f.name != "SUMMARY.csv"]

    if not csv_files:
        if verbose:
            print(f"No CSV files found in {input_dir}")
        return {'transformed': 0, 'failed': 0}

    if verbose:
        print(f"Found {len(csv_files)} CSV files to transform")

    transformed_files = []
    total_transformed = 0
    total_failed = 0

    for csv_file in csv_files:
        try:
            # Generate output base (no extension) — transform_tc_data appends _transformed.csv
            output_base = output_path / csv_file.stem

            # Transform
            result = transform_tc_data(str(csv_file), str(output_base), verbose=False)
            if result['success']:
                transformed_files.append(Path(f"{output_base}_transformed.csv"))
                total_transformed += 1
            else:
                total_failed += 1

        except Exception as e:
            if verbose:
                print(f"Error transforming {csv_file.name}: {e}")
            total_failed += 1

    # Summary
    if verbose:
        print("=" * 50)
        print(f"Transformation Complete")
        print("=" * 50)
        print(f"Successfully transformed: {total_transformed} files")
        print(f"Failed transformations: {total_failed} files")
        print(f"Output directory: {output_dir}")

    return {'transformed': total_transformed, 'failed': total_failed}


def transform_tc_data_from_file(filename: str,
                                output_dir: Optional[str] = None,
                                verbose: bool = True) -> Dict[str, Union[str, int, bool]]:
    """
    Transform tropical cyclone data from a raw CSV file and save results.

    This is a convenience function for processing raw CSV files from the extractor.
    It handles the complete transformation pipeline from raw CSV to standardized format.

    Args:
        filename (str): Path to the raw CSV file from extractor
        output_dir (str, optional): Output directory for saved files (default: same as input file)
        verbose (bool): Whether to print detailed progress information (default: True)

    Returns:
        dict: Summary dictionary with keys:
            - success (bool): Whether transformation was successful
            - csv_file (str): Path to saved main CSV file (None if failed)
            - records (int): Total number of records transformed
    """
    # Determine output directory - use same directory as input file if not specified
    if output_dir is None:
        output_dir = os.path.dirname(filename)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    if verbose:
        print(f"Transforming tropical cyclone data from: {filename}")

    # Generate output filename
    base_name = os.path.splitext(os.path.basename(filename))[0]
    base_path = os.path.join(output_dir, base_name)

    # Try to extract storm name from filename
    match = re.search(r'tropical_cyclone_track_([A-Z0-9-]+)', base_name)
    storm_name = match.group(1) if match else None

    # Transform data
    result = transform_tc_data(filename, base_path, storm_name=storm_name, verbose=verbose)

    if result['success']:
        if verbose:
            print("=" * 50)
            print(f"Summary:")
            print(f"   Successfully transformed: {result['records']} records")
            print(f"   Main CSV file: {result['csv_file']}")
            print("=" * 50)
    else:
        if verbose:
            print("Error: Failed to transform data from CSV file")

    return result