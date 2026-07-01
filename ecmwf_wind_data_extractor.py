#!/usr/bin/env python3
"""
ECMWF Ensemble Wind Data Extractor

This module provides functions to extract wind threshold polygons from ECMWF
ensemble wind GRIB files based on tropical cyclone track data.

The extractor processes:
- ECMWF ensemble wind forecasts (u10, v10 components)
- TC track data with buffered regions
- Multiple wind thresholds (34, 40, 50, 64, 83, 96, 113, 137 knots)
- Ensemble member matching (track member N → wind member N)

Output:
- Wind threshold contour polygons for each ensemble member
- Spatial statistics

References:
- ECMWF Wind Forecasts: https://www.ecmwf.int/en/forecasts/datasets/open-data
- TC Wind Thresholds: https://www.nhc.noaa.gov/aboutsshws.php
"""

import os
import logging
import warnings
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import unary_union

# Suppress warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_OUTPUT_DIR = "wind_extracted"
BUFFER_RADIUS_KM = 500  # Fixed buffer radius around tracks in km

# Wind thresholds in knots and m/s
WIND_THRESHOLDS = {
    34: 17.49,  # Tropical storm force
    40: 20.58,  # Strong tropical storm
    50: 25.72,  # Very strong tropical storm
    64: 32.92,  # Category 1 hurricane
    83: 42.70,  # Category 2 hurricane
    96: 49.39,  # Category 3 hurricane
    113: 58.12,  # Category 4 hurricane
    137: 70.48  # Category 5 hurricane
}


def load_track_data(csv_file: str, member_number: int, verbose: bool = True) -> pd.DataFrame:
    """
    Load tropical cyclone track data for a specific ensemble member.

    Args:
        csv_file (str): Path to transformed TC track CSV file
        member_number (int): Ensemble member number
        verbose (bool): Whether to print progress information

    Returns:
        pd.DataFrame: Track data for the specified member
    """
    df = pd.read_csv(csv_file)

    # Filter for specific member
    member_data = df[df['ensemble_member'] == member_number].copy()

    # Sort by valid_time
    member_data = member_data.sort_values('valid_time')

    if verbose:
        print(f"  Loaded {len(member_data)} track points for member {member_number}")
        if not member_data.empty:
            print(f"  Time range: {member_data['valid_time'].min()} to {member_data['valid_time'].max()}")

    return member_data


def create_buffered_track_polygon(track_data: pd.DataFrame, buffer_radius_km: float) -> Polygon:
    """
    Create a buffer polygon around tropical cyclone track.

    Args:
        track_data (pd.DataFrame): Track data with latitude/longitude
        buffer_radius_km (float): Buffer radius in kilometers

    Returns:
        Polygon: Buffered track polygon
    """
    lats = track_data['latitude'].values
    lons = track_data['longitude'].values

    # Vectorized: compute per-point buffer radius in degrees
    radius_deg_lat = buffer_radius_km / 111.0
    radius_deg_lon = buffer_radius_km / (111.0 * np.cos(np.radians(lats)))
    radius_degs = (radius_deg_lat + radius_deg_lon) / 2.0  # numpy array

    buffered_polygons = [
        Point(lon, lat).buffer(r)
        for lat, lon, r in zip(lats, lons, radius_degs)
    ]

    return unary_union(buffered_polygons)


def get_bounding_box(polygon: Polygon, buffer: float = 2.0) -> Dict[str, float]:
    """
    Get bounding box from polygon with buffer.

    Args:
        polygon (Polygon): Input polygon
        buffer (float): Buffer distance in degrees

    Returns:
        dict: Bounding box coordinates
    """
    bounds = polygon.bounds  # (minx, miny, maxx, maxy)
    return {
        'lon_min': bounds[0] - buffer,
        'lon_max': bounds[2] + buffer,
        'lat_min': bounds[1] - buffer,
        'lat_max': bounds[3] + buffer
    }


def load_wind_data(grib_file: str, member_number: int, bbox: Dict[str, float],
                   verbose: bool = True, indexpath: Optional[str] = None) -> xr.DataArray:
    """
    Load ensemble wind data for specific member and region.

    Args:
        grib_file (str): Path to wind GRIB file
        member_number (int): Ensemble member number
        bbox (dict): Bounding box coordinates
        verbose (bool): Whether to print progress information
        indexpath (str, optional): Directory path for GRIB index files (.idx).
                                   If provided, index files will be stored here instead
                                   of alongside the GRIB file. Useful for parallel processing
                                   to avoid concurrent index file conflicts.

    Returns:
        xr.DataArray: Wind speed data
    """
    # Open dataset with optional custom index path
    if indexpath:
        # Create process-specific index directory to avoid concurrent access conflicts
        # Each process gets its own index cache, preventing race conditions
        # while still benefiting from index file caching performance
        import multiprocessing
        import threading
        
        # Get unique identifier for this process/thread
        pid = os.getpid()
        thread_id = threading.get_ident()
        
        # Create process-specific subdirectory
        process_index_dir = os.path.join(indexpath, f"process_{pid}_thread_{thread_id}")
        os.makedirs(process_index_dir, exist_ok=True)
        
        # Build the index file path
        import pathlib
        grib_filename = pathlib.Path(grib_file).name
        index_file_path = os.path.join(process_index_dir, grib_filename + '.idx')
        
        # Open with process-specific index path
        ds = xr.open_dataset(
            grib_file, 
            engine='cfgrib',
            backend_kwargs={
                'indexpath': index_file_path,
                'errors': 'ignore'
            }
        )
    else:
        ds = xr.open_dataset(grib_file, engine='cfgrib')

    # Select specific ensemble member, mapping control member 51 to GRIB number 0
    try:
        # Extract u and v components (inside try so ds.close() fires on KeyError etc.)
        u10 = ds['u10']
        v10 = ds['v10']
        wind_speed = np.sqrt(u10 ** 2 + v10 ** 2)
        if 'number' in wind_speed.dims:
            desired_number = 0 if member_number == 51 else member_number
            if verbose:
                print(f"    Selecting wind ensemble member {member_number} (GRIB number {desired_number})")
            try:
                wind_speed = wind_speed.sel(number=desired_number)
            except Exception as e:
                logger.warning(f"Member {member_number} (GRIB {desired_number}) not found in wind data: {e}")
                return xr.DataArray()  # outer finally: ds.close() still fires
        else:
            if verbose:
                print(f"    NOTE: No ensemble dimension in wind data (likely control forecast)")

        # Subset to bounding box
        wind_region = wind_speed.sel(
            latitude=slice(bbox['lat_max'], bbox['lat_min']),
            longitude=slice(bbox['lon_min'], bbox['lon_max'])
        )

        if verbose:
            print(f"    Wind data shape: {wind_region.shape}")
            try:
                max_ms = float(wind_region.max())
                print(f"    Max wind: {max_ms:.1f} m/s ({max_ms / 0.5144:.1f} kt)")
            except Exception:
                pass

        wind_region.load()  # materialise into RAM before closing the file handle
        return wind_region
    finally:
        ds.close()


def load_wind_data_all_members(
    grib_file: str,
    bbox: Dict[str, float],
    indexpath: Optional[str] = None,
) -> xr.DataArray:
    """
    Load wind speed for ALL ensemble members from a GRIB file in a single open.

    Returns a DataArray with dims (number, latitude, longitude) for PF files,
    or (latitude, longitude) for CF files (control forecast, no number dim).
    Clipped to the given bounding box.
    """
    if indexpath:
        import pathlib
        index_file_path = os.path.join(indexpath, pathlib.Path(grib_file).name + '.idx')
        ds = xr.open_dataset(
            grib_file, engine='cfgrib',
            backend_kwargs={'indexpath': index_file_path, 'errors': 'ignore'},
        )
    else:
        ds = xr.open_dataset(grib_file, engine='cfgrib')

    try:
        wind_speed = np.sqrt(ds['u10'] ** 2 + ds['v10'] ** 2)
        wind_region = wind_speed.sel(
            latitude=slice(bbox['lat_max'], bbox['lat_min']),
            longitude=slice(bbox['lon_min'], bbox['lon_max']),
        )
        wind_region.load()  # materialise into RAM before closing the file handle
        return wind_region
    finally:
        ds.close()


def create_wind_threshold_contours(wind_data: xr.DataArray,
                                   thresholds: Dict[int, float],
                                   verbose: bool = True) -> Dict[int, Optional[Polygon]]:
    """
    Create contour polygons for each wind threshold.

    Args:
        wind_data (xr.DataArray): Wind speed data
        thresholds (dict): Wind thresholds (kt: m/s)
        verbose (bool): Whether to print progress information

    Returns:
        dict: Polygons for each threshold
    """
    contour_polygons = {}

    lons = wind_data.longitude.values
    lats = wind_data.latitude.values
    winds = wind_data.values

    # Create meshgrid
    lon_grid, lat_grid = np.meshgrid(lons, lats)

    # Sort thresholds by m/s value so matplotlib assigns allsegs[i] to levels[i]
    sorted_thresholds = sorted(thresholds.items(), key=lambda x: x[1])
    levels_ms = [ms for _, ms in sorted_thresholds]

    # One figure, one contour call for all thresholds
    fig, ax = plt.subplots()
    try:
        cs = ax.contour(lon_grid, lat_grid, winds, levels=levels_ms)
    except Exception as e:
        plt.close(fig)
        if verbose:
            print(f"  Error creating contours: {e}")
        return {kt: None for kt, _ in sorted_thresholds}
    plt.close(fig)

    for i, (threshold_kt, threshold_ms) in enumerate(sorted_thresholds):
        if verbose:
            print(f"    {threshold_kt} kt ({threshold_ms:.2f} m/s)...", end='', flush=True)

        polygons = []
        try:
            # cs.allsegs is the stable per-level segments API (confirmed matplotlib 3.11).
            # cs.collections was removed in matplotlib 3.9 and must not be used.
            # If allsegs is ever removed, fall back to contourpy (bundled with matplotlib).
            try:
                level_segs = cs.allsegs[i] if i < len(cs.allsegs) else []
            except AttributeError:
                from contourpy import contour_generator
                gen = contour_generator(x=lons, y=lats, z=winds)
                level_segs = gen.lines(threshold_ms) or []
            if len(level_segs) > 0:
                for segment in level_segs:
                    if len(segment) > 3:
                        try:
                            poly = Polygon(segment)
                            if poly.is_valid and poly.area > 0:
                                polygons.append(poly)
                        except Exception as e:
                            logger.debug(f"Skipping invalid polygon segment at {threshold_kt}kt: {e}")
                            continue
        except Exception as e:
            if verbose:
                print(f"  Error: {e}")
            contour_polygons[threshold_kt] = None
            continue

        if polygons:
            combined = unary_union(polygons)
            # unary_union can return GEOMETRYCOLLECTION when inputs share edges;
            # buffer(0) normalises it back to a (Multi)Polygon
            if combined.geom_type not in ('Polygon', 'MultiPolygon'):
                combined = combined.buffer(0)
            contour_polygons[threshold_kt] = combined
            if verbose:
                area_km2 = combined.area * 111 * 104
                print(f" {len(polygons)} contour(s), {area_km2:,.0f} km²")
        else:
            contour_polygons[threshold_kt] = None
            if verbose:
                print(" - No area")

    return contour_polygons


def polygon_to_wkt(polygon: Optional[Polygon]) -> Optional[str]:
    """
    Convert Shapely polygon to WKT format.

    Args:
        polygon (Polygon or None): Input polygon

    Returns:
        str or None: WKT string
    """
    if polygon is None:
        return None

    try:
        return polygon.wkt
    except Exception as e:
        logger.warning(f"Could not convert polygon to WKT: {e}")
        return None


def extract_wind_polygons_for_member(
        track_csv: str,
        wind_grib: str,
        member_number: int,
        buffer_radius_km: float = BUFFER_RADIUS_KM,
        thresholds: Dict[int, float] = WIND_THRESHOLDS,
        verbose: bool = True ) -> Dict[str, Union[Dict, pd.DataFrame, bool]]:
    """
    Extract wind threshold polygons for a single ensemble member.

    Args:
        track_csv (str): Path to transformed TC track CSV
        wind_grib (str): Path to wind GRIB file
        member_number (int): Ensemble member number
        buffer_radius_km (float): Buffer radius in km
        thresholds (dict): Wind thresholds to extract
        verbose (bool): Whether to print progress information

    Returns:
        dict: Extraction results with polygons and validation metrics
    """
    if verbose:
        print(f"\n{'=' * 60}")
        print(f"EXTRACTING MEMBER {member_number}")
        print(f"Track: {os.path.basename(track_csv)}")
        print(f"Wind: {os.path.basename(wind_grib)}")
        print(f"{'=' * 60}")

    try:
        # Load track data
        if verbose:
            print("\nStep 1: Loading track data...")
        track_data = load_track_data(track_csv, member_number, verbose)

        if track_data.empty:
            if verbose:
                print(f"  WARNING: No track data for member {member_number}")
            return {'success': False, 'error': 'No track data'}

        # Create buffered polygon
        if verbose:
            print(f"\nStep 2: Creating buffer ({buffer_radius_km} km)...")
        buffered_polygon = create_buffered_track_polygon(track_data, buffer_radius_km)
        bbox = get_bounding_box(buffered_polygon)

        if verbose:
            print(
                f"  Bounding box: [{bbox['lon_min']:.1f}, {bbox['lon_max']:.1f}] × [{bbox['lat_min']:.1f}, {bbox['lat_max']:.1f}]")

        # Load wind data
        if verbose:
            print("\nStep 3: Loading wind data...")
        wind_data = load_wind_data(wind_grib, member_number, bbox, verbose)

        # Extract contours
        if verbose:
            print("\nStep 4: Extracting wind threshold contours...")
        contours = create_wind_threshold_contours(wind_data, thresholds, verbose)

        # Prepare output
        result = {
            'success': True,
            'member': member_number,
            'contours': contours,
            'track_data': track_data,
            'bbox': bbox
        }

        return result

    except Exception as e:
        if verbose:
            print(f"\n Error extracting member {member_number}: {e}")
        return {'success': False, 'error': str(e), 'member': member_number}


def extract_multiple_storms(
        storm_configs: List[Dict[str, str]],
        wind_grib: str,
        output_dir: Optional[str] = None,
        buffer_radius_km: float = BUFFER_RADIUS_KM,
        thresholds: Dict[int, float] = WIND_THRESHOLDS,
        verbose: bool = True) -> Dict[str, Union[int, List[str], Dict]]:
    """
    Extract wind threshold polygons for multiple storms from a single wind GRIB file.
    
    This function efficiently processes multiple hurricane tracks using the same
    wind forecast data, avoiding repeated file I/O operations.
    
    Args:
        storm_configs (List[Dict]): List of storm configurations, each containing:
            - 'track_csv': Path to transformed TC track CSV
            - 'storm_name': Name of the storm (optional, for output naming)
        wind_grib (str): Path to wind GRIB file
        output_dir (str, optional): Output directory for envelope files
        buffer_radius_km (float): Buffer radius in km
        thresholds (dict): Wind thresholds to extract
        verbose (bool): Whether to print detailed progress information
        
    Returns:
        dict: Summary with results for all storms
        
    Example:
        storm_configs = [
            {'track_csv': 'JERRY_transformed.csv', 'storm_name': 'JERRY'},
            {'track_csv': 'RAYMOND_transformed.csv', 'storm_name': 'RAYMOND'},
            {'track_csv': 'NAKRI_transformed.csv', 'storm_name': 'NAKRI'}
        ]
        result = extract_multiple_storms(storm_configs, 'wind_ens_2025-10-10_r00_f024h.grib2')
    """
    if verbose:
        print("=" * 70)
        print("MULTI-STORM WIND EXTRACTION")
        print("=" * 70)
        print(f"Processing {len(storm_configs)} storms from single wind file")
        print(f"Wind file: {os.path.basename(wind_grib)}")
        print(f"Buffer radius: {buffer_radius_km} km")
        print(f"Wind thresholds: {list(thresholds.keys())} kt")
        print("=" * 70)
    
    # Load wind data once
    if verbose:
        print("\nLoading wind data (shared across all storms)...")
    
    try:
        # Open wind dataset once
        wind_ds = xr.open_dataset(wind_grib, engine='cfgrib')
    except Exception as e:
        if verbose:
            print(f"   Error loading wind data: {e}")
        return {'success': False, 'error': f'Failed to load wind data: {e}'}

    # Process each storm — wind_ds.close() in finally covers u10/v10 extraction too
    all_envelope_records = []
    storm_summaries = {}

    try:
      u10 = wind_ds['u10']
      v10 = wind_ds['v10']

      if verbose:
          print(f"  Wind data loaded: {wind_ds.dims}")
          print(f"  Available ensemble members: {wind_ds.number.values if 'number' in wind_ds.dims else 'N/A'}")

      for i, storm_config in enumerate(storm_configs):
        track_csv = storm_config['track_csv']
        storm_name = storm_config.get('storm_name', f'STORM_{i+1}')
        
        if verbose:
            print(f"\n{'=' * 50}")
            print(f"PROCESSING STORM {i+1}/{len(storm_configs)}: {storm_name}")
            print(f"Track file: {os.path.basename(track_csv)}")
            print(f"{'=' * 50}")
        
        try:
            # Load track data for this storm
            track_df = pd.read_csv(track_csv)
            storm_members = sorted(track_df['ensemble_member'].unique())
            
            if verbose:
                print(f"  Found {len(storm_members)} ensemble members for {storm_name}")
            
            # Get storm metadata
            track_id = track_df['track_id'].iloc[0] if 'track_id' in track_df.columns else storm_name
            forecast_time = track_df['forecast_time'].iloc[0] if 'forecast_time' in track_df.columns else None
            
            # Process each member for this storm
            storm_envelope_records = []
            
            for member in storm_members:
                if verbose:
                    print(f"    Processing member {member}...", end='', flush=True)
                
                try:
                    # Load track data for this member
                    member_track_data = track_df[track_df['ensemble_member'] == member].copy()
                    member_track_data = member_track_data.sort_values('valid_time')
                    
                    if member_track_data.empty:
                        if verbose:
                            print("  No track data")
                        continue
                    
                    # Create buffered polygon for this member
                    buffered_polygon = create_buffered_track_polygon(member_track_data, buffer_radius_km)
                    bbox = get_bounding_box(buffered_polygon)
                    
                    # Extract wind data for this member and region (reuse loaded data)
                    wind_speed = np.sqrt(u10 ** 2 + v10 ** 2)
                    
                    # Select specific ensemble member; member 51 = HRES control stored as number=0
                    if 'number' in wind_speed.dims:
                        grib_number = 0 if member == 51 else member
                        member_wind_data = wind_speed.sel(number=grib_number)
                    elif member == 51:
                        # CF GRIB file (single-member HRES) has no number dim — use directly
                        logger.warning(
                            f"  Member 51 (HRES): no 'number' dim in wind file — using data directly"
                        )
                        member_wind_data = wind_speed
                    else:
                        if verbose:
                            print(f"  No ensemble dimension for member {member} — skipping")
                        continue
                    
                    # Subset to bounding box
                    wind_region = member_wind_data.sel(
                        latitude=slice(bbox['lat_max'], bbox['lat_min']),
                        longitude=slice(bbox['lon_min'], bbox['lon_max'])
                    )
                    
                    # Create contours for this member
                    contours = create_wind_threshold_contours(wind_region, thresholds, verbose=False)
                    
                    # Create envelope records for this member (only for thresholds with polygons)
                    for threshold_kt, polygon in contours.items():
                        if polygon is not None:  # Only add rows with actual polygons
                            storm_envelope_records.append({
                                'forecast_time': forecast_time,
                                'track_id': track_id,
                                'ensemble_member': member,
                                'wind_threshold': threshold_kt,
                                'envelope_region': polygon_to_wkt(polygon)
                            })
                    
                    if verbose:
                        n_polygons = sum(1 for p in contours.values() if p is not None)
                        print(f"  {n_polygons} polygons")
                        
                except Exception as e:
                    if verbose:
                        print(f"  Error: {e}")
                    continue
            
            # Save envelope file for this storm
            if storm_envelope_records:
                storm_envelopes_df = pd.DataFrame(storm_envelope_records)
                
                # Generate output filename
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                    output_csv = os.path.join(output_dir, f"{storm_name}_envelopes.csv")
                else:
                    output_csv = f"{storm_name}_envelopes.csv"
                
                storm_envelopes_df.to_csv(output_csv, index=False)
                
                if verbose:
                    print(f"   Saved {len(storm_envelope_records)} envelope records to {output_csv}")
                
                # Add to overall results
                all_envelope_records.extend(storm_envelope_records)
                
                # Store summary
                storm_summaries[storm_name] = {
                    'envelope_file': output_csv,
                    'records': len(storm_envelope_records),
                    'members_processed': len(storm_members)
                }
            else:
                if verbose:
                    print(f"   No envelope records created for {storm_name}")
                storm_summaries[storm_name] = {
                    'envelope_file': None,
                    'records': 0,
                    'members_processed': 0
                }
                
        except Exception as e:
            if verbose:
                print(f"   Error processing {storm_name}: {e}")
            storm_summaries[storm_name] = {
                'envelope_file': None,
                'records': 0,
                'members_processed': 0,
                'error': str(e)
            }
    
    finally:
        wind_ds.close()
    
    # Overall summary
    if verbose:
        print("\n" + "=" * 70)
        print("MULTI-STORM EXTRACTION SUMMARY")
        print("=" * 70)
        print(f"Storms processed: {len(storm_configs)}")
        print(f"Total envelope records: {len(all_envelope_records)}")
        
        successful_storms = sum(1 for s in storm_summaries.values() if s['records'] > 0)
        print(f"Successful extractions: {successful_storms}/{len(storm_configs)}")
        
        print(f"\nPer-storm results:")
        for storm_name, summary in storm_summaries.items():
            status = "✓" if summary['records'] > 0 else "✗"
            print(f"  {status} {storm_name}: {summary['records']} records, {summary['members_processed']} members")
        
        print("=" * 70)
    
    return {
        'success': len(all_envelope_records) > 0,
        'total_records': len(all_envelope_records),
        'storm_summaries': storm_summaries,
        'processed_storms': len(storm_configs)
    }


def extract_all_members(
        track_csv: str,
        wind_grib: str,
        output_csv: Optional[str] = None,
        buffer_radius_km: float = BUFFER_RADIUS_KM,
        thresholds: Dict[int, float] = WIND_THRESHOLDS,
        verbose: bool = True) -> Dict[str, Union[str, int, pd.DataFrame]]:
    """
    Extract wind threshold polygons for all ensemble members.

    Output format matches TC envelope file structure:
    - forecast_time, track_id, ensemble_member, wind_threshold, envelope_region
    - One row per member per threshold

    Args:
        track_csv (str): Path to transformed TC track CSV
        wind_grib (str): Path to wind GRIB file
        output_csv (str, optional): Path to save output CSV (will auto-generate with _envelopes.csv suffix)
        buffer_radius_km (float): Buffer radius in km
        thresholds (dict): Wind thresholds to extract
        verbose (bool): Whether to print progress information

    Returns:
        dict: Summary with results DataFrame and statistics
    """
    if verbose:
        print("=" * 70)
        print("ECMWF ENSEMBLE WIND EXTRACTION")
        print("=" * 70)
        print(f"Track file: {track_csv}")
        print(f"Wind file: {wind_grib}")
        print(f"Buffer radius: {buffer_radius_km} km")
        print(f"Wind thresholds: {list(thresholds.keys())} kt")
        print("=" * 70)

    # Get list of members from track data
    track_df = pd.read_csv(track_csv)
    all_members = sorted(track_df['ensemble_member'].unique())

    if verbose:
        print(f"\nFound {len(all_members)} ensemble members in track data")
        print(f"Processing members: {all_members[0]} to {all_members[-1]}")

    # Get forecast metadata
    track_id = track_df['track_id'].iloc[0] if 'track_id' in track_df.columns else 'UNKNOWN'
    forecast_time = track_df['forecast_time'].iloc[0] if 'forecast_time' in track_df.columns else None

    # Process each member
    envelope_records = []

    for member in all_members:
        result = extract_wind_polygons_for_member(
            track_csv=track_csv,
            wind_grib=wind_grib,
            member_number=member,
            buffer_radius_km=buffer_radius_km,
            thresholds=thresholds,
            verbose=verbose
        )

        if result['success']:
            # Create envelope records (only for thresholds with polygons)
            for threshold_kt, polygon in result['contours'].items():
                if polygon is not None:  # Only add rows with actual polygons
                    envelope_records.append({
                        'forecast_time': forecast_time,
                        'track_id': track_id,
                        'ensemble_member': member,
                        'wind_threshold': threshold_kt,
                        'envelope_region': polygon_to_wkt(polygon)
                    })

    # Create DataFrames
    envelopes_df = pd.DataFrame(envelope_records)

    # Auto-generate output filename if not provided
    if output_csv is None:
        # Extract base name from input files
        track_base = os.path.splitext(os.path.basename(track_csv))[0]
        # Remove _transformed suffix if present
        track_base = track_base.replace('_transformed', '')
        output_csv = f"{track_base}_envelopes.csv"

    # Save envelope file (main output)
    envelopes_df.to_csv(output_csv, index=False)
    if verbose:
        print(f"\n Saved envelope polygons to: {output_csv}")

    # Print summary statistics
    if verbose and not envelopes_df.empty:
        print("\n" + "=" * 70)
        print("EXTRACTION SUMMARY")
        print("=" * 70)
        print(f"Successfully processed: {len(all_members)} members")
        print(f"Total envelope records: {len(envelopes_df)} (members × thresholds)")

        # Count polygons per threshold
        print(f"\nPolygons extracted per threshold:")
        for threshold in sorted(thresholds.keys()):
            n_polygons = envelopes_df[
                (envelopes_df['wind_threshold'] == threshold) &
                (envelopes_df['envelope_region'].notna())
                ].shape[0]
            print(f"  {threshold:>3d} kt: {n_polygons}/{len(all_members)}")

        print("=" * 70)

    return {
        'success': len(envelopes_df) > 0,
        'envelopes_df': envelopes_df,
        'n_processed': len(all_members),
        'envelope_file': output_csv
    }

