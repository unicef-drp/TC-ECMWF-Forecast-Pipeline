#!/usr/bin/env python3
"""
ECMWF TC Wind Combination Script

This module combines tropical cyclone track forecasts with ECMWF wind data
to produce wind threshold envelope files for visualization and analysis.

The combination process:
1. Takes transformed TC track data (from ecmwf_tc_data_transformer)
2. Takes ECMWF wind forecast data (from ecmwf_wind_data_downloader)
3. Extracts wind threshold polygons using ecmwf_wind_data_extractor
4. Produces TWO types of output files:
   - Individual forecast steps (separate polygons per time step)
   - Combined polygons (total area across all forecast steps)

Output files:
- {storm}_envelopes_individual.csv: Separate polygons for each forecast step
- {storm}_envelopes_combined.csv: Combined polygons per member/threshold

This script bridges the gap between:
- TC track data (individual forecast points)
- Wind forecast data (gridded wind fields)
- Wind envelope data (both individual and combined threshold polygons)
"""

import os
import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from ecmwf_wind_data_extractor import (
    create_buffered_track_polygon, 
    get_bounding_box,
    load_wind_data,
    create_wind_threshold_contours,
    polygon_to_wkt,
    WIND_THRESHOLDS
)
from shapely.ops import unary_union
from shapely import wkt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_tc_data_files(tc_data_dir: Path) -> List[Dict[str, str]]:
    """
    Find all transformed TC data files and extract storm information.
    
    Args:
        tc_data_dir: Directory containing transformed TC CSV files
        
    Returns:
        List of dictionaries with storm configuration
    """
    tc_files = list(tc_data_dir.glob("*_transformed.csv"))
    
    if not tc_files:
        logger.warning(f"No transformed TC files found in {tc_data_dir}")
        return []
    
    storm_configs = []
    
    for tc_file in tc_files:
        # Extract storm name from filename
        # Format: A_JSXX01ECEP120000_C_ECMP_20251012000000_tropical_cyclone_track_JERRY_-63p7degW_27degN_bufr4_extracted_transformed.csv
        filename = tc_file.name
        
        if 'tropical_cyclone_track_' in filename:
            storm_name = filename.split('tropical_cyclone_track_')[1].split('_')[0]
        else:
            # Fallback: use filename without extension
            storm_name = tc_file.stem.replace('_transformed', '')
        
        storm_configs.append({
            'track_csv': str(tc_file),
            'storm_name': storm_name
        })
        
        logger.info(f"Found TC data: {storm_name} -> {tc_file.name}")
    
    return storm_configs


def load_tc_track_data(track_csv: str) -> pd.DataFrame:
    """
    Load TC track data and prepare for processing.
    
    Args:
        track_csv: Path to transformed TC CSV file
        
    Returns:
        DataFrame with TC track data
    """
    df = pd.read_csv(track_csv)
    
    # Ensure valid_time is datetime
    df['valid_time'] = pd.to_datetime(df['valid_time'])
    
    # Sort by ensemble member and valid_time
    df = df.sort_values(['ensemble_member', 'valid_time'])
    
    return df


def find_wind_file_for_time(valid_time: datetime, lead_time: int, wind_files: List[Path], ensemble_member: int) -> Optional[Path]:
    """
    Find the wind GRIB file that corresponds to a specific valid time and lead time.
    
    Args:
        valid_time: Valid time from TC track data
        lead_time: Lead time in hours from TC track data
        wind_files: List of available wind GRIB files
        
    Returns:
        Path to matching wind file or None
    """
    # Wind files are named: wind_ens_2025-10-12_r12_f012h.grib2
    # TC valid_time is: 2025-10-13 12:00:00
    # The wind file date is the FORECAST START DATE, not the valid time date
    
    # Calculate forecast start date from valid_time and lead_time
    forecast_start_date = valid_time - pd.Timedelta(hours=lead_time)
    date_str = forecast_start_date.strftime('%Y-%m-%d')
    forecast_hour = f"f{lead_time:03d}h"
    
    # Look for wind file with matching forecast start date and forecast hour
    for wind_file in wind_files:
        filename = wind_file.name
        # Choose PF vs CF based on ensemble member
        type_tag = "_cf" if ensemble_member == 51 else "_pf"
        # Match e.g., wind_ens_YYYY-MM-DD_rHH_fHHHh_pf.grib2 or _cf.grib2
        if (
            f"wind_ens_{date_str}_r" in filename
            and f"_{forecast_hour}" in filename
            and filename.endswith(f"{type_tag}.grib2")
        ):
            return wind_file
    
    return None


def extract_wind_polygons_for_time_step(
    wind_file: Path,
    bbox: Dict[str, float],
    ensemble_member: int
) -> Dict[int, Optional[str]]:
    """
    Extract wind polygons for a specific time step and ensemble member.
    
    Args:
        wind_file: Path to wind GRIB file
        bbox: Bounding box for wind extraction
        ensemble_member: Ensemble member number
        
    Returns:
        Dictionary mapping wind thresholds to WKT polygons
    """
    try:
        # Load wind data for this member and region
        wind_data = load_wind_data(str(wind_file), ensemble_member, bbox, verbose=False)
        
        # Create contours for all thresholds
        contours = create_wind_threshold_contours(wind_data, WIND_THRESHOLDS, verbose=False)
        
        # Convert to WKT format
        wkt_polygons = {}
        for threshold, polygon in contours.items():
            wkt_polygons[threshold] = polygon_to_wkt(polygon)
        
        return wkt_polygons
        
    except Exception as e:
        logger.warning(f"Error extracting wind polygons for member {ensemble_member}: {e}")
        return {}


def combine_polygons_across_forecast_steps(envelope_records: List[Dict]) -> List[Dict]:
    """
    Combine polygons of the same wind threshold across all forecast steps for each member.
    
    Args:
        envelope_records: List of envelope records with polygons
        
    Returns:
        List of combined envelope records (one per threshold per member)
    """
    # Group by member and threshold
    grouped_data = {}
    
    for record in envelope_records:
        member = record['ensemble_member']
        threshold = record['wind_threshold']
        key = (member, threshold)
        
        if key not in grouped_data:
            grouped_data[key] = {
                'forecast_time': record['forecast_time'],
                'track_id': record['track_id'],
                'ensemble_member': member,
                'valid_time': record['valid_time'],
                'lead_time': record['lead_time'],
                'wind_threshold': threshold,
                'polygons': []
            }
        
        # Collect all polygons for this member/threshold combination
        if record['envelope_region']:
            try:
                polygon = wkt.loads(record['envelope_region'])
                if polygon and not polygon.is_empty:
                    grouped_data[key]['polygons'].append(polygon)
            except Exception as e:
                logger.warning(f"Error parsing polygon for member {member}, threshold {threshold}: {e}")
    
    # Combine polygons for each member/threshold combination
    combined_records = []
    
    for (member, threshold), data in grouped_data.items():
        if data['polygons']:
            try:
                # Combine all polygons using unary_union
                combined_polygon = unary_union(data['polygons'])
                
                # Convert back to WKT
                combined_wkt = polygon_to_wkt(combined_polygon)
                
                if combined_wkt:
                    combined_records.append({
                        # Use the same forecast issuance used for grouping (from individual rows)
                        'forecast_time': data['forecast_time'],
                        'track_id': data['track_id'],
                        'ensemble_member': member,
                        'lead_time': f"{min([r['lead_time'] for r in envelope_records if r['ensemble_member'] == member and r['wind_threshold'] == threshold])}-{max([r['lead_time'] for r in envelope_records if r['ensemble_member'] == member and r['wind_threshold'] == threshold])}",
                        'wind_threshold': threshold,
                        'envelope_region': combined_wkt
                    })
                    
                    
                    logger.debug(f"Combined {len(data['polygons'])} polygons for member {member}, threshold {threshold}")
                    
            except Exception as e:
                logger.warning(f"Error combining polygons for member {member}, threshold {threshold}: {e}")
    
    return combined_records


def find_wind_data_files(wind_data_dir: Path) -> List[Path]:
    """
    Find all wind GRIB files in the wind data directory.
    
    Args:
        wind_data_dir: Directory containing wind GRIB files
        
    Returns:
        List of wind GRIB file paths
    """
    wind_files = list(wind_data_dir.glob("*.grib2"))
    
    if not wind_files:
        logger.warning(f"No wind GRIB files found in {wind_data_dir}")
        return []
    
    logger.info(f"Found {len(wind_files)} wind GRIB files:")
    for wind_file in wind_files:
        logger.info(f"  - {wind_file.name}")
    
    return wind_files


def analyze_required_forecast_hours(tc_data_dir: Path, verbose: bool = True) -> int:
    """
    Analyze TC track data to determine the maximum forecast hour needed for wind data download.
    
    Args:
        tc_data_dir: Directory containing transformed TC CSV files
        verbose: Whether to print detailed analysis
        
    Returns:
        Maximum forecast hour needed (rounded up to 6-hour intervals with buffer)
    """
    max_hour = 0
    tc_files = list(tc_data_dir.glob("*_transformed.csv"))
    
    if verbose:
        logger.info("Analyzing TC track data to determine required forecast hours...")
    
    for csv_file in tc_files:
        try:
            df = pd.read_csv(csv_file)
            
            if df.empty:
                continue
            
            # Check if 'lead_time' column exists (from transformed data)
            if 'lead_time' in df.columns:
                storm_max_hour = df['lead_time'].max()
                max_hour = max(max_hour, storm_max_hour)
                if verbose:
                    logger.info(f"  {csv_file.stem}: max lead time = {storm_max_hour}h")
            else:
                # Fallback: calculate from step column if lead_time not available
                if 'step' in df.columns:
                    storm_max_hour = df['step'].max()
                    max_hour = max(max_hour, storm_max_hour)
                    if verbose:
                        logger.info(f"  {csv_file.stem}: max step = {storm_max_hour}h")
                
        except Exception as e:
            if verbose:
                logger.warning(f"Could not analyze {csv_file.name}: {e}")
            continue
    
    # Round up to next 6-hour interval and add buffer
    if max_hour > 0:
        # Round up to next 6-hour interval
        rounded_hour = ((max_hour + 5) // 6) * 6
        # Add 12-hour buffer for safety
        recommended_hour = min(rounded_hour + 12, 144)
        
        if verbose:
            logger.info(f"TC data analysis: max forecast hour needed = {max_hour}h")
            logger.info(f"Recommended wind forecast hours: 0 to {recommended_hour}h (every 6h)")
        
        return recommended_hour
    else:
        if verbose:
            logger.warning("Could not determine max forecast hour from TC data, using default")
        return 144


def process_wind_combination(
    tc_data_dir: Path,
    wind_data_dir: Path,
    output_dir: Path,
    buffer_radius_km: int = 500,
    max_ensemble_members: int = 3,
    verbose: bool = True
) -> Dict[str, int]:
    """
    Process wind combination for all TC storms, producing both individual and combined outputs.
    
    This function:
    1. Extracts wind polygons for each forecast step and ensemble member
    2. Saves individual forecast steps to {storm}_envelopes_individual.csv
    3. Combines polygons of the same wind threshold across all forecast steps
    4. Saves combined polygons to {storm}_envelopes_combined.csv
    
    Args:
        tc_data_dir: Directory containing transformed TC CSV files
        wind_data_dir: Directory containing wind GRIB files
        output_dir: Directory to save envelope CSV files
        buffer_radius_km: Buffer radius around TC tracks in km
        max_ensemble_members: Maximum number of ensemble members to process (for testing)
        verbose: Whether to print detailed progress
        
    Returns:
        Dictionary with processing statistics
    """
    logger.info("=" * 70)
    logger.info("ECMWF TC WIND COMBINATION PROCESSING")
    logger.info("=" * 70)
    
    # Find input files
    storm_configs = find_tc_data_files(tc_data_dir)
    wind_files = find_wind_data_files(wind_data_dir)
    
    if not storm_configs:
        logger.error("No TC data files found. Exiting.")
        return {'processed_storms': 0, 'total_envelope_records': 0, 'errors': 1}
    
    if not wind_files:
        logger.error("No wind data files found. Exiting.")
        return {'processed_storms': 0, 'total_envelope_records': 0, 'errors': 1}
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each storm
    total_envelope_records = 0
    processed_storms = 0
    errors = 0
    
    for storm_config in storm_configs:
        storm_name = storm_config['storm_name']
        track_csv = storm_config['track_csv']
        
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Processing storm: {storm_name}")
        logger.info(f"{'=' * 50}")
        
        try:
            # Load TC track data
            tc_data = load_tc_track_data(track_csv)
            logger.info(f"  Loaded {len(tc_data)} track points")
            
            # Create buffer around entire track
            track_polygon = create_buffered_track_polygon(tc_data, buffer_radius_km)
            bbox = get_bounding_box(track_polygon, buffer=2.0)
            logger.info(f"  Created track buffer: {buffer_radius_km}km")
            
            # Process each time step
            storm_envelope_records = []

            # Limit ensemble members for faster processing
            unique_members = list(tc_data['ensemble_member'].unique())[:max_ensemble_members]

            if verbose:
                logger.info(f"  Processing {len(unique_members)} ensemble members: {list(unique_members)}")

            total_members = len(unique_members)

            for idx, member in enumerate(unique_members, start=1):
                logger.info(f"  → Member {member} ({idx}/{total_members})")

                member_tc_data = tc_data[tc_data['ensemble_member'] == member]

                for _, track_point in member_tc_data.iterrows():
                    valid_time = track_point['valid_time']
                    lead_time = track_point['lead_time']
                    ensemble_member = track_point['ensemble_member']

                    # Find matching wind file
                    wind_file = find_wind_file_for_time(valid_time, lead_time, wind_files, ensemble_member)

                    if wind_file is None:
                        forecast_start_date = valid_time - pd.Timedelta(hours=lead_time)
                        date_str = forecast_start_date.strftime('%Y-%m-%d')
                        forecast_hour = f"f{lead_time:03d}h"
                        expected_pattern = (
                            f"wind_ens_{date_str}_r*_{forecast_hour}_{'cf' if ensemble_member == 51 else 'pf'}.grib2"
                        )
                        logger.warning(f"    No wind file found for {valid_time} (lead {lead_time}h) - looking for: {expected_pattern}")
                        continue

                    logger.info(f"    Processing {valid_time} (lead {lead_time}h) - {wind_file.name}")

                    # Extract wind polygons for this time step
                    wkt_polygons = extract_wind_polygons_for_time_step(
                        wind_file, bbox, ensemble_member
                    )

                    # Create envelope records for this time step
                    for threshold, wkt_polygon in wkt_polygons.items():
                        if wkt_polygon is not None:
                            storm_envelope_records.append({
                                'forecast_time': track_point['forecast_time'],
                                'track_id': storm_name,
                                'ensemble_member': ensemble_member,
                                'valid_time': valid_time,
                                'lead_time': lead_time,
                                'wind_threshold': threshold,
                                'envelope_region': wkt_polygon
                            })
            
            # Save both individual and combined outputs
            if storm_envelope_records:
                # Save individual forecast steps
                individual_df = pd.DataFrame(storm_envelope_records)

                # Derive forecast issuance time for filename suffix (YYYYMMDDTHHZ)
                try:
                    first_ft = pd.to_datetime(individual_df['forecast_time'].iloc[0]) if 'forecast_time' in individual_df.columns else None
                except Exception:
                    first_ft = None
                if first_ft is not None and not pd.isna(first_ft):
                    ft_suffix = first_ft.strftime('%Y%m%dT%HZ')
                    individual_file = output_dir / f"{storm_name}_{ft_suffix}_envelopes_individual.csv"
                else:
                    individual_file = output_dir / f"{storm_name}_envelopes_individual.csv"

                individual_df.to_csv(individual_file, index=False)
                
                logger.info(f"  Saved individual forecast steps: {individual_file.name}")
                logger.info(f"  - Individual records: {len(storm_envelope_records)}")
                
                # Combine polygons across forecast steps for each member/threshold
                logger.info(f"  Combining polygons across forecast steps...")
                combined_records = combine_polygons_across_forecast_steps(storm_envelope_records)
                
                if combined_records:
                    combined_df = pd.DataFrame(combined_records)

                    # Use same forecast issuance suffix for combined filename
                    if first_ft is not None and not pd.isna(first_ft):
                        ft_suffix = first_ft.strftime('%Y%m%dT%HZ')
                        combined_file = output_dir / f"{storm_name}_{ft_suffix}_envelopes_combined.csv"
                    else:
                        combined_file = output_dir / f"{storm_name}_envelopes_combined.csv"

                    combined_df.to_csv(combined_file, index=False)
                    
                    total_envelope_records += len(combined_records)
                    processed_storms += 1
                    
                    logger.info(f"✓ Successfully processed {storm_name}")
                    logger.info(f"  - Combined records: {len(combined_records)}")
                    logger.info(f"  - Combined file: {combined_file.name}")
                else:
                    logger.warning(f"  No combined polygons created for {storm_name}")
            else:
                logger.warning(f"  No envelope records created for {storm_name}")
                
        except Exception as e:
            logger.error(f"✗ Error processing {storm_name}: {e}")
            errors += 1
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("WIND COMBINATION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Storms processed: {processed_storms}/{len(storm_configs)}")
    logger.info(f"Total envelope records created: {total_envelope_records:,}")
    logger.info(f"Errors: {errors}")
    logger.info(f"Output directory: {output_dir}")
    
    return {
        'processed_storms': processed_storms,
        'total_envelope_records': total_envelope_records,
        'errors': errors
    }