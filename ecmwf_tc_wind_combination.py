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
import re
import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from ecmwf_wind_data_extractor import (
    create_buffered_track_polygon,
    get_bounding_box,
    get_longitude_windows,
    merge_contour_dicts,
    load_wind_data,
    load_wind_data_all_members,
    create_wind_threshold_contours,
    polygon_to_wkt,
    WIND_THRESHOLDS
)
from shapely.ops import unary_union
from shapely import wkt

from ecmwf_wind_raster_writer import save_wind_speed_raster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _match_wind_file(
    forecast_time,
    lead_time: int,
    ensemble_member: int,
    wind_files: List[Path],
) -> Optional[Path]:
    """
    Return the wind GRIB file matching the given forecast time, lead time, and member.

    Handles both pd.Timestamp and datetime inputs for forecast_time.
    Returns None if no matching file is found.
    """
    if isinstance(forecast_time, pd.Timestamp):
        forecast_time = forecast_time.to_pydatetime()
    elif isinstance(forecast_time, str):
        forecast_time = pd.to_datetime(forecast_time).to_pydatetime()

    date_str = forecast_time.strftime('%Y-%m-%d')
    run_hour = f"{forecast_time.hour:02d}"
    forecast_hour = f"f{lead_time:03d}h"
    type_tag = "_cf" if ensemble_member == 51 else "_pf"
    expected_run_tag = f"_r{run_hour}_"

    for wind_file in wind_files:
        filename = wind_file.name
        if (
            f"wind_ens_{date_str}" in filename
            and expected_run_tag in filename
            and f"_{forecast_hour}" in filename
            and filename.endswith(f"{type_tag}.grib2")
        ):
            return wind_file
    return None


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

        if '_storm_' in filename:
            m = re.search(r'_storm_([A-Z0-9-]+)_', filename)
            storm_name = m.group(1) if m else tc_file.stem
        elif 'tropical_cyclone_track_' in filename:
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

    # Ensure forecast_time is datetime if present (needed for matching wind files by run hour)
    if 'forecast_time' in df.columns:
        df['forecast_time'] = pd.to_datetime(df['forecast_time'])

    # Sort by ensemble member and valid_time
    df = df.sort_values(['ensemble_member', 'valid_time'])

    return df


def find_wind_file_for_time(valid_time: datetime, lead_time: int, wind_files: List[Path], ensemble_member: int, forecast_time: Optional[datetime] = None) -> Optional[Path]:
    """
    Find the wind GRIB file that corresponds to a specific valid time and lead time.

    Args:
        valid_time: Valid time from TC track data
        lead_time: Lead time in hours from TC track data
        wind_files: List of available wind GRIB files
        ensemble_member: Ensemble member number (51 for control, others for perturbed)
        forecast_time: Forecast issuance time (used to match run hour).
                       If None, derived from valid_time - lead_time.

    Returns:
        Path to matching wind file or None
    """
    if forecast_time is None:
        forecast_time = valid_time - pd.Timedelta(hours=lead_time)
    return _match_wind_file(forecast_time, lead_time, ensemble_member, wind_files)


def extract_wind_polygons_for_time_step(
    wind_file: Path,
    bbox: Dict[str, float],
    ensemble_member: int,
    indexpath: Optional[str] = None
) -> Dict[int, Optional[str]]:
    """
    Extract wind polygons for a specific time step and ensemble member.

    Args:
        wind_file: Path to wind GRIB file
        bbox: Bounding box for wind extraction
        ensemble_member: Ensemble member number
        indexpath: Optional directory path for GRIB index files (.idx).
                   If provided, index files will be stored here instead
                   of alongside the GRIB file. Useful for parallel processing
                   to avoid concurrent index file conflicts.

    Returns:
        Dictionary mapping wind thresholds to WKT polygons
    """
    try:
        # Load wind data for this member and region with optional index path
        # one window in the common case, two when bbox straddles the
        # antimeridian (see get_longitude_windows)
        wind_regions = load_wind_data(str(wind_file), ensemble_member, bbox, verbose=False, indexpath=indexpath)

        # Create contours per window, then merge (a single-window input
        # merges to that one dict unchanged, see merge_contour_dicts)
        sub_contours = [
            create_wind_threshold_contours(region, WIND_THRESHOLDS, verbose=False)
            for region in wind_regions
        ]
        contours = merge_contour_dicts(sub_contours)

        # Convert to WKT format
        wkt_polygons = {}
        for threshold, polygon in contours.items():
            wkt_polygons[threshold] = polygon_to_wkt(polygon)

        return wkt_polygons

    except Exception as e:
        logger.warning(f"Error extracting wind polygons for member {ensemble_member}: {e}")
        return {}


def _extract_polygons_all_members(wind_data_windows) -> Dict[int, Dict[int, Optional[str]]]:
    """
    Extract wind threshold polygons for every ensemble member from one or
    more already-loaded, already-windowed DataArrays.

    Args:
        wind_data_windows: List[xr.DataArray] from load_wind_data_all_members:
            length 1 (bbox clipped exactly as before) for the overwhelming
            majority of storms, length 2 only when the storm's bbox straddles
            the antimeridian. Each has dims (number, lat, lon) for PF or
            (lat, lon) for CF.

    Returns:
        {grib_member_number: {threshold_kt: wkt_string_or_None}}: per-member
        contours are extracted independently within each window, then
        unioned across windows per member/threshold (a length-1 input list
        is a verified no-op, producing exactly today's output).
    """
    # per_window[i] = {grib_member_number: {threshold_kt: Polygon_or_None}}
    per_window: List[Dict[int, Dict[int, Optional[object]]]] = []

    for wind_data in wind_data_windows:
        window_results: Dict[int, Dict[int, Optional[object]]] = {}
        if 'number' in wind_data.dims:
            for grib_number in wind_data.number.values:
                member_wind = wind_data.sel(number=grib_number)
                contours = create_wind_threshold_contours(member_wind, WIND_THRESHOLDS, verbose=False)
                window_results[int(grib_number)] = contours
        else:
            # CF file: single control member (maps to pipeline member 51, GRIB number 0)
            contours = create_wind_threshold_contours(wind_data, WIND_THRESHOLDS, verbose=False)
            window_results[0] = contours
        per_window.append(window_results)

    if len(per_window) == 1:
        # No antimeridian split: return polygons->wkt directly, identical
        # to the original single-window behavior.
        return {
            member: {kt: polygon_to_wkt(p) for kt, p in contours.items()}
            for member, contours in per_window[0].items()
        }

    # Merge across windows per member, per threshold
    all_members = set()
    for window_results in per_window:
        all_members.update(window_results.keys())

    results: Dict[int, Dict[int, Optional[str]]] = {}
    for member in all_members:
        member_contour_dicts = [
            window_results[member] for window_results in per_window
            if member in window_results
        ]
        merged_contours = merge_contour_dicts(member_contour_dicts)
        results[member] = {kt: polygon_to_wkt(p) for kt, p in merged_contours.items()}

    return results


def _save_wind_rasters_for_lead_time(wind_windows: List, output_dir: Path, storm_name: str,
                                      ft_suffix: Optional[str], lead_time, label: str,
                                      dataset_label: str = 'wind') -> int:
    """
    Persist the already-loaded wind/gust speed field for one (storm, forecast
    cycle, lead time, PF-or-CF) combination as GeoTIFF(s), alongside the CSV
    envelope output in the same output_dir. Additive only: does not touch
    the existing polygon-extraction path at all, callers still call
    _extract_polygons_all_members() (wind) or their
    own per-member selection (gust, ecmwf_gust_envelope_extractor.py) on the
    same *_windows unchanged. Shared by both wind (own module) and gust
    (imported by ecmwf_gust_envelope_extractor.py), one real
    implementation, not two drifting copies.

    Args:
        wind_windows: List[xr.DataArray] from load_wind_data_all_members()
            (wind) or load_gust_data_all_members() (gust), length 1 in the
            overwhelming majority of cases, length 2 only for a storm whose
            bbox straddles the antimeridian (see get_longitude_windows);
            one file is written per real window, with a `_w{index}` filename
            suffix only when there is more than one, so the common case's
            filename stays simple.
        output_dir: same directory process_wind_combination()/
            process_gust_combination() already write their own
            *_envelopes_individual.csv/*_envelopes_combined.csv/
            *_gust_envelopes_*.csv files into.
        ft_suffix: forecast issuance time, pre-formatted 'YYYYMMDDTHHZ'
            (same convention the CSV filenames already use) or None if
            unavailable for this cycle.
        lead_time: forecast lead time in hours (int).
        label: 'pf' or 'cf', which GRIB file this field came from.
        dataset_label: 'wind' (default) or 'gust', selects both the
            filename's `{dataset_label}_raster_` token (so wind and gust
            rasters never collide/glob-match each other in the same
            output_dir) and the GeoTIFF's own self-describing tag (see
            save_wind_speed_raster()'s own docstring).

    Returns:
        Number of raster files actually written (0 on total failure; never
        raises, a raster-write problem must not abort wind/gust polygon
        processing, which is the real, existing, more critical output).
    """
    written = 0
    for window_idx, window_data in enumerate(wind_windows):
        try:
            if window_data.size == 0:
                continue
            suffix = f"_w{window_idx}" if len(wind_windows) > 1 else ""
            if ft_suffix:
                raster_name = f"{storm_name}_{ft_suffix}_{dataset_label}_raster_{label}_{lead_time}h{suffix}.tif"
            else:
                raster_name = f"{storm_name}_{dataset_label}_raster_{label}_{lead_time}h{suffix}.tif"
            save_wind_speed_raster(window_data, str(output_dir / raster_name), dataset_label=dataset_label)
            written += 1
        except Exception as e:
            logger.warning(f"    Could not save {dataset_label} raster ({label}, lead {lead_time}h, window {window_idx}): {e}")
    return written


def combine_polygons_across_forecast_steps(envelope_records: List[Dict]) -> List[Dict]:
    """
    Combine polygons of the same wind threshold across all forecast steps for each member.

    Args:
        envelope_records: List of envelope records with polygons

    Returns:
        List of combined envelope records (one per threshold per member)
    """
    # Group by member and threshold; track lead_time min/max in one pass
    grouped_data = {}
    lead_time_range = {}  # (member, threshold) -> (min_lt, max_lt)

    for record in envelope_records:
        member = record['ensemble_member']
        threshold = record['wind_threshold']
        key = (member, threshold)
        lt = record['lead_time']

        if key not in grouped_data:
            grouped_data[key] = {
                'forecast_time': record['forecast_time'],
                'track_id': record['track_id'],
                'ensemble_member': member,
                'valid_time': record['valid_time'],
                'wind_threshold': threshold,
                'polygons': []
            }
            lead_time_range[key] = (lt, lt)
        else:
            min_lt, max_lt = lead_time_range[key]
            lead_time_range[key] = (min(min_lt, lt), max(max_lt, lt))

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
                # unary_union can return GEOMETRYCOLLECTION when inputs share edges;
                # buffer(0) normalises it back to a (Multi)Polygon
                if combined_polygon.geom_type not in ('Polygon', 'MultiPolygon'):
                    combined_polygon = combined_polygon.buffer(0)

                # Convert back to WKT
                combined_wkt = polygon_to_wkt(combined_polygon)

                if combined_wkt:
                    min_lt, max_lt = lead_time_range[(member, threshold)]
                    combined_records.append({
                        'forecast_time': data['forecast_time'],
                        'track_id': data['track_id'],
                        'ensemble_member': member,
                        'lead_time': f"{min_lt}-{max_lt}",
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
    # Check for multiple extensions (.grib2, .grb2, and .bin files with 'grib' in name)
    wind_files = list(wind_data_dir.glob("*.grib2")) + \
                 list(wind_data_dir.glob("*.grb2")) + \
                 [f for f in wind_data_dir.glob("*.bin") if 'grib' in f.name.lower()]

    if not wind_files:
        logger.warning(f"No wind GRIB files found in {wind_data_dir}")
        return []

    logger.info(f"Found {len(wind_files)} wind GRIB files:")
    for wind_file in wind_files:
        logger.info(f"  - {wind_file.name}")

    return wind_files


_pool_wind_files: List[Path] = []


def _init_process_pool(wind_files: List[Path]) -> None:
    """ProcessPoolExecutor initializer, stores wind_files in a per-process global."""
    global _pool_wind_files
    _pool_wind_files = wind_files


def _process_lead_time_parallel(args: Tuple) -> Tuple[List[Dict], str]:
    """Wrapper for parallel execution, reads wind_files from the process global."""
    lead_time, rows_at_lead, has_forecast_time, bbox, storm_name, index_dir, output_dir, publish_raster = args
    return _process_lead_time_worker(
        (lead_time, rows_at_lead, has_forecast_time, _pool_wind_files, bbox, storm_name, index_dir, output_dir, publish_raster)
    )


def _process_lead_time_worker(args: Tuple) -> Tuple[List[Dict], str]:
    """
    Worker function to process every ensemble member at a single lead time in
    one call. Must be top-level function for pickling.

    Loads the PF file (covers all 50 perturbed members) and the CF file
    (control member) once each for this lead time, matching the sequential
    path's one-GRIB-open-per-timestep design. The earlier per-track-point
    worker submitted one task per (member, lead_time) pair and re-opened the
    same PF/CF file once per member, doing roughly 25x more GRIB decompression
    work than necessary, since one PF file already contains all 50 members.

    Also persists the wind-speed raster for this lead time directly from
    this worker process to the shared output_dir. Safe: each lead time's
    raster filename is distinct, so no write collision between parallel workers, and this
    avoids pickling the (multi-MB) wind arrays back through the
    ProcessPoolExecutor's own result channel just to write them in the
    parent.

    Args:
        args: Tuple of (lead_time, rows_at_lead, has_forecast_time, wind_files,
              bbox, storm_name, index_dir, output_dir, publish_raster)

    Returns:
        Tuple of (list of envelope records for every member at this lead time,
        log message)
    """
    import traceback

    from ecmwf_wind_data_extractor import load_wind_data_all_members

    lead_time, rows_at_lead, has_forecast_time, wind_files, bbox, storm_name, index_dir, output_dir, publish_raster = args

    try:
        sample = rows_at_lead[0]
        valid_time = sample['valid_time']
        forecast_time = sample.get('forecast_time') if has_forecast_time else None
        ft_suffix = None
        if forecast_time is not None:
            try:
                ft_suffix = pd.to_datetime(forecast_time).strftime('%Y%m%dT%HZ')
            except Exception:
                ft_suffix = None

        pf_file = _match_wind_file(forecast_time, lead_time, 1, wind_files)
        cf_file = _match_wind_file(forecast_time, lead_time, 51, wind_files)

        warnings = []

        pf_results: Dict[int, Dict[int, Optional[str]]] = {}
        if pf_file:
            try:
                pf_wind = load_wind_data_all_members(str(pf_file), bbox, indexpath=index_dir)
                pf_results = _extract_polygons_all_members(pf_wind)
            except Exception as e:
                return [], f"Error loading PF wind at lead {lead_time}h: {e}\n{traceback.format_exc()}"
            # Own try/except, deliberately separate from the polygon-extraction one
            # above. Sharing one try/except meant a raster-save failure here (even though
            # _save_wind_rasters_for_lead_time() is itself verified exception-safe
            # today) would have discarded pf_results and returned early with a
            # misleading "Error loading PF wind" message, losing the real,
            # already-successfully-extracted polygon output for this lead time.
            # Raster output must never be able to take the real polygon output
            # down with it.
            if publish_raster:
                try:
                    _save_wind_rasters_for_lead_time(pf_wind, output_dir, storm_name, ft_suffix, lead_time, 'pf')
                except Exception as e:
                    logger.warning(f"    Could not save PF wind raster at lead {lead_time}h: {e}")
        else:
            warnings.append(f"No PF wind file for lead {lead_time}h")

        cf_results: Dict[int, Dict[int, Optional[str]]] = {}
        if cf_file:
            try:
                cf_wind = load_wind_data_all_members(str(cf_file), bbox, indexpath=index_dir)
                cf_results = _extract_polygons_all_members(cf_wind)
            except Exception as e:
                return [], f"Error loading CF wind at lead {lead_time}h: {e}\n{traceback.format_exc()}"
            if publish_raster:
                try:
                    _save_wind_rasters_for_lead_time(cf_wind, output_dir, storm_name, ft_suffix, lead_time, 'cf')
                except Exception as e:
                    logger.warning(f"    Could not save CF wind raster at lead {lead_time}h: {e}")
        else:
            warnings.append(f"No CF wind file for lead {lead_time}h")

        envelope_records = []
        for row in rows_at_lead:
            ensemble_member = row['ensemble_member']
            row_forecast_time = row.get('forecast_time') if has_forecast_time else None
            grib_number = 0 if ensemble_member == 51 else ensemble_member

            wkt_polygons = (
                cf_results.get(grib_number, {}) if ensemble_member == 51
                else pf_results.get(grib_number, {})
            )

            if not wkt_polygons and ensemble_member == 51 and cf_file:
                warnings.append(f"Member 51: no CF wind polygons at lead {lead_time}h")

            for threshold, wkt_polygon in wkt_polygons.items():
                if wkt_polygon is not None:
                    envelope_records.append({
                        'forecast_time': row_forecast_time,
                        'track_id': storm_name,
                        'ensemble_member': ensemble_member,
                        'valid_time': row['valid_time'],
                        'lead_time': lead_time,
                        'wind_threshold': threshold,
                        'envelope_region': wkt_polygon,
                    })

        log_msg = f"Processed lead {lead_time}h ({len(rows_at_lead)} members)"
        if warnings:
            log_msg += " | " + " | ".join(warnings)
        return envelope_records, log_msg

    except Exception as e:
        return [], f"Error processing lead time {lead_time}h: {e}\n{traceback.format_exc()}"


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
            # Read only the time columns to avoid loading large WKT polygon strings
            df = pd.read_csv(csv_file, usecols=lambda c: c in ('lead_time', 'step'))

            if df.empty:
                continue

            # Prefer lead_time; fall back to step
            if 'lead_time' in df.columns:
                storm_max_hour = df['lead_time'].max()
                max_hour = max(max_hour, storm_max_hour)
                if verbose:
                    logger.info(f"  {csv_file.stem}: max lead time = {storm_max_hour}h")
            elif 'step' in df.columns:
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
        # Add 12-hour buffer for safety, capped at 144h: this cap is a hard
        # external data-availability limit, not a tunable knob: ECMWF only
        # publishes 0.25deg operational ENS wind fields out to 144h, so no
        # request beyond that would return real data anyway.
        uncapped_hour = rounded_hour + 12
        recommended_hour = min(uncapped_hour, 144)

        if verbose:
            logger.info(f"TC data analysis: max forecast hour needed = {max_hour}h")
            logger.info(f"Recommended wind forecast hours: 0 to {recommended_hour}h (every 6h)")
            if uncapped_hour > 144:
                logger.warning(
                    f"Track data needs wind coverage out to {uncapped_hour}h, but ECMWF's "
                    f"0.25deg operational ENS wind fields are only published to 144h -- wind "
                    f"envelope coverage for this storm will have NO data for lead times "
                    f"{150}h through {uncapped_hour}h (in 6h steps). This is a hard upstream "
                    f"data-availability limit, not a bug; the storm's own track/position "
                    f"forecast is unaffected, only wind-hazard coverage beyond 144h."
                )

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
    max_ensemble_members: int = None,
    verbose: bool = True,
    use_process_pool: bool = False,
    max_workers: int = 4,
    publish_raster: bool = False
) -> Dict[str, int]:
    """
    Process wind combination for all TC storms, producing both individual and combined outputs.

    This function:
    1. Extracts wind polygons for each forecast step and ensemble member
    2. Saves individual forecast steps to {storm}_envelopes_individual.csv
    3. Combines polygons of the same wind threshold across all forecast steps
    4. Saves combined polygons to {storm}_envelopes_combined.csv

    Can process track points sequentially or concurrently using ProcessPoolExecutor.

    Args:
        tc_data_dir: Directory containing transformed TC CSV files
        wind_data_dir: Directory containing wind GRIB files
        output_dir: Directory to save envelope CSV files
        buffer_radius_km: Buffer radius around TC tracks in km
        max_ensemble_members: Maximum number of ensemble members to process (for testing)
        verbose: Whether to print detailed progress
        use_process_pool: If True, use ProcessPoolExecutor for concurrent track point processing.
                         Default: False (sequential processing for backward compatibility)
        max_workers: Number of concurrent workers (only used if use_process_pool=True)

    Returns:
        Dictionary with processing statistics
    """
    pool_type = "ProcessPool" if use_process_pool else "Sequential"
    logger.info("=" * 70)
    logger.info(f"ECMWF TC WIND COMBINATION PROCESSING ({pool_type})")
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

            # Limit ensemble members for faster processing: sort before slicing so
            # the slice is deterministic regardless of CSV row order
            unique_members = sorted(tc_data['ensemble_member'].unique())[:max_ensemble_members]  # None = all
            filtered_tc_data = tc_data[tc_data['ensemble_member'].isin(unique_members)]

            if verbose:
                logger.info(f"  Processing {len(unique_members)} ensemble members: {list(unique_members)}")

            # Create storm-specific index directory to avoid concurrent .idx file conflicts
            index_dir = str(wind_data_dir / '.idx_cache' / storm_name)
            os.makedirs(index_dir, exist_ok=True)

            # Process lead times in parallel if enabled. Batches by lead time
            # (not by individual track point) so each worker opens the PF/CF
            # GRIB file for that lead time exactly once and extracts every
            # member from that single load, matching the sequential path's
            # design instead of re-opening the same file once per member.
            if use_process_pool and len(filtered_tc_data) > 1:
                has_forecast_time = 'forecast_time' in filtered_tc_data.columns
                all_rows = filtered_tc_data.to_dict('records')
                unique_lead_times = sorted({r['lead_time'] for r in all_rows})
                rows_by_lead = {
                    lt: [r for r in all_rows if r['lead_time'] == lt]
                    for lt in unique_lead_times
                }

                logger.info(f"  Processing {len(unique_lead_times)} timestep(s) in parallel "
                            f"with {max_workers} workers ({len(all_rows)} member-timesteps total)")

                with ProcessPoolExecutor(
                    max_workers=max_workers,
                    initializer=_init_process_pool,
                    initargs=(wind_files,),
                ) as executor:
                    lead_time_args = [
                        (lead_time, rows_by_lead[lead_time], has_forecast_time, bbox, storm_name, index_dir, output_dir, publish_raster)
                        for lead_time in unique_lead_times
                    ]

                    futures = {
                        executor.submit(_process_lead_time_parallel, args): lead_time
                        for args, lead_time in zip(lead_time_args, unique_lead_times)
                    }

                    for future in as_completed(futures):
                        lead_time = futures[future]
                        try:
                            envelope_records, log_msg = future.result()
                            if log_msg and verbose:
                                logger.info(f"    {log_msg}")
                            storm_envelope_records.extend(envelope_records)
                        except Exception as e:
                            logger.warning(f"    Error processing lead time {lead_time}h: {e}")
            else:
                # Sequential processing: timestep-first to open each GRIB file only once.
                # Each _pf.grib2 file contains all 50 perturbed members; loading it once
                # and extracting all members avoids N_members redundant GRIB decompression
                # cycles per timestep (typically a ~25x speedup over the member-first loop).
                has_forecast_time = 'forecast_time' in filtered_tc_data.columns
                all_rows = filtered_tc_data.to_dict('records')
                unique_lead_times = sorted({r['lead_time'] for r in all_rows})

                if verbose:
                    logger.info(f"  Processing {len(unique_lead_times)} timestep(s) × "
                                f"{len(unique_members)} member(s) (one GRIB open per timestep)")

                for lead_time in unique_lead_times:
                    rows_at_lead = [r for r in all_rows if r['lead_time'] == lead_time]
                    sample = rows_at_lead[0]
                    valid_time = sample['valid_time']
                    forecast_time = sample.get('forecast_time') if has_forecast_time else None
                    ft_suffix = None
                    if forecast_time is not None:
                        try:
                            ft_suffix = pd.to_datetime(forecast_time).strftime('%Y%m%dT%HZ')
                        except Exception:
                            ft_suffix = None

                    if verbose:
                        logger.info(f"  → lead {lead_time}h  ({valid_time})")

                    # Find PF and CF files for this timestep (one of each covers all members)
                    pf_file = _match_wind_file(forecast_time, lead_time, 1, wind_files)
                    cf_file = _match_wind_file(forecast_time, lead_time, 51, wind_files)

                    # Load and extract all members from each file in one open
                    pf_results: Dict[int, Dict[int, Optional[str]]] = {}
                    if pf_file:
                        try:
                            pf_wind = load_wind_data_all_members(str(pf_file), bbox, indexpath=index_dir)
                            pf_results = _extract_polygons_all_members(pf_wind)
                        except Exception as e:
                            logger.warning(f"    Error loading PF wind at lead {lead_time}h: {e}")
                            pf_wind = None
                        # Own try/except, deliberately separate from the polygon-extraction
                        # one above (see the identical fix + rationale in
                        # _process_lead_time_worker(), this same file). Raster output must
                        # never be attributed as (or able to cause) a polygon-load failure.
                        if publish_raster and pf_wind is not None:
                            try:
                                _save_wind_rasters_for_lead_time(pf_wind, output_dir, storm_name, ft_suffix, lead_time, 'pf')
                            except Exception as e:
                                logger.warning(f"    Could not save PF wind raster at lead {lead_time}h: {e}")
                    else:
                        logger.warning(f"    No PF wind file for lead {lead_time}h")

                    cf_results: Dict[int, Dict[int, Optional[str]]] = {}
                    if cf_file:
                        try:
                            cf_wind = load_wind_data_all_members(str(cf_file), bbox, indexpath=index_dir)
                            cf_results = _extract_polygons_all_members(cf_wind)
                        except Exception as e:
                            logger.warning(f"    Error loading CF wind at lead {lead_time}h: {e}")
                            cf_wind = None
                        if publish_raster and cf_wind is not None:
                            try:
                                _save_wind_rasters_for_lead_time(cf_wind, output_dir, storm_name, ft_suffix, lead_time, 'cf')
                            except Exception as e:
                                logger.warning(f"    Could not save CF wind raster at lead {lead_time}h: {e}")
                    else:
                        logger.warning(f"    No CF wind file for lead {lead_time}h")

                    # Create envelope records for every member at this timestep
                    for row in rows_at_lead:
                        ensemble_member = row['ensemble_member']
                        row_forecast_time = row.get('forecast_time') if has_forecast_time else None
                        grib_number = 0 if ensemble_member == 51 else ensemble_member

                        wkt_polygons = (
                            cf_results.get(grib_number, {}) if ensemble_member == 51
                            else pf_results.get(grib_number, {})
                        )

                        if not wkt_polygons and ensemble_member == 51:
                            logger.warning(
                                f"    Member 51: no CF wind polygons at lead {lead_time}h"
                            )

                        for threshold, wkt_polygon in wkt_polygons.items():
                            if wkt_polygon is not None:
                                storm_envelope_records.append({
                                    'forecast_time': row_forecast_time,
                                    'track_id': storm_name,
                                    'ensemble_member': ensemble_member,
                                    'valid_time': row['valid_time'],
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

                    # Validate member count: warn if any threshold has fewer members than expected.
                    # max_ensemble_members doubles as a processing cap AND the "expected" count here:
                    # when it's None (no cap given), there's no real expected value to check against,
                    # so this is a genuine, explicitly-logged skip rather than silently computing
                    # expected=actual_members, which made the mismatch condition permanently False
                    # while still looking like a real check ran.
                    actual_members = combined_df['ensemble_member'].nunique()
                    if max_ensemble_members is None:
                        logger.info(
                            f"  Member count for {storm_name}: {actual_members} "
                            f"(no max_ensemble_members cap given, skipping mismatch check)"
                        )
                    elif actual_members < max_ensemble_members:
                        missing = max_ensemble_members - actual_members
                        logger.warning(
                            f"  Member count mismatch for {storm_name}: "
                            f"expected {max_ensemble_members}, got {actual_members} "
                            f"({missing} member(s) missing -- check GRIB files)"
                        )

                    total_envelope_records += len(combined_records)
                    processed_storms += 1

                    logger.info(f"✓ Successfully processed {storm_name}")
                    logger.info(f"  - Combined records: {len(combined_records)}")
                    logger.info(f"  - Members in output: {actual_members}")
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