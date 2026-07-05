#!/usr/bin/env python3
"""
ECMWF Gust Envelope Extractor

Reads per-lead-time 10fg (maximum wind gust over each 6h period) GRIB files
and extracts gust threshold contour polygons per storm. Mirrors
ecmwf_tc_wind_combination.py but for peak gusts:
  - source: gust_ens_YYYYMMDD_rHH_f{lead}h_{pf|cf}.grib2
  - field:  10fg scalar (no u/v combination needed)
  - thresholds: GUST_THRESHOLDS_MS (m/s)
  - output: TC_GUST_ENVELOPES_INDIVIDUAL / TC_GUST_ENVELOPES_COMBINED

Thresholds mirror Saffir-Simpson equivalent kt categories converted to m/s:
  17 ≈ 34 kt  21 ≈ 40 kt  26 ≈ 50 kt  33 ≈ 64 kt
  43 ≈ 83 kt  49 ≈ 96 kt  58 ≈ 113 kt  70 ≈ 137 kt
"""

import logging
import re
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Dict, List, Optional
from shapely.ops import unary_union
from shapely import wkt as shapely_wkt

from ecmwf_wind_data_extractor import (
    create_buffered_track_polygon,
    create_wind_threshold_contours,
    get_bounding_box,
    polygon_to_wkt,
)
from ecmwf_tc_wind_combination import find_tc_data_files, load_tc_track_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Gust thresholds in m/s (keys are integer m/s labels; values are the actual speed thresholds)
GUST_THRESHOLDS_MS: Dict[int, float] = {
    17: 17.49,
    21: 20.58,
    26: 25.72,
    33: 32.92,
    43: 42.70,
    49: 49.39,
    58: 58.12,
    70: 70.48,
}

# Skip step 0: 10fg at T+0 covers no accumulation period
_SKIP_STEP_ZERO = True


def load_gust_data_all_members(grib_file: str, bbox: Dict) -> xr.DataArray:
    """
    Load 10fg (max wind gust) from a gust GRIB file and crop to bounding box.

    For PF files the returned DataArray has a 'number' dimension (members 1-50).
    For CF files there is no 'number' dimension.

    Args:
        grib_file: Path to gust GRIB2 file
        bbox: Bounding box dict with keys 'lat_min', 'lat_max', 'lon_min', 'lon_max'

    Returns:
        Loaded, cropped DataArray for the 10fg field
    """
    ds = xr.open_dataset(
        grib_file,
        engine='cfgrib',
        backend_kwargs={'errors': 'ignore', 'indexpath': ''},
    )
    try:
        # cfgrib decodes 10fg (steps ≤ 90h) as 'fg10' and 10fg3 (steps > 90h) as 'fg310'
        if 'fg10' in ds.data_vars:
            ds_var = ds['fg10']
        elif 'fg310' in ds.data_vars:
            ds_var = ds['fg310']
        else:
            first_var = list(ds.data_vars)[0]
            logger.debug(f"Expected gust var not found in {Path(grib_file).name}, using '{first_var}'")
            ds_var = ds[first_var]

        # Crop to bounding box (latitude sliced high→low to match GRIB convention)
        ds_var = ds_var.sel(
            latitude=slice(bbox['lat_max'], bbox['lat_min']),
            longitude=slice(bbox['lon_min'], bbox['lon_max']),
        )
        ds_var = ds_var.load()
        return ds_var
    finally:
        ds.close()


def _match_gust_file(lead_time: int, file_type: str, gust_files: List[str]) -> Optional[str]:
    """
    Find the gust GRIB file matching the given lead time and type (pf or cf).

    Args:
        lead_time: Forecast lead time in hours
        file_type: 'pf' or 'cf'
        gust_files: List of gust GRIB file paths (str)

    Returns:
        Path string of matching file, or None
    """
    target_suffix = f'_f{lead_time:03d}h_{file_type}.grib2'
    for f in gust_files:
        name = Path(f).name
        if name.startswith('gust_ens_') and name.endswith(target_suffix):
            return f
    return None


def combine_gust_polygons_across_forecast_steps(all_step_records: List[Dict]) -> List[Dict]:
    """
    Union gust envelope polygons of the same threshold across all forecast steps per member.

    Args:
        all_step_records: List of individual gust envelope records (one per member × step × threshold)

    Returns:
        List of combined records (one per member × threshold) with unioned geometry
    """
    grouped_data: Dict = {}
    lead_time_range: Dict = {}

    for record in all_step_records:
        member = record['ensemble_member']
        threshold = record['gust_threshold']
        key = (member, threshold)
        lt = record['lead_time']

        if key not in grouped_data:
            grouped_data[key] = {
                'forecast_time': record['forecast_time'],
                'track_id': record['track_id'],
                'ensemble_member': member,
                'valid_time': record['valid_time'],
                'gust_threshold': threshold,
                'polygons': [],
            }
            lead_time_range[key] = (lt, lt)
        else:
            min_lt, max_lt = lead_time_range[key]
            lead_time_range[key] = (min(min_lt, lt), max(max_lt, lt))

        if record['envelope_region']:
            try:
                polygon = shapely_wkt.loads(record['envelope_region'])
                if polygon and not polygon.is_empty:
                    grouped_data[key]['polygons'].append(polygon)
            except Exception as e:
                logger.warning(
                    f"Error parsing gust polygon for member {member}, threshold {threshold}: {e}"
                )

    combined_records = []

    for (member, threshold), data in grouped_data.items():
        if data['polygons']:
            try:
                combined_polygon = unary_union(data['polygons'])
                # unary_union can return GEOMETRYCOLLECTION; buffer(0) normalises to (Multi)Polygon
                if combined_polygon.geom_type not in ('Polygon', 'MultiPolygon'):
                    combined_polygon = combined_polygon.buffer(0)

                combined_wkt = polygon_to_wkt(combined_polygon)

                if combined_wkt:
                    min_lt, max_lt = lead_time_range[(member, threshold)]
                    combined_records.append({
                        'forecast_time': data['forecast_time'],
                        'track_id': data['track_id'],
                        'ensemble_member': member,
                        'lead_time': f"{min_lt}-{max_lt}",
                        'gust_threshold': threshold,
                        'envelope_region': combined_wkt,
                    })
                    logger.debug(
                        f"Combined {len(data['polygons'])} gust polygons "
                        f"for member {member}, threshold {threshold}"
                    )

            except Exception as e:
                logger.warning(
                    f"Error combining gust polygons for member {member}, threshold {threshold}: {e}"
                )

    return combined_records


def process_gust_combination(
    tc_data_dir,
    gust_data_dir,
    output_dir,
    buffer_radius_km: int = 1000,
    verbose: bool = False,
) -> Dict:
    """
    Extract gust envelope polygons for all TC storms and write individual + combined CSVs.

    Sequential implementation (no ProcessPoolExecutor).

    Args:
        tc_data_dir: Directory containing transformed TC CSV files
        gust_data_dir: Directory containing gust GRIB files (gust_ens_* prefix)
        output_dir: Directory to write output CSV files
        buffer_radius_km: Buffer radius around TC track for spatial crop (default 1000 km —
            larger than the wind extractor's 500 km because the 17 m/s gust threshold
            extends ~1.3× further than the equivalent sustained-wind radius)
        verbose: Whether to log extra detail

    Returns:
        dict with 'processed_storms' and 'total_envelope_files'
    """
    tc_data_dir = Path(tc_data_dir)
    gust_data_dir = Path(gust_data_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("ECMWF GUST ENVELOPE EXTRACTION")
    logger.info("=" * 70)

    # Find available gust PF files to determine lead times on disk
    pf_files = sorted(gust_data_dir.glob('gust_ens_*_pf.grib2'))
    if not pf_files:
        logger.warning(f"No gust PF files found in {gust_data_dir}")
        return {'processed_storms': 0, 'total_envelope_files': 0}

    available_lead_times = []
    for f in pf_files:
        m = re.search(r'_f(\d+)h_', f.name)
        if m:
            lt = int(m.group(1))
            if not (_SKIP_STEP_ZERO and lt == 0):
                available_lead_times.append(lt)
    available_lead_times = sorted(set(available_lead_times))
    logger.info(f"Available gust lead times: {available_lead_times}")

    # Collect all gust file paths (both pf and cf)
    gust_files = [str(f) for f in gust_data_dir.glob('gust_ens_*.grib2')]

    # Find storm CSV files
    storm_configs = find_tc_data_files(tc_data_dir)
    if not storm_configs:
        logger.warning("No TC data files found — nothing to process")
        return {'processed_storms': 0, 'total_envelope_files': 0}

    processed_storms = 0
    total_envelope_files = 0

    for storm_config in storm_configs:
        storm_name = storm_config['storm_name']
        track_csv = storm_config['track_csv']

        logger.info(f"\n{'=' * 50}")
        logger.info(f"Gust envelopes for storm: {storm_name}")
        logger.info(f"{'=' * 50}")

        try:
            track_df = load_tc_track_data(track_csv)
            logger.info(f"  Loaded {len(track_df)} track points")

            track_polygon = create_buffered_track_polygon(track_df, buffer_radius_km)
            bbox = get_bounding_box(track_polygon, buffer=4.0)

            has_forecast_time = 'forecast_time' in track_df.columns
            all_rows = track_df.to_dict('records')

            # Restrict to lead times present in both track data and on disk
            track_lead_times = {r['lead_time'] for r in all_rows}
            lead_times_to_process = sorted(
                lt for lt in available_lead_times if lt in track_lead_times
            )

            if verbose:
                logger.info(f"  Processing lead times: {lead_times_to_process}")

            storm_records: List[Dict] = []

            for lead_time in lead_times_to_process:
                rows_at_lead = [r for r in all_rows if r.get('lead_time') == lead_time]
                if not rows_at_lead:
                    continue

                if verbose:
                    logger.info(f"  → lead {lead_time}h")

                pf_file = _match_gust_file(lead_time, 'pf', gust_files)
                cf_file = _match_gust_file(lead_time, 'cf', gust_files)

                # Load PF gust DataArray (all members 1-50)
                pf_da: Optional[xr.DataArray] = None
                if pf_file:
                    try:
                        pf_da = load_gust_data_all_members(pf_file, bbox)
                    except Exception as e:
                        logger.warning(f"    Error loading PF gust at lead {lead_time}h: {e}")
                else:
                    logger.warning(f"    No PF gust file for lead {lead_time}h")

                # Load CF gust DataArray (member 51)
                cf_da: Optional[xr.DataArray] = None
                if cf_file:
                    try:
                        cf_da = load_gust_data_all_members(cf_file, bbox)
                    except Exception as e:
                        logger.warning(f"    Error loading CF gust at lead {lead_time}h: {e}")
                else:
                    logger.warning(f"    No CF gust file for lead {lead_time}h")

                # Process each member at this lead time
                for row in rows_at_lead:
                    ensemble_member = row['ensemble_member']
                    row_forecast_time = row.get('forecast_time') if has_forecast_time else None
                    valid_time = row['valid_time']

                    if ensemble_member == 51:
                        # Control member — use CF DataArray directly (no 'number' dim)
                        member_da = cf_da
                    else:
                        # Perturbed member 1-50 — select by GRIB number
                        if pf_da is None:
                            continue
                        grib_number = ensemble_member  # grib member == pipeline member for PF
                        try:
                            if 'number' in pf_da.dims:
                                member_da = pf_da.sel(number=grib_number)
                            else:
                                # PF file decoded without number dim (single-member edge case)
                                member_da = pf_da
                        except Exception as e:
                            logger.warning(
                                f"    Error selecting member {ensemble_member} at lead {lead_time}h: {e}"
                            )
                            continue

                    if member_da is None:
                        continue

                    try:
                        contours = create_wind_threshold_contours(member_da, GUST_THRESHOLDS_MS)
                    except Exception as e:
                        logger.warning(
                            f"    Error extracting gust contours for member {ensemble_member} "
                            f"at lead {lead_time}h: {e}"
                        )
                        continue

                    for threshold, polygon in contours.items():
                        wkt_polygon = polygon_to_wkt(polygon)
                        if wkt_polygon is not None:
                            storm_records.append({
                                'forecast_time': row_forecast_time,
                                'track_id': storm_name,
                                'ensemble_member': ensemble_member,
                                'valid_time': valid_time,
                                'lead_time': lead_time,
                                'gust_threshold': threshold,
                                'envelope_region': wkt_polygon,
                            })

            if storm_records:
                individual_df = pd.DataFrame(storm_records)

                # Derive forecast issuance time for filename suffix
                try:
                    first_ft = (
                        pd.to_datetime(individual_df['forecast_time'].iloc[0])
                        if 'forecast_time' in individual_df.columns
                        else None
                    )
                except Exception:
                    first_ft = None

                if first_ft is not None and not pd.isna(first_ft):
                    ft_suffix = first_ft.strftime('%Y%m%dT%HZ')
                    individual_file = output_dir / f"{storm_name}_{ft_suffix}_gust_envelopes_individual.csv"
                else:
                    individual_file = output_dir / f"{storm_name}_gust_envelopes_individual.csv"

                individual_df.to_csv(individual_file, index=False)
                logger.info(
                    f"  Saved gust individual: {individual_file.name} "
                    f"({len(storm_records)} records)"
                )
                total_envelope_files += 1

                # Combine polygons across forecast steps
                combined_records = combine_gust_polygons_across_forecast_steps(storm_records)
                if combined_records:
                    combined_df = pd.DataFrame(combined_records)
                    if first_ft is not None and not pd.isna(first_ft):
                        ft_suffix = first_ft.strftime('%Y%m%dT%HZ')
                        combined_file = output_dir / f"{storm_name}_{ft_suffix}_gust_envelopes_combined.csv"
                    else:
                        combined_file = output_dir / f"{storm_name}_gust_envelopes_combined.csv"
                    combined_df.to_csv(combined_file, index=False)
                    logger.info(
                        f"  Saved gust combined: {combined_file.name} "
                        f"({len(combined_records)} records)"
                    )
                    total_envelope_files += 1
                else:
                    logger.warning(f"  No combined gust polygons for {storm_name}")

                processed_storms += 1

            else:
                logger.warning(f"  No gust envelope records for {storm_name}")

        except Exception as e:
            logger.error(f"Error processing gust envelopes for {storm_name}: {e}")

    logger.info("\n" + "=" * 70)
    logger.info("GUST ENVELOPE EXTRACTION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Storms processed: {processed_storms}/{len(storm_configs)}")
    logger.info(f"Output files written: {total_envelope_files}")
    logger.info(f"Output directory: {output_dir}")

    return {
        'processed_storms': processed_storms,
        'total_envelope_files': total_envelope_files,
    }
