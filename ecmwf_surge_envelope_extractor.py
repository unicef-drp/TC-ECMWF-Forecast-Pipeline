#!/usr/bin/env python3
"""
ECMWF Storm Surge Envelope Extractor

Computes coastal storm surge for every ensemble member and lead time from the
ECMWF 10 m wind already downloaded by step 4, and extracts surge threshold
polygons per storm. Mirrors ecmwf_gust_envelope_extractor.py:
  - source: wind_ens_YYYY-MM-DD_rHH_f{lead}h_{pf|cf}.grib2 (u10/v10, step 4)
  - surge:  CLIMADA TCSurgeBathtub method (see bathtub_surge below)
  - thresholds: SURGE_THRESHOLDS_M (keys are integer cm labels)
  - output: TC_SURGE_ENVELOPES_INDIVIDUAL / TC_SURGE_ENVELOPES_COMBINED

Surge method
    Same formulation as CLIMADA Petals TCSurgeBathtub.from_tc_winds
    (climada_petals/hazard/tc_surge_bathtub.py), re-implemented here with
    numpy only so the pipeline does not depend on CLIMADA at run time:
      1. wind-to-surge: linear relation from Xu (2010), fitted on SLOSH runs
            surge = 0.1023 * max(V - 26.8224, 0) + 1.8288        [m, V in m/s]
      2. subtract ground elevation and an inland decay of 0.2 m per km of
         distance to the coast (Pielke & Pielke 1997)
      3. keep coastal land only: elevation 0 to 10 m, within 50 km of the coast
      4. negative values are set to 0
    Equivalence with CLIMADA is checked by tests/test_surge_bathtub.py.

Elevation
    Coastal lowland cells (elevation + distance to coast) are read from the
    cache built once by setup_surge_dem.py (SRTM through GigaSpatial, 15"
    grid by default). The surge is computed on that grid, not on the 0.25
    degree wind grid, so the footprint follows the real coastline.
"""

import argparse
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from rasterio.features import shapes
from rasterio.windows import Window, from_bounds
from shapely.geometry import MultiPolygon, shape
from shapely.ops import unary_union

from ecmwf_wind_data_extractor import (
    create_buffered_track_polygon,
    get_bounding_box,
    get_longitude_windows,
    load_wind_data_all_members,
    polygon_to_wkt,
)
from ecmwf_tc_wind_combination import (
    find_tc_data_files,
    load_tc_track_data,
    _match_wind_file,
    _resolve_run_forecast_time,
    _synthesize_rows_by_lead,
    _wind_lead_times_for_run,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Surge thresholds in m (keys are integer cm labels, stored in SURGE_THRESHOLD)
SURGE_THRESHOLDS_M: Dict[int, float] = {
    50: 0.5,
    100: 1.0,
    200: 2.0,
    300: 3.0,
}

# Wind below 34 kt is not tropical-cyclone wind: no surge is computed there.
# Same value as the lowest AOS wind threshold and CLIMADA TropCyclone's own
# intensity threshold (17.5 m/s).
MIN_TC_WIND_MS = 17.49

# CLIMADA TCSurgeBathtub defaults
INLAND_DECAY_RATE_M_PER_KM = 0.2
SEA_LEVEL_RISE_M = 0.0
MIN_ELEVATION_M = 0.0     # CLIMADA drops cells below 0 m (assumed protected)
MAX_ELEVATION_M = 10.0    # CLIMADA MAX_ELEVATION
MAX_DIST_COAST_KM = 50.0  # CLIMADA MAX_DIST_COAST

# Same spatial crop as the wind step (step 5)
TRACK_BUFFER_KM = 500
BBOX_BUFFER_DEG = 2.0

# Must be kept in sync with STAGE_PREFIX / OUTPUT_FILENAME in setup_surge_dem.py
SURGE_DEM_STAGE_PREFIX = "surge/coastal_dem/v1"
SURGE_DEM_FILENAME = "surge_coastal_dem.tif"


# ---------------------------------------------------------------------------
# Surge formula
# ---------------------------------------------------------------------------

def bathtub_surge(wind_ms: np.ndarray, elevation_m: np.ndarray, dist_coast_km: np.ndarray,
                  inland_decay_rate: float = INLAND_DECAY_RATE_M_PER_KM,
                  sea_level_rise: float = SEA_LEVEL_RISE_M,
                  min_elevation: float = MIN_ELEVATION_M,
                  max_elevation: float = MAX_ELEVATION_M,
                  max_dist_km: float = MAX_DIST_COAST_KM) -> np.ndarray:
    """Surge height in m, CLIMADA TCSurgeBathtub formulation.

    wind_ms: (..., n_cells) wind speed, already set to 0 below MIN_TC_WIND_MS.
    elevation_m, dist_coast_km: (n_cells,) cell elevation and distance to coast.
    """
    wind_ms = np.asarray(wind_ms, dtype=np.float64)
    coastal = ((elevation_m >= min_elevation)
               & (elevation_m <= max_elevation + sea_level_rise)
               & (dist_coast_km <= max_dist_km))

    surge = 0.1023 * np.fmax(wind_ms - 26.8224, 0.0) + 1.8288
    surge -= elevation_m + inland_decay_rate * dist_coast_km - sea_level_rise
    surge = np.fmax(surge, 0.0)
    return np.where((wind_ms > 0) & coastal, surge, 0.0)


# ---------------------------------------------------------------------------
# Coastal elevation cache
# ---------------------------------------------------------------------------

def resolve_surge_dem(source: str, local_dir, snowflake_conn=None,
                      snowflake_stage_name: Optional[str] = None,
                      cache_dir: Optional[Path] = None,
                      blob_account_url: Optional[str] = None, blob_sas_token: Optional[str] = None,
                      blob_container: Optional[str] = None) -> Path:
    """Return a local path to the surge DEM cache (same source logic as the JRC cache)."""
    source = source.lower()
    if source == "local":
        path = Path(local_dir) / SURGE_DEM_FILENAME
        if path.exists():
            return path
        raise FileNotFoundError(f"{SURGE_DEM_FILENAME} not found in {local_dir} (SURGE_DEM_SOURCE=local). "
                                f"Run setup_surge_dem.py --local-only {local_dir} first.")

    cache_dir = Path(cache_dir) if cache_dir else Path("surge_dem_cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / SURGE_DEM_FILENAME
    if path.exists():
        return path

    if source == "snowflake":
        if not snowflake_conn or not snowflake_stage_name:
            raise ValueError("snowflake_conn and snowflake_stage_name required for SURGE_DEM_SOURCE=snowflake")
        cursor = snowflake_conn.cursor()
        try:
            cursor.execute(f"GET @{snowflake_stage_name}/{SURGE_DEM_STAGE_PREFIX}/{SURGE_DEM_FILENAME} "
                           f"'file://{cache_dir.resolve().as_posix()}'")
        finally:
            cursor.close()
    elif source == "blob":
        if not (blob_account_url and blob_sas_token and blob_container):
            raise ValueError("ACCOUNT_URL, SAS_TOKEN and CONTAINER_NAME required for SURGE_DEM_SOURCE=blob")
        from glofas_downloader import download_from_blob
        download_from_blob(blob_account_url, blob_sas_token, blob_container,
                           f"{SURGE_DEM_STAGE_PREFIX}/{SURGE_DEM_FILENAME}", path)
    else:
        raise ValueError(f"Unknown SURGE_DEM_SOURCE: {source} (expected local, snowflake or blob)")

    if not path.exists():
        raise FileNotFoundError(f"{SURGE_DEM_FILENAME} not found on {source} at "
                                f"{SURGE_DEM_STAGE_PREFIX}/. Run setup_surge_dem.py first.")
    return path


class CoastalCells:
    """Low-lying coastal cells of the DEM cache inside one longitude window."""

    def __init__(self, dem_path: Path, window_bbox: Dict[str, float]):
        with rasterio.open(dem_path) as src:
            b = src.bounds
            overlaps = not (window_bbox['lon_max'] <= b.left or window_bbox['lon_min'] >= b.right
                            or window_bbox['lat_max'] <= b.bottom or window_bbox['lat_min'] >= b.top)
            if overlaps:
                win = from_bounds(window_bbox['lon_min'], window_bbox['lat_min'],
                                  window_bbox['lon_max'], window_bbox['lat_max'], src.transform)
                win = win.round_offsets().round_lengths().intersection(Window(0, 0, src.width, src.height))
                elev = src.read(1, window=win)
                dist = src.read(2, window=win)
                self.transform = src.window_transform(win)
            else:
                # Storm area outside the cache extent (e.g. a regional cache): no coastal cells
                elev = dist = np.full((0, 0), np.nan, dtype=np.float32)
                self.transform = src.transform
        self.shape = elev.shape
        rows, cols = np.nonzero(np.isfinite(elev) & np.isfinite(dist))
        self.rows, self.cols = rows, cols
        self.elevation = elev[rows, cols].astype(np.float64)
        self.dist_km = dist[rows, cols].astype(np.float64)
        xs, ys = rasterio.transform.xy(self.transform, rows, cols, offset="center")
        self.lon, self.lat = np.asarray(xs), np.asarray(ys)

    def __len__(self):
        return len(self.rows)

    def wind_at_cells(self, wind: xr.DataArray) -> np.ndarray:
        """Bilinear interpolation of the 0.25 degree wind at cell centres.
        Returns (..., n_cells), 0 outside the wind window and below MIN_TC_WIND_MS."""
        pts = wind.interp(latitude=xr.DataArray(self.lat, dims="cell"),
                          longitude=xr.DataArray(self.lon, dims="cell"),
                          method="linear")
        values = np.nan_to_num(pts.values, nan=0.0)
        values[values < MIN_TC_WIND_MS] = 0.0
        return values

    def polygons(self, surge: np.ndarray, thresholds: Dict[int, float]) -> Dict[int, Optional[object]]:
        """Union of the grid cells at or above each threshold, as (Multi)Polygons."""
        out = {}
        for label, value in thresholds.items():
            hit = surge >= value
            if not hit.any():
                out[label] = None
                continue
            r, c = self.rows[hit], self.cols[hit]
            r0, c0 = r.min(), c.min()
            mask = np.zeros((r.max() - r0 + 1, c.max() - c0 + 1), dtype=np.uint8)
            mask[r - r0, c - c0] = 1
            sub_transform = self.transform * rasterio.Affine.translation(c0, r0)
            geoms = [shape(g) for g, v in shapes(mask, mask=mask.astype(bool), transform=sub_transform) if v == 1]
            # shapes() already returns one polygon per connected region, never
            # overlapping, so no (slow) union is needed here
            out[label] = (geoms[0] if len(geoms) == 1 else MultiPolygon(geoms)) if geoms else None
        return out


# ---------------------------------------------------------------------------
# Output records
# ---------------------------------------------------------------------------

def _polygons_to_records(cells: List[CoastalCells], member_surge: List[Optional[np.ndarray]],
                         thresholds: Dict[int, float], base: Dict) -> List[Dict]:
    """One record per threshold reached, polygons merged across longitude windows."""
    merged: Dict[int, List] = {k: [] for k in thresholds}
    for c, surge in zip(cells, member_surge):
        if surge is None or not (surge >= min(thresholds.values())).any():
            continue
        for label, poly in c.polygons(surge, thresholds).items():
            if poly is not None:
                merged[label].append(poly)

    records = []
    for label, polys in merged.items():
        if not polys:
            continue
        geom = polys[0] if len(polys) == 1 else unary_union(polys)
        wkt_poly = polygon_to_wkt(geom)
        if wkt_poly:
            records.append({**base, 'surge_threshold': label, 'envelope_region': wkt_poly})
    return records


# ---------------------------------------------------------------------------
# Main entry point (called by pipeline_core.step5c_extract_surge_envelopes)
# ---------------------------------------------------------------------------

def process_surge_combination(
    tc_data_dir,
    wind_data_dir,
    output_dir,
    dem_path,
    thresholds: Dict[int, float] = SURGE_THRESHOLDS_M,
    verbose: bool = False,
) -> Dict:
    """
    Extract surge envelope polygons for all TC storms and write individual + combined CSVs.

    Args:
        tc_data_dir: Directory containing transformed TC CSV files (step 3)
        wind_data_dir: Directory containing wind GRIB files (step 4, wind_ens_* prefix)
        output_dir: Directory to write output CSV files
        dem_path: Local path to the surge DEM cache (see resolve_surge_dem)
        thresholds: Surge thresholds, {cm label: m}

    Returns:
        dict with 'processed_storms', 'total_envelope_files' and 'timings'
    """
    tc_data_dir, wind_data_dir, output_dir = Path(tc_data_dir), Path(wind_data_dir), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("ECMWF STORM SURGE ENVELOPE EXTRACTION (bathtub)")
    logger.info("=" * 70)

    wind_files = [f for f in wind_data_dir.glob('wind_ens_*.grib2')]
    if not wind_files:
        logger.warning(f"No wind files found in {wind_data_dir}")
        return {'processed_storms': 0, 'total_envelope_files': 0, 'timings': {}}

    storm_configs = find_tc_data_files(tc_data_dir)
    if not storm_configs:
        logger.warning("No TC data files found -- nothing to process")
        return {'processed_storms': 0, 'total_envelope_files': 0, 'timings': {}}

    processed_storms, total_files, timings = 0, 0, {}

    # One GDAL environment for the whole step: rasterio.features.shapes() would
    # otherwise open (and tear down) a new one on every call, thousands per storm.
    with rasterio.Env():
        for storm_config in storm_configs:
            storm_name = storm_config['storm_name']
            logger.info(f"\n{'=' * 50}\nSurge envelopes for storm: {storm_name}\n{'=' * 50}")
            t_storm = time.time()
            try:
                total_files += _process_storm(storm_name, storm_config['track_csv'], wind_files,
                                              Path(dem_path), output_dir, thresholds, verbose)
                processed_storms += 1
                timings[storm_name] = round(time.time() - t_storm, 1)
                logger.info(f"  {storm_name}: {timings[storm_name]} s")
            except Exception as e:
                logger.error(f"  Surge extraction failed for {storm_name}: {e}", exc_info=verbose)

    logger.info(f"Surge: {processed_storms} storm(s), {total_files} file(s) written in {output_dir}")
    return {'processed_storms': processed_storms, 'total_envelope_files': total_files, 'timings': timings}


def _process_storm(storm_name: str, track_csv: str, wind_files: List[Path], dem_path: Path,
                   output_dir: Path, thresholds: Dict[int, float], verbose: bool) -> int:
    """Surge envelopes for one storm. Returns the number of CSV files written."""
    n_files = 0
    track_df = load_tc_track_data(track_csv)
    bbox = get_bounding_box(create_buffered_track_polygon(track_df, TRACK_BUFFER_KM),
                            buffer=BBOX_BUFFER_DEG)
    lon_windows = get_longitude_windows(bbox)

    cells = [CoastalCells(dem_path, w) for w in lon_windows]
    n_cells = sum(len(c) for c in cells)
    logger.info(f"  {n_cells:,} coastal lowland cells in the storm area")
    if n_cells == 0:
        logger.info("  No low-lying coast near this storm -- no surge envelopes")
        return 0

    has_ft = 'forecast_time' in track_df.columns
    run_ft = _resolve_run_forecast_time(track_df, has_ft)
    lead_times = _wind_lead_times_for_run(run_ft, wind_files) if run_ft is not None \
        else sorted(track_df['lead_time'].unique())
    members = sorted(track_df['ensemble_member'].unique())
    rows_by_lead = _synthesize_rows_by_lead(lead_times, members, run_ft)

    records: List[Dict] = []
    max_surge: Dict[int, List[Optional[np.ndarray]]] = {}
    lead_times_seen: Dict[int, List[int]] = {}
    for lead_time in lead_times:
        pf_file = _match_wind_file(run_ft, lead_time, 1, wind_files)
        cf_file = _match_wind_file(run_ft, lead_time, 51, wind_files)
        pf_windows = load_wind_data_all_members(str(pf_file), bbox) if pf_file else []
        cf_windows = load_wind_data_all_members(str(cf_file), bbox) if cf_file else []
        if not pf_windows and not cf_windows:
            logger.warning(f"    No wind file for lead {lead_time}h")
            continue

        # Surge for all members at once, per longitude window:
        # {member: [surge array per window]}
        surge_by_member: Dict[int, List[Optional[np.ndarray]]] = {}
        for i, c in enumerate(cells):
            if len(c) == 0:
                continue
            if pf_windows and 'number' in pf_windows[i].dims:
                pf_surge = bathtub_surge(c.wind_at_cells(pf_windows[i]), c.elevation, c.dist_km)
                for k, number in enumerate(pf_windows[i]['number'].values):
                    surge_by_member.setdefault(int(number), [None] * len(cells))[i] = pf_surge[k]
            if cf_windows:
                surge_by_member.setdefault(51, [None] * len(cells))[i] = bathtub_surge(
                    c.wind_at_cells(cf_windows[i]), c.elevation, c.dist_km)

        for row in rows_by_lead[lead_time]:
            member = row['ensemble_member']
            member_surge = surge_by_member.get(member)
            if member_surge is None:
                continue
            lead_times_seen.setdefault(member, []).append(lead_time)

            # Running maximum over lead times: the combined envelope
            # (union over steps of surge >= threshold) is exactly the
            # envelope of the maximum surge, cell by cell
            peak = max_surge.setdefault(member, [None] * len(cells))
            for i, surge in enumerate(member_surge):
                if surge is not None:
                    peak[i] = surge if peak[i] is None else np.maximum(peak[i], surge)

            records.extend(_polygons_to_records(cells, member_surge, thresholds, {
                'forecast_time': row['forecast_time'],
                'track_id': storm_name,
                'ensemble_member': member,
                'valid_time': row['valid_time'],
                'lead_time': lead_time,
            }))
        if verbose:
            logger.info(f"  lead {lead_time}h: {len(records)} records so far")

    ft_suffix = pd.to_datetime(run_ft).strftime('%Y%m%dT%HZ') if run_ft is not None else None
    stem = f"{storm_name}_{ft_suffix}" if ft_suffix else storm_name

    if records:
        individual_file = output_dir / f"{stem}_surge_envelopes_individual.csv"
        pd.DataFrame(records).to_csv(individual_file, index=False)
        logger.info(f"  Saved surge individual: {individual_file.name} ({len(records)} records)")
        n_files += 1

        combined = []
        for member, peak in max_surge.items():
            lts = lead_times_seen[member]
            combined.extend(_polygons_to_records(cells, peak, thresholds, {
                'forecast_time': run_ft,
                'track_id': storm_name,
                'ensemble_member': member,
                'lead_time': f"{min(lts)}-{max(lts)}",
            }))
        if combined:
            combined_file = output_dir / f"{stem}_surge_envelopes_combined.csv"
            pd.DataFrame(combined).to_csv(combined_file, index=False)
            logger.info(f"  Saved surge combined: {combined_file.name} ({len(combined)} records)")
            n_files += 1
    else:
        logger.info("  No surge above the lowest threshold for any member -- no files written")

    return n_files


def main():
    parser = argparse.ArgumentParser(description="Standalone run of the storm surge step (for testing)")
    parser.add_argument("--tc-dir", default=os.getenv('TRANSFORMED_DATA_DIR', 'tc_data_transformed'))
    parser.add_argument("--wind-dir", default=os.getenv('WIND_DATA_DIR', 'wind_data'))
    parser.add_argument("--output-dir", default=os.getenv('WIND_EXTRACTED_DIR', 'wind_extracted'))
    parser.add_argument("--dem", default=str(Path(os.getenv('SURGE_DEM_LOCAL_DIR', 'surge_dem')) / SURGE_DEM_FILENAME))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    process_surge_combination(args.tc_dir, args.wind_dir, args.output_dir, args.dem, verbose=args.verbose)


if __name__ == "__main__":
    main()
