#!/usr/bin/env python3
"""
ECMWF Storm Surge Raster Extractor

Computes coastal storm surge for every ensemble member and lead time from the
ECMWF 10 m wind already downloaded by step 4, and writes, for each storm and
forecast cycle, one GeoTIFF of the maximum surge depth per member:
  - source: wind_ens_YYYY-MM-DD_rHH_f{lead}h_{pf|cf}.grib2 (u10/v10, step 4)
  - surge:  CLIMADA TCSurgeBathtub method (see bathtub_surge below)
  - output: {storm}_{YYYYMMDDTHHZ}_surge_raster.tif, one band per ensemble
            member (bands 1-50: perturbed members 1-50, band 51: control),
            maximum surge depth above ground in m over all lead times. No
            threshold is applied: the exposure step chooses its own.

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

Elevation and output grid
    Coastal lowland cells (elevation + distance to coast) are read from the
    cache built once by setup_surge_dem.py (DeltaDTM coastal terrain model, 15"
    grid by default). The surge is computed on that grid, not on the 0.25
    degree wind grid, so the footprint follows the real coastline, and the
    raster is written on the same grid, cropped to the coastal cells of the
    storm area:
      - coastal cell:        maximum surge depth in m (0 = not flooded)
      - everything else:     nodata (NaN): sea, inland or higher ground
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
from rasterio.windows import Window, from_bounds
from rasterio.windows import transform as window_transform

from ecmwf_wind_data_extractor import (
    create_buffered_track_polygon,
    get_bounding_box,
    get_longitude_windows,
    load_wind_data_all_members,
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

# Pipeline member numbering (same as the tracks): 1-50 perturbed, 51 control
CONTROL_MEMBER = 51

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
            self.elevation_source = src.tags().get('elevation_source', 'unknown')
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


# ---------------------------------------------------------------------------
# Raster output
# ---------------------------------------------------------------------------

def write_surge_raster(cells: CoastalCells, peaks: Dict[int, Optional[np.ndarray]],
                       members: List[int], output_path: Path, tags: Dict) -> None:
    """Write one multi-band GeoTIFF of maximum surge depth (m), one band per member.

    The raster is on the DEM cache grid, cropped to the bounding box of the
    coastal cells. Coastal cells hold the surge depth (0 when not flooded),
    every other pixel is nodata (NaN). A member without any wind file gets an
    all-nodata band. Bands are written one at a time to keep memory low.
    """
    r0, r1 = int(cells.rows.min()), int(cells.rows.max())
    c0, c1 = int(cells.cols.min()), int(cells.cols.max())
    height, width = r1 - r0 + 1, c1 - c0 + 1
    rr, cc = cells.rows - r0, cells.cols - c0

    profile = {
        'driver': 'GTiff', 'height': height, 'width': width, 'count': len(members),
        'dtype': 'float32', 'crs': 'EPSG:4326',
        'transform': window_transform(Window(c0, r0, width, height), cells.transform),
        'nodata': np.nan, 'compress': 'deflate', 'predictor': 3,
        'tiled': True, 'blockxsize': 256, 'blockysize': 256, 'BIGTIFF': 'IF_SAFER',
    }
    band = np.full((height, width), np.nan, dtype=np.float32)
    with rasterio.open(output_path, 'w', **profile) as dst:
        for i, member in enumerate(members, start=1):
            band.fill(np.nan)
            peak = peaks.get(member)
            if peak is not None:
                band[rr, cc] = peak
            dst.write(band, i)
            dst.set_band_description(i, f'member_{member}')
        dst.update_tags(**{k: str(v) for k, v in tags.items()})


# ---------------------------------------------------------------------------
# Main entry point (called by pipeline_core.step5c_compute_surge_rasters)
# ---------------------------------------------------------------------------

def process_surge_combination(
    tc_data_dir,
    wind_data_dir,
    output_dir,
    dem_path,
    verbose: bool = False,
) -> Dict:
    """
    Compute the surge of all TC storms and write one maximum-surge raster per
    storm and forecast cycle (two for a storm crossing the antimeridian).

    Args:
        tc_data_dir: Directory containing transformed TC CSV files (step 3)
        wind_data_dir: Directory containing wind GRIB files (step 4, wind_ens_* prefix)
        output_dir: Directory to write the surge GeoTIFFs
        dem_path: Local path to the surge DEM cache (see resolve_surge_dem)

    Returns:
        dict with 'processed_storms', 'raster_files' (list of Path) and 'timings'
    """
    tc_data_dir, wind_data_dir, output_dir = Path(tc_data_dir), Path(wind_data_dir), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("ECMWF STORM SURGE RASTERS (bathtub)")
    logger.info("=" * 70)

    wind_files = [f for f in wind_data_dir.glob('wind_ens_*.grib2')]
    if not wind_files:
        logger.warning(f"No wind files found in {wind_data_dir}")
        return {'processed_storms': 0, 'raster_files': [], 'timings': {}}

    storm_configs = find_tc_data_files(tc_data_dir)
    if not storm_configs:
        logger.warning("No TC data files found -- nothing to process")
        return {'processed_storms': 0, 'raster_files': [], 'timings': {}}

    processed_storms, raster_files, timings = 0, [], {}

    with rasterio.Env():
        for storm_config in storm_configs:
            storm_name = storm_config['storm_name']
            logger.info(f"\n{'=' * 50}\nSurge raster for storm: {storm_name}\n{'=' * 50}")
            t_storm = time.time()
            try:
                raster_files += _process_storm(storm_name, storm_config['track_csv'], wind_files,
                                               Path(dem_path), output_dir, verbose)
                processed_storms += 1
                timings[storm_name] = round(time.time() - t_storm, 1)
                logger.info(f"  {storm_name}: {timings[storm_name]} s")
            except Exception as e:
                logger.error(f"  Surge computation failed for {storm_name}: {e}", exc_info=verbose)

    logger.info(f"Surge: {processed_storms} storm(s), {len(raster_files)} raster(s) written in {output_dir}")
    return {'processed_storms': processed_storms, 'raster_files': raster_files, 'timings': timings}


def _process_storm(storm_name: str, track_csv: str, wind_files: List[Path], dem_path: Path,
                   output_dir: Path, verbose: bool) -> List[Path]:
    """Maximum surge per member for one storm. Returns the raster files written."""
    track_df = load_tc_track_data(track_csv)
    bbox = get_bounding_box(create_buffered_track_polygon(track_df, TRACK_BUFFER_KM),
                            buffer=BBOX_BUFFER_DEG)
    lon_windows = get_longitude_windows(bbox)

    cells = [CoastalCells(dem_path, w) for w in lon_windows]
    n_cells = sum(len(c) for c in cells)
    logger.info(f"  {n_cells:,} coastal lowland cells in the storm area")
    if n_cells == 0:
        logger.info("  No low-lying coast near this storm -- no surge raster")
        return []

    has_ft = 'forecast_time' in track_df.columns
    run_ft = _resolve_run_forecast_time(track_df, has_ft)
    lead_times = _wind_lead_times_for_run(run_ft, wind_files) if run_ft is not None \
        else sorted(track_df['lead_time'].unique())
    members = sorted(int(m) for m in track_df['ensemble_member'].unique())
    rows_by_lead = _synthesize_rows_by_lead(lead_times, members, run_ft)

    # Running maximum over lead times, per member and per longitude window
    max_surge: Dict[int, List[Optional[np.ndarray]]] = {}
    lead_times_seen: List[int] = []
    for lead_time in lead_times:
        pf_file = _match_wind_file(run_ft, lead_time, 1, wind_files)
        cf_file = _match_wind_file(run_ft, lead_time, CONTROL_MEMBER, wind_files)
        pf_windows = load_wind_data_all_members(str(pf_file), bbox) if pf_file else []
        cf_windows = load_wind_data_all_members(str(cf_file), bbox) if cf_file else []
        if not pf_windows and not cf_windows:
            logger.warning(f"    No wind file for lead {lead_time}h")
            continue
        lead_times_seen.append(int(lead_time))

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
                surge_by_member.setdefault(CONTROL_MEMBER, [None] * len(cells))[i] = bathtub_surge(
                    c.wind_at_cells(cf_windows[i]), c.elevation, c.dist_km)

        for row in rows_by_lead[lead_time]:
            member = row['ensemble_member']
            member_surge = surge_by_member.get(member)
            if member_surge is None:
                continue
            peak = max_surge.setdefault(member, [None] * len(cells))
            for i, surge in enumerate(member_surge):
                if surge is not None:
                    peak[i] = surge if peak[i] is None else np.maximum(peak[i], surge)
        if verbose:
            logger.info(f"  lead {lead_time}h: {len(max_surge)} member(s) so far")

    if not max_surge:
        logger.info("  No wind data for this storm -- no surge raster")
        return []

    ft_suffix = pd.to_datetime(run_ft).strftime('%Y%m%dT%HZ') if run_ft is not None else None
    stem = f"{storm_name}_{ft_suffix}" if ft_suffix else storm_name
    tags = {
        'dataset': 'surge',
        'units': 'm',
        'description': 'Maximum storm surge depth above ground over all lead times, '
                       'CLIMADA TCSurgeBathtub method; 0 = coastal cell not flooded, '
                       'nodata = not low-lying coastal land',
        'storm': storm_name,
        'forecast_time': run_ft if run_ft is not None else '',
        'lead_times_h': f"{min(lead_times_seen)}-{max(lead_times_seen)}",
        'member_numbers': ','.join(str(m) for m in members),
        'control_member': CONTROL_MEMBER,
        'elevation_source': cells[0].elevation_source,
    }

    written = []
    for i, c in enumerate(cells):
        if len(c) == 0:
            continue
        suffix = f"_w{i}" if len(cells) > 1 else ""
        path = output_dir / f"{stem}_surge_raster{suffix}.tif"
        peaks = {m: p[i] for m, p in max_surge.items()}
        write_surge_raster(c, peaks, members, path, tags)
        flooded = sum(int((p[i] > 0).any()) for p in max_surge.values() if p[i] is not None)
        logger.info(f"  Saved {path.name}: {len(members)} bands, {len(c):,} coastal cells, "
                    f"{flooded} member(s) with surge > 0")
        written.append(path)
    return written


def main():
    parser = argparse.ArgumentParser(description="Standalone run of the storm surge step (for testing)")
    parser.add_argument("--tc-dir", default=os.getenv('TRANSFORMED_DATA_DIR', 'tc_data_transformed'))
    parser.add_argument("--wind-dir", default=os.getenv('WIND_DATA_DIR', 'wind_data'))
    parser.add_argument("--output-dir", default=os.getenv('WIND_EXTRACTED_DIR', 'wind_extracted'),
                        help="Folder for the surge GeoTIFFs")
    parser.add_argument("--dem", default=str(Path(os.getenv('SURGE_DEM_LOCAL_DIR', 'surge_dem')) / SURGE_DEM_FILENAME))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    process_surge_combination(args.tc_dir, args.wind_dir, args.output_dir, args.dem, verbose=args.verbose)


if __name__ == "__main__":
    main()
