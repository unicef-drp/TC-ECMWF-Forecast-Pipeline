#!/usr/bin/env python3
"""
GloFAS Riverine Discharge Downloader

Downloads the GloFAS v4.0 operational ensemble discharge forecast (51 members)
from the Copernicus Emergency Management Service Early Warning Data Store (EWDS),
converts to Zarr ZipStore, and uploads to a Snowflake internal stage.

Data source: `cdsapi` against `cems-glofas-forecast` on
  https://ewds.climate.copernicus.eu/api  (NOT ecmwf-opendata — GloFAS is a separate
  CEMS product, not part of ECMWF's Open Data dissemination).

Storage format:
  Zarr ZipStore (.zarr.zip) — single file, chunked (1, 1, lat, lon), float32, zstd.
  dims: (member=51, step=7, lat, lon)
  values: river discharge in the last 24h (m3/s), one value per day, not accumulated
          (unlike tp/ro — no period-difference math needed at read time).
  spatial: 60°S–60°N (matches ecmwf_met_downloader.py's clip, for consistency across
           all "always-on, TC-independent" hazard layers).

Cadence — the key difference from tp/ro:
  GloFAS's operational medium-range product is driven only by the 00 UTC IFS ENS
  cycle and is published to CDS at most ONCE PER CALENDAR DAY (the `cems-glofas-
  forecast` CDS schema has no run-hour selector, only year/month/day). The TC
  pipeline runs 4x/day (00/06/12/18Z cycles), so 3 of the 4 daily runs will find a
  same-day GloFAS file already cached and skip the download entirely. The Zarr/stage
  cache key is therefore DATE-ONLY (no run_time component), unlike tp/ro's per-run key.

Publication lag: a given calendar day's GloFAS forecast is not always available
  immediately; this module falls back to the previous day if the current day's
  request fails, logging clearly which date was actually used.

Dtype note: float32, not float16 (unlike tp/ro). Global river discharge has a far
  larger dynamic range than precipitation-in-mm; tiny streams under 1 m3/s up to
  major rivers exceeding 100,000+ m3/s. float16's max representable value (65,504)
  risks silent overflow to inf for the world's largest rivers.
"""

import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Union

import numpy as np
import xarray as xr
import zarr
import zarr.storage
from zarr.codecs import BloscCodec, BloscShuffle

logger = logging.getLogger(__name__)

# Spatial clip: matches ecmwf_met_downloader.py's LAT_MIN/LAT_MAX for consistency
# across all always-on, TC-independent hazard layers.
LAT_MIN = -60.0
LAT_MAX = 60.0
AREA = [LAT_MAX, -180.0, LAT_MIN, 180.0]  # N, W, S, E

# Daily-resolution lead times: 1-7 days. 168h (not 144h like tp/ro) to pad for
# GloFAS's once-daily cadence against the pipeline's 4x-daily run schedule
LEADTIME_HOURS = ["24", "48", "72", "96", "120", "144", "168"]

DEFAULT_GLOFAS_DIR = 'glofas_data'
MAX_PUBLICATION_LAG_DAYS = 1  # how many days back to retry if today's forecast isn't published yet

# Threshold tier used to decide which cells are worth storing at all
FILTER_RP = "2.0"

# Everything GloFAS-related on the stage lives under glofas/. Forecast Zarrs at
# glofas/{date}/, thresholds at glofas/thresholds_cache/. Must be kept in sync
# with STAGE_PREFIX in setup_glofas_thresholds.py, since there is no shared
# import between the two entry points.
THRESHOLD_STAGE_PREFIX = "glofas/thresholds_cache"


# ---------------------------------------------------------------------------
# RP2 threshold loading (for sparse cell filtering)
# ---------------------------------------------------------------------------

THRESHOLD_BASE_URL = "https://confluence.ecmwf.int/download/attachments/242067380"


def _download_threshold_from_ecmwf(rp: str, dest_path: Path) -> None:
    """Direct HTTP download of one official RP threshold file. No CDS/Snowflake
    auth needed (plain confluence attachment). Self-healing fallback of last
    resort when neither a local cache nor the Snowflake stage has the file yet."""
    import requests
    fname = f"flood_threshold_glofas_v4_rl_{rp}.nc"
    url = f"{THRESHOLD_BASE_URL}/{fname}"
    logger.info(f"  RP{rp} threshold not cached anywhere — downloading directly from {url} ...")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def _fetch_threshold_file(threshold_source: str, threshold_local_dir: Union[str, Path],
                           snowflake_conn=None, snowflake_stage_name: Optional[str] = None,
                           cache_dir: Optional[Path] = None) -> Path:
    """
    Return a local path to the cached RP2 threshold file. Self-healing cascade —
    setup_glofas_thresholds.py is the recommended one-time path (avoids the ~173MB
    fallback download landing mid-pipeline-run), but is no longer a hard
    prerequisite: a missing cache at every tier now triggers a direct ECMWF
    download rather than failing outright.

    threshold_source='snowflake': local runtime cache -> Snowflake stage GET ->
      (miss) download directly from ECMWF, save locally AND PUT to the stage so
      future runs (this or other containers) hit the now-populated stage cache.
    threshold_source='local': threshold_local_dir -> (miss, if a Snowflake
      connection happens to be available) stage GET -> (miss or no connection)
      download directly from ECMWF, save to threshold_local_dir only (no stage
      push — 'local' means prefer to keep this run's data local).
    """
    fname = f"rl_{FILTER_RP}.nc"

    if threshold_source == "local":
        local_dir = Path(threshold_local_dir)
        local_path = local_dir / fname
        if local_path.exists():
            return local_path

        if snowflake_conn and snowflake_stage_name:
            local_dir.mkdir(parents=True, exist_ok=True)
            if _try_stage_get(snowflake_stage_name, fname, local_dir, snowflake_conn):
                return local_path

        local_dir.mkdir(parents=True, exist_ok=True)
        _download_threshold_from_ecmwf(FILTER_RP, local_path)
        return local_path

    if threshold_source == "snowflake":
        if not snowflake_conn or not snowflake_stage_name:
            raise ValueError("snowflake_conn and snowflake_stage_name required for "
                              "GLOFAS_THRESHOLD_SOURCE=snowflake")
        cache_dir = Path(cache_dir) if cache_dir else Path(DEFAULT_GLOFAS_DIR) / "thresholds_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        local_path = cache_dir / fname
        if local_path.exists():
            return local_path

        if _try_stage_get(snowflake_stage_name, fname, cache_dir, snowflake_conn):
            return local_path

        # Self-healing: not cached anywhere — fetch directly and populate the
        # stage so future runs (this container or any other) hit the cache.
        _download_threshold_from_ecmwf(FILTER_RP, local_path)
        stage_path = f'{THRESHOLD_STAGE_PREFIX}/{fname}'
        upload_to_snowflake_stage(local_path, snowflake_stage_name, stage_path, snowflake_conn)
        return local_path

    raise ValueError(f"Unknown threshold_source: {threshold_source!r} (must be 'local' or 'snowflake')")


def _try_stage_get(stage_name: str, fname: str, dest_dir: Path, conn) -> bool:
    """Best-effort GET of a threshold file from the Snowflake stage. Returns False
    (not raises) on any failure — missing file, no permissions, network issue —
    so the caller can fall through to the direct-ECMWF-download tier."""
    local_path = dest_dir / fname
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"GET @{stage_name}/{THRESHOLD_STAGE_PREFIX}/{fname} "
                f"'file://{dest_dir.resolve().as_posix()}'"
            )
        finally:
            cursor.close()
    except Exception as e:
        logger.info(f"  Stage GET for {fname} failed or file not present: {e}")
        return False
    return local_path.exists()


def _load_rp2_grid(lats: np.ndarray, lons: np.ndarray, threshold_path: Path) -> np.ndarray:
    """
    Load the RP2 threshold, clipped to the forecast's exact lat/lon bounds.

    Official files use lat/lon coord names (descending lat) — different from the
    forecast's own latitude/longitude — and cover the full GloFAS domain, wider than
    our 60S-60N clip, hence the .sel() slice. Non-positive values (no river/ocean
    cells, per GloFAS convention) become NaN so they never count as "exceeded."
    """
    official = xr.open_dataset(threshold_path)
    try:
        # Epsilon pad: the official file's coordinate values carry floating-point
        # noise (e.g. 72.52499999999998 vs the forecast's 72.525), so an exact-bound
        # slice can silently drop the first/last row or column. Padding by well under
        # half a grid cell (0.05deg native resolution) avoids that without risking
        # pulling in a neighboring cell.
        eps = 1e-3
        clipped = official.sel(lat=slice(float(lats.max()) + eps, float(lats.min()) - eps),
                                lon=slice(float(lons.min()) - eps, float(lons.max()) + eps))
        thr = clipped[f"rl_{FILTER_RP}"].values
        if thr.shape != (len(lats), len(lons)):
            raise ValueError(
                f"RP2 threshold grid shape {thr.shape} does not match forecast grid "
                f"({len(lats)}, {len(lons)}) — check for a resolution/version mismatch"
            )
        return np.where(thr > 0, thr, np.nan)
    finally:
        official.close()


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _retrieve_glofas(client, forecast_date: datetime, product_type: str, target: Path) -> None:
    """One CDS request for cems-glofas-forecast. product_type is 'ensemble_perturbed_forecasts'
    (50 members) or 'control_forecast' (1 member)."""
    client.retrieve(
        "cems-glofas-forecast",
        {
            "system_version": "operational",
            "hydrological_model": "lisflood",
            "product_type": product_type,
            "variable": "river_discharge_in_the_last_24_hours",
            "year": forecast_date.strftime("%Y"),
            "month": forecast_date.strftime("%m"),
            "day": forecast_date.strftime("%d"),
            "leadtime_hour": LEADTIME_HOURS,
            "data_format": "netcdf",
            "download_format": "unarchived",
            "area": AREA,
        },
        str(target),
    )


def _download_for_date(client, forecast_date: datetime, raw_dir: Path) -> Optional[Dict[str, Path]]:
    """Attempt to download both products for one calendar date. Returns None (not raises)
    if GloFAS hasn't published that date yet, so the caller can fall back a day.

    Submits both CDS requests concurrently — client.retrieve() blocks on the full
    submit -> queue-wait -> download cycle, and the two requests' queue waits are
    independent of each other, so running them sequentially means the ensemble
    request's queue wait (often the bulk of the total time, given it's the larger
    of the two) fully elapses before the control request is even submitted.
    Concurrent submission lets both queue waits overlap instead of stack."""
    date_str = forecast_date.strftime("%Y%m%d")
    ens_path = raw_dir / f"glofas_ens_{date_str}.nc"
    ctrl_path = raw_dir / f"glofas_ctrl_{date_str}.nc"

    jobs = []
    if not ens_path.exists():
        jobs.append(("ensemble_perturbed_forecasts", ens_path))
    if not ctrl_path.exists():
        jobs.append(("control_forecast", ctrl_path))

    try:
        if jobs:
            with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
                futures = {
                    executor.submit(_retrieve_glofas, client, forecast_date, product_type, target): target
                    for product_type, target in jobs
                }
                for future in as_completed(futures):
                    future.result()  # re-raises here if that request failed
        return {"ens": ens_path, "ctrl": ctrl_path}
    except Exception as e:
        logger.warning(f"  GloFAS not available for {date_str}: {e}")
        ens_path.unlink(missing_ok=True)
        ctrl_path.unlink(missing_ok=True)
        return None


def download_with_fallback(forecast_date: datetime, raw_dir: Path,
                            max_lag_days: int = MAX_PUBLICATION_LAG_DAYS) -> Optional[Dict]:
    """
    Try forecast_date first; if GloFAS hasn't published it yet, fall back to
    earlier days up to max_lag_days. Returns {'paths': {...}, 'actual_date': datetime}
    or None if nothing was available within the lag window.
    """
    import cdsapi
    # Prefer explicit env vars (SPCS/GHA — no ~/.cdsapirc dotfile mounted) over the
    # default ~/.cdsapirc-file discovery (local dev). cdsapi.Client(url=None, key=None)
    # falls back to the dotfile automatically when both are None.
    client = cdsapi.Client(url=os.getenv('CDSAPI_URL'), key=os.getenv('CDSAPI_KEY'))

    for lag in range(max_lag_days + 1):
        candidate = forecast_date - timedelta(days=lag)
        if lag > 0:
            logger.info(f"  Falling back to {candidate.strftime('%Y-%m-%d')} "
                        f"(requested date not yet published)")
        paths = _download_for_date(client, candidate, raw_dir)
        if paths is not None:
            return {"paths": paths, "actual_date": candidate}

    logger.error(f"  GloFAS unavailable for {forecast_date.strftime('%Y-%m-%d')} "
                 f"and {max_lag_days} day(s) prior — skipping")
    return None


# ---------------------------------------------------------------------------
# Zarr ZipStore builder
# ---------------------------------------------------------------------------

def build_zarr_zipstore(paths: Dict[str, Path], actual_date: datetime, output_dir: Path,
                         threshold_path: Path) -> Path:
    """
    Build a Zarr ZipStore from the ensemble + control NetCDFs, keeping only cells
    where at least one member on at least one day exceeds the RP2 threshold.

    data array:  shape (member=51, step=7, n_cells) — sparse, filtered
    cell_lat/cell_lon: shape (n_cells,) — coordinates of each kept cell
    dtype:  float32
    comp:   Blosc/zstd-3/bitshuffle

    Cache key is DATE-ONLY (no run_time) — matches GloFAS's once-daily cadence.
    Returns path to the .zarr.zip file.
    """
    date_str = actual_date.strftime("%Y%m%d")
    zip_path = output_dir / f"river_{date_str}.zarr.zip"

    if zip_path.exists():
        logger.info(f"  {zip_path.name} already exists — skipping build")
        return zip_path

    logger.info(f"  Building Zarr for GloFAS discharge ({len(LEADTIME_HOURS)} steps x 51 members) ...")

    ens = None
    ctrl = None
    try:
        ens = xr.open_dataset(paths["ens"])
        ctrl = xr.open_dataset(paths["ctrl"])
        lats = ens.latitude.values
        lons = ens.longitude.values
        pf_numbers = sorted(int(n) for n in ens.number.values)

        if not np.allclose(ctrl.latitude.values, lats, atol=1e-4) or \
           not np.allclose(ctrl.longitude.values, lons, atol=1e-4):
            raise ValueError("Control forecast grid does not match ensemble grid")

        n_pf = len(pf_numbers)
        n_steps = len(LEADTIME_HOURS)
        n_lat, n_lon = len(lats), len(lons)
        n_members = n_pf + 1
        n_cells_total = n_lat * n_lon

        # Process in latitude bands instead of materializing the whole (51, 7, 2400,
        # 7200) dense array at once. The raw NetCDF stores dis24 as float64, and even
        # after casting to float32 early, the full dense global array alone is ~23GB
        # but only ~2-3% of cells ever survive the RP2 filter (see n_cells_kept/
        # n_cells_total below), so holding the full dense grid in memory at all is
        # wasteful by ~40x. That ~23GB dense peak is what silently OOM-killed both the
        # very first local Docker test (Docker Desktop's VM: 7.65GB) and a later SPCS
        # job run (58Gi container limit), no traceback either time, the kernel just
        # SIGKILLs it. Filtering band-by-band bounds peak memory to one band's dense
        # array (a few hundred MB) plus the small sparse output, independent of global
        # grid size.
        rp2 = _load_rp2_grid(lats, lons, threshold_path)
        LAT_CHUNK = 200
        ens_da = ens["dis24"].squeeze("forecast_reference_time")
        ctrl_da = ctrl["dis24"].squeeze("forecast_reference_time")

        sparse_parts, cell_lat_parts, cell_lon_parts = [], [], []
        running_max = -np.inf

        for lat_start in range(0, n_lat, LAT_CHUNK):
            lat_end = min(lat_start + LAT_CHUNK, n_lat)
            band_rows = lat_end - lat_start

            # member index 0-49 = ECMWF members 1-50 (pf); index 50 = control.
            # Matches TC_TRACKS.ENSEMBLE_MEMBER / wind pipeline convention (51 = control).
            band = np.empty((n_members, n_steps, band_rows, n_lon), dtype=np.float32)
            band[:n_pf] = ens_da.isel(latitude=slice(lat_start, lat_end)).values
            band[n_pf] = ctrl_da.isel(latitude=slice(lat_start, lat_end)).values
            running_max = max(running_max, float(np.nanmax(band)))

            rp2_band = rp2[lat_start:lat_end]
            finite = np.isfinite(band) & np.isfinite(rp2_band)[np.newaxis, np.newaxis, :, :]
            exceed = (band > rp2_band[np.newaxis, np.newaxis, :, :]) & finite
            any_exceed_band = exceed.any(axis=(0, 1))  # (band_rows, lon)
            local_lat_idx, local_lon_idx = np.nonzero(any_exceed_band)
            if len(local_lat_idx) == 0:
                continue

            sparse_parts.append(band[:, :, local_lat_idx, local_lon_idx])  # (member, step, n_local)
            cell_lat_parts.append(lats[lat_start:lat_end][local_lat_idx])
            cell_lon_parts.append(lons[local_lon_idx])

        sparse_data = (np.concatenate(sparse_parts, axis=2) if sparse_parts
                       else np.empty((n_members, n_steps, 0), dtype=np.float32))
        cell_lat = (np.concatenate(cell_lat_parts) if cell_lat_parts else np.empty(0)).astype(np.float64)
        cell_lon = (np.concatenate(cell_lon_parts) if cell_lon_parts else np.empty(0)).astype(np.float64)
        n_cells = sparse_data.shape[2]

        logger.info(f"  Max discharge: {running_max:.0f} m3/s")
        logger.info(f"  Sparse filter (RP{FILTER_RP}yr): kept {n_cells:,} of {n_cells_total:,} cells "
                    f"({100 * n_cells / n_cells_total:.2f}%)")

        store = None
        try:
            store = zarr.storage.ZipStore(str(zip_path), mode="w")
            root = zarr.open_group(store=store, mode="w")
            chunk_cells = min(n_cells, 4096) if n_cells > 0 else 1
            z = root.create_array(
                "data",
                shape=sparse_data.shape,
                chunks=(1, 1, chunk_cells),
                dtype="float32",
                compressors=BloscCodec(cname="zstd", clevel=3, shuffle=BloscShuffle.bitshuffle),
            )
            z[:] = sparse_data
            root.create_array("cell_lat", shape=cell_lat.shape, dtype="float64")[:] = cell_lat
            root.create_array("cell_lon", shape=cell_lon.shape, dtype="float64")[:] = cell_lon

            member_numbers = pf_numbers + [51]
            root.attrs.update({
                "param": "dis24",
                "forecast_date": date_str,
                "leadtime_hours": [int(h) for h in LEADTIME_HOURS],
                "lat_min": float(lats.min()), "lat_max": float(lats.max()),
                "lon_min": float(lons.min()), "lon_max": float(lons.max()),
                "n_members": n_members,
                "member_numbers": member_numbers,
                "units": "m3 s-1",
                "n_cells_kept": int(n_cells),
                "n_cells_total": int(n_cells_total),
                "filter_threshold": f"RP{FILTER_RP}yr",
                "description": (
                    "GloFAS v4.0 operational ensemble river discharge (dis24, m3/s), "
                    "issued from the 00Z IFS ENS cycle, published once per calendar day. "
                    "Zarr index i -> ECMWF member member_numbers[i] (1-50=ENS pf, 51=control). "
                    "Matches TC_TRACKS.ENSEMBLE_MEMBER and wind/met pipeline member numbering. "
                    f"Steps: {LEADTIME_HOURS} hours (daily resolution). "
                    f"Clip: {LAT_MIN}S-{LAT_MAX}N. Values are NOT accumulated (unlike tp/ro) — "
                    "each step is discharge in the preceding 24h, read directly, no differencing needed. "
                    "SPARSE FORMAT: 'data' is (member, step, n_cells), NOT a dense lat/lon grid — "
                    "only cells where >=1 member on >=1 day exceeded the RP2yr threshold are "
                    "included (see n_cells_kept/n_cells_total/filter_threshold attrs). Use "
                    "cell_lat/cell_lon (parallel arrays, same n_cells length) to locate each "
                    "stored cell — do not assume a rectangular lat/lon index."
                ),
            })
        except BaseException:
            if store is not None:
                store.close()
            zip_path.unlink(missing_ok=True)
            raise
        store.close()
    finally:
        if ens is not None:
            ens.close()
        if ctrl is not None:
            ctrl.close()

    size_mb = zip_path.stat().st_size / 1024 / 1024
    logger.info(f"  Written: {zip_path.name}  ({size_mb:.0f} MB)")
    return zip_path


# ---------------------------------------------------------------------------
# Snowflake stage upload (identical pattern to ecmwf_met_downloader.py)
# ---------------------------------------------------------------------------

def upload_to_snowflake_stage(zip_path: Path, stage_name: str, stage_path: str, conn) -> bool:
    """PUT a ZipStore file to a Snowflake internal stage."""
    stage_dir = '/'.join(stage_path.split('/')[:-1])
    put_sql = (
        f"PUT 'file://{zip_path.resolve().as_posix()}' "
        f"@{stage_name}/{stage_dir}/ "
        f"OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
    )
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(put_sql)
            result = cursor.fetchone()
        finally:
            cursor.close()
        status = result[6] if result else 'unknown'
        logger.info(f'  PUT {zip_path.name} -> @{stage_name}/{stage_path}  [{status}]')
        return status in ('UPLOADED', 'SKIPPED')
    except Exception as e:
        logger.error(f'  Stage upload failed: {e}')
        return False


def stage_file_exists(stage_name: str, stage_path: str, conn) -> bool:
    """Check whether a same-day file is already staged, to skip a redundant download
    entirely (across the 4x-daily pipeline cycles that share one GloFAS run per day)."""
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(f"LIST @{stage_name}/{stage_path}")
            return cursor.fetchone() is not None
        finally:
            cursor.close()
    except Exception as e:
        logger.warning(f"  Could not check stage for existing file: {e}")
        return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def download_glofas_forecast(
        date: Union[str, datetime],
        output_dir: str = DEFAULT_GLOFAS_DIR,
        snowflake_conn=None,
        snowflake_stage_name: Optional[str] = None,
        upload_to_stage: bool = True,
        cleanup_raw: bool = True,
        verbose: bool = True,
        threshold_source: str = "snowflake",
        threshold_local_dir: Union[str, Path] = "glofas_thresholds",
) -> Dict:
    """
    Download GloFAS discharge forecast for one date, build Zarr ZipStore, store result.

    Args:
        date:                 Forecast date as 'YYYY-MM-DD' or datetime (the TC pipeline's
                               run date — GloFAS's cache key is date-only regardless of run_time)
        output_dir:           Working directory for raw NetCDF + ZipStore files
        snowflake_conn:       Active Snowflake connection (required if upload_to_stage=True,
                               or if threshold_source='snowflake')
        snowflake_stage_name: Stage name without @ (e.g. 'AOTS_ANALYSIS')
        upload_to_stage:      If True, PUT ZipStore to Snowflake stage; if False, keep local
        cleanup_raw:          Delete raw NetCDF files after Zarr is built
        verbose:              Log progress
        threshold_source:     'snowflake' (default) or 'local' — where to read the cached
                               RP2 threshold file from for sparse cell filtering (see
                               setup_glofas_thresholds.py; GLOFAS_THRESHOLD_SOURCE env var)
        threshold_local_dir:  Local dir containing rl_2.0.nc, used when threshold_source='local'

    Returns dict: success, zip_path, stage_path, forecast_date (date actually used,
                  may lag the requested date — see module docstring), param.
    """
    forecast_date = datetime.strptime(date, '%Y-%m-%d') if isinstance(date, str) else date
    date_str = forecast_date.strftime("%Y%m%d")

    output_path = Path(output_dir)
    raw_dir = output_path / 'raw_tmp'
    output_path.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(exist_ok=True)

    if verbose:
        logger.info('=' * 70)
        logger.info(f'GloFAS v4.0 — riverine discharge  {forecast_date.strftime("%Y-%m-%d")}')
        logger.info(f'  Steps: {len(LEADTIME_HOURS)} (24-168h daily)  |  Members: 51 (50 pf + 1 control)')
        logger.info(f'  Spatial: {LAT_MIN} deg - {LAT_MAX} deg  |  Cadence: once/calendar day')
        logger.info('=' * 70)

    for lag in range(MAX_PUBLICATION_LAG_DAYS + 1):
        candidate = forecast_date - timedelta(days=lag)
        candidate_str = candidate.strftime("%Y%m%d")
        candidate_path = output_path / f'river_{candidate_str}.zarr.zip'
        if not candidate_path.exists():
            continue

        if upload_to_stage and snowflake_conn and snowflake_stage_name:
            candidate_stage_path = f'glofas/{candidate_str}/river_{candidate_str}.zarr.zip'
            if not stage_file_exists(snowflake_stage_name, candidate_stage_path, snowflake_conn):
                ok = upload_to_snowflake_stage(candidate_path, snowflake_stage_name,
                                                candidate_stage_path, snowflake_conn)
                if not ok:
                    return {'success': False, 'zip_path': candidate_path, 'stage_path': None,
                             'forecast_date': candidate, 'param': 'dis24', 'cached': False}
            logger.info(f'  {candidate_stage_path} — day-level cache hit (local file, verified staged)')
            return {'success': True, 'zip_path': candidate_path, 'stage_path': candidate_stage_path,
                     'forecast_date': candidate, 'param': 'dis24', 'cached': True}

        logger.info(f'  {candidate_path.name} already exists locally — skipping (day-level cache hit)')
        return {'success': True, 'zip_path': candidate_path, 'stage_path': None,
                 'forecast_date': candidate, 'param': 'dis24', 'cached': True}

    if upload_to_stage and snowflake_conn and snowflake_stage_name:
        for lag in range(MAX_PUBLICATION_LAG_DAYS + 1):
            candidate = forecast_date - timedelta(days=lag)
            candidate_str = candidate.strftime("%Y%m%d")
            candidate_stage_path = f'glofas/{candidate_str}/river_{candidate_str}.zarr.zip'
            if stage_file_exists(snowflake_stage_name, candidate_stage_path, snowflake_conn):
                logger.info(f'  {candidate_stage_path} already staged — skipping (day-level cache hit)')
                return {'success': True, 'zip_path': None, 'stage_path': candidate_stage_path,
                         'forecast_date': candidate, 'param': 'dis24', 'cached': True}

    # Step 1: Download (with same-day/prior-day fallback)
    result = download_with_fallback(forecast_date, raw_dir)
    if result is None:
        return {'success': False, 'zip_path': None, 'stage_path': None,
                 'forecast_date': forecast_date, 'param': 'dis24', 'cached': False}
    actual_date = result['actual_date']

    # Step 1.5: Locate the cached RP2 threshold file (required for sparse filtering)
    try:
        threshold_path = _fetch_threshold_file(
            threshold_source, threshold_local_dir,
            snowflake_conn=snowflake_conn, snowflake_stage_name=snowflake_stage_name,
            cache_dir=output_path / 'thresholds_cache',
        )
    except Exception as e:
        logger.error(f'  Could not load RP2 threshold file: {e}')
        return {'success': False, 'zip_path': None, 'stage_path': None,
                 'forecast_date': forecast_date, 'param': 'dis24', 'cached': False}

    # Step 2: Build Zarr ZipStore
    try:
        zip_path = build_zarr_zipstore(result['paths'], actual_date, output_path, threshold_path)
    except Exception as e:
        logger.error(f'  Zarr build failed: {e}')
        return {'success': False, 'zip_path': None, 'stage_path': None,
                 'forecast_date': forecast_date, 'param': 'dis24', 'cached': False}

    # Step 3: Upload to Snowflake stage (or keep local)
    stage_path = None
    if upload_to_stage:
        if not snowflake_conn or not snowflake_stage_name:
            logger.error('  snowflake_conn and snowflake_stage_name required for stage upload')
            return {'success': False, 'zip_path': zip_path, 'stage_path': None,
                     'forecast_date': forecast_date, 'param': 'dis24', 'cached': False}

        actual_date_str = actual_date.strftime("%Y%m%d")
        stage_path = f'glofas/{actual_date_str}/{zip_path.name}'
        ok = upload_to_snowflake_stage(zip_path, snowflake_stage_name, stage_path, snowflake_conn)
        if not ok:
            return {'success': False, 'zip_path': zip_path, 'stage_path': None,
                     'forecast_date': forecast_date, 'param': 'dis24', 'cached': False}

    # Step 4: Clean up raw NetCDF files
    if cleanup_raw:
        for f in raw_dir.glob('*.nc'):
            f.unlink(missing_ok=True)
        if verbose:
            logger.info(f'  Cleaned up raw NetCDF files from {raw_dir}')

    if verbose:
        logger.info(f'  Done — {"@" + snowflake_stage_name + "/" + stage_path if stage_path else zip_path}')

    return {
        'success': True,
        'zip_path': zip_path,
        'stage_path': stage_path,
        'forecast_date': actual_date,
        'param': 'dis24',
        'cached': False,
    }
