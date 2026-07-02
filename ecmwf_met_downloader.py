#!/usr/bin/env python3
"""
ECMWF Ensemble Parameter Downloader

Downloads gridded IFS ENS parameters from ECMWF Open Data, converts to Zarr
ZipStore, and uploads to a Snowflake internal stage.

Parameters currently downloaded (both per pipeline run):
  tp  — total precipitation (accumulated mm from T+0)
  ro  — total runoff = surface runoff (sro) + subsurface drainage (ssro).
        Land-only (HTESSEL is a land surface model; ro=0 over ocean).
        Provides a soil-aware pluvial flood signal over land.
        Note: sro and ssro are NOT available separately in ECMWF Open Data.

Storage format:
  Zarr ZipStore (.zarr.zip) — single file, chunked (1, 1, lat, lon), float16, zstd.
  dims: (member=51, step=25, lat, lon)
  values: accumulated from T+0 (subtract consecutive steps for period values)
  spatial: 60°S–60°N (all TC-active basins; Arctic/Antarctica excluded)

Storage destinations:
  DATA_PIPELINE_DB=SNOWFLAKE  →  PUT to Snowflake internal stage @{SNOWFLAKE_STAGE_NAME}
  DATA_PIPELINE_DB=LOCAL       →  ZipStore kept in met_data/ on disk

Member 51 (HRES control):
  stream=enfo, type=cf was deprecated in IFS Cycle 50r1 (May 2026).
  Member 51 is now downloaded as stream=oper, type=fc (HRES deterministic forecast).
  HRES open data is at 0.25° — same grid as ENS, no regridding needed.
"""

import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import numpy as np
import xarray as xr
import zarr
import zarr.storage
from zarr.codecs import BloscCodec, BloscShuffle
from ecmwf.opendata import Client

logger = logging.getLogger(__name__)

# Spatial clip: covers all TC-active ocean basins, excludes Arctic and Antarctica
LAT_MIN = -60.0
LAT_MAX = 60.0

# All 25 forecast steps: 0–144h at 6h intervals (ENS pf covers all run times)
FORECAST_STEPS = list(range(0, 145, 6))

# HRES (stream=oper, type=fc) max step per run time.
# All run times publish HRES to T+144h per ECMWF IFS documentation.
HRES_MAX_STEP = {0: 144, 6: 144, 12: 144, 18: 144}

DEFAULT_MET_DIR = 'met_data'


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _download_pf_step(client: Client, param: str, forecast_date: datetime,
                      run_time: int, step: int, grib_dir: Path) -> Dict:
    """Download one step of ENS perturbed members 1–50 (stream=enfo, type=pf)."""
    fname = f'{param}_pf_{forecast_date.strftime("%Y-%m-%d")}_r{run_time:02d}_f{step:03d}h.grib2'
    fpath = grib_dir / fname

    if fpath.exists():
        return {'success': True, 'step': step, 'type': 'pf', 'filepath': fpath}

    try:
        client.retrieve(
            date=forecast_date, time=run_time,
            stream='enfo', type='pf',
            step=step, param=param,
            target=str(fpath),
        )
        return {'success': True, 'step': step, 'type': 'pf', 'filepath': fpath}
    except Exception as e:
        logger.error(f'  [FAIL] {param} pf +{step}h: {e}')
        return {'success': False, 'step': step, 'type': 'pf', 'filepath': None, 'error': str(e)}


def _download_hres_step(client: Client, param: str, forecast_date: datetime,
                        run_time: int, step: int, grib_dir: Path) -> Dict:
    """
    Download one step of HRES control (member 51).
    stream=oper, type=fc replaces deprecated stream=enfo, type=cf since Cycle 50r1.
    Published at 0.25° — same grid as ENS, stacks directly.
    """
    fname = f'{param}_hres_{forecast_date.strftime("%Y-%m-%d")}_r{run_time:02d}_f{step:03d}h.grib2'
    fpath = grib_dir / fname

    if fpath.exists():
        return {'success': True, 'step': step, 'type': 'hres', 'filepath': fpath}

    try:
        client.retrieve(
            date=forecast_date, time=run_time,
            stream='oper', type='fc',
            step=step, param=param,
            target=str(fpath),
        )
        return {'success': True, 'step': step, 'type': 'hres', 'filepath': fpath}
    except Exception as e:
        logger.error(f'  [FAIL] {param} hres +{step}h: {e}')
        return {'success': False, 'step': step, 'type': 'hres', 'filepath': None, 'error': str(e)}


def _download_all_steps(param: str, forecast_date: datetime, run_time: int,
                        grib_dir: Path, max_workers: int = 4) -> bool:
    """Download all steps (pf + hres) concurrently. Returns True if all expected files succeeded."""
    client = Client(source='ecmwf')
    hres_max = HRES_MAX_STEP.get(run_time, 144)
    hres_steps = [s for s in FORECAST_STEPS if s <= hres_max]
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for step in FORECAST_STEPS:
            futures.append(executor.submit(_download_pf_step, client, param, forecast_date, run_time, step, grib_dir))
        for step in hres_steps:
            futures.append(executor.submit(_download_hres_step, client, param, forecast_date, run_time, step, grib_dir))

        for future in as_completed(futures):
            result = future.result()
            if not result['success']:
                failed += 1

    total_expected = len(FORECAST_STEPS) + len(hres_steps)
    if failed:
        logger.error(f'{failed}/{total_expected} downloads failed for {param}')
    elif len(hres_steps) < len(FORECAST_STEPS):
        logger.info(f'  Note: HRES capped at step {hres_max}h for {run_time:02d}Z run (steps >{hres_max}h filled with NaN)')
    return failed == 0


# ---------------------------------------------------------------------------
# GRIB → numpy
# ---------------------------------------------------------------------------

def _load_grib_step(param: str, pf_path: Path, hres_path: Path,
                    lats_ref=None, lons_ref=None):
    """
    Load one forecast step from pf + hres GRIB2 files.
    Returns (members [51, lat, lon] float32 mm, lats, lons).
    Members 0–49: ENS perturbed.  Member 50: HRES control (NaN if hres_path absent).
    """
    idx_pf = str(pf_path) + '.idx'

    ds_pf = xr.open_dataset(str(pf_path), engine='cfgrib',
                             backend_kwargs={'errors': 'ignore', 'indexpath': idx_pf})
    try:
        if not ds_pf.data_vars:
            raise ValueError(f"cfgrib decoded no variables from {pf_path} — check GRIB2 integrity")
        if param not in ds_pf.data_vars:
            fallback = list(ds_pf.data_vars)[0]
            logger.warning(f"Variable '{param}' not in {list(ds_pf.data_vars)}; using '{fallback}' from {pf_path.name}")
            var_pf = fallback
        else:
            var_pf = param
        tp_pf = (ds_pf[var_pf].sel(latitude=slice(LAT_MAX, LAT_MIN)) * 1000)  # m → mm
        lats = tp_pf.latitude.values
        lons = tp_pf.longitude.values
        if lats_ref is not None and not np.allclose(lats, lats_ref, atol=1e-4):
            raise ValueError(f"Lat grid mismatch in {pf_path.name}: shape {lats.shape} vs ref {lats_ref.shape}")
        if lons_ref is not None and not np.allclose(lons, lons_ref, atol=1e-4):
            raise ValueError(f"Lon grid mismatch in {pf_path.name}: shape {lons.shape} vs ref {lons_ref.shape}")
        pf_numbers = sorted(tp_pf.number.values.tolist())
        arrays = [tp_pf.sel(number=m).values for m in pf_numbers]
    finally:
        ds_pf.close()

    if not hres_path.exists():
        # HRES not published for this step (06Z/18Z run beyond T+90h) — fill with NaN
        arrays.append(np.full_like(arrays[0], np.nan, dtype=np.float32))
    else:
        idx_hres = str(hres_path) + '.idx'
        ds_hres = xr.open_dataset(str(hres_path), engine='cfgrib',
                                   backend_kwargs={'errors': 'ignore', 'indexpath': idx_hres})
        try:
            if not ds_hres.data_vars:
                raise ValueError(f"cfgrib decoded no variables from {hres_path} — check GRIB2 integrity")
            if param not in ds_hres.data_vars:
                fallback = list(ds_hres.data_vars)[0]
                logger.warning(f"Variable '{param}' not in {list(ds_hres.data_vars)}; using '{fallback}' from {hres_path.name}")
                var_hres = fallback
            else:
                var_hres = param
            tp_hres = (ds_hres[var_hres].sel(latitude=slice(LAT_MAX, LAT_MIN)) * 1000)
            hres_lats = tp_hres.latitude.values
            hres_lons = tp_hres.longitude.values
            if not np.allclose(hres_lats, lats, atol=1e-4):
                raise ValueError(f"Lat grid mismatch in {hres_path.name}: shape {hres_lats.shape} vs PF {lats.shape}")
            if not np.allclose(hres_lons, lons, atol=1e-4):
                raise ValueError(f"Lon grid mismatch in {hres_path.name}: shape {hres_lons.shape} vs PF {lons.shape}")
            arrays.append(tp_hres.values)  # HRES appended last → index 50 = ECMWF member 51
        finally:
            ds_hres.close()

    return np.stack(arrays, axis=0).astype(np.float32), lats, lons, pf_numbers


# ---------------------------------------------------------------------------
# Zarr ZipStore builder
# ---------------------------------------------------------------------------

def build_zarr_zipstore(param: str, forecast_date: datetime, run_time: int,
                        grib_dir: Path, output_dir: Path) -> Path:
    """
    Build a Zarr ZipStore from all 25 forecast steps.

    dims:   (member=51, step=25, lat, lon)
    dtype:  float16  (sufficient precision for precipitation in mm)
    chunks: (1, 1, lat, lon)  — one chunk per member per step; fast random access
    comp:   Blosc/zstd-3/bitshuffle
    values: accumulated mm from T+0 (raw ECMWF output)
             → period values = step_n − step_(n-6) computed at read time

    Returns path to the .zarr.zip file.
    """
    run_str = f'{forecast_date.strftime("%Y%m%d")}_{run_time:02d}'
    zip_path = output_dir / f'{param}_{run_str}.zarr.zip'

    if zip_path.exists():
        logger.info(f'  {zip_path.name} already exists — skipping')
        return zip_path

    logger.info(f'  Building Zarr for {param} ({len(FORECAST_STEPS)} steps × 51 members) ...')

    all_steps: List[np.ndarray] = []
    lats = lons = None
    pf_numbers = None  # GRIB member numbers 1–50, captured from first step

    for step in FORECAST_STEPS:
        pf_name   = f'{param}_pf_{forecast_date.strftime("%Y-%m-%d")}_r{run_time:02d}_f{step:03d}h.grib2'
        hres_name = f'{param}_hres_{forecast_date.strftime("%Y-%m-%d")}_r{run_time:02d}_f{step:03d}h.grib2'
        members, step_lats, step_lons, step_pf_numbers = _load_grib_step(
            param, grib_dir / pf_name, grib_dir / hres_name,
            lats_ref=lats, lons_ref=lons,
        )
        if lats is None:
            lats, lons = step_lats, step_lons
        if pf_numbers is None:
            pf_numbers = step_pf_numbers
        all_steps.append(members)  # each: (51, lat, lon)

    # Stack and transpose: (step=25, member=51, lat, lon) → (member=51, step=25, lat, lon)
    arr = np.transpose(np.stack(all_steps, axis=0), (1, 0, 2, 3)).astype(np.float16)
    n_members, n_steps, n_lat, n_lon = arr.shape

    logger.info(f'  Array shape: {arr.shape}  max accumulated: {float(arr.max()):.0f} mm')

    # Open outside try so we can always close and clean up on any failure
    store = zarr.storage.ZipStore(str(zip_path), mode='w')
    try:
        root  = zarr.open_group(store=store, mode='w')
        z = root.create_array(
            'data',
            shape=arr.shape,
            chunks=(1, 1, n_lat, n_lon),
            dtype='float16',
            compressors=BloscCodec(cname='zstd', clevel=3, shuffle=BloscShuffle.bitshuffle),
        )
        z[:] = arr

        # member_numbers[i] = ECMWF member number for Zarr index i
        # Matches TC_TRACKS.ENSEMBLE_MEMBER and wind pipeline member numbering (both 1-based)
        # ENS pf: Zarr index 0-49 → ECMWF members 1-50
        # HRES:   Zarr index 50   → ECMWF member 51
        member_numbers = (pf_numbers or list(range(1, 51))) + [51]

        root.attrs.update({
            'param':          param,
            'forecast_date':  forecast_date.strftime('%Y-%m-%d'),
            'run_time':       run_time,
            'steps':          FORECAST_STEPS,
            'accumulated':    True,
            'lat_min':        float(lats[-1]),
            'lat_max':        float(lats[0]),
            'lon_min':        float(lons[0]),
            'lon_max':        float(lons[-1]),
            'n_members':      n_members,
            'member_numbers': member_numbers,
            'description': (
                f'ECMWF ENS {param} accumulated from T+0 (mm). '
                f'Zarr index i → ECMWF member member_numbers[i] (1-50=ENS pf, 51=HRES). '
                f'Matches TC_TRACKS.ENSEMBLE_MEMBER and wind pipeline member numbering. '
                f'Steps: 0-144h at 6h intervals. period[a,b] = data[member,b] - data[member,a]. '
                f'Clip: {LAT_MIN}S-{LAT_MAX}N. '
                f'ro=0 over ocean (HTESSEL is land-only).'
                if param == 'ro' else
                f'ECMWF ENS {param} accumulated from T+0 (mm). '
                f'Zarr index i → ECMWF member member_numbers[i] (1-50=ENS pf, 51=HRES). '
                f'Matches TC_TRACKS.ENSEMBLE_MEMBER and wind pipeline member numbering. '
                f'Steps: 0-144h at 6h intervals. period[a,b] = data[member,b] - data[member,a]. '
                f'Clip: {LAT_MIN}S-{LAT_MAX}N.'
            ),
        })
    except BaseException:
        store.close()
        zip_path.unlink(missing_ok=True)  # prevent corrupt file blocking future runs
        raise
    store.close()

    size_mb = zip_path.stat().st_size / 1024 / 1024
    logger.info(f'  Written: {zip_path.name}  ({size_mb:.0f} MB)')
    return zip_path


# ---------------------------------------------------------------------------
# Snowflake stage upload
# ---------------------------------------------------------------------------

def upload_to_snowflake_stage(zip_path: Path, stage_name: str,
                              stage_path: str, conn) -> bool:
    """
    PUT a ZipStore file to a Snowflake internal stage.

    Args:
        zip_path:   Local path to the .zarr.zip file
        stage_name: Snowflake stage name (e.g. 'AOTS_ANALYSIS')
        stage_path: Path within the stage (e.g. 'met/20260629_00/tp.zarr.zip')
        conn:       Active Snowflake connector connection

    Returns True on success.
    """
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
        logger.info(f'  PUT {zip_path.name} → @{stage_name}/{stage_path}  [{status}]')
        return status in ('UPLOADED', 'SKIPPED')
    except Exception as e:
        logger.error(f'  Stage upload failed: {e}')
        return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

SUPPORTED_PARAMS = {'tp', 'ro'}


def download_ensemble_param(
        param: str,
        date: Union[str, datetime],
        run_time: int,
        output_dir: str = DEFAULT_MET_DIR,
        snowflake_conn=None,
        snowflake_stage_name: Optional[str] = None,
        upload_to_stage: bool = True,
        cleanup_grib: bool = True,
        max_workers: int = 4,
        verbose: bool = True,
) -> Dict:
    """
    Download one ECMWF ENS parameter, build Zarr ZipStore, store result.

    Args:
        param:                Parameter short name ('tp' or 'ro')
        date:                 Forecast date as 'YYYY-MM-DD' or datetime
        run_time:             Model run hour (0, 6, 12, or 18)
        output_dir:           Working directory for GRIB2 + ZipStore files
        snowflake_conn:       Active Snowflake connection (required if upload_to_stage=True)
        snowflake_stage_name: Stage name without @ (e.g. 'AOTS_ANALYSIS')
        upload_to_stage:      If True, PUT ZipStore to Snowflake stage (DATA_PIPELINE_DB=SNOWFLAKE)
                              If False, keep ZipStore locally (DATA_PIPELINE_DB=LOCAL)
        cleanup_grib:         Delete GRIB2 files after Zarr is built
        max_workers:          Concurrent download workers
        verbose:              Log progress

    Returns dict:
        success:       bool
        zip_path:      Path to local ZipStore (always present on success)
        stage_path:    Stage path within stage_name, or None if LOCAL mode
        forecast_date: datetime
        run_time:      int
        param:         str
    """
    if param not in SUPPORTED_PARAMS:
        raise ValueError(f"param must be one of {SUPPORTED_PARAMS}, got '{param}'")

    forecast_date = datetime.strptime(date, '%Y-%m-%d') if isinstance(date, str) else date

    output_path = Path(output_dir)
    grib_dir    = output_path / 'grib_tmp'
    output_path.mkdir(parents=True, exist_ok=True)
    grib_dir.mkdir(exist_ok=True)

    run_str = f'{forecast_date.strftime("%Y%m%d")}_{run_time:02d}'

    if verbose:
        logger.info('=' * 70)
        logger.info(f'ECMWF ENS — {param.upper()}  {forecast_date.strftime("%Y-%m-%d")} {run_time:02d}Z')
        logger.info(f'  Steps: {len(FORECAST_STEPS)} (0–144h at 6h)  |  Members: 51 (50 pf + 1 hres)')
        logger.info(f'  Spatial: {LAT_MIN}°–{LAT_MAX}°  |  Upload: {"stage" if upload_to_stage else "local"}')
        logger.info('=' * 70)

    # Step 1: Download all GRIB2 files
    ok = _download_all_steps(param, forecast_date, run_time, grib_dir, max_workers)
    if not ok:
        return {'success': False, 'zip_path': None, 'stage_path': None,
                'forecast_date': forecast_date, 'run_time': run_time, 'param': param}

    if verbose:
        hres_count = len([s for s in FORECAST_STEPS if s <= HRES_MAX_STEP.get(run_time, 144)])
        logger.info(f'  Downloaded {len(FORECAST_STEPS) + hres_count} GRIB2 files')

    # Step 2: Build Zarr ZipStore
    try:
        zip_path = build_zarr_zipstore(param, forecast_date, run_time, grib_dir, output_path)
    except Exception as e:
        logger.error(f'  Zarr build failed: {e}')
        return {'success': False, 'zip_path': None, 'stage_path': None,
                'forecast_date': forecast_date, 'run_time': run_time, 'param': param}

    # Step 3: Upload to Snowflake stage (or keep local)
    stage_path = None
    if upload_to_stage:
        if not snowflake_conn or not snowflake_stage_name:
            logger.error('  snowflake_conn and snowflake_stage_name required for stage upload')
            return {'success': False, 'zip_path': zip_path, 'stage_path': None,
                    'forecast_date': forecast_date, 'run_time': run_time, 'param': param}

        stage_path = f'met/{run_str}/{zip_path.name}'
        ok = upload_to_snowflake_stage(zip_path, snowflake_stage_name, stage_path, snowflake_conn)
        if not ok:
            return {'success': False, 'zip_path': zip_path, 'stage_path': None,
                    'forecast_date': forecast_date, 'run_time': run_time, 'param': param}

    # Step 4: Clean up GRIB2 files
    if cleanup_grib:
        removed = 0
        for f in grib_dir.glob('*.grib2'):
            f.unlink(missing_ok=True)
            removed += 1
        # also remove .idx sidecar files
        for f in grib_dir.glob('*.idx'):
            f.unlink(missing_ok=True)
        if verbose:
            logger.info(f'  Cleaned up GRIB2 files from {grib_dir}')

    if verbose:
        logger.info(f'  ✓ {param.upper()} done — {"@" + snowflake_stage_name + "/" + stage_path if stage_path else zip_path}')

    return {
        'success':       True,
        'zip_path':      zip_path,
        'stage_path':    stage_path,
        'forecast_date': forecast_date,
        'run_time':      run_time,
        'param':         param,
    }
