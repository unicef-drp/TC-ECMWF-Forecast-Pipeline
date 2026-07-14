#!/usr/bin/env python3
"""
GloFAS discharge x JRC flood-extent masking

Combines the sparse per-cell discharge-exceedance probability already built by
glofas_downloader.py with JRC v2.1's static, pre-cached flood-extent rasters

  - Full 6-tier severity ladder: RP2/RP5 (no native JRC map: reuse RP10's
    own extent as a labeled upper-bound stand-in) + RP10/RP20/RP50/RP100
    (native JRC tier match).
  - Nearest-cell "hard" assignment: each JRC pixel within a GloFAS cell's
    0.05deg footprint inherits that cell's own probability.
  - Permanent water excluded (JRC's own permanent_water_class band).
  - uparea < 500km^2 is flagged via a `below_min_basin` column, NOT excluded.
    IMPORTANT: this is NOT a confidence flag on the GloFAS discharge forecast
    or the RP threshold comparison, both are dense, global, unrestricted
    products (confirmed directly: every cell has a real RP threshold value,
    even down to ~28km^2 catchments). 500km^2 is specifically JRC's own
    minimum-catchment cutoff for generating the flood-EXTENT map itself (the
    LISFLOOD-FP hydraulic simulation), below it, there may be no flood
    geometry to combine with at all, a matching-availability gap, not a
    forecast-quality one. See
    https://confluence.ecmwf.int/spaces/CEMS/pages/340774762/CEMS-Flood+flood+inundation+maps

Output: one tiled GeoTIFF per RP tier per forecast date, 7 bands (one per
lead step, 24h-168h), globally scoped (60S-60N, no country/region split,
mirrors the raw discharge Zarr's own "nominally global, actually sparse"
storage philosophy; GloFAS's real active-cell coverage on any given day is a
small fraction of the full band, so a sparse/nodata-heavy tiled GeoTIFF
compresses well despite nominal global extent).
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import xarray as xr
import zarr
import zarr.storage

from glofas_downloader import (
    EXTENT_RP_LEVELS,
    _fetch_threshold_file,
    _nearest_grid_value,
    upload_to_snowflake_stage,
)

logger = logging.getLogger(__name__)

# RP2/RP5 have no native JRC map (JRC only covers RP10 and up), they borrow
# RP10's own extent as a labeled upper-bound stand-in, since flood extent
# grows monotonically with return period (confirmed both at the aggregate
# area level and at the pixel level).
EXTENT_SOURCE_TIER = {"2.0": "10.0", "5.0": "10.0", "10.0": "10.0",
                       "20.0": "20.0", "50.0": "50.0", "100.0": "100.0"}
IS_STANDIN = {"2.0": True, "5.0": True, "10.0": False,
              "20.0": False, "50.0": False, "100.0": False}

DEPTH_THRESHOLD_M = 0.1  # NOT a indication of not harmful, but used as a noise floor

MIN_BASIN_KM2 = 500.0    # JRC's own minimum-catchment cutoff for generating the flood-EXTENT
                          # map (LISFLOOD-FP simulation), NOT a GloFAS discharge/threshold
                          # confidence cutoff, see module docstring
# https://confluence.ecmwf.int/spaces/CEMS/pages/340774762/CEMS-Flood+flood+inundation+maps

JRC_VERSION = "v2_1"
JRC_STAGE_PREFIX = f"glofas/jrc_extent_cache/{JRC_VERSION}"
# Same "glofas/{date}/" prefix the raw discharge Zarr already uses (not a
# separate "glofas/extent/{date}/" tree), both are per-cycle outputs of the
# same forecast, so they live in the same date folder on stage.
EXTENT_STAGE_PREFIX = "glofas"

# JRC depth band filenames per tier, and the permanent-water band
JRC_DEPTH_FILENAME = {"10.0": "jrc_rp10.tif", "20.0": "jrc_rp20.tif",
                       "50.0": "jrc_rp50.tif", "100.0": "jrc_rp100.tif"}
JRC_WATER_FILENAME = "jrc_permanent_water.tif"


# ---------------------------------------------------------------------------
# JRC cache resolution (local cache -> Snowflake stage GET; NO direct-from-JRC
# fallback: unlike the RP-threshold/uparea cascades in glofas_downloader.py,
# this module never self-heals by re-fetching from JRC itself if both the
# local cache and the stage miss. A miss here is a hard, loud failure with a 
# clear pointer to setup_jrc_extents.py, not a silent skip.)
# ---------------------------------------------------------------------------

def _resolve_jrc_file(fname: str, jrc_source: str, jrc_local_dir: Union[str, Path],
                       snowflake_conn=None, snowflake_stage_name: Optional[str] = None,
                       cache_dir: Optional[Path] = None) -> Path:
    if jrc_source == "local":
        local_path = Path(jrc_local_dir) / fname
        if not local_path.exists():
            raise FileNotFoundError(
                f"JRC cache file {fname} not found in {jrc_local_dir} (GLOFAS_JRC_SOURCE=local). "
                f"Run setup_jrc_extents.py --local-only {jrc_local_dir} first."
            )
        return local_path

    if jrc_source == "snowflake":
        if not snowflake_conn or not snowflake_stage_name:
            raise ValueError("snowflake_conn and snowflake_stage_name required for GLOFAS_JRC_SOURCE=snowflake")
        cache_dir = Path(cache_dir) if cache_dir else Path("glofas_data") / "jrc_extent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        local_path = cache_dir / fname
        if local_path.exists():
            return local_path
        cursor = snowflake_conn.cursor()
        try:
            cursor.execute(
                f"GET @{snowflake_stage_name}/{JRC_STAGE_PREFIX}/{fname} "
                f"'file://{cache_dir.resolve().as_posix()}'"
            )
        finally:
            cursor.close()
        if not local_path.exists():
            raise FileNotFoundError(
                f"JRC cache file {fname} not found on stage at {JRC_STAGE_PREFIX}/{fname}. "
                f"Run setup_jrc_extents.py first (no credentials needed -- direct JRC download)."
            )
        return local_path

    raise ValueError(f"Unknown jrc_source: {jrc_source!r} (must be 'local' or 'snowflake')")


def resolve_jrc_cache(jrc_source: str, jrc_local_dir: Union[str, Path],
                       snowflake_conn=None, snowflake_stage_name: Optional[str] = None,
                       cache_dir: Optional[Path] = None) -> Dict[str, Path]:
    """Returns whichever of {'10.0', '20.0', '50.0', '100.0', 'water'} were
    successfully resolved: per-file tolerant, NOT all-or-nothing. A single
    missing/corrupted JRC file (e.g. jrc_rp50.tif) only needs to disable the
    tier(s) that specifically depend on it (RP50, via EXTENT_SOURCE_TIER),
    not every tier."""
    paths = {}
    for rp, fname in JRC_DEPTH_FILENAME.items():
        try:
            paths[rp] = _resolve_jrc_file(fname, jrc_source, jrc_local_dir, snowflake_conn,
                                           snowflake_stage_name, cache_dir)
        except Exception as e:
            logger.warning(f"  JRC cache file for RP{rp} unavailable, that tier will be skipped: {e}")
    try:
        paths["water"] = _resolve_jrc_file(JRC_WATER_FILENAME, jrc_source, jrc_local_dir, snowflake_conn,
                                            snowflake_stage_name, cache_dir)
    except Exception as e:
        logger.warning(f"  JRC permanent-water cache file unavailable, every tier will be skipped "
                        f"(all tiers need it): {e}")
    return paths


# ---------------------------------------------------------------------------
# Per-tier exceedance probability (vectorized point lookup against the
# already-sparse cell_lat/cell_lon
# ---------------------------------------------------------------------------

def _load_threshold_at_cells(cell_lat: np.ndarray, cell_lon: np.ndarray, rp: str,
                              threshold_path: Path) -> np.ndarray:
    """Vectorized via _nearest_grid_value (direct index arithmetic), NOT
    xarray's own multi-point .sel(method="nearest"), which is catastrophically
    slow at real cell counts on this grid."""
    official = xr.open_dataset(threshold_path)
    try:
        thr = _nearest_grid_value(cell_lat, cell_lon, official[f"rl_{rp}"].values,
                                   official["lat"].values, official["lon"].values)
        return np.where(thr > 0, thr, np.nan)
    finally:
        official.close()


def compute_tier_probabilities(cell_lat: np.ndarray, cell_lon: np.ndarray, data: np.ndarray,
                                n_members: int, threshold_source: str,
                                threshold_local_dir: Union[str, Path],
                                snowflake_conn=None, snowflake_stage_name: Optional[str] = None,
                                cache_dir: Optional[Path] = None) -> Dict[str, np.ndarray]:
    """
    Returns {rp: array of shape (n_steps, n_cells)}, one entry per
    EXTENT_RP_LEVELS tier -- fraction of members exceeding that tier's
    threshold, per cell per lead step. RP thresholds are monotonic by
    construction, so this is well-defined for every cell already kept by the
    upstream RP2 sparse filter (see glofas_downloader.py).

    ONLY used in the demo notebook for now. 
    """
    n_steps = data.shape[1]
    result = {}
    for rp in EXTENT_RP_LEVELS:
        threshold_path = _fetch_threshold_file(
            threshold_source, threshold_local_dir, snowflake_conn=snowflake_conn,
            snowflake_stage_name=snowflake_stage_name, cache_dir=cache_dir, rp=rp,
        )
        thr = _load_threshold_at_cells(cell_lat, cell_lon, rp, threshold_path)  # (n_cells,)
        # (n_members, n_steps, n_cells) > (n_cells,) broadcasts correctly over the last axis
        exceed = data > thr[np.newaxis, np.newaxis, :]
        prob = np.where(np.isnan(thr)[np.newaxis, :], 0.0,
                         exceed.sum(axis=0) / n_members)  # (n_steps, n_cells)
        result[rp] = prob.astype(np.float32)
        logger.info(f"  RP{rp}: {int((prob > 0).any(axis=0).sum())} of {len(cell_lat)} cells "
                    f"with nonzero probability on >=1 step")
    return result


def compute_tier_member_exceedance(cell_lat: np.ndarray, cell_lon: np.ndarray, data: np.ndarray,
                                    threshold_source: str, threshold_local_dir: Union[str, Path],
                                    snowflake_conn=None, snowflake_stage_name: Optional[str] = None,
                                    cache_dir: Optional[Path] = None) -> Dict[str, np.ndarray]:
    """
    Returns {rp: array of shape (n_members, n_steps, n_cells)} boolean: per-member
    exceedance of that tier's threshold, NOT collapsed across the member axis (unlike
    compute_tier_probabilities(), which discards this to produce one aggregate
    probability). Powers combine_tier_per_member_parquet()'s per-member flood-extent
    output.

    NaN thresholds (a cell with no valid RP value in the official threshold file) do
    not need explicit masking here: `data > nan` already evaluates to False for
    every member/step, so those cells simply never exceed, same as the aggregate path.
    """
    n_steps = data.shape[1]
    result = {}
    for rp in EXTENT_RP_LEVELS:
        threshold_path = _fetch_threshold_file(
            threshold_source, threshold_local_dir, snowflake_conn=snowflake_conn,
            snowflake_stage_name=snowflake_stage_name, cache_dir=cache_dir, rp=rp,
        )
        thr = _load_threshold_at_cells(cell_lat, cell_lon, rp, threshold_path)  # (n_cells,)
        exceed = data > thr[np.newaxis, np.newaxis, :]  # (n_members, n_steps, n_cells)
        result[rp] = exceed
        logger.info(f"  RP{rp}: {int(exceed.any(axis=(0, 1)).sum())} of {len(cell_lat)} cells "
                    f"with >=1 member exceeding on >=1 step")
    return result


# ---------------------------------------------------------------------------
# Shared per-cell JRC window/mask logic
# ---------------------------------------------------------------------------

def _cell_jrc_window_and_mask(lat: float, lon: float, depth_src, water_src, transform):
    """
    Computes one cell's padded window against the cached JRC raster, reads
    depth+water for it, and returns (window, mask) where mask is the
    pixel-membership + depth-threshold + not-water boolean array for pixels
    genuinely belonging to this cell, or None if the cell falls entirely
    outside the cached JRC raster's extent, or no pixel in its window
    actually qualifies. member/step-independent: computed once per cell,
    reused across every member and lead step that cell contributes to.
    """
    half_cell = 0.025  # GloFAS's own native 0.05deg cell half-width
    cell_size = 2 * half_cell

    def cell_key(lat_arr, lon_arr):
        klat = np.round((lat_arr - half_cell) / cell_size) * cell_size + half_cell
        klon = np.round((lon_arr - half_cell) / cell_size) * cell_size + half_cell
        return klat, klon

    target_klat, target_klon = cell_key(np.array([lat]), np.array([lon]))
    target_klat, target_klon = float(target_klat[0]), float(target_klon[0])

    # Buffer by one pixel beyond the cell's own bounds and round OUTWARD (floor
    # the start, ceil the end). Guarantees the
    # window fully contains every pixel that could possibly round to this cell's
    # key, the cell-key membership test below is what actually decides
    # inclusion, this window is deliberately generous, not exact.
    inv = ~transform
    west, north = lon - half_cell - abs(transform.a), lat + half_cell + abs(transform.e)
    east, south = lon + half_cell + abs(transform.a), lat - half_cell - abs(transform.e)
    col_start_f, row_start_f = inv * (west, north)
    col_end_f, row_end_f = inv * (east, south)
    col_off = int(np.floor(min(col_start_f, col_end_f)))
    row_off = int(np.floor(min(row_start_f, row_end_f)))
    col_end = int(np.ceil(max(col_start_f, col_end_f)))
    row_end = int(np.ceil(max(row_start_f, row_end_f)))
    window = rasterio.windows.Window(col_off, row_off, col_end - col_off, row_end - row_off)
    # Window.intersection() raises rasterio.errors.WindowError (not a
    # zero-size Window) when there is no overlap at all, it can only ever
    # return a window with strictly positive width/height
    try:
        window = window.intersection(rasterio.windows.Window(0, 0, depth_src.width, depth_src.height))
    except rasterio.errors.WindowError:
        return None  # cell falls outside the cached band's extent

    depth = depth_src.read(1, window=window)
    water = water_src.read(1, window=window)

    rows, cols = np.indices(depth.shape)
    px_lon, px_lat = rasterio.transform.xy(transform, rows + window.row_off, cols + window.col_off)
    px_lon = np.array(px_lon).reshape(depth.shape)
    px_lat = np.array(px_lat).reshape(depth.shape)
    pklat, pklon = cell_key(px_lat, px_lon)
    belongs = (np.abs(pklat - target_klat) < 1e-9) & (np.abs(pklon - target_klon) < 1e-9)

    mask = (depth > DEPTH_THRESHOLD_M) & (water == 0) & belongs
    if not mask.any():
        return None

    return window, mask


# ---------------------------------------------------------------------------
# Combination: nearest-cell hard assignment onto the cached JRC extent grid
# ---------------------------------------------------------------------------

def combine_tier(rp: str, cell_lat: np.ndarray, cell_lon: np.ndarray,
                  prob_by_step: np.ndarray, keep_mask: np.ndarray,
                  jrc_paths: Dict[str, Path], leadtime_hours: List[int],
                  out_path: Path) -> Dict:
    """
    Write one multi-band GeoTIFF (one band per lead step) for a single RP
    tier.

    ONLY used in the demo notebook for now. 
    """
    source_tier = EXTENT_SOURCE_TIER[rp]
    depth_path = jrc_paths[source_tier]
    water_path = jrc_paths["water"]
    n_steps = len(leadtime_hours)

    with rasterio.open(depth_path) as depth_src, rasterio.open(water_path) as water_src:
        if depth_src.transform != water_src.transform or depth_src.shape != water_src.shape:
            raise ValueError(
                f"JRC depth ({depth_path.name}) and permanent-water ({water_path.name}) rasters "
                f"do not share the same grid -- setup_jrc_extents.py must produce co-registered outputs"
            )
        profile = depth_src.profile.copy()
        profile.update(count=n_steps, dtype="float32", nodata=0.0,
                        tiled=True, blockxsize=512, blockysize=512,
                        compress="DEFLATE", predictor=2, sparse_ok="YES", bigtiff="IF_SAFER")

        active_idx = np.where(keep_mask)[0]
        n_pixels_written = 0
        transform = depth_src.transform

        out_path.parent.mkdir(parents=True, exist_ok=True)

        with rasterio.open(out_path, "w", **profile):
            pass
        n_cells_failed = 0
        with rasterio.open(out_path, "r+") as dst:
            for i in active_idx:
                try:
                    result = _cell_jrc_window_and_mask(
                        float(cell_lat[i]), float(cell_lon[i]), depth_src, water_src, transform)
                    if result is None:
                        continue
                    window, mask = result

                    for step_idx in range(n_steps):
                        p = float(prob_by_step[step_idx, i])
                        if p <= 0:
                            continue
                        existing = dst.read(step_idx + 1, window=window)
                        band = np.where(mask, p, existing)
                        dst.write(band, step_idx + 1, window=window)

                    n_pixels_written += int(mask.sum())
                except Exception as e:
                    n_cells_failed += 1
                    logger.warning(f"  RP{rp}: cell ({cell_lat[i]:.4f}, {cell_lon[i]:.4f}) failed, "
                                    f"skipping just this cell: {e}")
                    continue
        if n_cells_failed:
            logger.warning(f"  RP{rp}: {n_cells_failed} of {len(active_idx)} cells failed and were skipped")

    return {"rp": rp, "n_cells": int(len(active_idx)), "n_pixels": n_pixels_written,
            "is_standin": IS_STANDIN[rp], "path": out_path}


def combine_tier_per_member_parquet(rp: str, cell_lat: np.ndarray, cell_lon: np.ndarray,
                                     exceed_by_member: Dict[str, np.ndarray], keep_mask: np.ndarray,
                                     jrc_paths: Dict[str, Path], leadtime_hours: List[int],
                                     member_numbers: np.ndarray, out_path: Path,
                                     below_min_basin: Optional[np.ndarray] = None,
                                     flush_every_rows: int = 2_000_000) -> Dict:
    """
    Writes one Parquet file for a single RP tier: one row per (pixel, member,
    step) that IS flooded for that member after JRC pixel-matching: sparse,
    True-only (the overwhelming majority of pixel/member/step combinations
    are NOT flooded, so only writing the positive case keeps this small).
    
    Columns: pixel_lat, pixel_lon, member (real ECMWF member id, 1-50 pf /
    51 control), step_h, below_min_basin (True if this cell's own uparea is
    known to be below JRC's own 500km^2 minimum-catchment cutoff for
    generating flood-extent maps, a matching-availability flag, not a
    statement about this cell's discharge forecast being less trustworthy;
    `below_min_basin` is per-cell (shape (n_cells,), aligned to cell_lat/
    cell_lon), broadcast onto every row a given cell contributes. None (the
    default) means "not evaluated", every row gets False, same as an
    unknown/NaN uparea value upstream.

    Reuses _cell_jrc_window_and_mask(): the exact same window/JRC-read/
    membership logic combine_tier() uses for the aggregate case, so the
    expensive per-cell geometry (computed once per cell, unaffected by
    member count) is identical between the two paths. Only the innermost
    "what do we do with this cell's mask" step differs: instead of blending
    across members into one probability value, every member that exceeds
    this cell's threshold on a given step gets its own row for every masked
    pixel in that cell.

    Streams to disk via a pyarrow ParquetWriter, flushing every
    `flush_every_rows` accumulated rows, rather than building one giant
    in-memory table across every cell before writing once.
    """
    source_tier = EXTENT_SOURCE_TIER[rp]
    depth_path = jrc_paths[source_tier]
    water_path = jrc_paths["water"]
    n_steps = len(leadtime_hours)
    exceed = exceed_by_member[rp]  # (n_members, n_steps, n_cells) boolean
    if below_min_basin is None:
        below_min_basin = np.zeros(len(cell_lat), dtype=bool)

    schema = pa.schema([
        ("pixel_lat", pa.float64()), ("pixel_lon", pa.float64()),
        ("member", pa.int16()), ("step_h", pa.int16()),
        ("below_min_basin", pa.bool_()),
    ])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lat_chunks, lon_chunks, member_chunks, step_chunks, basin_chunks = [], [], [], [], []
    buffered_rows = 0
    total_rows = 0
    writer = None

    def flush():
        nonlocal lat_chunks, lon_chunks, member_chunks, step_chunks, basin_chunks, buffered_rows, total_rows
        if not lat_chunks:
            return
        table = pa.table({
            "pixel_lat": np.concatenate(lat_chunks),
            "pixel_lon": np.concatenate(lon_chunks),
            "member": np.concatenate(member_chunks).astype(np.int16),
            "step_h": np.concatenate(step_chunks),
            "below_min_basin": np.concatenate(basin_chunks),
        }, schema=schema)
        writer.write_table(table)
        total_rows += buffered_rows
        lat_chunks, lon_chunks, member_chunks, step_chunks, basin_chunks = [], [], [], [], []
        buffered_rows = 0

    try:
        with rasterio.open(depth_path) as depth_src, rasterio.open(water_path) as water_src:
            if depth_src.transform != water_src.transform or depth_src.shape != water_src.shape:
                raise ValueError(
                    f"JRC depth ({depth_path.name}) and permanent-water ({water_path.name}) rasters "
                    f"do not share the same grid -- setup_jrc_extents.py must produce co-registered outputs"
                )
            # Only create the output file AFTER the co-registration check above
            # can no longer raise, matches combine_tier()'s own ordering
            # (its GeoTIFF is likewise only opened once that same check has
            # already passed). Creating the writer any earlier would leave a
            # stale, misleadingly "successful-looking" empty Parquet file on
            # disk if this tier fails validation before processing any cells.
            writer = pq.ParquetWriter(str(out_path), schema, compression="snappy")

            transform = depth_src.transform
            active_idx = np.where(keep_mask)[0]
            n_cells_failed = 0

            for i in active_idx:
                # Same per-cell exception isolation as combine_tier(): one cell's
                # failure must only drop that cell, never the whole tier. The
                # periodic flush() call below is deliberately OUTSIDE this
                # try/except. A flush-time write error is a real, tier-level
                # I/O failure (not this cell's fault), and letting it propagate
                # (instead of being caught and mislabeled as "this cell failed"
                # while its already-buffered rows from many prior cells are
                # silently retained rather than retried or discarded) is the
                # correct behavior: it aborts this tier loudly via the outer
                # try/finally, same as a genuine unrecoverable error should.
                try:
                    result = _cell_jrc_window_and_mask(
                        float(cell_lat[i]), float(cell_lon[i]), depth_src, water_src, transform)
                    if result is None:
                        continue
                    window, mask = result

                    # Pixel lat/lon for this cell's masked pixels, member/step
                    # independent, computed once per cell and reused below.
                    mask_rows, mask_cols = np.where(mask)
                    if mask_rows.size == 0:
                        continue
                    px_lon_1d, px_lat_1d = rasterio.transform.xy(
                        transform, mask_rows + window.row_off, mask_cols + window.col_off)
                    px_lat_1d = np.asarray(px_lat_1d, dtype=np.float64)
                    px_lon_1d = np.asarray(px_lon_1d, dtype=np.float64)
                    n_px = px_lat_1d.size

                    for step_idx in range(n_steps):
                        members_exceeding = np.where(exceed[:, step_idx, i])[0]
                        if members_exceeding.size == 0:
                            continue
                        member_ids = member_numbers[members_exceeding]
                        n_mem = member_ids.size
                        # Cartesian product of this cell's masked pixels x this
                        # step's exceeding members, vectorized (np.tile/repeat),
                        # not a Python-level double loop over members.
                        lat_chunks.append(np.tile(px_lat_1d, n_mem))
                        lon_chunks.append(np.tile(px_lon_1d, n_mem))
                        member_chunks.append(np.repeat(member_ids, n_px))
                        step_chunks.append(np.full(n_mem * n_px, leadtime_hours[step_idx], dtype=np.int16))
                        basin_chunks.append(np.full(n_mem * n_px, bool(below_min_basin[i])))
                        buffered_rows += n_mem * n_px
                except Exception as e:
                    n_cells_failed += 1
                    logger.warning(f"  RP{rp}: cell ({cell_lat[i]:.4f}, {cell_lon[i]:.4f}) failed, "
                                    f"skipping just this cell: {e}")
                    continue

                if buffered_rows >= flush_every_rows:
                    flush()
            if n_cells_failed:
                logger.warning(f"  RP{rp}: {n_cells_failed} of {len(active_idx)} cells failed and were skipped")

        flush()  # final partial batch
    finally:
        if writer is not None:
            writer.close()

    return {"rp": rp, "n_cells": int(len(active_idx)), "n_rows": total_rows,
            "is_standin": IS_STANDIN[rp], "path": out_path}


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def run_glofas_extent_masking(
        zarr_path: Path,
        forecast_date: datetime,
        output_dir: Union[str, Path],
        threshold_source: str,
        threshold_local_dir: Union[str, Path],
        jrc_source: str,
        jrc_local_dir: Union[str, Path],
        snowflake_conn=None,
        snowflake_stage_name: Optional[str] = None,
        upload_to_stage: bool = True,
        min_basin_km2: float = MIN_BASIN_KM2,
) -> List[Dict]:
    """
    Reads the sparse cell set from the just-built raw-discharge Zarr,
    computes per-tier per-member exceedance, combines with the cached JRC
    extent + permanent-water rasters, writes one Parquet file per RP tier
    (one row per pixel/member/step that's flooded for that member, JRC
    pixel-matched, plus a `below_min_basin` column flagging cells under
    JRC's own 500km^2 minimum-catchment cutoff for generating flood-extent
    maps, and (if upload_to_stage) PUTs each to
    the Snowflake stage. Best-effort per tier, so one tier's failure is logged
    and skipped, never aborts the others or the raw-discharge upload that
    already succeeded upstream.

    Returns a list of metadata dicts (one per successfully-written tier):
    {rp, is_standin, stage_path, n_cells, n_rows}, the caller maps `rp`
    into a PARAM value ('extent_rp{N}_bymember') and loads these alongside
    raw discharge rows via load_riverine_metadata_to_snowflake() (one shared
    RIVER_FORECASTS table, not a separate RIVER_EXTENT_FORECASTS).
    """
    date_str = forecast_date.strftime("%Y%m%d")
    output_path = Path(output_dir)
    cache_dir = output_path / "thresholds_cache"

    store = zarr.storage.ZipStore(str(zarr_path), mode="r")
    try:
        root = zarr.open_group(store=store, mode="r")
        cell_lat = root["cell_lat"][:]
        cell_lon = root["cell_lon"][:]
        data = root["data"][:]
        leadtime_hours = list(root.attrs["leadtime_hours"])
        member_numbers = np.asarray(root.attrs["member_numbers"])
        cell_uparea_km2 = root["cell_uparea_km2"][:] if "cell_uparea_km2" in root else None
    finally:
        store.close()

    n_cells = len(cell_lat)
    logger.info(f"Extent masking: {n_cells:,} candidate cells from {zarr_path.name}")
    if n_cells == 0:
        logger.info("  No active cells today, nothing to combine")
        return []

    # NOT used as an exclusion criteria
    below_min_basin = (np.zeros(n_cells, dtype=bool) if cell_uparea_km2 is None
                       else (~np.isnan(cell_uparea_km2) & (cell_uparea_km2 < min_basin_km2)))
    if cell_uparea_km2 is not None:
        n_flagged = int(below_min_basin.sum())
        logger.info(f"  uparea flag (< {min_basin_km2:.0f}km^2): {n_flagged:,} of {n_cells:,} cells flagged "
                    f"below JRC's own minimum-catchment cutoff for flood-extent maps")
    else:
        logger.warning("  No cell_uparea_km2 in this Zarr, below_min_basin will be False for every cell this run")

    try:
        exceed_by_tier = compute_tier_member_exceedance(
            cell_lat, cell_lon, data, threshold_source, threshold_local_dir,
            snowflake_conn=snowflake_conn, snowflake_stage_name=snowflake_stage_name, cache_dir=cache_dir,
        )
    except Exception as e:
        logger.error(f"  Could not compute per-tier member exceedance, aborting extent masking: {e}")
        return []

    # resolve_jrc_cache() is itself per-file tolerant (never raises)
    # missing/corrupted JRC file for one tier must not take down every tier.
    # This outer try/except only guards against a genuinely unexpected error
    # in the resolution machinery itself, not an individual missing file.
    try:
        jrc_paths = resolve_jrc_cache(jrc_source, jrc_local_dir, snowflake_conn=snowflake_conn,
                                       snowflake_stage_name=snowflake_stage_name,
                                       cache_dir=output_path / "jrc_extent_cache")
    except Exception as e:
        logger.error(f"  Could not resolve JRC cache at all, aborting extent masking: {e}")
        return []

    results = []
    for rp in EXTENT_RP_LEVELS:
        source_tier = EXTENT_SOURCE_TIER[rp]
        if source_tier not in jrc_paths or "water" not in jrc_paths:
            logger.info(f"  RP{rp}: required JRC cache file(s) unavailable ({source_tier} depth "
                        f"and/or permanent-water) — skipping this tier")
            continue

        exceed = exceed_by_tier[rp]  # (n_members, n_steps, n_cells)
        keep_mask = exceed.any(axis=(0, 1))
        if not keep_mask.any():
            logger.info(f"  RP{rp}: no cells survive filtering — skipping")
            continue

        # rp_int (not the raw "10.0" string) so the filename matches the extent_rp{N}
        # PARAM convention used everywhere else for this tier (e.g. extent_rp10, not
        # extent_rp10.0). Local path mirrors the Snowflake stage's own {date}/ layout
        # exactly (no separate "extent/" subfolder), both raw discharge and this
        # per-member output are per-cycle products of the same forecast date.
        rp_int = int(float(rp))
        out_path = output_path / date_str / f"river_extent_rp{rp_int}_bymember_{date_str}.parquet"
        try:
            info = combine_tier_per_member_parquet(rp, cell_lat, cell_lon, exceed_by_tier, keep_mask,
                                                    jrc_paths, leadtime_hours, member_numbers, out_path,
                                                    below_min_basin=below_min_basin)
        except Exception as e:
            logger.error(f"  RP{rp}: combination failed, skipping this tier: {e}")
            continue
        n_flagged_cells = int((keep_mask & below_min_basin).sum())
        logger.info(f"  RP{rp}: {info['n_cells']} cells ({n_flagged_cells} below_min_basin), "
                    f"{info['n_rows']:,} flooded pixel/member/step rows -> {out_path.name}")

        stage_path = None
        if upload_to_stage:
            if not snowflake_conn or not snowflake_stage_name:
                logger.warning(f"  RP{rp}: upload_to_stage requested but no Snowflake connection — keeping local only")
            else:
                stage_path = f"{EXTENT_STAGE_PREFIX}/{date_str}/{out_path.name}"
                ok = upload_to_snowflake_stage(out_path, snowflake_stage_name, stage_path, snowflake_conn)
                if not ok:
                    logger.error(f"  RP{rp}: stage upload failed — skipping metadata row for this tier")
                    continue

        results.append({
            "rp": rp, "is_standin": IS_STANDIN[rp], "stage_path": stage_path,
            "n_cells": info["n_cells"], "n_rows": info["n_rows"],
        })

    return results
