#!/usr/bin/env python3
"""
One-time setup: cache JRC v2.1 flood-extent + permanent-water rasters for the
GloFAS discharge x JRC extent-masking step (glofas_extent_masking.py).

NOT part of the recurring pipeline: run this manually once (or again only if
JRC ships a new model version, or the cached bbox/resolution needs changing).
Mirrors setup_glofas_thresholds.py's own one-time-script shape exactly.

Source: JRC Global River Flood Hazard Maps v2.1 (dataset version 2.1.2),
        published as direct, anonymous, unauthenticated GeoTIFF downloads on
        the JRC Data Store

        Base URL: https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/
        271 tiles per return period (RP10/RP20/RP50/RP75/RP100/RP200/RP500),
        271 permanent-water tiles. 3 arc-second (~90m) native resolution.
        Built on LISFLOOD/LISFLOOD-FP, the same model family GloFAS's own
        discharge forecast uses.

Coverage: the same 60S-60N band GloFAS's own discharge fetch already uses
        (glofas_downloader.py's AREA constant), NOT a per-country list (this
        repo has no country concept anywhere, and none is introduced here).
        Only JRC tiles intersecting this band are downloaded/cached.

Mosaicking: JRC's own tiles are downloaded as-is, then streamed one at a time
        into a single per-band output GeoTIFF via rasterio.warp.reproject()
        (never an in-memory full-mosaic array; see build_band_mosaic) so
        glofas_extent_masking.py's resolve_jrc_cache()/combine_tier() can keep
        treating each band as one file, unchanged.

Usage:
    python3 setup_jrc_extents.py                                            # upload to Snowflake stage
    python3 setup_jrc_extents.py --local-only glofas_data/jrc_extent_cache  # keep local only, for offline dev
                                                                            # (this is glofas_extent_masking.py's
                                                                            # own local default: one root dir,
                                                                            # no separate top-level cache)
    python3 setup_jrc_extents.py --bbox 88 20 93 26                         # smaller region, for testing
                                                                            # (west south east north)
"""

import argparse
import json
import logging
import math
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import rasterio
import requests
from rasterio.transform import from_origin
from rasterio.warp import reproject, Resampling
from rasterio.windows import Window, transform as window_transform

sys.path.insert(0, str(Path(__file__).parent / "github_actions"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

JRC_FTP_BASE = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard"
JRC_TILE_EXTENTS_URL = f"{JRC_FTP_BASE}/tile_extents.geojson"

# Internal RP-tier keys (shared with glofas_downloader.py/glofas_extent_masking.py)
# -> JRC's own folder/filename RP token.
RP_FOLDER = {"10.0": "RP10", "20.0": "RP20", "50.0": "RP50", "100.0": "RP100"}
WATER_FOLDER = "Permanent_WaterBodies"

# Matches glofas_downloader.py's AREA clip (LAT_MIN/LAT_MAX = -60/60), not a country list
GLOBAL_BBOX = (-180.0, -60.0, 180.0, 60.0)  # west, south, east, north

DEFAULT_SCALE_M = 150.0  # output mosaic resolution; JRC's own native tiles are ~90m

# Must be kept in sync with JRC_STAGE_PREFIX / JRC_DEPTH_FILENAME / JRC_WATER_FILENAME
# in glofas_extent_masking.py, since there is no shared import between this
# one-time script and the recurring daily module.
JRC_VERSION = "v2_1"
STAGE_PREFIX = f"glofas/jrc_extent_cache/{JRC_VERSION}"
OUTPUT_FILENAME = {"10.0": "jrc_rp10.tif", "20.0": "jrc_rp20.tif",
                    "50.0": "jrc_rp50.tif", "100.0": "jrc_rp100.tif",
                    "water": "jrc_permanent_water.tif"}

MAX_FETCH_RETRIES = 4
RETRY_BACKOFF_BASE_S = 2.0


class PersistentTileFetchError(RuntimeError):
    """Raised when a tile download fails after all retries. Must propagate all
    the way up and abort that band's whole mosaic build (see
    build_band_mosaic), never silently skip a tile that JRC's own tile index
    says should exist, since that would leave a real geographic hole in the
    cached raster indistinguishable from "genuinely no flood risk here"."""


def _load_jrc_tile_index(bbox: Tuple[float, float, float, float],
                          cache_path: Optional[Path] = None) -> List[Tuple[int, str, Tuple[float, float, float, float]]]:
    """Fetch (or reuse a local cache of) JRC's own tile_extents.geojson and
    return (id, name, tile_bbox) for every tile intersecting bbox. JRC only
    publishes tiles that actually have data (271 of the theoretical 432
    10x10deg cells in a 60S-60N band)"""
    if cache_path and cache_path.exists():
        geojson_text = cache_path.read_text()
    else:
        resp = requests.get(JRC_TILE_EXTENTS_URL, timeout=60)
        resp.raise_for_status()
        geojson_text = resp.text
        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(geojson_text)

    geojson = json.loads(geojson_text)
    west, south, east, north = bbox
    tiles = []
    for feature in geojson["features"]:
        tid = feature["properties"]["id"]
        name = feature["properties"]["name"]
        lons = [c[0] for c in feature["geometry"]["coordinates"][0]]
        lats = [c[1] for c in feature["geometry"]["coordinates"][0]]
        tbbox = (min(lons), min(lats), max(lons), max(lats))
        tw, ts, te, tn = tbbox
        if te > west and tw < east and tn > south and ts < north:
            tiles.append((tid, name, tbbox))
    return tiles


def download_tile(url: str, dest_path: Path) -> None:
    """Download one JRC tile file with retry+backoff on transient errors.
    A 404 here is unexpected (JRC's own tile index said this file should
    exist) and is treated the same as any other persistent failure, never
    silently skipped, since that would leave an undetected hole in the cache."""
    last_error = None
    for attempt in range(MAX_FETCH_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=120)
        except requests.exceptions.RequestException as e:
            last_error = e
        else:
            if resp.status_code == 200:
                dest_path.write_bytes(resp.content)
                return
            last_error = RuntimeError(f"HTTP {resp.status_code}")

        if attempt < MAX_FETCH_RETRIES:
            backoff = RETRY_BACKOFF_BASE_S * (2 ** attempt)
            logger.warning(f"    {url}: {last_error} (attempt {attempt + 1}/"
                            f"{MAX_FETCH_RETRIES + 1}) -- retrying in {backoff:.0f}s")
            time.sleep(backoff)

    raise PersistentTileFetchError(f"Persistent failure downloading {url}: {last_error}")


def build_band_mosaic(rp_key: str, bbox: Tuple[float, float, float, float], scale_m: float,
                       work_dir: Path, out_path: Path, tile_index: List[Tuple[int, str, Tuple]]) -> None:
    """Download every JRC tile intersecting bbox for one RP tier (or the
    permanent-water layer), mosaic into a single output GeoTIFF with
    COG-equivalent tiling. Skips tiles already downloaded (resumable)."""
    is_water = (rp_key == "water")
    folder = WATER_FOLDER if is_water else RP_FOLDER[rp_key]
    logger.info(f"  {folder}: {len(tile_index)} JRC tiles to fetch")

    tile_dir = work_dir / folder
    tile_dir.mkdir(parents=True, exist_ok=True)
    tile_paths = []
    for i, (tid, name, tbbox) in enumerate(tile_index):
        if is_water:
            fname = f"ID{tid}_{name}_permanent_water.tif"
        else:
            fname = f"ID{tid}_{name}_{folder}_depth.tif"
        tile_path = tile_dir / fname
        if not tile_path.exists():
            url = f"{JRC_FTP_BASE}/{folder}/{fname}"
            try:
                download_tile(url, tile_path)
            except PersistentTileFetchError:
                # Deliberately NOT caught-and-skipped: JRC's own tile index
                # said this file exists, so a persistent failure must abort
                # this band's whole mosaic rather than silently cache a
                # raster with a real geographic hole in it.
                raise
        tile_paths.append(tile_path)
        if (i + 1) % 50 == 0:
            logger.info(f"    {i + 1}/{len(tile_index)} tiles downloaded")

    if not tile_paths:
        raise RuntimeError(f"{folder}: no tiles intersect bbox {bbox} -- check bbox")

    logger.info(f"  {folder}: mosaicking {len(tile_paths)} tiles ...")
    # bounds/res are pinned to the FULL requested bbox/scale (not derived from
    # whichever tiles this band happened to have) so every band's output
    # shares the exact same shape/transform, permanent_water_class and the
    # RP*_depth tiers don't necessarily cover identical tile sets at the
    # domain's edges, and mismatched bounds would trip the co-registration
    # check below after every band has already been fully downloaded/mosaicked.
    pixel_deg = scale_m / 111_000.0
    west, south, east, north = bbox
    width = round((east - west) / pixel_deg)
    height = round((north - south) / pixel_deg)
    out_transform = from_origin(west, north, pixel_deg, pixel_deg)

    with rasterio.open(tile_paths[0]) as ref:
        profile = ref.profile.copy()
    # JRC's own Permanent_WaterBodies tiles encode ONLY "1" (water); every
    # other pixel, including genuine land that just isn't water, is
    # nodata (255), not an explicit 0 the way the depth bands' nodata works.
    # combine_tier()'s mask requires water==0 for "not water"; left as 255
    # that condition is never true anywhere, silently zeroing every cell
    # (confirmed directly: 0 combined pixels despite a real, populated depth
    # mosaic). Fixed by remapping nodata->0 for this band specifically
    # safe because combine_tier()'s depth>0.1m condition already gates real
    # coverage, so collapsing "land, not water" and "no JRC coverage here"
    # into the same 0 loses no information the mask logic actually uses.
    water_nodata = ref.nodata if is_water else None
    if is_water:
        profile["nodata"] = 0
    profile.update(height=height, width=width, transform=out_transform, count=1,
                    tiled=True, blockxsize=512, blockysize=512,
                    compress="DEFLATE", predictor=2, bigtiff="IF_SAFER", sparse_ok="YES")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Stream each tile into its own window of the output raster rather than
    # materializing the full mosaic as one in-memory array (rasterio.merge()'s
    # approach), a single band at true 60S-60N/150m scale would be ~95GB as
    # one uncompressed array. Peak memory here is bounded by a single tile's
    # window (a few MB), not the whole mosaic.
    #
    # reproject(destination=rasterio.band(dst, 1)) does NOT read-modify-write:
    # each call fills dst_nodata across the ENTIRE destination extent outside
    # the source's own footprint, silently wiping out every previously-written
    # tile (confirmed directly: writing tile 2 zeroed out tile 1's already-
    # written pixels). Same class of bug as the combine_tier() overlap bug
    # fixed earlier; fixed the same way: reproject into a small array scoped
    # to just this tile's own destination window, then np.where() it against
    # the window's EXISTING contents before writing back.
    nodata = profile.get("nodata")
    dtype = profile["dtype"]
    inv_transform = ~out_transform
    with rasterio.open(out_path, "w", **profile):
        pass
    with rasterio.open(out_path, "r+") as dst:
        for tile_path in tile_paths:
            with rasterio.open(tile_path) as tsrc:
                tw, ts, te, tn = tsrc.bounds
                c0, r0 = inv_transform * (tw, tn)
                c1, r1 = inv_transform * (te, ts)
                col_off = max(int(math.floor(min(c0, c1))), 0)
                row_off = max(int(math.floor(min(r0, r1))), 0)
                col_end = min(int(math.ceil(max(c0, c1))), width)
                row_end = min(int(math.ceil(max(r0, r1))), height)
                if col_end <= col_off or row_end <= row_off:
                    continue  # tile's footprint doesn't actually intersect the output grid
                win = Window(col_off, row_off, col_end - col_off, row_end - row_off)
                win_transform = window_transform(win, out_transform)
                existing = dst.read(1, window=win)
                reprojected = np.full((row_end - row_off, col_end - col_off), nodata, dtype=dtype)
                if is_water:
                    src_array = tsrc.read(1)
                    src_array = np.where(src_array == water_nodata, 0, src_array).astype(dtype)
                    src_nodata_arg = None
                else:
                    src_array = rasterio.band(tsrc, 1)
                    src_nodata_arg = tsrc.nodata
                reproject(
                    source=src_array,
                    destination=reprojected,
                    src_transform=tsrc.transform,
                    src_crs=tsrc.crs,
                    src_nodata=src_nodata_arg,
                    dst_transform=win_transform,
                    dst_crs=dst.crs,
                    dst_nodata=nodata,
                    resampling=Resampling.nearest,
                )
                combined = np.where(reprojected != nodata, reprojected, existing)
                dst.write(combined, 1, window=win)

    size_mb = out_path.stat().st_size / 1024 / 1024
    logger.info(f"  {folder}: wrote {out_path.name} ({size_mb:.0f} MB, shape ({height}, {width}))")


def upload_to_stage(local_path: Path, stage_name: str, conn) -> bool:
    stage_path = f"{STAGE_PREFIX}/{local_path.name}"
    put_sql = (
        f"PUT 'file://{local_path.resolve().as_posix()}' "
        f"@{stage_name}/{STAGE_PREFIX}/ "
        f"OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
    )
    cursor = conn.cursor()
    try:
        cursor.execute(put_sql)
        result = cursor.fetchone()
    finally:
        cursor.close()
    status = result[6] if result else 'unknown'
    logger.info(f"  PUT {local_path.name} -> @{stage_name}/{stage_path}  [{status}]")
    return status in ('UPLOADED', 'SKIPPED')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local-only", metavar="DIR", default=None,
                         help="Keep files in DIR only, do not upload to Snowflake stage "
                              "(for offline dev; matches GLOFAS_JRC_SOURCE=local)")
    parser.add_argument("--dest-dir", default="jrc_extents_tmp",
                         help="Local working directory for downloads/mosaics (default: jrc_extents_tmp)")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("WEST", "SOUTH", "EAST", "NORTH"),
                         default=None, help="Override the default 60S-60N global bbox "
                                             "(for testing against a smaller region)")
    parser.add_argument("--scale", type=float, default=DEFAULT_SCALE_M,
                         help=f"Output mosaic resolution in meters (default: {DEFAULT_SCALE_M})")
    args = parser.parse_args()

    bbox = tuple(args.bbox) if args.bbox else GLOBAL_BBOX
    dest_dir = Path(args.dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info(f"JRC v2.1 flood-extent cache setup -- bbox {bbox}, scale {args.scale}m")
    logger.info(f"Source: direct JRC download (no GEE, no auth, no quota)")
    logger.info("=" * 70)

    # "already built" is otherwise judged purely by filename (jrc_rp10.tif etc.), with no
    # check that an existing file was actually built for THIS run's bbox/scale, a leftover
    # small-bbox test mosaic in the same dest_dir would be silently reused as the finished
    # cache and uploaded to the Snowflake stage as if it were the real global build. This
    # manifest records the bbox/scale that produced dest_dir's current files, and forces a
    # full rebuild (rather than a partial, mixed-provenance one) whenever they don't match.
    manifest_path = dest_dir / ".mosaic_manifest.json"
    current_manifest = {"bbox": list(bbox), "scale": args.scale}
    if manifest_path.exists():
        try:
            prev_manifest = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError):
            prev_manifest = None
        if prev_manifest != current_manifest:
            logger.warning(
                f"  Existing files in {dest_dir} were built with a different bbox/scale "
                f"({prev_manifest}) than this run ({current_manifest}) -- deleting them and "
                f"forcing a full rebuild rather than silently reusing a stale/mismatched mosaic"
            )
            for rp_key in RP_FOLDER:
                (dest_dir / OUTPUT_FILENAME[rp_key]).unlink(missing_ok=True)
            (dest_dir / OUTPUT_FILENAME["water"]).unlink(missing_ok=True)
    manifest_path.write_text(json.dumps(current_manifest))

    tile_index = _load_jrc_tile_index(bbox, cache_path=dest_dir / "tile_extents.geojson")
    logger.info(f"  {len(tile_index)} JRC tiles intersect bbox {bbox}")

    out_paths = {}
    for rp_key in RP_FOLDER:
        out_path = dest_dir / OUTPUT_FILENAME[rp_key]
        if not out_path.exists():
            build_band_mosaic(rp_key, bbox, args.scale, dest_dir / "raw_tiles", out_path, tile_index)
        else:
            logger.info(f"  {OUTPUT_FILENAME[rp_key]} already built -- skipping")
        out_paths[rp_key] = out_path

    water_out = dest_dir / OUTPUT_FILENAME["water"]
    if not water_out.exists():
        build_band_mosaic("water", bbox, args.scale, dest_dir / "raw_tiles", water_out, tile_index)
    else:
        logger.info(f"  {OUTPUT_FILENAME['water']} already built -- skipping")
    out_paths["water"] = water_out

    # Co-registration check: glofas_extent_masking.py hard-requires this.
    with rasterio.open(out_paths["10.0"]) as ref:
        ref_transform, ref_shape = ref.transform, ref.shape
    for key, p in out_paths.items():
        with rasterio.open(p) as src:
            if src.transform != ref_transform or src.shape != ref_shape:
                logger.error(f"  {p.name} is not co-registered with {out_paths['10.0'].name} "
                              f"({src.shape} vs {ref_shape}) -- glofas_extent_masking.py requires "
                              f"identical grids across all cached bands")
                sys.exit(1)
    logger.info("  Co-registration check passed -- all bands share one grid")

    if args.local_only:
        local_only_dir = Path(args.local_only)
        local_only_dir.mkdir(parents=True, exist_ok=True)
        for p in out_paths.values():
            target = local_only_dir / p.name
            if p.resolve() != target.resolve():
                target.write_bytes(p.read_bytes())
        logger.info(f"Done -- {len(out_paths)} files kept locally in {local_only_dir} "
                    f"(GLOFAS_JRC_SOURCE=local should point here)")
        return

    import os
    from snowflake_loader import get_snowflake_connection  # noqa: local import, only needed for stage upload
    for var in ('SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
                'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'):
        if not os.getenv(var):
            logger.error(f"{var} not set -- required for Snowflake stage upload")
            sys.exit(1)
    stage_name = os.getenv('SNOWFLAKE_STAGE_NAME')
    if not stage_name:
        logger.error("SNOWFLAKE_STAGE_NAME not set -- required for Snowflake stage upload")
        sys.exit(1)

    conn = get_snowflake_connection()
    try:
        for p in out_paths.values():
            if not upload_to_stage(p, stage_name, conn):
                logger.error(f"  Upload failed for {p.name}")
                sys.exit(1)
    finally:
        conn.close()

    logger.info(f"Done -- {len(out_paths)} JRC cache files staged at @{stage_name}/{STAGE_PREFIX}/")


if __name__ == "__main__":
    main()
