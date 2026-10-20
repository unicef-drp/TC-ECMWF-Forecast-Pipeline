#!/usr/bin/env python3
"""
One-time setup: build the coastal lowland elevation cache used by the storm
surge step (ecmwf_surge_envelope_extractor.py).

NOT part of the recurring pipeline: run this manually once (or again only if
the cached bbox/resolution/limits need changing). Mirrors
setup_jrc_extents.py's own one-time-script shape.

What is cached
    One GeoTIFF on a regular lat/lon grid (default 15 arc-seconds, ~450 m),
    two bands, NaN everywhere except low-lying coastal land:
      band 1  elevation_m     mean SRTM elevation of the land part of the cell
      band 2  dist_coast_km   distance from the cell centre to the coastline
    A cell is kept when it is on land, its elevation is <= --max-elevation
    (default 10 m) and it lies within --max-dist-km (default 50 km) of the
    coast. These are the CLIMADA TCSurgeBathtub defaults (MAX_ELEVATION,
    MAX_DIST_COAST). Negative elevations are kept here; the surge step
    applies CLIMADA's own elevation >= 0 rule at run time, so it stays a
    configurable run-time choice rather than being baked into the cache.

Sources
    Elevation: NASA SRTM 90 m (SRTMGL3), downloaded through GigaSpatial's
        NasaSRTMHandler (Earthdata login: EARTHDATA_USERNAME /
        EARTHDATA_PASSWORD). Alternatively, --dem points at an existing DEM
        GeoTIFF and no download happens (offline dev / testing).
    Land and coastline: Natural Earth 10 m land + minor islands, through
        cartopy's own shapereader (cartopy is already a pipeline dependency).

Coverage
    Default bbox is the SRTM coverage band, 56S-60N, NOT a per-country list
    (this repo has no country concept, and none is introduced here). Only 1x1
    degree tiles within --max-dist-km of a coastline are processed.

Usage:
    python3 setup_surge_dem.py                                           # global, upload to Snowflake stage
    python3 setup_surge_dem.py --local-only surge_dem                    # global, keep local only
    python3 setup_surge_dem.py --bbox -79 17 -76 19 --local-only surge_dem   # small region, for testing
    python3 setup_surge_dem.py --bbox -79 17 -76 19 --dem JAM_srtm90.tif --local-only surge_dem
                                                                         # existing DEM, no download
"""

import argparse
import json
import logging
import math
import os
import sys
import time
import warnings
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from rasterio.warp import reproject, Resampling
from rasterio.windows import Window
from scipy.spatial import cKDTree
import shapely
from shapely.geometry import box
from shapely.ops import unary_union

sys.path.insert(0, str(Path(__file__).parent / "github_actions"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SRTM coverage band (west, south, east, north)
GLOBAL_BBOX = (-180.0, -56.0, 180.0, 60.0)

SRTM_ARCSEC = 3                     # SRTMGL3 native resolution
SRTM_PER_DEG = 3600 // SRTM_ARCSEC  # 1200 cells per degree
DEFAULT_RESOLUTION_ARCSEC = 15      # ~450 m at the equator

# CLIMADA TCSurgeBathtub defaults (climada_petals/hazard/tc_surge_bathtub.py)
DEFAULT_MAX_DIST_KM = 50.0
DEFAULT_MAX_ELEVATION_M = 10.0

EARTH_RADIUS_KM = 6371.0
COAST_DENSIFY_DEG = 0.005           # coastline vertex spacing (~500 m) for the distance lookup

# Must be kept in sync with SURGE_DEM_STAGE_PREFIX / SURGE_DEM_FILENAME in
# ecmwf_surge_envelope_extractor.py (no shared import between this one-time
# script and the recurring module, same convention as setup_jrc_extents.py).
SURGE_DEM_VERSION = "v1"
STAGE_PREFIX = f"surge/coastal_dem/{SURGE_DEM_VERSION}"
OUTPUT_FILENAME = "surge_coastal_dem.tif"


# ---------------------------------------------------------------------------
# Land and coastline (Natural Earth 10 m)
# ---------------------------------------------------------------------------

def load_land_and_coast(bbox: Tuple[float, float, float, float], pad_deg: float):
    """Return (land geometries clipped to the padded bbox, coastline lines).

    The coastline is taken from the boundaries of the UNCLIPPED land polygons,
    then clipped, so the artificial edges created by clipping the land to the
    bbox are never mistaken for coast.
    """
    from cartopy.io import shapereader

    w, s, e, n = bbox
    area = box(w - pad_deg, s - pad_deg, e + pad_deg, n + pad_deg)

    land_parts, coast_parts = [], []
    for name in ("land", "minor_islands"):
        path = shapereader.natural_earth(resolution="10m", category="physical", name=name)
        for geom in shapereader.Reader(path).geometries():
            if geom is None or not geom.intersects(area):
                continue
            geom = shapely.make_valid(geom)
            land_parts.append(geom.intersection(area))
            coast_parts.append(geom.boundary.intersection(area))

    land = [g for g in land_parts if not g.is_empty]
    coast = [g for g in coast_parts if not g.is_empty]
    logger.info(f"  Natural Earth: {len(land)} land polygons, {len(coast)} coastline pieces in padded bbox")
    return land, coast


def build_coast_index(coast: List) -> Optional[cKDTree]:
    """KD-tree on densified coastline vertices, in 3D unit-sphere coordinates."""
    if not coast:
        return None
    lines = shapely.segmentize(np.array(coast, dtype=object), max_segment_length=COAST_DENSIFY_DEG)
    xy = shapely.get_coordinates(lines)
    logger.info(f"  Coastline index: {len(xy):,} vertices")
    return cKDTree(_to_unit_xyz(xy[:, 1], xy[:, 0]))


def _to_unit_xyz(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    lat_r, lon_r = np.radians(lat), np.radians(lon)
    return np.column_stack((np.cos(lat_r) * np.cos(lon_r),
                            np.cos(lat_r) * np.sin(lon_r),
                            np.sin(lat_r)))


def distance_to_coast_km(tree: cKDTree, lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    chord, _ = tree.query(_to_unit_xyz(lat, lon))
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.clip(chord / 2.0, 0.0, 1.0))


def coastal_tiles(bbox: Tuple[float, float, float, float], coast: List,
                  max_dist_km: float) -> List[Tuple[int, int]]:
    """1x1 degree tiles (lat0, lon0) whose extent, grown by max_dist_km, touches a coastline."""
    if not coast:
        return []
    tree = shapely.STRtree(coast)
    w, s, e, n = (math.floor(bbox[0]), math.floor(bbox[1]), math.ceil(bbox[2]), math.ceil(bbox[3]))
    dlat = max_dist_km / 111.0
    tiles = []
    for lat0 in range(s, n):
        cos_lat = max(math.cos(math.radians(max(abs(lat0), abs(lat0 + 1)))), 0.1)
        dlon = max_dist_km / (111.0 * cos_lat)
        for lon0 in range(w, e):
            grown = box(lon0 - dlon, lat0 - dlat, lon0 + 1 + dlon, lat0 + 1 + dlat)
            if len(tree.query(grown, predicate="intersects")):
                tiles.append((lat0, lon0))
    return tiles


# ---------------------------------------------------------------------------
# Elevation sources
# ---------------------------------------------------------------------------

def _tile_name(lat0: int, lon0: int) -> str:
    ns = "N" if lat0 >= 0 else "S"
    ew = "E" if lon0 >= 0 else "W"
    return f"{ns}{abs(lat0):02d}{ew}{abs(lon0):03d}"


def download_srtm_tiles(tiles: List[Tuple[int, int]], srtm_dir: Path) -> Dict[Tuple[int, int], Path]:
    """Download SRTMGL3 tiles through GigaSpatial. Returns {(lat0, lon0): zip path}
    for the tiles that exist (all-ocean tiles have no SRTM file, which is expected)."""
    from gigaspatial.handlers.nasa import NasaSRTMHandler
    from gigaspatial.handlers.nasa.srtm import NasaSRTMConfig

    for var in ("EARTHDATA_USERNAME", "EARTHDATA_PASSWORD"):
        if not os.getenv(var):
            raise RuntimeError(f"{var} not set -- required to download SRTM through GigaSpatial "
                               f"(or pass --dem to use an existing DEM)")

    srtm_dir.mkdir(parents=True, exist_ok=True)
    config = NasaSRTMConfig(
        resolution="90m",
        base_path=srtm_dir,
        earthdata_username=os.environ["EARTHDATA_USERNAME"],
        earthdata_password=os.environ["EARTHDATA_PASSWORD"],
    )
    handler = NasaSRTMHandler(config=config)

    # Shrunk boxes so each request maps to exactly its own SRTM tile, never a neighbour
    area = unary_union([box(lon0 + 0.01, lat0 + 0.01, lon0 + 0.99, lat0 + 0.99) for lat0, lon0 in tiles])
    logger.info(f"  Requesting {len(tiles)} SRTM tiles through GigaSpatial (missing ones = ocean)")
    handler.ensure_data_available(source=area)

    found = {}
    for lat0, lon0 in tiles:
        path = config.get_data_unit_path(_tile_name(lat0, lon0))
        if Path(path).exists():
            found[(lat0, lon0)] = Path(path)
    logger.info(f"  {len(found)} SRTM tiles available locally")
    return found


def _open_srtm_zip(zip_path: Path):
    with zipfile.ZipFile(zip_path) as zf:
        hgt = next((m for m in zf.namelist() if m.lower().endswith(".hgt")), None)
    if hgt is None:
        raise RuntimeError(f"No .hgt file inside {zip_path}")
    return rasterio.open(f"zip://{zip_path.resolve().as_posix()}!{hgt}")


def read_tile_elevation(src, lat0: int, lon0: int) -> np.ndarray:
    """Elevation for one 1x1 degree tile on the native 3 arc-second grid, NaN where no data."""
    out = np.full((SRTM_PER_DEG, SRTM_PER_DEG), np.nan, dtype=np.float32)
    reproject(
        source=rasterio.band(src, 1),
        destination=out,
        src_nodata=src.nodata if src.nodata is not None else -32768,
        dst_transform=from_origin(lon0, lat0 + 1, 1.0 / SRTM_PER_DEG, 1.0 / SRTM_PER_DEG),
        dst_crs="EPSG:4326",
        dst_nodata=np.nan,
        resampling=Resampling.nearest,
    )
    return out


# ---------------------------------------------------------------------------
# Per-tile processing
# ---------------------------------------------------------------------------

def process_tile(elev3: np.ndarray, lat0: int, lon0: int, land_tree, land: List,
                 coast_index: cKDTree, factor: int, max_dist_km: float,
                 max_elevation: float) -> Optional[Tuple[np.ndarray, np.ndarray]]:
    """Aggregate one tile to the output grid. Returns (elevation_m, dist_coast_km)
    arrays of shape (n, n), NaN outside low-lying coastal land, or None when the
    tile has no such cell."""
    tile_box = box(lon0, lat0, lon0 + 1, lat0 + 1)
    hits = [land[i] for i in land_tree.query(tile_box, predicate="intersects")]
    if not hits:
        return None

    transform3 = from_origin(lon0, lat0 + 1, 1.0 / SRTM_PER_DEG, 1.0 / SRTM_PER_DEG)
    on_land = rasterize(hits, out_shape=elev3.shape, transform=transform3,
                        fill=0, default_value=1, dtype="uint8").astype(bool)
    if not on_land.any():
        return None

    # Mean elevation of the LAND part of each output cell (sea pixels excluded)
    elev3 = np.where(on_land, elev3, np.nan)
    n = SRTM_PER_DEG // factor
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)  # all-NaN (sea) blocks
        elev = np.nanmean(elev3.reshape(n, factor, n, factor), axis=(1, 3)).astype(np.float32)

    keep = np.isfinite(elev) & (elev <= max_elevation)
    if not keep.any():
        return None

    step = 1.0 / n
    rows, cols = np.nonzero(keep)
    lat = lat0 + 1 - (rows + 0.5) * step
    lon = lon0 + (cols + 0.5) * step
    dist = distance_to_coast_km(coast_index, lat, lon).astype(np.float32)

    within = dist <= max_dist_km
    if not within.any():
        return None

    out_elev = np.full((n, n), np.nan, dtype=np.float32)
    out_dist = np.full((n, n), np.nan, dtype=np.float32)
    out_elev[rows[within], cols[within]] = elev[rows[within], cols[within]]
    out_dist[rows[within], cols[within]] = dist[within]
    return out_elev, out_dist


def build_surge_dem(bbox: Tuple[float, float, float, float], resolution_arcsec: int,
                    max_dist_km: float, max_elevation: float, out_path: Path,
                    srtm_dir: Path, dem_path: Optional[Path] = None) -> Dict:
    if resolution_arcsec % SRTM_ARCSEC:
        raise ValueError(f"--resolution must be a multiple of {SRTM_ARCSEC} arc-seconds")
    factor = resolution_arcsec // SRTM_ARCSEC
    if SRTM_PER_DEG % factor:
        raise ValueError(f"--resolution must divide one degree evenly (got {resolution_arcsec}\")")
    n = SRTM_PER_DEG // factor

    # Snap to whole degrees so every 1x1 tile maps to one output window
    w, s, e, nn = (math.floor(bbox[0]), math.floor(bbox[1]), math.ceil(bbox[2]), math.ceil(bbox[3]))
    width, height = (e - w) * n, (nn - s) * n

    # Pad the land/coast lookup so coastlines just outside the bbox still count
    # for distance (2x covers the longitude stretch up to 60 degrees latitude)
    pad_deg = 2.0 * max_dist_km / 111.0 + 0.1
    land, coast = load_land_and_coast((w, s, e, nn), pad_deg=pad_deg)
    coast_index = build_coast_index(coast)
    tiles = coastal_tiles((w, s, e, nn), coast, max_dist_km)
    logger.info(f"  {len(tiles)} coastal 1x1 degree tiles in bbox ({w}, {s}, {e}, {nn})")
    if not tiles:
        raise RuntimeError("No coastline in bbox -- check --bbox")

    land_tree = shapely.STRtree(land)

    dem_src = rasterio.open(dem_path) if dem_path else None
    srtm_zips = {} if dem_src else download_srtm_tiles(tiles, srtm_dir)

    profile = {
        "driver": "GTiff", "dtype": "float32", "count": 2, "nodata": np.nan,
        "width": width, "height": height, "crs": "EPSG:4326",
        "transform": from_origin(w, nn, 1.0 / n, 1.0 / n),
        "tiled": True, "blockxsize": 256, "blockysize": 256,
        "compress": "deflate", "predictor": 3, "BIGTIFF": "IF_SAFER", "SPARSE_OK": "TRUE",
    }

    kept_cells, written_tiles, missing = 0, 0, 0
    t0 = time.time()
    try:
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.set_band_description(1, "elevation_m")
            dst.set_band_description(2, "dist_coast_km")
            dst.update_tags(
                resolution_arcsec=resolution_arcsec, max_dist_km=max_dist_km,
                max_elevation_m=max_elevation,
                elevation_source=str(dem_path.name) if dem_path else "NASA SRTMGL3 via GigaSpatial",
                coastline_source="Natural Earth 10m land + minor islands",
                version=SURGE_DEM_VERSION,
            )
            for i, (lat0, lon0) in enumerate(tiles, 1):
                if dem_src is not None:
                    elev3 = read_tile_elevation(dem_src, lat0, lon0)
                elif (lat0, lon0) in srtm_zips:
                    with _open_srtm_zip(srtm_zips[(lat0, lon0)]) as src:
                        elev3 = read_tile_elevation(src, lat0, lon0)
                else:
                    missing += 1
                    continue

                result = process_tile(elev3, lat0, lon0, land_tree, land, coast_index,
                                      factor, max_dist_km, max_elevation)
                if result is None:
                    continue
                out_elev, out_dist = result
                window = Window((lon0 - w) * n, (nn - (lat0 + 1)) * n, n, n)
                dst.write(out_elev, 1, window=window)
                dst.write(out_dist, 2, window=window)
                kept_cells += int(np.isfinite(out_elev).sum())
                written_tiles += 1
                if i % 100 == 0:
                    logger.info(f"    {i}/{len(tiles)} tiles processed ({time.time() - t0:.0f}s)")
    finally:
        if dem_src is not None:
            dem_src.close()

    stats = {"tiles_in_bbox": len(tiles), "tiles_written": written_tiles,
             "tiles_without_elevation": missing, "coastal_lowland_cells": kept_cells,
             "seconds": round(time.time() - t0, 1)}
    logger.info(f"  {written_tiles} tiles written, {kept_cells:,} coastal lowland cells "
                f"({missing} tiles without elevation data), {stats['seconds']}s")
    return stats


# ---------------------------------------------------------------------------
# Upload (same pattern as setup_jrc_extents.py)
# ---------------------------------------------------------------------------

def upload_to_stage(local_path: Path, stage_name: str, conn) -> bool:
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
    logger.info(f"  PUT {local_path.name} -> @{stage_name}/{STAGE_PREFIX}/{local_path.name}  [{status}]")
    return status in ('UPLOADED', 'SKIPPED')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local-only", metavar="DIR", default=None,
                        help="Keep the cache in DIR only, do not upload to Snowflake stage "
                             "(for offline dev; matches SURGE_DEM_SOURCE=local)")
    parser.add_argument("--dest-dir", default="surge_dem_tmp",
                        help="Local working directory (default: surge_dem_tmp)")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("WEST", "SOUTH", "EAST", "NORTH"),
                        default=None, help="Override the default 56S-60N SRTM coverage bbox")
    parser.add_argument("--resolution", type=int, default=DEFAULT_RESOLUTION_ARCSEC,
                        help=f"Output resolution in arc-seconds, multiple of 3 "
                             f"(default: {DEFAULT_RESOLUTION_ARCSEC})")
    parser.add_argument("--max-dist-km", type=float, default=DEFAULT_MAX_DIST_KM,
                        help=f"Keep cells within this distance of the coast (default: {DEFAULT_MAX_DIST_KM})")
    parser.add_argument("--max-elevation", type=float, default=DEFAULT_MAX_ELEVATION_M,
                        help=f"Keep cells at or below this elevation in m (default: {DEFAULT_MAX_ELEVATION_M})")
    parser.add_argument("--dem", default=None,
                        help="Existing DEM GeoTIFF to use instead of downloading SRTM (offline dev/testing)")
    args = parser.parse_args()

    bbox = tuple(args.bbox) if args.bbox else GLOBAL_BBOX
    dest_dir = Path(args.dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / OUTPUT_FILENAME

    logger.info("=" * 70)
    logger.info(f"Storm surge coastal DEM cache setup -- bbox {bbox}, {args.resolution}\" grid")
    logger.info(f"Limits: elevation <= {args.max_elevation} m, distance to coast <= {args.max_dist_km} km")
    logger.info(f"Elevation: {args.dem or 'NASA SRTMGL3 via GigaSpatial'}")
    logger.info("=" * 70)

    # Same stale-output guard as setup_jrc_extents.py: never reuse a cache built
    # for a different bbox/resolution/limits as if it were this run's.
    manifest_path = dest_dir / ".surge_dem_manifest.json"
    manifest = {"bbox": list(bbox), "resolution_arcsec": args.resolution,
                "max_dist_km": args.max_dist_km, "max_elevation": args.max_elevation,
                "dem": args.dem}
    if out_path.exists():
        try:
            previous = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError):
            previous = None
        if previous == manifest:
            logger.info(f"  {OUTPUT_FILENAME} already built with the same settings -- skipping build")
        else:
            logger.warning(f"  Existing {OUTPUT_FILENAME} was built with other settings ({previous}) -- rebuilding")
            out_path.unlink()

    if not out_path.exists():
        build_surge_dem(bbox, args.resolution, args.max_dist_km, args.max_elevation, out_path,
                        srtm_dir=dest_dir / "srtm_raw",
                        dem_path=Path(args.dem) if args.dem else None)
        manifest_path.write_text(json.dumps(manifest))

    size_mb = out_path.stat().st_size / 1e6
    logger.info(f"  {OUTPUT_FILENAME}: {size_mb:.1f} MB")

    if args.local_only:
        local_only_dir = Path(args.local_only)
        local_only_dir.mkdir(parents=True, exist_ok=True)
        target = local_only_dir / OUTPUT_FILENAME
        if out_path.resolve() != target.resolve():
            target.write_bytes(out_path.read_bytes())
        logger.info(f"Done -- kept locally in {target} (SURGE_DEM_SOURCE=local should point here)")
        return

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
        if not upload_to_stage(out_path, stage_name, conn):
            logger.error(f"  Upload failed for {out_path.name}")
            sys.exit(1)
    finally:
        conn.close()
    logger.info(f"Done -- surge DEM cache staged at @{stage_name}/{STAGE_PREFIX}/{OUTPUT_FILENAME}")


if __name__ == "__main__":
    main()
