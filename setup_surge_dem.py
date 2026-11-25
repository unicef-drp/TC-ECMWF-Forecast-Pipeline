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
      band 1  elevation_m     mean DeltaDTM elevation of the land part of the cell
      band 2  dist_coast_km   distance from the cell centre to the coastline
    A cell is kept when it is on land, its elevation is below --max-elevation
    (default 10 m; strictly below, as DeltaDTM clips higher land at 10 m) and
    it lies within --max-dist-km (default 50 km) of the coast. These are the
    CLIMADA TCSurgeBathtub defaults (MAX_ELEVATION, MAX_DIST_COAST).
    Negative elevations are kept here; the surge step applies CLIMADA's own
    elevation >= 0 rule at run time, so it stays a configurable run-time
    choice rather than being baked into the cache.

Sources
    Elevation: DeltaDTM v1.1 (Pronk et al., 2024, Scientific Data 11, 273),
        a global coastal bare-earth terrain model at 1 arc-second (~30 m),
        vertical reference EGM2008, licence CC BY 4.0. Sea, lakes and rivers
        are nodata; land above 10 m is clipped at 10 m, which has no effect
        here (a cell at 10 m is never flooded by the bathtub). DeltaDTM was
        selected over SRTM and GEM-15 in the Deliverable 4 sensitivity tests.
        Downloaded automatically from 4TU.ResearchData
        (https://doi.org/10.4121/21997565) into --deltadtm-dir: first the tile
        index (deltadtm_tiles.gpkg), then only the continent zip archives that
        cover the coastal tiles of --bbox (North_America.zip, 6.9 GB, for the
        Caribbean; ~36 GB for the global 56S-60N run, Asia.zip alone 17 GB).
        Archives already downloaded are reused and interrupted downloads
        resume; each archive is checked against its size and md5. The tiles
        are read in place, without unzipping.
        Alternatively, --dem points at an existing DEM GeoTIFF and nothing is
        downloaded (offline dev / testing).
    Land and coastline: Natural Earth 10 m land + minor islands, through
        cartopy's own shapereader (cartopy is already a pipeline dependency).

Coverage
    Default bbox is 56S-60N (all tropical-cyclone basins), NOT a per-country
    list (this repo has no country concept, and none is introduced here).
    Only 1x1 degree tiles within --max-dist-km of a coastline are processed.

Destination (the three sources SURGE_DEM_SOURCE can read from)
    --upload blob       Azure Blob Storage, surge/coastal_dem/v1/ in CONTAINER_NAME (default)
                        (ACCOUNT_URL, SAS_TOKEN, CONTAINER_NAME, same variables as the pipeline)
    --upload snowflake  Snowflake stage, @SNOWFLAKE_STAGE_NAME/surge/coastal_dem/v1/
    --local-only DIR    kept locally only, no upload

Usage:
    python3 setup_surge_dem.py --deltadtm-dir deltadtm                       # global, Blob Storage
    python3 setup_surge_dem.py --deltadtm-dir deltadtm --upload snowflake    # global, Snowflake stage
    python3 setup_surge_dem.py --deltadtm-dir deltadtm --local-only surge_dem
    python3 setup_surge_dem.py --deltadtm-dir deltadtm --bbox -79 17 -76 19 --local-only surge_dem
                                                                         # small region, for testing
    python3 setup_surge_dem.py --bbox -79 17 -76 19 --dem JAM_deltadtm.tif --local-only surge_dem
                                                                         # existing DEM, no archive
"""

import argparse
import hashlib
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

sys.path.insert(0, str(Path(__file__).parent / "github_actions"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Tropical-cyclone latitudes (west, south, east, north)
GLOBAL_BBOX = (-180.0, -56.0, 180.0, 60.0)

DEM_ARCSEC = 1                      # DeltaDTM native resolution (1 arc-second, ~30 m)
DEM_PER_DEG = 3600 // DEM_ARCSEC    # 3600 cells per degree
DEFAULT_RESOLUTION_ARCSEC = 15      # ~450 m at the equator

DELTADTM_LABEL = "DeltaDTM v1.1 (Pronk et al. 2024), EGM2008"
DELTADTM_URL = "https://doi.org/10.4121/21997565"
# 4TU.ResearchData API: list of the dataset files with their download URL, size and md5
DELTADTM_FILES_API = "https://data.4tu.nl/v2/articles/1da2e70f-6c4d-4b03-86bd-b53e789cc629/files"
DELTADTM_TILES_GPKG = "deltadtm_tiles.gpkg"   # tile index: 1x1 degree tile -> continent zip

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
# Elevation source (DeltaDTM)
# ---------------------------------------------------------------------------

def _deltadtm_files() -> Dict[str, Dict]:
    """File list of the DeltaDTM dataset on 4TU.ResearchData: {name: {size, download_url, md5}}."""
    import requests
    r = requests.get(DELTADTM_FILES_API, timeout=60)
    r.raise_for_status()
    return {f["name"]: {"size": int(f["size"]), "url": f["download_url"], "md5": f.get("computed_md5")}
            for f in r.json()}


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _download(url: str, size: int, md5: Optional[str], dest: Path) -> None:
    """Download url to dest, resuming a partial .part file, then check size and md5.
    A file already in place with the expected size is kept as is."""
    import requests
    if dest.exists() and dest.stat().st_size == size:
        return
    part = dest.with_name(dest.name + ".part")
    for attempt in range(1, 6):
        done = part.stat().st_size if part.exists() else 0
        if done > size:
            part.unlink()
            done = 0
        headers = {"Range": f"bytes={done}-"} if done else {}
        logger.info(f"    {dest.name}: {size / 1e9:.2f} GB"
                    + (f", resuming at {done / 1e9:.2f} GB" if done else "")
                    + (f" (attempt {attempt})" if attempt > 1 else ""))
        try:
            with requests.get(url, headers=headers, stream=True, timeout=120) as r:
                r.raise_for_status()
                if done and r.status_code != 206:   # server ignored the range: start again
                    done = 0
                with open(part, "ab" if done else "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
        except requests.RequestException as e:
            logger.warning(f"    {dest.name}: download interrupted ({e}), retrying")
            time.sleep(10)
            continue
        if part.stat().st_size == size:
            break
    else:
        raise RuntimeError(f"Could not download {dest.name} after 5 attempts; run again to resume")
    if part.stat().st_size != size:
        raise RuntimeError(f"{dest.name}: got {part.stat().st_size} bytes, expected {size}; run again to resume")
    if md5 and _md5(part) != md5:
        part.unlink()
        raise RuntimeError(f"{dest.name}: checksum mismatch, file removed; run again to download it again")
    part.rename(dest)


def _read_tile_table(gpkg: Path) -> Dict[Tuple[int, int], Tuple[str, str]]:
    """{(lat0, lon0): (zip name, tile file name)} from deltadtm_tiles.gpkg, read with
    sqlite3 (a GeoPackage is an SQLite database), so no GIS dependency is needed.
    latitude/longitude are the south-west corner of each 1x1 degree tile."""
    import sqlite3
    con = sqlite3.connect(gpkg)
    try:
        table = con.execute("SELECT table_name FROM gpkg_contents WHERE data_type = 'features'").fetchone()[0]
        rows = con.execute(f'SELECT latitude, longitude, zipfile, tile FROM "{table}"').fetchall()
    finally:
        con.close()
    return {(int(round(lat)), int(round(lon))): (zf, tile) for lat, lon, zf, tile in rows}


def prepare_deltadtm(deltadtm_dir: Path, tiles: List[Tuple[int, int]]) -> Dict[Tuple[int, int], str]:
    """Download the DeltaDTM tile index and the continent zip archives needed for
    `tiles` into deltadtm_dir (only the missing ones, resuming partial downloads),
    then map each tile (lat0, lon0) to its GeoTIFF inside its zip, read in place
    without unzipping. Tiles absent from DeltaDTM (no land below 10 m) are skipped."""
    deltadtm_dir = Path(deltadtm_dir)
    deltadtm_dir.mkdir(parents=True, exist_ok=True)
    gpkg = deltadtm_dir / DELTADTM_TILES_GPKG

    files = None
    if not gpkg.exists():
        files = _deltadtm_files()
        f = files[DELTADTM_TILES_GPKG]
        _download(f["url"], f["size"], f["md5"], gpkg)
    table = _read_tile_table(gpkg)

    needed = sorted({table[t][0] for t in tiles if t in table})
    missing = [z for z in needed if not (deltadtm_dir / z).exists()]
    if missing:
        files = files or _deltadtm_files()
        total = sum(files[z]["size"] for z in missing) / 1e9
        logger.info(f"  Downloading {len(missing)} DeltaDTM archive(s) from 4TU.ResearchData "
                    f"({total:.1f} GB): {', '.join(missing)}")
        for z in missing:
            _download(files[z]["url"], files[z]["size"], files[z]["md5"], deltadtm_dir / z)
    logger.info(f"  DeltaDTM archives used: {', '.join(needed) or 'none'}")

    # Path of each tile inside its zip (the folder layout inside the archive is not assumed)
    members: Dict[str, Dict[str, str]] = {}
    for z in needed:
        with zipfile.ZipFile(deltadtm_dir / z) as zf:
            members[z] = {Path(m).name: m for m in zf.namelist() if m.lower().endswith(".tif")}

    uris = {}
    for t in tiles:
        if t not in table:
            continue
        z, tile = table[t]
        member = members.get(z, {}).get(tile)
        if member is None:
            logger.warning(f"    {tile} listed in {DELTADTM_TILES_GPKG} but not found in {z}")
            continue
        uris[t] = f"zip://{(deltadtm_dir / z).resolve().as_posix()}!{member}"
    return uris


def read_tile_elevation(src, lat0: int, lon0: int) -> np.ndarray:
    """Elevation for one 1x1 degree tile on a regular 1 arc-second grid, NaN where
    there is no data (sea, lakes, rivers). Also regrids the DeltaDTM tiles above
    50 degrees latitude, whose longitude spacing is coarser than 1 arc-second."""
    out = np.full((DEM_PER_DEG, DEM_PER_DEG), np.nan, dtype=np.float32)
    reproject(
        source=rasterio.band(src, 1),
        destination=out,
        src_nodata=src.nodata,
        dst_transform=from_origin(lon0, lat0 + 1, 1.0 / DEM_PER_DEG, 1.0 / DEM_PER_DEG),
        dst_crs="EPSG:4326",
        dst_nodata=np.nan,
        resampling=Resampling.nearest,
    )
    return out


# ---------------------------------------------------------------------------
# Per-tile processing
# ---------------------------------------------------------------------------

def process_tile(elev1: np.ndarray, lat0: int, lon0: int, land_tree, land: List,
                 coast_index: cKDTree, factor: int, max_dist_km: float,
                 max_elevation: float) -> Optional[Tuple[np.ndarray, np.ndarray]]:
    """Aggregate one tile to the output grid. Returns (elevation_m, dist_coast_km)
    arrays of shape (n, n), NaN outside low-lying coastal land, or None when the
    tile has no such cell."""
    tile_box = box(lon0, lat0, lon0 + 1, lat0 + 1)
    hits = [land[i] for i in land_tree.query(tile_box, predicate="intersects")]
    if not hits:
        return None

    transform1 = from_origin(lon0, lat0 + 1, 1.0 / DEM_PER_DEG, 1.0 / DEM_PER_DEG)
    on_land = rasterize(hits, out_shape=elev1.shape, transform=transform1,
                        fill=0, default_value=1, dtype="uint8").astype(bool)
    if not on_land.any():
        return None

    # Mean elevation of the LAND part of each output cell: pixels outside the
    # Natural Earth land, and DEM nodata (sea, lakes, rivers), are excluded
    elev1 = np.where(on_land, elev1, np.nan)
    n = DEM_PER_DEG // factor
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)  # all-NaN (sea) blocks
        elev = np.nanmean(elev1.reshape(n, factor, n, factor), axis=(1, 3)).astype(np.float32)

    # Strictly below the limit: DeltaDTM clips all land above 10 m to exactly
    # 10 m, so a cell whose mean is 10 m has only higher ground. Keeping it
    # would add the whole inland area to the cache, for cells the bathtub can
    # never flood (that would need a wind above 100 m/s).
    keep = np.isfinite(elev) & (elev < max_elevation)
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
                    deltadtm_dir: Optional[Path] = None, dem_path: Optional[Path] = None) -> Dict:
    if resolution_arcsec % DEM_ARCSEC:
        raise ValueError(f"--resolution must be a multiple of {DEM_ARCSEC} arc-second")
    factor = resolution_arcsec // DEM_ARCSEC
    if DEM_PER_DEG % factor:
        raise ValueError(f"--resolution must divide one degree evenly (got {resolution_arcsec}\")")
    n = DEM_PER_DEG // factor

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
    deltadtm_tiles = {} if dem_src else prepare_deltadtm(deltadtm_dir, tiles)

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
                elevation_source=str(dem_path.name) if dem_path else DELTADTM_LABEL,
                coastline_source="Natural Earth 10m land + minor islands",
                version=SURGE_DEM_VERSION,
            )
            for i, (lat0, lon0) in enumerate(tiles, 1):
                if dem_src is not None:
                    elev1 = read_tile_elevation(dem_src, lat0, lon0)
                elif (lat0, lon0) in deltadtm_tiles:
                    with rasterio.open(deltadtm_tiles[(lat0, lon0)]) as src:
                        elev1 = read_tile_elevation(src, lat0, lon0)
                else:
                    # No DeltaDTM tile: no land below 10 m there, or its continent zip is missing
                    missing += 1
                    continue

                result = process_tile(elev1, lat0, lon0, land_tree, land, coast_index,
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
# Upload: Snowflake stage (same pattern as setup_jrc_extents.py) or Blob Storage
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


def upload_to_blob_storage(local_path: Path, account_url: str, sas_token: str, container: str) -> bool:
    """Upload to Azure Blob Storage under the same prefix as the Snowflake stage,
    where resolve_surge_dem() reads it back when SURGE_DEM_SOURCE=blob."""
    from ecmwf_met_downloader import upload_to_blob  # noqa: local import, only needed for Blob upload
    return upload_to_blob(local_path, account_url, sas_token, container,
                          f"{STAGE_PREFIX}/{local_path.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--deltadtm-dir", default="deltadtm",
                        help=f"Folder where the DeltaDTM archives are downloaded and kept, from "
                             f"{DELTADTM_URL} (default: deltadtm)")
    parser.add_argument("--upload", choices=("snowflake", "blob"), default="blob",
                        help="Where to upload the cache, under surge/coastal_dem/v1/: Snowflake "
                             "stage or Azure Blob Storage (default: snowflake)")
    parser.add_argument("--local-only", metavar="DIR", default=None,
                        help="Keep the cache in DIR only, no upload "
                             "(for offline dev; matches SURGE_DEM_SOURCE=local)")
    parser.add_argument("--dest-dir", default="surge_dem_tmp",
                        help="Local working directory (default: surge_dem_tmp)")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("WEST", "SOUTH", "EAST", "NORTH"),
                        default=None, help="Override the default 56S-60N bbox")
    parser.add_argument("--resolution", type=int, default=DEFAULT_RESOLUTION_ARCSEC,
                        help=f"Output resolution in arc-seconds, dividing one degree evenly "
                             f"(default: {DEFAULT_RESOLUTION_ARCSEC})")
    parser.add_argument("--max-dist-km", type=float, default=DEFAULT_MAX_DIST_KM,
                        help=f"Keep cells within this distance of the coast (default: {DEFAULT_MAX_DIST_KM})")
    parser.add_argument("--max-elevation", type=float, default=DEFAULT_MAX_ELEVATION_M,
                        help=f"Keep cells below this elevation in m (default: {DEFAULT_MAX_ELEVATION_M})")
    parser.add_argument("--dem", default=None,
                        help="Existing DEM GeoTIFF to use instead of the DeltaDTM archives (offline dev/testing)")
    args = parser.parse_args()

    bbox = tuple(args.bbox) if args.bbox else GLOBAL_BBOX
    dest_dir = Path(args.dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / OUTPUT_FILENAME
    elevation_label = args.dem or f"{DELTADTM_LABEL}, archives in {args.deltadtm_dir}/"

    logger.info("=" * 70)
    logger.info(f"Storm surge coastal DEM cache setup -- bbox {bbox}, {args.resolution}\" grid")
    logger.info(f"Limits: elevation < {args.max_elevation} m, distance to coast <= {args.max_dist_km} km")
    logger.info(f"Elevation: {elevation_label}")
    logger.info("=" * 70)

    # Check the upload settings before a possibly hours-long build, not after
    if not args.local_only:
        required = (('ACCOUNT_URL', 'SAS_TOKEN', 'CONTAINER_NAME') if args.upload == "blob" else
                    ('SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_WAREHOUSE',
                     'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_STAGE_NAME'))
        missing_vars = [v for v in required if not os.getenv(v)]
        if missing_vars:
            logger.error(f"{', '.join(missing_vars)} not set -- required for the {args.upload} upload "
                         f"(or use --local-only DIR)")
            sys.exit(1)

    # Same stale-output guard as setup_jrc_extents.py: never reuse a cache built
    # for a different bbox/resolution/limits/elevation source as if it were this run's.
    manifest_path = dest_dir / ".surge_dem_manifest.json"
    manifest = {"bbox": list(bbox), "resolution_arcsec": args.resolution,
                "max_dist_km": args.max_dist_km, "max_elevation": args.max_elevation,
                "max_elevation_rule": "strictly below", "dem": args.dem or DELTADTM_LABEL}
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
                        deltadtm_dir=Path(args.deltadtm_dir),
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

    if args.upload == "blob":
        container = os.environ['CONTAINER_NAME']
        if not upload_to_blob_storage(out_path, os.environ['ACCOUNT_URL'], os.environ['SAS_TOKEN'], container):
            logger.error(f"  Upload failed for {out_path.name}")
            sys.exit(1)
        logger.info(f"Done -- surge DEM cache on Blob at {container}/{STAGE_PREFIX}/{OUTPUT_FILENAME} "
                    f"(SURGE_DEM_SOURCE=blob)")
        return

    from snowflake_loader import get_snowflake_connection  # noqa: local import, only needed for stage upload
    stage_name = os.environ['SNOWFLAKE_STAGE_NAME']
    conn = get_snowflake_connection()
    try:
        if not upload_to_stage(out_path, stage_name, conn):
            logger.error(f"  Upload failed for {out_path.name}")
            sys.exit(1)
    finally:
        conn.close()
    logger.info(f"Done -- surge DEM cache staged at @{stage_name}/{STAGE_PREFIX}/{OUTPUT_FILENAME} "
                f"(SURGE_DEM_SOURCE=snowflake)")


if __name__ == "__main__":
    main()
