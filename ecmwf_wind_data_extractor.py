#!/usr/bin/env python3
"""
ECMWF Ensemble Wind Data Extractor

This module provides functions to extract wind threshold polygons from ECMWF
ensemble wind GRIB files based on tropical cyclone track data.

The extractor processes:
- ECMWF ensemble wind forecasts (u10, v10 components)
- TC track data with buffered regions
- Multiple wind thresholds (34, 40, 50, 64, 83, 96, 113, 137 knots)
- Ensemble member matching (track member N → wind member N)

Output:
- Wind threshold contour polygons for each ensemble member
- Spatial statistics

References:
- ECMWF Wind Forecasts: https://www.ecmwf.int/en/forecasts/datasets/open-data
- TC Wind Thresholds: https://www.nhc.noaa.gov/aboutsshws.php
"""

import os
import logging
import warnings
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import unary_union

# Suppress warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_OUTPUT_DIR = "wind_extracted"

# Wind thresholds in knots and m/s
WIND_THRESHOLDS = {
    34: 17.49,  # Tropical storm force
    40: 20.58,  # Strong tropical storm
    50: 25.72,  # Very strong tropical storm
    64: 32.92,  # Category 1 hurricane
    83: 42.70,  # Category 2 hurricane
    96: 49.39,  # Category 3 hurricane
    113: 58.12,  # Category 4 hurricane
    137: 70.48  # Category 5 hurricane
}


def _unwrap_track_longitudes(track_data: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of track_data with 'longitude' unwrapped into a continuous
    coordinate space, independently per ensemble member when that column is
    present.

    Fixes the antimeridian-crossing bug: a single member's own track can
    legitimately jump from just under +180 to just over -180 between
    consecutive lead times as the storm crosses the dateline. Naive min/max
    bounding-box math over the raw values misreads that as a near-global
    span (e.g. NANGKA 2026-08-12 12Z member 34: 179.1 deg -> -179.5 deg
    read as a ~359 deg-wide box instead of the true ~1 deg step). Sorting
    each member's own points by time and applying np.unwrap(period=360)
    turns that into a continuous, physically correct sequence (179.1 ->
    180.5) with no discontinuity for the union/bounding-box math to trip
    over.

    A track with no dateline crossing is returned with 'longitude'
    numerically unchanged: np.unwrap is a no-op whenever no consecutive gap
    exceeds the wrap period, so this is a verified zero-behavior-change
    path for the overwhelming majority of real storms.

    This function only feeds the internal buffered-polygon/bounding-box
    computation used to size the GRIB extraction window (see
    get_longitude_windows below) — it is never itself stored as an
    envelope, so no corresponding "wrap back into [-180, 180]" step is
    needed here.
    """
    df = track_data.copy()

    sort_col = None
    for candidate in ('valid_time', 'lead_time'):
        if candidate in df.columns:
            sort_col = candidate
            break

    def _unwrap_group(g: pd.DataFrame) -> pd.DataFrame:
        if sort_col is not None:
            g = g.sort_values(sort_col)
        g = g.copy()
        g['longitude'] = np.unwrap(g['longitude'].to_numpy(dtype=float), period=360.0)
        return g

    if 'ensemble_member' in df.columns and df['ensemble_member'].nunique() > 1:
        df = df.groupby('ensemble_member', group_keys=False).apply(_unwrap_group)
    else:
        df = _unwrap_group(df)

    return _reconcile_cross_member_longitudes(df)


def _reconcile_cross_member_longitudes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Second pass, after per-member unwrapping: reconcile a GROUP-level
    antimeridian straddle that no single member's own trajectory exhibits.

    _unwrap_track_longitudes above only fixes a jump within one member's
    own time-ordered sequence. It is a correct no-op when, say, member A's
    entire track sits at raw longitudes ~176-177.5 deg and member B's
    entire track sits at ~-176 to -177.5 deg, neither member individually
    crosses the seam, so per-member np.unwrap never fires for either, but
    the true physical positions are only ~5 deg apart (both hugging the
    dateline from opposite sides), while the raw combined min/max reads as
    ~353 deg wide. That corrupted-looking bbox is real: it was reproduced
    directly from this exact scenario during a real council review.

    Uses the standard "largest circular gap" method: sort every point's
    longitude, find the widest gap between consecutive values, including
    the wraparound gap (from the highest value back to the lowest value
    + 360). If the wraparound gap is itself the widest, the points are
    genuinely not antimeridian-adjacent as a group, this is the ordinary
    case (including the already-correct single-member NANGKA-style crossing
    handled above, which per-member unwrap already leaves as one dense,
    gap-free cluster) and this function is a verified no-op. If some other
    gap is wider, the true cluster wraps through +/-180 at that gap
    instead, so every value up to and including it gets shifted by +360 to
    make the whole group numerically contiguous.

    Sanity bound: only applies the shift if doing so actually produces a
    plausibly-sized (<90 deg, matching this project's own established "no
    real single-storm envelope is wider than this" convention used
    elsewhere) reconciled cluster. A real council review found that,
    without this bound, the largest-gap method would also fire on
    genuinely scattered, non-antimeridian data whenever a group's total
    spread happens to approach 360 deg, confirmed not reachable by any
    real TC ensemble (tested clean against real, genuinely wide-spread
    storms), but with no guard to distinguish that from a real upstream
    data anomaly (e.g. a parsing bug placing one member on the wrong side
    of the planet).
    """
    lons = df['longitude'].to_numpy(dtype=float)
    if len(lons) < 2:
        return df

    sorted_lons = np.sort(np.unique(lons))
    if len(sorted_lons) < 2:
        return df

    internal_gaps = np.diff(sorted_lons)
    wrap_gap = (sorted_lons[0] + 360.0) - sorted_lons[-1]

    widest_internal_idx = int(np.argmax(internal_gaps))
    if internal_gaps[widest_internal_idx] <= wrap_gap:
        # The wraparound gap is the widest (or tied) -- not antimeridian-
        # adjacent as a group. No-op.
        return df

    cutoff = sorted_lons[widest_internal_idx]
    shifted = np.where(lons <= cutoff, lons + 360.0, lons)
    reconciled_span = float(shifted.max() - shifted.min())

    _SANITY_MAX_SPAN_DEG = 90.0
    if reconciled_span > _SANITY_MAX_SPAN_DEG:
        logger.warning(
            f"_reconcile_cross_member_longitudes: widest internal "
            f"gap ({internal_gaps[widest_internal_idx]:.2f} deg) exceeds the "
            f"wraparound gap ({wrap_gap:.2f} deg), but shifting would still "
            f"leave a {reconciled_span:.2f} deg-wide cluster -- too wide to "
            f"plausibly be a real antimeridian-hugging storm ensemble. "
            f"Leaving longitudes unchanged rather than risk mis-reprojecting "
            f"genuinely scattered/anomalous data."
        )
        return df

    df = df.copy()
    df['longitude'] = shifted
    return df


def create_buffered_track_polygon(track_data: pd.DataFrame, buffer_radius_km: float) -> Polygon:
    """
    Create a buffer polygon around tropical cyclone track.

    Args:
        track_data (pd.DataFrame): Track data with latitude/longitude
        buffer_radius_km (float): Buffer radius in kilometers

    Returns:
        Polygon: Buffered track polygon. May extend beyond the valid
            [-180, 180] longitude range when the track (or ensemble spread)
            crosses the antimeridian — see _unwrap_track_longitudes above
            and get_longitude_windows below, which is how callers turn this
            back into real, native-range GRIB extraction window(s). This
            polygon itself is never stored as an envelope.
    """
    track_data = _unwrap_track_longitudes(track_data)

    lats = track_data['latitude'].values
    lons = track_data['longitude'].values

    # Vectorized: compute per-point buffer radius in degrees
    radius_deg_lat = buffer_radius_km / 111.0
    radius_deg_lon = buffer_radius_km / (111.0 * np.cos(np.radians(lats)))
    radius_degs = (radius_deg_lat + radius_deg_lon) / 2.0  # numpy array

    buffered_polygons = [
        Point(lon, lat).buffer(r)
        for lat, lon, r in zip(lats, lons, radius_degs)
    ]

    return unary_union(buffered_polygons)


def get_bounding_box(polygon: Polygon, buffer: float = 2.0) -> Dict[str, float]:
    """
    Get bounding box from polygon with buffer.

    Args:
        polygon (Polygon): Input polygon
        buffer (float): Buffer distance in degrees

    Returns:
        dict: Bounding box coordinates. 'lon_min'/'lon_max' may fall outside
            [-180, 180] when the input polygon was built from unwrapped,
            antimeridian-crossing track data (see create_buffered_track_polygon)
            — pass this dict through get_longitude_windows() before using it
            to slice a real GRIB file's longitude coordinate.
    """
    bounds = polygon.bounds  # (minx, miny, maxx, maxy)
    return {
        'lon_min': bounds[0] - buffer,
        'lon_max': bounds[2] + buffer,
        'lat_min': bounds[1] - buffer,
        'lat_max': bounds[3] + buffer
    }


def get_longitude_windows(bbox: Dict[str, float]) -> List[Dict[str, float]]:
    """
    Convert a bbox (possibly with lon_min/lon_max outside [-180, 180], from
    an antimeridian-crossing track's unwrapped bounding box) into one or two
    real, native-range windows suitable for slicing a GRIB file's own
    longitude coordinate, which is always within [-180, 180].

    Returns [bbox] unchanged, a single window, identical to the input,
    whenever lon_min/lon_max already fall within [-180, 180]. This is the
    overwhelming majority case (any storm that doesn't cross the
    antimeridian) and is a verified no-op: callers looping over this
    function's output process exactly one window with exactly the original
    bounds, so existing non-crossing behavior is unchanged.

    When the bbox does extend past one edge, returns two windows that
    together cover the same real span: the part already inside
    [-180, 180], and the overflowing part shifted back by 360 degrees into
    its real native-range position on the other side of the antimeridian.

    Handles both edges overflowing at once (e.g. lon_min=-184, lon_max=184)
    by returning the FULL native range as a single window, not two
    partial slivers. This is mathematically forced, not a judgment call:
    lon_max > 180 and lon_min < -180 together guarantee lon_max - lon_min
    > 360, and since longitude is periodic with period 360, any request
    spanning more than a full circle covers every native value at least
    once -- the correct window really is the whole globe. An earlier
    version of this function got this backwards (returned two ~4deg-wide
    slivers, silently dropping ~96% of the real requested area with no
    error) -- caught by a real council review that reproduced it directly
    against the real code. Reaching this branch at all should be rare in
    practice once the caller's own bbox has been through
    _reconcile_cross_member_longitudes() first (which keeps a real
    storm's reconciled span well under 360 -- confirmed via a real,
    realistic-scale reproduction), so hitting it is itself a signal of an
    anomalously wide upstream input, not a normal antimeridian case.
    """
    lon_min = bbox['lon_min']
    lon_max = bbox['lon_max']

    if lon_min >= -180.0 and lon_max <= 180.0:
        return [dict(bbox)]

    overflow_east = lon_max > 180.0
    overflow_west = lon_min < -180.0

    if overflow_east and overflow_west:
        logger.warning(
            f"get_longitude_windows: bbox spans >360 deg "
            f"(lon_min={lon_min:.2f}, lon_max={lon_max:.2f}, "
            f"width={lon_max - lon_min:.2f}) -- returning the full native "
            f"longitude range as a single window. This indicates an "
            f"anomalously wide upstream input, not a normal antimeridian case."
        )
        return [{**bbox, 'lon_min': -180.0, 'lon_max': 180.0}]

    windows = []
    if overflow_east:
        windows.append({**bbox, 'lon_min': max(lon_min, -180.0), 'lon_max': 180.0})
        windows.append({**bbox, 'lon_min': -180.0, 'lon_max': lon_max - 360.0})
    elif overflow_west:
        windows.append({**bbox, 'lon_min': lon_min + 360.0, 'lon_max': 180.0})
        windows.append({**bbox, 'lon_min': -180.0, 'lon_max': min(lon_max, 180.0)})

    return windows


def merge_contour_dicts(
        dicts: List[Dict[int, Optional[Polygon]]]) -> Dict[int, Optional[Polygon]]:
    """
    Union same-threshold-key polygons across multiple per-window contour
    dicts (see get_longitude_windows) into the final combined per-threshold
    result.

    A single-element input list is returned as that one dict, unchanged —
    the common non-antimeridian-crossing case (one window) is therefore a
    verified no-op: the exact same dict create_wind_threshold_contours
    already produced today, with no extra union/copy step.
    """
    if len(dicts) == 1:
        return dicts[0]

    all_keys = set()
    for d in dicts:
        all_keys.update(d.keys())

    merged: Dict[int, Optional[Polygon]] = {}
    for key in all_keys:
        polys = [d[key] for d in dicts if d.get(key) is not None]
        if not polys:
            merged[key] = None
        elif len(polys) == 1:
            merged[key] = polys[0]
        else:
            combined = unary_union(polys)
            if combined.geom_type not in ('Polygon', 'MultiPolygon'):
                combined = combined.buffer(0)
            merged[key] = combined

    return merged


def load_wind_data(grib_file: str, member_number: int, bbox: Dict[str, float],
                   verbose: bool = True, indexpath: Optional[str] = None) -> List[xr.DataArray]:
    """
    Load ensemble wind data for specific member and region.

    Args:
        grib_file (str): Path to wind GRIB file
        member_number (int): Ensemble member number
        bbox (dict): Bounding box coordinates. May have lon_min/lon_max
            outside [-180, 180] (see get_bounding_box) — this function
            slices the real GRIB longitude range once per real window
            returned by get_longitude_windows(bbox), rather than assuming
            a single contiguous slice.
        verbose (bool): Whether to print progress information
        indexpath (str, optional): Directory path for GRIB index files (.idx).
                                   If provided, index files will be stored here instead
                                   of alongside the GRIB file. Useful for parallel processing
                                   to avoid concurrent index file conflicts.

    Returns:
        List[xr.DataArray]: One DataArray per real longitude window — length
            1 for the overwhelming majority (non-antimeridian-crossing) case,
            identical to what this function used to return directly; length
            2 only when bbox straddles the antimeridian. Callers must run
            contour extraction once per element and merge results (see
            merge_contour_dicts) rather than concatenating these arrays —
            a raw concat would reintroduce a coordinate discontinuity at the
            180/-180 seam that contour tracing can't interpret correctly.
    """
    # Open dataset with optional custom index path
    if indexpath:
        # Create process-specific index directory to avoid concurrent access conflicts
        # Each process gets its own index cache, preventing race conditions
        # while still benefiting from index file caching performance
        import threading
        
        # Get unique identifier for this process/thread
        pid = os.getpid()
        thread_id = threading.get_ident()
        
        # Create process-specific subdirectory
        process_index_dir = os.path.join(indexpath, f"process_{pid}_thread_{thread_id}")
        os.makedirs(process_index_dir, exist_ok=True)
        
        # Build the index file path
        import pathlib
        grib_filename = pathlib.Path(grib_file).name
        index_file_path = os.path.join(process_index_dir, grib_filename + '.idx')
        
        # Open with process-specific index path
        ds = xr.open_dataset(
            grib_file, 
            engine='cfgrib',
            backend_kwargs={
                'indexpath': index_file_path,
                'errors': 'ignore'
            }
        )
    else:
        ds = xr.open_dataset(grib_file, engine='cfgrib')

    # Select specific ensemble member, mapping control member 51 to GRIB number 0
    try:
        # Extract u and v components (inside try so ds.close() fires on KeyError etc.)
        u10 = ds['u10']
        v10 = ds['v10']
        wind_speed = np.sqrt(u10 ** 2 + v10 ** 2)
        if 'number' in wind_speed.dims:
            desired_number = 0 if member_number == 51 else member_number
            if verbose:
                print(f"    Selecting wind ensemble member {member_number} (GRIB number {desired_number})")
            try:
                wind_speed = wind_speed.sel(number=desired_number)
            except Exception as e:
                logger.warning(f"Member {member_number} (GRIB {desired_number}) not found in wind data: {e}")
                return [xr.DataArray()]  # outer finally: ds.close() still fires
        else:
            if verbose:
                print(f"    NOTE: No ensemble dimension in wind data (likely control forecast)")

        # Subset to bounding box — one real native-range window in the
        # overwhelming majority of cases, two only when bbox straddles the
        # antimeridian (see get_longitude_windows)
        windows = get_longitude_windows(bbox)
        wind_regions = []
        for window in windows:
            region = wind_speed.sel(
                latitude=slice(bbox['lat_max'], bbox['lat_min']),
                longitude=slice(window['lon_min'], window['lon_max'])
            )
            region.load()  # materialise into RAM before closing the file handle
            wind_regions.append(region)

        if verbose:
            for region in wind_regions:
                print(f"    Wind data shape: {region.shape}")
                try:
                    max_ms = float(region.max())
                    print(f"    Max wind: {max_ms:.1f} m/s ({max_ms / 0.5144:.1f} kt)")
                except Exception:
                    pass

        return wind_regions
    finally:
        ds.close()


def load_wind_data_all_members(
    grib_file: str,
    bbox: Dict[str, float],
    indexpath: Optional[str] = None,
) -> List[xr.DataArray]:
    """
    Load wind speed for ALL ensemble members from a GRIB file in a single open.

    Each returned DataArray has dims (number, latitude, longitude) for PF
    files, or (latitude, longitude) for CF files (control forecast, no
    number dim).

    Returns:
        List[xr.DataArray]: One DataArray per real longitude window — length
            1 (bbox clipped exactly as before) for the overwhelming majority
            of storms; length 2 only when bbox straddles the antimeridian
            (see get_bounding_box / get_longitude_windows). Callers must
            extract contours once per element and merge results (see
            merge_contour_dicts), never concatenate these arrays directly.
    """
    if indexpath:
        import pathlib
        index_file_path = os.path.join(indexpath, pathlib.Path(grib_file).name + '.idx')
        ds = xr.open_dataset(
            grib_file, engine='cfgrib',
            backend_kwargs={'indexpath': index_file_path, 'errors': 'ignore'},
        )
    else:
        ds = xr.open_dataset(grib_file, engine='cfgrib')

    try:
        wind_speed = np.sqrt(ds['u10'] ** 2 + ds['v10'] ** 2)
        windows = get_longitude_windows(bbox)
        wind_regions = []
        for window in windows:
            region = wind_speed.sel(
                latitude=slice(bbox['lat_max'], bbox['lat_min']),
                longitude=slice(window['lon_min'], window['lon_max']),
            )
            region.load()  # materialise into RAM before closing the file handle
            wind_regions.append(region)
        return wind_regions
    finally:
        ds.close()


def create_wind_threshold_contours(wind_data: xr.DataArray,
                                   thresholds: Dict[int, float],
                                   verbose: bool = True,
                                   unit_label: str = 'kt') -> Dict[int, Optional[Polygon]]:
    """
    Create contour polygons for each wind threshold.

    Args:
        wind_data (xr.DataArray): Wind speed data
        thresholds (dict): Wind thresholds (key: m/s) — the dict key's own unit
            depends on the caller: wind passes real kt values (34, 40, 50...),
            gust passes integer m/s labels (17, 21, 26...); see
            ecmwf_gust_envelope_extractor.py's own GUST_THRESHOLDS_MS docstring.
        verbose (bool): Whether to print progress information
        unit_label (str): Unit to print next to the threshold key in the
            progress log (default 'kt', matching wind's own convention).
            Gust's own caller passes 'm/s' here so the progress log doesn't
            mislabel gust's m/s-keyed thresholds as knots — this is
            display-only, it never affects the stored contour/envelope data,
            which always keys off the same dict key passed in either way.

    Returns:
        dict: Polygons for each threshold
    """
    contour_polygons = {}

    lons = wind_data.longitude.values
    lats = wind_data.latitude.values
    winds = wind_data.values

    # Create meshgrid
    lon_grid, lat_grid = np.meshgrid(lons, lats)

    # Sort thresholds by m/s value so matplotlib assigns allsegs[i] to levels[i]
    sorted_thresholds = sorted(thresholds.items(), key=lambda x: x[1])
    levels_ms = [ms for _, ms in sorted_thresholds]

    # One figure, one contour call for all thresholds
    fig, ax = plt.subplots()
    try:
        cs = ax.contour(lon_grid, lat_grid, winds, levels=levels_ms)
    except Exception as e:
        plt.close(fig)
        if verbose:
            print(f"  Error creating contours: {e}")
        return {kt: None for kt, _ in sorted_thresholds}
    plt.close(fig)

    for i, (threshold_kt, threshold_ms) in enumerate(sorted_thresholds):
        if verbose:
            print(f"    {threshold_kt} {unit_label} ({threshold_ms:.2f} m/s)...", end='', flush=True)

        polygons = []
        try:
            # cs.allsegs is the stable per-level segments API (confirmed matplotlib 3.11).
            # cs.collections was removed in matplotlib 3.9 and must not be used.
            # If allsegs is ever removed, fall back to contourpy (bundled with matplotlib).
            try:
                level_segs = cs.allsegs[i] if i < len(cs.allsegs) else []
            except AttributeError:
                from contourpy import contour_generator
                gen = contour_generator(x=lons, y=lats, z=winds)
                level_segs = gen.lines(threshold_ms) or []
            if len(level_segs) > 0:
                for segment in level_segs:
                    if len(segment) > 3:
                        try:
                            poly = Polygon(segment)
                            if poly.is_valid and poly.area > 0:
                                polygons.append(poly)
                        except Exception as e:
                            logger.debug(f"Skipping invalid polygon segment at {threshold_kt}kt: {e}")
                            continue
        except Exception as e:
            if verbose:
                print(f"  Error: {e}")
            contour_polygons[threshold_kt] = None
            continue

        if polygons:
            combined = unary_union(polygons)
            # unary_union can return GEOMETRYCOLLECTION when inputs share edges;
            # buffer(0) normalises it back to a (Multi)Polygon
            if combined.geom_type not in ('Polygon', 'MultiPolygon'):
                combined = combined.buffer(0)
            contour_polygons[threshold_kt] = combined
            if verbose:
                area_km2 = combined.area * 111 * 104
                print(f" {len(polygons)} contour(s), {area_km2:,.0f} km²")
        else:
            contour_polygons[threshold_kt] = None
            if verbose:
                print(" - No area")

    return contour_polygons


_ANTIMERIDIAN_EPS = 1e-7  # degrees; far below GRIB grid spacing (0.25 deg), zero real-world effect


def _nudge_antimeridian_vertices(geom):
    """
    Shift any vertex sitting at exactly +180 or -180 longitude inward by a
    negligible epsilon.

    A window from get_longitude_windows() can legitimately end at exactly
    -180.0 (a real native GRIB grid coordinate, not an artifact), and
    contour tracing can place a real vertex there. Snowflake's GEOGRAPHY
    parser rejects that exact value outright ("Invalid Lng/Lat pair:
    '-180,...'"), even though it's valid WKT and valid to shapely. Nudging
    by 1e-7 degrees (~1cm) has no real-world effect and avoids the
    rejection at storage time. A no-op for the overwhelming majority of
    polygons, which never touch the antimeridian at all.

    shapely.ops.transform (shapely 2.x) calls its callback once per
    coordinate *ring*, passing all of that ring's x values (and y, and
    optionally z) as a single batched array/tuple, not scalar-per-call —
    the nudge must be vectorized, not a plain Python `if x == 180.0`.
    """
    from shapely.ops import transform

    def _nudge(x, y, z=None):
        x = np.asarray(x, dtype=float).copy()
        # Clamp, not exact-match: upstream window/buffer arithmetic can
        # overshoot the boundary by float noise (e.g. -180.00000000000003,
        # seen in real NANGKA data) which is itself already out of the
        # valid [-180, 180] range and would also fail Snowflake's parser
        # an exact == -180.0 check misses that. Anything at or past the
        # boundary gets pulled back in.
        x[x <= -180.0] = -180.0 + _ANTIMERIDIAN_EPS
        x[x >= 180.0] = 180.0 - _ANTIMERIDIAN_EPS
        return (x, y, z) if z is not None else (x, y)

    return transform(_nudge, geom)


def polygon_to_wkt(polygon: Optional[Polygon]) -> Optional[str]:
    """
    Convert Shapely polygon to WKT format.

    Args:
        polygon (Polygon or None): Input polygon

    Returns:
        str or None: WKT string
    """
    if polygon is None:
        return None

    try:
        polygon = _nudge_antimeridian_vertices(polygon)
        return polygon.wkt
    except Exception as e:
        logger.warning(f"Could not convert polygon to WKT: {e}")
        return None

