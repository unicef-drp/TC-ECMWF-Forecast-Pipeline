#!/usr/bin/env python3
"""
Wind/gust speed-field raster persistence.

Persists the wind/gust speed field for wind/gust impact computation. This is
cheap because `load_wind_data_all_members()`/`load_gust_data_all_members()`
(ecmwf_wind_data_extractor.py / ecmwf_gust_envelope_extractor.py) already
materialize the full ensemble wind-speed field into RAM before it gets
reduced to threshold-contour polygons and discarded. This module persists
that same, already-loaded array instead of throwing it away. Additive
only, the existing polygon output/pipeline is completely unchanged by this.

Not currently consumed downstream. DATAPIPELINE's own impact-computation
rewrite instead rasterizes the already-precise threshold-contour polygons on
its own side, which reproduces production results exactly and needs no raw
field at all. Kept available for a possible future raw-hazard visualization
layer (matching precip/river's own existing raw-view pattern): it would read
these GeoTIFFs the same way DATAPIPELINE's own
grid_to_geotiff_to_tifprocessor()/create_precip_tile_view() already reads
precip's raster output, multi-band here (one band per ensemble member)
instead of precip's single already-member-aggregated band, since wind/gust's
downstream impact views need real per-member results (create_tracks_view_
from_envelopes() reports per-member severity), not a pre-aggregated
probability.
"""

import logging
from typing import List, Tuple

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


def _affine_from_pixel_centers(lat_values: np.ndarray, lon_values: np.ndarray):
    """
    Real affine transform (rasterio convention: row 0 = north) derived from
    real ECMWF grid coordinate arrays, which are pixel CENTERS (xarray/cfgrib
    decodes GRIB lat/lon as center points, not corners). Expands half a pixel
    on each edge to get the raster's true bounds, not just reusing the
    center coordinates as if they were corners (which would silently shift
    every pixel by half a cell).

    Returns:
        (transform, already_north_up): already_north_up is False when the
        input latitude array is ascending (south to north). ECMWF's own
        GRIB convention is descending (north to south, matching the bbox
        slice `latitude=slice(bbox['lat_max'], bbox['lat_min'])` in
        load_wind_data_all_members()), so this is expected to be True in
        practice, but checked explicitly rather than assumed, since silently
        writing a vertically-flipped raster would be a real, hard-to-notice
        correctness bug for every downstream consumer.

    Guards against a single-gridpoint-wide dimension (`len(...) == 1`), which
    would otherwise raise `IndexError` on the `values[1]` access below. This
    is a real, reachable case, not just theoretical: get_longitude_windows()'s
    antimeridian split can produce a sliver window narrower than one real
    0.25-degree ECMWF gridpoint, which `.sel(longitude=slice(...))` then
    reduces to exactly one point. Falls back to ECMWF's own real,
    well-documented 0.25-degree grid spacing when a dimension can't provide
    its own real resolution from two points.
    """
    _ECMWF_GRID_RES = 0.25  # real, documented ECMWF Open Data ENS grid spacing

    lon_res = float(lon_values[1] - lon_values[0]) if len(lon_values) > 1 else _ECMWF_GRID_RES
    lat_res = float(lat_values[1] - lat_values[0]) if len(lat_values) > 1 else -_ECMWF_GRID_RES
    west = float(lon_values[0]) - lon_res / 2
    east = float(lon_values[-1]) + lon_res / 2

    from rasterio.transform import from_bounds

    if lat_res < 0:
        # Descending (north -> south), rasterio's own expected row-0-is-north
        # convention. No data flip needed.
        north = float(lat_values[0]) - lat_res / 2
        south = float(lat_values[-1]) + lat_res / 2
        already_north_up = True
    else:
        # Ascending (south -> north). Data must be flipped before writing.
        north = float(lat_values[-1]) + lat_res / 2
        south = float(lat_values[0]) - lat_res / 2
        already_north_up = False

    height = len(lat_values)
    width = len(lon_values)
    transform = from_bounds(west, south, east, north, width, height)
    return transform, already_north_up


def save_wind_speed_raster(wind_data: xr.DataArray, output_path: str, dataset_label: str = 'wind') -> List[int]:
    """
    Persist an already-loaded wind/gust speed DataArray as a multi-band
    GeoTIFF, one band per ensemble member.

    Args:
        wind_data: dims (number, latitude, longitude) for a PF file (all
            perturbed members in one array, real GRIB member numbers as the
            'number' coordinate), or (latitude, longitude) for a CF file
            (control forecast, no 'number' dim; treated as a single band,
            GRIB member number 0, matching this repo's own existing
            control-forecast convention elsewhere, e.g.
            ecmwf_tc_wind_combination.py's per-member polygon extraction;
            this writer stays at the raw GRIB-number level rather than
            re-deriving the GRIB-0 -> pipeline-member-51 mapping itself, to
            avoid a second, possibly-drifting copy of that rule).
        output_path: real local filesystem path to write the GeoTIFF to.
            Caller owns upload-to-Blob/Snowflake-stage and local cleanup,
            matching every other *_to_geotiff-shaped helper already in this
            codebase family (DATAPIPELINE's own
            grid_to_geotiff_to_tifprocessor()).
        dataset_label: 'wind' or 'gust', written into the GeoTIFF's own tags
            so a reader can self-describe the file without depending on the
            filename alone.

    Returns:
        The real GRIB member numbers written, in band order (1-based band i
        <-> the i-th entry of this list). Lets the caller log/verify this
        without re-deriving it from the DataArray a second time.
    """
    import rasterio

    if 'number' in wind_data.dims:
        member_numbers = [int(n) for n in wind_data.number.values]
        data = wind_data.values  # (member, lat, lon)
    else:
        member_numbers = [0]  # control forecast: GRIB number 0
        data = wind_data.values[np.newaxis, :, :]

    lat_values = wind_data.latitude.values
    lon_values = wind_data.longitude.values
    transform, already_north_up = _affine_from_pixel_centers(lat_values, lon_values)
    if not already_north_up:
        data = data[:, ::-1, :]
        logger.debug("save_wind_speed_raster: ascending latitude input, flipped to north-up before writing")

    write_dtype = data.dtype if np.issubdtype(data.dtype, np.floating) else np.dtype('float32')
    n_bands, height, width = data.shape

    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': n_bands,
        'dtype': write_dtype,
        'crs': 'EPSG:4326',
        'transform': transform,
        'nodata': np.nan,
        'compress': 'deflate',  # lossless, keeps file size down
    }
    with rasterio.open(output_path, 'w', **profile) as dst:
        for i, member in enumerate(member_numbers, start=1):
            band = data[i - 1].astype(write_dtype)
            dst.write(band, i)
            dst.set_band_description(i, f'member_{member}')
        dst.update_tags(dataset=dataset_label, member_numbers=','.join(str(m) for m in member_numbers))

    return member_numbers


def read_wind_speed_raster(input_path: str) -> Tuple[np.ndarray, List[int], dict]:
    """
    Real read-back, for self-verification and for a possible future raw-hazard
    visualization consumer (see save_wind_speed_raster()'s own module-level
    docstring; DATAPIPELINE's own impact computation does not use this reader,
    it rasterizes the threshold-contour polygons directly on its own side).

    Returns:
        (data, member_numbers, meta): data has shape (n_bands, height,
        width), north-up (row 0 = north, matching save_wind_speed_raster()'s
        own write convention). member_numbers parsed back from the file's
        own tags (not re-guessed from band order alone, though the two are
        guaranteed consistent by construction here). meta is rasterio's own
        profile dict (transform, crs, etc.) for a caller that needs it (e.g.
        to sample at real tile centroids the same way
        create_precip_tile_view() already does for precip).
    """
    import rasterio

    with rasterio.open(input_path) as src:
        data = src.read()
        tags = src.tags()
        member_numbers = [int(m) for m in tags.get('member_numbers', '').split(',') if m]
        meta = src.meta.copy()

    return data, member_numbers, meta
