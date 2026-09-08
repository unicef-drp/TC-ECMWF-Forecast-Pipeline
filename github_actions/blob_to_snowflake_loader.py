#!/usr/bin/env python3
"""
Blob -> Snowflake table loader (standalone, run separately from the per-cycle pipeline).

The per-cycle pipeline (main.py / glofas_pipeline.py, DATA_PIPELINE_DB=BLOB) writes tracks/
envelope CSVs and the raw discharge/extent Zarr/Parquet files to Blob, but deliberately opens
no Snowflake connection at all in that mode -- that's what makes BLOB mode genuinely
independent of Snowflake for the write side. TC_TRACKS/TC_ENVELOPES_COMBINED/RIVER_FORECASTS
themselves are still real Snowflake tables, though, and nothing currently loads Blob-resident
data into them. This script is that separate, later step: list what's in Blob, download each
file to a temp local path, and call the SAME load_csv_to_snowflake()/
load_riverine_metadata_to_snowflake() functions the per-cycle pipeline already uses in
SNOWFLAKE mode, unchanged.

Two real Snowflake table families, each with a different metadata shape:
  - TC_TRACKS / TC_ENVELOPES_COMBINED / TC_ENVELOPES_INDIVIDUAL / TC_GUST_ENVELOPES_COMBINED /
    TC_GUST_ENVELOPES_INDIVIDUAL: loaded straight from the CSV files under tracks/ and
    envelopes/ at the container root, table_type inferred from filename exactly like
    main.py's own step7_load() SNOWFLAKE branch already does.
  - RIVER_FORECASTS: not a per-file table, a pointer table. The actual discharge/extent
    Zarr/Parquet files already live in Blob under glofas/<date>/; this reconstructs the same
    metadata rows (forecast_time, param, stage_path, is_standin) that glofas_pipeline.py's own
    SNOWFLAKE-mode run would have built from the pipeline's own return values, purely from the
    date and the real filenames already present in Blob.

Dry-run by default: lists exactly what would be loaded and does not open a Snowflake
connection or write anything, unless --execute is passed explicitly. This is a deliberate
safety structure, not just a documented convention -- running this script with no flags is
always safe to do against real production Blob data.

Usage:
    python github_actions/blob_to_snowflake_loader.py --tracks-envelopes           # dry run
    python github_actions/blob_to_snowflake_loader.py --tracks-envelopes --execute # real load
    python github_actions/blob_to_snowflake_loader.py --river-forecasts --date 2026-09-02
    python github_actions/blob_to_snowflake_loader.py --river-forecasts --date 2026-09-02 --execute
"""

import argparse
import logging
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TRACKS_PREFIX = 'tracks'
ENVELOPES_PREFIX = 'envelopes'
GLOFAS_PREFIX = 'glofas'

# Mirrors main.py's step7_load() SNOWFLAKE-branch inference exactly.
_ENVELOPE_TABLE_RULES = [
    ('gust_envelopes_individual', 'TC_GUST_ENVELOPES_INDIVIDUAL'),
    ('gust_envelopes_combined', 'TC_GUST_ENVELOPES_COMBINED'),
    ('individual', 'TC_ENVELOPES_INDIVIDUAL'),
    ('combined', 'TC_ENVELOPES_COMBINED'),
]

# Mirrors glofas_extent_masking.py's own EXTENT_RP_LEVELS/IS_STANDIN.
_EXTENT_RP_LEVELS = ["2.0", "5.0", "10.0", "20.0", "50.0", "100.0"]
_IS_STANDIN = {"2.0": True, "5.0": True, "10.0": False, "20.0": False, "50.0": False, "100.0": False}


def _list_blob_files(prefix: str, account_url: str, sas_token: str, container: str) -> List[str]:
    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient(account_url=account_url, credential=sas_token)
    return [b.name for b in client.get_container_client(container).list_blobs(name_starts_with=prefix)]


def _download_blob(blob_path: str, account_url: str, sas_token: str, container: str, local_dir: Path) -> Path:
    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient(account_url=account_url, credential=sas_token)
    blob_client = client.get_container_client(container).get_blob_client(blob_path)
    local_path = local_dir / Path(blob_path).name
    with open(local_path, 'wb') as f:
        f.write(blob_client.download_blob().readall())
    return local_path


def _table_type_for(blob_path: str) -> Optional[str]:
    name = Path(blob_path).name
    if blob_path.startswith(f'{TRACKS_PREFIX}/'):
        return 'TC_TRACKS'
    if blob_path.startswith(f'{ENVELOPES_PREFIX}/'):
        for needle, table_type in _ENVELOPE_TABLE_RULES:
            if needle in name:
                return table_type
    return None


def load_tracks_envelopes_from_blob(account_url: str, sas_token: str, container: str,
                                     conn=None, blob_paths: Optional[List[str]] = None,
                                     execute: bool = False) -> dict:
    """
    Loads every tracks/ and envelopes/ CSV currently in Blob into the real Snowflake tables,
    via the existing, unchanged load_csv_to_snowflake().

    Args:
        blob_paths: explicit list of blob paths to load, e.g. from one specific forecast
            cycle. If None, loads everything currently under tracks/ and envelopes/ -- the
            caller is responsible for knowing whether that set has already been loaded before
            (this function has no its-own-yet notion of "already loaded", it will happily
            re-MERGE the same rows again, which load_csv_to_snowflake()'s own MERGE semantics
            make idempotent, but at real Snowflake compute cost for files already loaded).
        execute: real Snowflake writes only happen when this is True. False (default) only
            lists what would be loaded and returns without downloading or connecting.

    Returns:
        {'planned': [...], 'loaded': [...], 'skipped': [...], 'errors': [...]}
    """
    from snowflake_loader import load_csv_to_snowflake

    if blob_paths is None:
        blob_paths = (_list_blob_files(TRACKS_PREFIX, account_url, sas_token, container)
                      + _list_blob_files(ENVELOPES_PREFIX, account_url, sas_token, container))

    plan = []
    for blob_path in blob_paths:
        table_type = _table_type_for(blob_path)
        if table_type is None:
            logger.warning(f"Unrecognized file, skipping: {blob_path}")
            continue
        plan.append((blob_path, table_type))

    result = {'planned': plan, 'loaded': [], 'skipped': [], 'errors': []}

    if not execute:
        logger.info(f"DRY RUN: would load {len(plan)} file(s) into Snowflake -- pass execute=True to run for real")
        for blob_path, table_type in plan:
            logger.info(f"  {blob_path} -> {table_type}")
        return result

    if conn is None:
        raise ValueError("execute=True requires a real Snowflake connection")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        for blob_path, table_type in plan:
            try:
                local_path = _download_blob(blob_path, account_url, sas_token, container, tmp_dir)
                rows = load_csv_to_snowflake(local_path, conn, table_type=table_type)
                if rows is None:
                    result['errors'].append(blob_path)
                    logger.error(f"Failed to load {blob_path} into {table_type}")
                else:
                    result['loaded'].append((blob_path, table_type, rows))
                    logger.info(f"Loaded {blob_path} -> {table_type} ({rows} rows)")
            except Exception as e:
                result['errors'].append(blob_path)
                logger.error(f"Error loading {blob_path}: {e}")

    return result


def load_river_forecasts_from_blob(account_url: str, sas_token: str, container: str,
                                    date_str: str, conn=None, execute: bool = False) -> dict:
    """
    Reconstructs RIVER_FORECASTS metadata rows for one forecast date purely from the real
    filenames already present in Blob under glofas/<date_str>/, then loads them via the
    existing, unchanged load_riverine_metadata_to_snowflake().

    Args:
        date_str: 'YYYYMMDD', matching glofas_downloader.py's own date_str convention.
        execute: real Snowflake writes only happen when this is True.

    Returns:
        {'planned': [...], 'loaded_rows': int, 'error': str or None}
    """
    from snowflake_loader import load_riverine_metadata_to_snowflake

    prefix = f'{GLOFAS_PREFIX}/{date_str}/'
    blob_paths = _list_blob_files(prefix, account_url, sas_token, container)

    metadata_rows = []
    discharge_re = re.compile(rf'^river_{date_str}\.zarr\.zip$')
    extent_re = re.compile(rf'^river_extent_rp(\d+(?:\.\d+)?)_bymember_{date_str}\.parquet$')

    for blob_path in blob_paths:
        name = Path(blob_path).name
        if discharge_re.match(name):
            metadata_rows.append({'forecast_time': date_str, 'param': 'dis24', 'stage_path': blob_path})
            continue
        m = extent_re.match(name)
        if m:
            rp = m.group(1)
            rp_key = rp if rp in _IS_STANDIN else f'{rp}.0'
            metadata_rows.append({
                'forecast_time': date_str,
                'param': f'extent_rp{int(float(rp))}_bymember',
                'is_standin': _IS_STANDIN.get(rp_key),
                'stage_path': blob_path,
            })

    result = {'planned': metadata_rows, 'loaded_rows': 0, 'error': None}

    if not execute:
        logger.info(f"DRY RUN: would load {len(metadata_rows)} RIVER_FORECASTS row(s) -- "
                    f"pass execute=True to run for real")
        for row in metadata_rows:
            logger.info(f"  {row}")
        return result

    if conn is None:
        raise ValueError("execute=True requires a real Snowflake connection")
    if not metadata_rows:
        logger.warning(f"No real GloFAS files found in Blob under {prefix}")
        return result

    try:
        rows = load_riverine_metadata_to_snowflake(metadata_rows, conn)
        result['loaded_rows'] = rows
        logger.info(f"Loaded {rows} metadata row(s) into RIVER_FORECASTS")
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Error loading RIVER_FORECASTS metadata: {e}")

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--tracks-envelopes', action='store_true', help='Load tracks/envelopes CSVs from Blob')
    parser.add_argument('--river-forecasts', action='store_true', help='Load RIVER_FORECASTS metadata from Blob')
    parser.add_argument('--date', help='YYYY-MM-DD, required for --river-forecasts')
    parser.add_argument('--execute', action='store_true',
                         help='Actually write to Snowflake. Without this, only lists what would happen.')
    args = parser.parse_args()

    if not args.tracks_envelopes and not args.river_forecasts:
        parser.error('Specify --tracks-envelopes and/or --river-forecasts')
    if args.river_forecasts and not args.date:
        parser.error('--river-forecasts requires --date')

    account_url = os.environ['ACCOUNT_URL']
    sas_token = os.environ['SAS_TOKEN']
    container = os.environ['CONTAINER_NAME']

    conn = None
    if args.execute:
        from snowflake_loader import get_snowflake_connection
        conn = get_snowflake_connection()

    try:
        if args.tracks_envelopes:
            result = load_tracks_envelopes_from_blob(account_url, sas_token, container,
                                                       conn=conn, execute=args.execute)
            if result['errors']:
                logger.error(f"{len(result['errors'])} file(s) failed to load")
                sys.exit(1)
        if args.river_forecasts:
            date_str = args.date.replace('-', '')
            result = load_river_forecasts_from_blob(account_url, sas_token, container,
                                                      date_str, conn=conn, execute=args.execute)
            if result['error']:
                sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
