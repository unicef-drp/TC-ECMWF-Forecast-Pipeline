#!/usr/bin/env python3
"""
One-time setup: cache the official GloFAS v4.0 RP threshold files.

NOT part of the recurring pipeline, run this manually once (or again only if
ECMWF releases a new GloFAS version). Downloads ECMWF's official, pre-computed
return-period discharge threshold grids and PUTs them to a fixed Snowflake stage path. 

Source: https://confluence.ecmwf.int/spaces/CEMS/pages/242067380/Auxiliary+Data
        ("GloFAS Flood Thresholds" section)

Levels cached: 2, 5, 10, 20, 50, 100, 200, 500yr

Usage:
    python3 setup_glofas_thresholds.py                     # upload to Snowflake stage
    python3 setup_glofas_thresholds.py --local-only DIR    # keep local only, for offline dev
"""

import argparse
import logging
import sys
from pathlib import Path

import requests

# This is a manually-run, one-time setup script (not a pipeline entry point), so it
# uses github_actions/snowflake_loader.py's password-auth connection helper rather
# than picking a specific deployment target. Import added to sys.path lazily, only
# when actually needed (Snowflake upload path), to keep --local-only usable without
# github_actions/ present.
sys.path.insert(0, str(Path(__file__).parent / "github_actions"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://confluence.ecmwf.int/download/attachments/242067380"
RP_LEVELS = ["2.0", "5.0", "10.0", "20.0", "50.0", "100.0", "200.0", "500.0"]
# Same relative path as the local runtime cache (glofas_downloader.py's
# glofas_data/thresholds_cache/) so the stage's canonical location and the local
# GET-cache mirror each other exactly, one convention, not two. Must be kept in
# sync with THRESHOLD_STAGE_PREFIX in glofas_downloader.py, since there is no
# shared import between the two entry points, so this is a manually-maintained
# constant.
STAGE_PREFIX = "glofas/thresholds_cache"


def download_threshold_file(rp: str, dest_dir: Path) -> Path:
    """Download one official threshold file if not already present locally."""
    fname = f"flood_threshold_glofas_v4_rl_{rp}.nc"
    dest_path = dest_dir / f"rl_{rp}.nc"
    if dest_path.exists():
        logger.info(f"  RP{rp}: already downloaded locally, skipping fetch")
        return dest_path

    url = f"{BASE_URL}/{fname}"
    logger.info(f"  RP{rp}: downloading from {url} ...")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    size_mb = dest_path.stat().st_size / 1024 / 1024
    logger.info(f"  RP{rp}: saved {dest_path.name} ({size_mb:.0f} MB)")
    return dest_path


def upload_to_stage(local_path: Path, stage_name: str, conn) -> bool:
    """PUT one threshold file to the fixed glofas/thresholds_cache/ stage path."""
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
                              "(for offline dev; matches GLOFAS_THRESHOLD_SOURCE=local)")
    parser.add_argument("--dest-dir", default="glofas_thresholds_tmp",
                         help="Local working directory for downloads (default: glofas_thresholds_tmp)")
    args = parser.parse_args()

    dest_dir = Path(args.dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info(f"GloFAS RP threshold setup — {len(RP_LEVELS)} levels: {', '.join(RP_LEVELS)}")
    logger.info("=" * 70)

    local_paths = []
    for rp in RP_LEVELS:
        try:
            local_paths.append(download_threshold_file(rp, dest_dir))
        except Exception as e:
            logger.error(f"  RP{rp}: download failed: {e}")
            sys.exit(1)

    if args.local_only:
        local_only_dir = Path(args.local_only)
        local_only_dir.mkdir(parents=True, exist_ok=True)
        for p in local_paths:
            target = local_only_dir / p.name
            if p.resolve() != target.resolve():
                target.write_bytes(p.read_bytes())
        logger.info(f"Done — {len(local_paths)} files kept locally in {local_only_dir} "
                    f"(GLOFAS_THRESHOLD_SOURCE=local should point here)")
        return

    import os
    from snowflake_loader import get_snowflake_connection  # noqa: local import, only needed for stage upload
    for var in ('SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
                'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'):
        if not os.getenv(var):
            logger.error(f"{var} not set — required for Snowflake stage upload")
            sys.exit(1)
    stage_name = os.getenv('SNOWFLAKE_STAGE_NAME')
    if not stage_name:
        logger.error("SNOWFLAKE_STAGE_NAME not set — required for Snowflake stage upload")
        sys.exit(1)

    conn = get_snowflake_connection()
    try:
        for p in local_paths:
            if not upload_to_stage(p, stage_name, conn):
                logger.error(f"  Upload failed for {p.name}")
                sys.exit(1)
    finally:
        conn.close()

    logger.info(f"Done — {len(local_paths)} threshold files staged at "
                f"@{stage_name}/{STAGE_PREFIX}/")


if __name__ == "__main__":
    main()
