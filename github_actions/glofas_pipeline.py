#!/usr/bin/env python3
"""
GitHub Actions entry point — standalone GloFAS riverine discharge pipeline
(password auth).

A fully separate job from the main TC forecast pipeline (ecmwf-tc-pipline.yml /
github_actions/main.py), NOT wired into its steps. Two reasons this is standalone:

1. GloFAS is TC-independent (a global, always-on hazard layer, not tied to any storm)
   and only publishes once per calendar day, ~11h after the 00Z ECMWF cycle — that
   doesn't map cleanly onto any of the TC pipeline's 4 daily run slots (03/09/15/21 UTC,
   themselves aligned to the 18Z/00Z/06Z/12Z TC cycles).
2. A full global GloFAS fetch (60S-60N, 51 members, 7 daily steps) can take far longer
   than the TC pipeline's other steps — bundling it in would force the whole TC
   pipeline's timeout to accommodate GloFAS's own, unrelated duration.

Requires the RP2 threshold file to already be cached — see setup_glofas_thresholds.py
(run once, manually, not part of any recurring schedule).

Shared config/orchestration lives in glofas_pipeline_core.py (mirrors how
github_actions/main.py adds only password-auth specifics on top of
pipeline_core.py's BasePipelineConfig).
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from glofas_pipeline_core import BaseGlofasConfig, run_glofas_pipeline
from snowflake_loader import get_snowflake_connection, load_riverine_metadata_to_snowflake

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('glofas_pipeline.log'),
    ],
)
logger = logging.getLogger(__name__)


class PipelineConfig(BaseGlofasConfig):
    """Configuration for the GitHub Actions GloFAS pipeline (password auth) — no
    additions needed over the shared base, matching github_actions/main.py's own
    empty PipelineConfig(BasePipelineConfig) subclass."""


def main():
    logger.info("=" * 70)
    logger.info("GLOFAS RIVERINE DISCHARGE PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Start: {datetime.now().isoformat()}")

    config = PipelineConfig()
    if not config.validate():
        logger.error("Configuration validation failed. Exiting.")
        sys.exit(1)

    upload_to_stage = (config.data_pipeline_db == 'SNOWFLAKE')

    conn = None
    if config.needs_snowflake_creds():
        os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
        os.environ['SNOWFLAKE_USER'] = config.sf_user
        os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
        os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
        os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
        os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema
        conn = get_snowflake_connection()

    try:
        result = run_glofas_pipeline(config, snowflake_conn=conn)
        if result is None:
            sys.exit(1)

        if upload_to_stage and conn:
            if result.get('stage_path'):
                metadata_rows = [{
                    'forecast_time': result['forecast_date'],
                    'param': result['param'],
                    'stage_path': result['stage_path'],
                }]
                rows = load_riverine_metadata_to_snowflake(metadata_rows, conn)
                logger.info(f"Loaded {rows} metadata row(s) into RIVER_FORECASTS")

                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT COUNT(*) FROM RIVER_FORECASTS")
                    logger.info(f"RIVER_FORECASTS total rows: {cursor.fetchone()[0]:,}")
                finally:
                    cursor.close()
            else:
                logger.warning(
                    "No stage_path in result (local day-cache hit before this run's "
                    "data was ever staged) — skipping metadata load this run"
                )

        logger.info("GloFAS pipeline completed successfully!")
        sys.exit(0)

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
