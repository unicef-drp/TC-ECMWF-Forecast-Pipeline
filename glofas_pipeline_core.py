#!/usr/bin/env python3
"""
Shared GloFAS pipeline core: configuration base and download orchestration.

Mirrors pipeline_core.py's role for the main TC pipeline: common fields and logic
live here once; each entry point adds only its own auth specifics:
  - github_actions/glofas_pipeline.py  — password auth
  - snowflake/glofas_spcs_pipeline.py  — SPCS OAuth / private-key / password auth

Snowflake LOADING (load_riverine_metadata_to_snowflake + RIVER_FORECASTS
verification) deliberately stays in each entry point, not here. Same reason
step7_load/phase4_snowflake_loading aren't shared in pipeline_core.py: the loader
function itself comes from a different module per entry point
(github_actions/snowflake_loader.py vs snowflake/snowflake_loader.py).
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from glofas_downloader import download_glofas_forecast

logger = logging.getLogger(__name__)


class BaseGlofasConfig:
    """
    Shared configuration fields for both GloFAS entry points. Deliberately not
    BasePipelineConfig (pipeline_core.py), that carries many TC-specific fields
    (wind, BUFR paths, run_time validation) that don't apply to this standalone
    pipeline.
    """

    def __init__(self):
        self.data_pipeline_db = os.getenv('DATA_PIPELINE_DB', 'LOCAL').upper()

        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.snowflake_stage_name = os.getenv('SNOWFLAKE_STAGE_NAME')

        # RP thresholds default to Snowflake even when DATA_PIPELINE_DB=LOCAL — they're
        # static reference files, cached independently of the per-run storage backend.
        self.glofas_threshold_source = os.getenv('GLOFAS_THRESHOLD_SOURCE', 'snowflake').lower()
        self.glofas_threshold_local_dir = os.getenv('GLOFAS_THRESHOLD_LOCAL_DIR', 'glofas_thresholds')

        self.glofas_data_dir = os.getenv('GLOFAS_DATA_DIR', 'glofas_data')
        self.cleanup_after_load = os.getenv('CLEANUP_AFTER_LOAD', 'true').lower() == 'true'

        # Optional override — defaults to "today" (UTC). GloFAS's cache key is
        # date-only; there is no TC context here to derive a date from.
        self.download_date = os.getenv('DOWNLOAD_DATE')  # YYYY-MM-DD, optional

    def needs_snowflake_creds(self) -> bool:
        return self.data_pipeline_db == 'SNOWFLAKE' or self.glofas_threshold_source == 'snowflake'

    def validate(self) -> bool:
        """Base validation (simple password auth). Subclasses with different auth
        requirements (e.g. SPCS OAuth/private-key) should override this."""
        if self.glofas_threshold_source not in ('snowflake', 'local'):
            logger.error(f"Invalid GLOFAS_THRESHOLD_SOURCE: {self.glofas_threshold_source}. "
                         "Must be 'snowflake' or 'local'")
            return False

        if not self.needs_snowflake_creds():
            logger.info("DATA_PIPELINE_DB=LOCAL, GLOFAS_THRESHOLD_SOURCE=local — "
                        "Snowflake credentials not required")
            return True

        missing = [var for var, val in (
            ('SNOWFLAKE_ACCOUNT', self.sf_account), ('SNOWFLAKE_USER', self.sf_user),
            ('SNOWFLAKE_PASSWORD', self.sf_password), ('SNOWFLAKE_WAREHOUSE', self.sf_warehouse),
            ('SNOWFLAKE_DATABASE', self.sf_database), ('SNOWFLAKE_SCHEMA', self.sf_schema),
        ) if not val]
        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            return False

        if self.data_pipeline_db == 'SNOWFLAKE' and not self.snowflake_stage_name:
            logger.error("SNOWFLAKE_STAGE_NAME is required when DATA_PIPELINE_DB=SNOWFLAKE")
            return False

        return True


def run_glofas_pipeline(config: BaseGlofasConfig, snowflake_conn=None) -> Optional[Dict]:
    """
    Shared orchestration: determine the forecast date, download/build/upload via
    glofas_downloader, log progress. Returns the result dict from
    download_glofas_forecast(), or None if it failed (already logged).
    """
    forecast_date = config.download_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
    logger.info(f"Forecast date: {forecast_date}")
    logger.info(f"Data pipeline mode: {config.data_pipeline_db}")
    logger.info(f"Threshold source: {config.glofas_threshold_source}")

    upload_to_stage = (config.data_pipeline_db == 'SNOWFLAKE')

    result = download_glofas_forecast(
        date=forecast_date,
        output_dir=config.glofas_data_dir,
        snowflake_conn=snowflake_conn,
        snowflake_stage_name=config.snowflake_stage_name,
        upload_to_stage=upload_to_stage,
        cleanup_raw=config.cleanup_after_load,
        verbose=True,
        threshold_source=config.glofas_threshold_source,
        threshold_local_dir=config.glofas_threshold_local_dir,
    )

    if not result['success']:
        logger.error("GloFAS download/upload failed")
        return None

    return result
