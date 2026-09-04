#!/usr/bin/env python3
"""
SPCS entry point: standalone GloFAS riverine discharge pipeline (SPCS OAuth /
private-key / password auth).

A fully separate job from spcs_pipeline.py (the main TC forecast pipeline), NOT
wired into its steps or scheduled alongside it (GloFAS's own ~11h publication cadence doesn't map
onto the TC pipeline's 4x-daily run slots, and a full global fetch can take far
longer than the TC pipeline's other steps, so bundling them would force one
unrelated schedule/timeout onto the other).

Requires the RP2 threshold file to already be cached
(run once, manually, not part of any recurring schedule). Trigger via a Snowflake
TASK calling EXECUTE JOB SERVICE, scheduled once daily comfortably past GloFAS's
~11h publication latency (e.g. 12-13 UTC), see snowflake/README.md for the job
service execution pattern used by the main pipeline.

Shared config/orchestration lives in glofas_pipeline_core.py (mirrors how
spcs_pipeline.py adds only SPCS-specific auth on top of pipeline_core.py's
BasePipelineConfig, rather than duplicating the common fields/steps).
"""

import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

from glofas_pipeline_core import (
    BaseGlofasConfig,
    run_glofas_pipeline,
    run_glofas_submit_pipeline,
    run_glofas_extent_pipeline,
)
from glofas_downloader import MAX_PUBLICATION_LAG_DAYS
from snowflake.snowflake_loader import (
    get_snowflake_connection,
    load_riverine_metadata_to_snowflake,
    save_cds_request_ids,
    load_cds_request_ids,
)

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
    """Configuration for the SPCS GloFAS pipeline (OAuth / private key auth): adds
    only SPCS-specific fields on top of the shared base, mirroring how
    snowflake/spcs_pipeline.py's own PipelineConfig extends BasePipelineConfig."""

    def __init__(self):
        super().__init__()

        self.sf_private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        self.sf_private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        # SPCS deployments commonly rely on these defaults rather than requiring
        # every env var set explicitly; matches spcs_pipeline.py's own PipelineConfig.
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'MY_WH')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE', 'MY_DB')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

        self.spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
        self.spcs_token_path = os.getenv('SPCS_TOKEN_PATH', '/snowflake/session/token')

    def validate(self) -> bool:
        """Overrides the base (simple password-only) validation: SPCS supports
        OAuth, private-key, or password auth, each with different requirements.
        BLOB, SNOWFLAKE, and LOCAL are genuinely independent, mirroring the base
        class's own validate() (glofas_pipeline_core.py)."""
        if self.data_pipeline_db not in ('SNOWFLAKE', 'BLOB', 'LOCAL'):
            logger.error(f"Invalid DATA_PIPELINE_DB: {self.data_pipeline_db}. "
                         "Must be 'SNOWFLAKE', 'BLOB', or 'LOCAL'")
            return False

        if self.glofas_mode not in ('submit', 'process'):
            logger.error(f"Invalid GLOFAS_MODE: {self.glofas_mode}. Must be 'submit' or 'process'")
            return False

        if self.glofas_threshold_source not in ('snowflake', 'local', 'blob'):
            logger.error(f"Invalid GLOFAS_THRESHOLD_SOURCE: {self.glofas_threshold_source}. "
                         "Must be 'snowflake', 'local', or 'blob'")
            return False

        if self.glofas_extent_enabled and self.glofas_jrc_source not in ('snowflake', 'local', 'blob'):
            logger.error(f"Invalid GLOFAS_JRC_SOURCE: {self.glofas_jrc_source}. "
                         "Must be 'snowflake', 'local', or 'blob'")
            return False

        # Checked unconditionally, before the Snowflake early-return below: a fully-
        # BLOB config needs zero Snowflake creds and must not skip this as a side effect.
        if self.needs_blob_creds():
            blob_missing = [var for var, val in (
                ('ACCOUNT_URL', self.blob_account_url),
                ('SAS_TOKEN', self.blob_sas_token),
                ('CONTAINER_NAME', self.blob_container),
            ) if not val]
            if blob_missing:
                logger.error(f"Missing required Blob environment variables: {', '.join(blob_missing)}")
                return False

        if not self.needs_snowflake_creds():
            logger.info("No Snowflake-sourced path is configured (DATA_PIPELINE_DB, "
                        "GLOFAS_THRESHOLD_SOURCE, and GLOFAS_JRC_SOURCE are all 'local'/'blob', "
                        "or extent masking is disabled) -- Snowflake credentials not required")
            return True

        if not self.sf_account:
            logger.error("Missing required environment variable: SNOWFLAKE_ACCOUNT")
            return False

        if self.spcs_run:
            if not Path(self.spcs_token_path).is_file():
                logger.error(f"SPCS token file not found: {self.spcs_token_path}")
                return False
            if not os.getenv('SNOWFLAKE_HOST'):
                logger.error("Missing required environment variable for SPCS mode: SNOWFLAKE_HOST")
                return False
            if not os.getenv('SNOWFLAKE_PORT'):
                logger.error("Missing required environment variable for SPCS mode: SNOWFLAKE_PORT")
                return False
        else:
            if not self.sf_user:
                logger.error("Missing required environment variable: SNOWFLAKE_USER")
                return False
            if not self.sf_password and not self.sf_private_key_path:
                logger.error("Missing auth: set SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
                return False
            if self.sf_private_key_path and not Path(self.sf_private_key_path).is_file():
                logger.error(f"Private key file not found: {self.sf_private_key_path}")
                return False

        # Scoped to _needs_stage_operations(), not the broader needs_snowflake_creds()
        # a GLOFAS_MODE=submit-only config never touches the stage (just a plain
        # GLOFAS_CDS_REQUESTS table MERGE), so it must not be forced to configure a
        # stage name it will never use.
        if self._needs_stage_operations() and not self.snowflake_stage_name:
            logger.error("SNOWFLAKE_STAGE_NAME is required whenever DATA_PIPELINE_DB=SNOWFLAKE, "
                         "GLOFAS_THRESHOLD_SOURCE=snowflake, or extent masking is enabled with "
                         "GLOFAS_JRC_SOURCE=snowflake")
            return False

        return True


def _open_snowflake_conn(config: PipelineConfig):
    """Open a Snowflake connection using the auth mode active in config, mirrors
    spcs_pipeline.py's _open_snowflake_conn."""
    os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
    os.environ['SNOWFLAKE_USER'] = config.sf_user or ''
    os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
    os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
    os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema

    if config.spcs_run:
        os.environ['SPCS_RUN'] = 'true'
        os.environ['SPCS_TOKEN_PATH'] = config.spcs_token_path
        for key in ('SNOWFLAKE_PASSWORD', 'SNOWFLAKE_PRIVATE_KEY_PATH',
                    'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'):
            os.environ.pop(key, None)
        logger.info("Connecting with SPCS OAuth authentication")
    elif config.sf_private_key_path:
        os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'] = config.sf_private_key_path
        if config.sf_private_key_passphrase:
            os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'] = config.sf_private_key_passphrase
        os.environ.pop('SNOWFLAKE_PASSWORD', None)
        logger.info(f"Connecting with private key: {config.sf_private_key_path}")
    else:
        os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
        for key in ('SNOWFLAKE_PRIVATE_KEY_PATH', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'):
            os.environ.pop(key, None)
        logger.info("Connecting with password authentication")

    return get_snowflake_connection()


def main():
    logger.info("=" * 70)
    logger.info("GLOFAS RIVERINE DISCHARGE PIPELINE (SPCS)")
    logger.info("=" * 70)
    logger.info(f"Start: {datetime.now().isoformat()}")

    config = PipelineConfig()
    if not config.validate():
        logger.error("Configuration validation failed. Exiting.")
        sys.exit(1)

    # Gates the RIVER_FORECASTS metadata-row load, not the file upload itself: see
    # github_actions/glofas_pipeline.py's identical fix for the full rationale.
    upload_to_stage = config.data_pipeline_db in ('SNOWFLAKE', 'BLOB')

    conn = None
    if config.needs_snowflake_creds():
        conn = _open_snowflake_conn(config)

    try:
        if config.glofas_mode == 'submit':
            forecast_date_str = config.download_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
            forecast_date_dt = datetime.strptime(forecast_date_str, '%Y-%m-%d')

            # Idempotency guard: no execution-order guarantee between separate
            # scheduled triggers, so a retry or a delayed run of this same
            # trigger must not silently overwrite/orphan an already-live CDS job.
            # max_lag_days=0: only checking forecast_date_str itself, not the
            # broader fallback window resume uses to look up an existing submission.
            if conn:
                existing = load_cds_request_ids(forecast_date_dt, conn, max_lag_days=0)
                if existing:
                    logger.info(f"CDS requests already submitted for "
                                f"{existing['actual_date'].strftime('%Y-%m-%d')} "
                                f"({existing['requests']}) -- skipping duplicate submission")
                    sys.exit(0)

            submitted = run_glofas_submit_pipeline(config, snowflake_conn=conn)
            if submitted is None:
                logger.error("GloFAS submit failed (not yet published within the lag window)")
                sys.exit(1)

            if not submitted['requests']:
                # Already fully downloaded/staged (this trigger likely ran late,
                # after the process trigger's own fallback already completed the
                # day)
                logger.info("GloFAS submit step completed successfully! (nothing to submit, "
                            "already downloaded)")
                sys.exit(0)

            if conn:
                rows = save_cds_request_ids(submitted['actual_date'], submitted['requests'], conn)
                if rows < len(submitted['requests']):
                    logger.error(f"Only saved {rows} of {len(submitted['requests'])} CDS request "
                                 f"ID(s) -- the process step will not be able to resume the "
                                 f"unsaved request(s) and will submit a fresh pair instead, "
                                 f"wasting the CDS requests just fired")
                    sys.exit(1)
                logger.info(f"Saved {rows} CDS request ID(s) for the later process step to resume")
            else:
                logger.warning("No Snowflake connection -- submitted request IDs cannot be "
                                "resumed later; the process step will submit fresh instead")

            logger.info("GloFAS submit step completed successfully!")
            sys.exit(0)

        # GLOFAS_MODE=process (default): resume a prior submit step's request IDs
        # if one was saved for this date; falls back to a fresh submit-and-block
        # (today's original behavior) if there's nothing to resume from.
        pre_submitted = None
        if conn:
            forecast_date_str = config.download_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
            pre_submitted = load_cds_request_ids(datetime.strptime(forecast_date_str, '%Y-%m-%d'), conn,
                                                  max_lag_days=MAX_PUBLICATION_LAG_DAYS)

        result = run_glofas_pipeline(config, snowflake_conn=conn, pre_submitted=pre_submitted)
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
                    "data was ever staged) -- skipping metadata load this run"
                )

        # Extent-masking step (GloFAS x JRC v2.1)
        extent_results = run_glofas_extent_pipeline(config, snowflake_conn=conn, discharge_result=result)
        if extent_results and upload_to_stage and conn:
            metadata_rows = [{
                'forecast_time': result['forecast_date'],
                'param': f"extent_rp{int(float(r['rp']))}_bymember",
                'is_standin': r['is_standin'],
                'stage_path': r['stage_path'],
            } for r in extent_results if r.get('stage_path')]
            if metadata_rows:
                rows = load_riverine_metadata_to_snowflake(metadata_rows, conn)
                logger.info(f"Loaded {rows} extent metadata row(s) into RIVER_FORECASTS")

        logger.info("GloFAS pipeline completed successfully!")
        sys.exit(0)

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
