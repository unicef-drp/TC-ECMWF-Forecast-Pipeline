#!/usr/bin/env python3
"""
GitHub Actions entry point: standalone GloFAS riverine discharge pipeline
(password auth).

A fully separate job from the main TC forecast pipeline (ecmwf-tc-pipline.yml /
github_actions/main.py), NOT wired into its steps. Two reasons this is standalone:

1. GloFAS is TC-independent (a global, always-on hazard layer, not tied to any storm)
   and only publishes once per calendar day, ~11h after the 00Z ECMWF cycle: that
   doesn't map cleanly onto any of the TC pipeline's 4 daily run slots (03/09/15/21 UTC,
   themselves aligned to the 18Z/00Z/06Z/12Z TC cycles).
2. A full global GloFAS fetch (60S-60N, 51 members, 7 daily steps) can take far longer
   than the TC pipeline's other steps, bundling it in would force the whole TC
   pipeline's timeout to accommodate GloFAS's own, unrelated duration.

Requires the RP2 threshold file to already be cached, see setup_glofas_thresholds.py
(run once, manually, not part of any recurring schedule).

Shared config/orchestration lives in glofas_pipeline_core.py (mirrors how
github_actions/main.py adds only password-auth specifics on top of
pipeline_core.py's BasePipelineConfig).
"""

import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from glofas_pipeline_core import (
    BaseGlofasConfig,
    run_glofas_pipeline,
    run_glofas_submit_pipeline,
    run_glofas_extent_pipeline,
)
from glofas_downloader import MAX_PUBLICATION_LAG_DAYS
from snowflake_loader import (
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
    """Configuration for the GitHub Actions GloFAS pipeline (password auth): no
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

    # Gates the RIVER_FORECASTS metadata-row load below, not the actual discharge/extent
    # file upload itself (that's config.data_pipeline_db-aware internally, in
    # download_glofas_forecast()/run_glofas_extent_masking()). Must include BLOB too, not
    # just SNOWFLAKE: a real remote upload happens in both modes, so the pointer row should
    # be recorded in both. Best-effort like the sibling MET_FORECASTS write in
    # github_actions/main.py: the write below opens its own ad-hoc connection whenever real
    # Snowflake credentials are present (have_snowflake_creds, independent of `conn` above,
    # which is only opened when needs_snowflake_creds() says the configured MODE requires
    # it -- a fully-independent BLOB config leaves `conn` None even with real credentials
    # sitting in the environment); a genuinely credential-less deployment logs a warning
    # and skips it instead of writing nothing silently.
    upload_to_stage = config.data_pipeline_db in ('SNOWFLAKE', 'BLOB')

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
        if config.glofas_mode == 'submit':
            forecast_date_str = config.download_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
            forecast_date_dt = datetime.strptime(forecast_date_str, '%Y-%m-%d')

            # Idempotency guard: GitHub Actions gives no execution-order guarantee
            # between separate schedule entries, so a retry or a delayed run of
            # this same trigger must not silently overwrite/orphan an already-live
            # CDS job. max_lag_days=0, only checking forecast_date_str itself,
            # not the broader fallback window resume uses to look up an existing
            # submission.
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

        # RIVER_FORECASTS pointer rows: `conn` above is only opened when needs_snowflake_
        # creds() says the configured MODE requires it, so a fully-independent BLOB config
        # leaves it None even when real Snowflake credentials are sitting right there in the
        # environment -- previously left the discharge/extent files sitting in Blob with
        # nothing in Snowflake ever pointing at them. write_conn reuses `conn` if already
        # open, otherwise opens its own ad-hoc connection whenever real credentials are
        # present, matching the sibling MET_FORECASTS write in github_actions/main.py.
        have_snowflake_creds = all([
            config.sf_account, config.sf_user, config.sf_password,
            config.sf_warehouse, config.sf_database, config.sf_schema,
        ])
        write_conn = conn
        write_conn_is_ad_hoc = False
        if upload_to_stage and write_conn is None and have_snowflake_creds:
            os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
            os.environ['SNOWFLAKE_USER'] = config.sf_user
            os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
            os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
            os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
            os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema
            write_conn = get_snowflake_connection()
            write_conn_is_ad_hoc = True

        try:
            if upload_to_stage and write_conn:
                if result.get('stage_path'):
                    metadata_rows = [{
                        'forecast_time': result['forecast_date'],
                        'param': result['param'],
                        'stage_path': result['stage_path'],
                    }]
                    rows = load_riverine_metadata_to_snowflake(metadata_rows, write_conn)
                    logger.info(f"Loaded {rows} metadata row(s) into RIVER_FORECASTS")

                    cursor = write_conn.cursor()
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
            elif upload_to_stage:
                logger.warning(
                    "No Snowflake credentials configured -- RIVER_FORECASTS pointer row(s) "
                    "not written; the discharge/extent data is safely in Blob, but nothing "
                    "in Snowflake records where it is until a RIVER_FORECASTS row is "
                    "written separately"
                )

            # Extent-masking step (GloFAS x JRC v2.1), best-effort, never fails this
            # run even if it produces nothing. See run_glofas_extent_pipeline's own
            # docstring for why (a JRC-cache hiccup must not take down the
            # already-critical raw discharge pipeline that shares this run).
            extent_results = run_glofas_extent_pipeline(config, snowflake_conn=conn, discharge_result=result)
            if extent_results and upload_to_stage and write_conn:
                metadata_rows = [{
                    'forecast_time': result['forecast_date'],
                    'param': f"extent_rp{int(float(r['rp']))}_bymember",
                    'is_standin': r['is_standin'],
                    'stage_path': r['stage_path'],
                } for r in extent_results if r.get('stage_path')]
                if metadata_rows:
                    rows = load_riverine_metadata_to_snowflake(metadata_rows, write_conn)
                    logger.info(f"Loaded {rows} extent metadata row(s) into RIVER_FORECASTS")
        finally:
            if write_conn_is_ad_hoc:
                write_conn.close()

        logger.info("GloFAS pipeline completed successfully!")
        sys.exit(0)

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
