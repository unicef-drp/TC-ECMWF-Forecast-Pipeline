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
from pathlib import Path
from typing import Dict, List, Optional

from glofas_downloader import (
    download_glofas_forecast,
    submit_glofas_requests,
    stage_file_exists,
    EXTENT_RP_LEVELS,
)
from glofas_extent_masking import run_glofas_extent_masking

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

        self.glofas_data_dir = os.getenv('GLOFAS_DATA_DIR', 'glofas_data')

        # RP thresholds default to Snowflake even when DATA_PIPELINE_DB=LOCAL
        # they're static reference files, cached independently of the per-run storage backend.
        # Local default lives under glofas_data_dir (not a separate top-level dir) so
        # local storage has one root, mirroring the Snowflake stage's own single
        # glofas/ root (thresholds_cache/, jrc_extent_cache/, and {date}/ all under it).
        self.glofas_threshold_source = os.getenv('GLOFAS_THRESHOLD_SOURCE', 'snowflake').lower()
        self.glofas_threshold_local_dir = os.getenv('GLOFAS_THRESHOLD_LOCAL_DIR',
                                                     f'{self.glofas_data_dir}/thresholds_cache')

        # JRC extent-masking cache: same "static, cached independently of the
        # per-run storage backend" reasoning as the RP thresholds above. Populated
        # by setup_jrc_extents.py (one-time, direct JRC download, run out-of-band
        # never by the recurring pipeline itself, see glofas_extent_masking.py's docstring).
        self.glofas_jrc_source = os.getenv('GLOFAS_JRC_SOURCE', 'snowflake').lower()
        self.glofas_jrc_local_dir = os.getenv('GLOFAS_JRC_LOCAL_DIR',
                                               f'{self.glofas_data_dir}/jrc_extent_cache')
        self.glofas_extent_enabled = os.getenv('GLOFAS_EXTENT_ENABLED', 'true').lower() == 'true'

        self.cleanup_after_load = os.getenv('CLEANUP_AFTER_LOAD', 'true').lower() == 'true'

        # Optional override: defaults to "today" (UTC). GloFAS's cache key is
        # date-only; there is no TC context here to derive a date from.
        self.download_date = os.getenv('DOWNLOAD_DATE')  # YYYY-MM-DD, optional

        # Idle-wait-time cost fix: 'submit' fires the 2 CDS requests
        # (fast, no queue wait) and exits; 'process' resumes a prior submit's
        # request IDs, waiting-then-downloading.
        self.glofas_mode = os.getenv('GLOFAS_MODE', 'process').lower()

    def _needs_stage_operations(self) -> bool:
        """True if at least one of these paths actually GETs/PUTs a *file* on the
        Snowflake stage (as opposed to just needing a connection for table
        reads/writes) -- used to scope the SNOWFLAKE_STAGE_NAME requirement in
        validate() separately from the broader needs_snowflake_creds()."""
        return (self.data_pipeline_db == 'SNOWFLAKE'
                or self.glofas_threshold_source == 'snowflake'
                or (self.glofas_extent_enabled and self.glofas_jrc_source == 'snowflake'))

    def needs_snowflake_creds(self) -> bool:
        # glofas_mode == 'submit' needs a connection too: save_cds_request_ids()
        # is how a submit run's whole result reaches the later process step. A
        # submit run with no connection would still fire 2 real, billable CDS
        # requests and then just discard their request. Unlike the
        # other three conditions, this one only needs a connection for a plain
        # table MERGE (GLOFAS_CDS_REQUESTS).
        return self._needs_stage_operations() or self.glofas_mode == 'submit'

    def validate(self) -> bool:
        """Base validation (simple password auth). Subclasses with different auth
        requirements (e.g. SPCS OAuth/private-key) should override this."""
        if self.glofas_mode not in ('submit', 'process'):
            logger.error(f"Invalid GLOFAS_MODE: {self.glofas_mode}. Must be 'submit' or 'process'")
            return False

        if self.glofas_threshold_source not in ('snowflake', 'local'):
            logger.error(f"Invalid GLOFAS_THRESHOLD_SOURCE: {self.glofas_threshold_source}. "
                         "Must be 'snowflake' or 'local'")
            return False

        # Gated on glofas_extent_enabled, matching needs_snowflake_creds()'s own
        # gating of this same field two lines below — an unused/blank/invalid
        # GLOFAS_JRC_SOURCE left over from before extent masking was enabled
        # (or before setup_jrc_extents.py has ever been run, exactly the state
        # sample_env.txt documents GLOFAS_EXTENT_ENABLED=false for) must not
        # fail validation and take down the raw discharge pipeline over a
        # value that's never actually consulted when extent masking is off.
        if self.glofas_extent_enabled and self.glofas_jrc_source not in ('snowflake', 'local'):
            logger.error(f"Invalid GLOFAS_JRC_SOURCE: {self.glofas_jrc_source}. "
                         "Must be 'snowflake' or 'local'")
            return False

        if not self.needs_snowflake_creds():
            logger.info("DATA_PIPELINE_DB=LOCAL, GLOFAS_THRESHOLD_SOURCE=local, and "
                        "(extent disabled or GLOFAS_JRC_SOURCE=local) — Snowflake credentials not required")
            return True

        missing = [var for var, val in (
            ('SNOWFLAKE_ACCOUNT', self.sf_account), ('SNOWFLAKE_USER', self.sf_user),
            ('SNOWFLAKE_PASSWORD', self.sf_password), ('SNOWFLAKE_WAREHOUSE', self.sf_warehouse),
            ('SNOWFLAKE_DATABASE', self.sf_database), ('SNOWFLAKE_SCHEMA', self.sf_schema),
        ) if not val]
        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            return False

        # _needs_stage_operations() (a narrower check than needs_snowflake_creds()
        # above, which also covers glofas_mode=='submit', that path only does a
        # plain table MERGE, not stage file GETs/PUTs) being True here means AT
        # LEAST ONE of data_pipeline_db=='SNOWFLAKE', glofas_threshold_source==
        # 'snowflake', or (extent_enabled and glofas_jrc_source=='snowflake') is
        # true, so snowflake_stage_name is required for all of them, not just the
        # data_pipeline_db=='SNOWFLAKE' case (that narrower check previously let
        # a DATA_PIPELINE_DB=LOCAL run with default threshold/jrc sources pass
        # validation with no stage name configured, only to fail later after
        # already burning a real CDS API call). A GLOFAS_MODE=submit-only config
        # (DATA_PIPELINE_DB=LOCAL, thresholds/JRC local or disabled) correctly
        # does NOT require a stage name, it never touches the stage.
        if self._needs_stage_operations() and not self.snowflake_stage_name:
            logger.error("SNOWFLAKE_STAGE_NAME is required whenever DATA_PIPELINE_DB=SNOWFLAKE, "
                         "GLOFAS_THRESHOLD_SOURCE=snowflake, or extent masking is enabled with "
                         "GLOFAS_JRC_SOURCE=snowflake")
            return False

        return True


def _glofas_already_downloaded(config: BaseGlofasConfig, forecast_date: datetime,
                                snowflake_conn=None) -> Optional[datetime]:
    """
    Returns forecast_date if forecast_date's own raw discharge Zarr already
    exists locally or on the Snowflake stage; None otherwise.

    Guards GLOFAS_MODE=submit against firing a redundant, wasted pair of CDS
    requests when its own scheduled trigger runs late (GitHub Actions gives no
    execution-order guarantee between separate schedule entries) after the
    process trigger's own submit-and-block fallback has already completed the
    day.

    SAME-DAY only (no MAX_PUBLICATION_LAG_DAYS loop): a stale prior day's file
    always exists once that day's own run has completed, so looping over the
    lag window here would wrongly report today as
    "already downloaded" without ever attempting today's real CDS request,
    the real multi-day publication-lag fallback already lives in, and must stay
    owned by, download_with_fallback()/resume_glofas_download() in
    glofas_downloader.py, which only fall back after a genuine attempt at the
    requested date comes back empty.
    """
    candidate_str = forecast_date.strftime("%Y%m%d")
    local_path = Path(config.glofas_data_dir) / candidate_str / f'river_{candidate_str}.zarr.zip'
    if local_path.exists():
        return forecast_date
    if snowflake_conn and config.snowflake_stage_name:
        stage_path = f'glofas/{candidate_str}/river_{candidate_str}.zarr.zip'
        if stage_file_exists(config.snowflake_stage_name, stage_path, snowflake_conn):
            return forecast_date
    return None


def run_glofas_submit_pipeline(config: BaseGlofasConfig, snowflake_conn=None) -> Optional[Dict]:
    """
    GLOFAS_MODE=submit: fire the 2 CDS requests and return immediately (no queue
    wait), the fast half of the idle-wait-time cost fix. Persisting the
    returned {'actual_date', 'requests'} dict (via each entry point's own
    save_cds_request_ids(), see snowflake_loader.py) is deliberately left to the
    entry point, matching how Snowflake LOADING already stays out of this shared
    core module (see module docstring).

    If today's discharge data is already fully downloaded/staged (see
    _glofas_already_downloaded()), skips submission entirely and returns
    {'actual_date': ..., 'requests': {}}, an empty requests dict, which the
    entry point should treat as "nothing to persist, already done", not a
    failure.

    Returns None if GloFAS wasn't available within the publication-lag window
    (already logged by submit_glofas_requests() itself), the entry point should
    treat that exactly like a failed run of the original blocking path.
    """
    forecast_date_str = config.download_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
    forecast_date = datetime.strptime(forecast_date_str, '%Y-%m-%d')
    logger.info(f"Forecast date: {forecast_date_str}  (GLOFAS_MODE=submit)")

    already = _glofas_already_downloaded(config, forecast_date, snowflake_conn)
    if already is not None:
        logger.info(f"  {already.strftime('%Y-%m-%d')} already fully downloaded/staged -- "
                    f"skipping submission (this trigger likely ran after the process trigger's "
                    f"own fallback had already completed the day)")
        return {"actual_date": already, "requests": {}}

    return submit_glofas_requests(forecast_date)


def run_glofas_pipeline(config: BaseGlofasConfig, snowflake_conn=None,
                         pre_submitted: Optional[Dict] = None) -> Optional[Dict]:
    """
    Shared orchestration: determine the forecast date, download/build/upload via
    glofas_downloader, log progress. Returns the result dict from
    download_glofas_forecast(), or None if it failed (already logged).

    pre_submitted: optional {'actual_date', 'requests'} from a prior submit step
    (GLOFAS_MODE=process resuming via each entry point's own
    load_cds_request_ids()), forwarded to download_glofas_forecast() to resume
    waiting on those requests instead of submitting fresh. None (default): behaves
    exactly as before, submits fresh and blocks through the full wait.
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
        pre_submitted=pre_submitted,
    )

    if not result['success']:
        logger.error("GloFAS download/upload failed")
        return None

    return result


def run_glofas_extent_pipeline(config: BaseGlofasConfig, snowflake_conn=None,
                                discharge_result: Optional[Dict] = None) -> List[Dict]:
    """
    Shared orchestration for the GloFAS x JRC extent-masking step, called
    right after run_glofas_pipeline() succeeds, in the same daily run, using
    the sparse cell set that step just built (discharge_result['zip_path']).
    """
    if not config.glofas_extent_enabled:
        logger.info("GLOFAS_EXTENT_ENABLED=false — skipping extent-masking step")
        return []

    if discharge_result is None or not discharge_result.get('zip_path'):
        logger.warning("No local Zarr path from the discharge step (e.g. a stage-only day-cache "
                        "hit) — extent masking needs the local sparse cell data, skipping this run")
        return []

    logger.info(f"JRC source: {config.glofas_jrc_source}")
    upload_to_stage = (config.data_pipeline_db == 'SNOWFLAKE')

    try:
        results = run_glofas_extent_masking(
            zarr_path=Path(discharge_result['zip_path']),
            forecast_date=discharge_result['forecast_date'],
            output_dir=config.glofas_data_dir,
            threshold_source=config.glofas_threshold_source,
            threshold_local_dir=config.glofas_threshold_local_dir,
            jrc_source=config.glofas_jrc_source,
            jrc_local_dir=config.glofas_jrc_local_dir,
            snowflake_conn=snowflake_conn,
            snowflake_stage_name=config.snowflake_stage_name,
            upload_to_stage=upload_to_stage,
        )
    except Exception as e:
        logger.error(f"GloFAS extent-masking step failed entirely, continuing without it: {e}")
        return []

    logger.info(f"Extent masking: {len(results)} of {len(EXTENT_RP_LEVELS)} RP tiers produced output")
    return results
