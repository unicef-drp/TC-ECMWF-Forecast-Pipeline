#!/usr/bin/env python3
"""
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

UNSUPPORTED BY SNOWFLAKE - CUSTOMER SUPPORTED ONLY

SPCS entry point: concurrent pipeline execution with SPCS OAuth / private-key auth.

Extends pipeline_core with:
  - ProcessPoolExecutor concurrent transformation (controlled by USE_PROCESS_POOL)
  - SPCS OAuth, private-key, and password authentication modes
  - Phase-level timing for performance monitoring

Execution phases:
  1. Download TC data → extract named storms → split per-storm CSVs
  2. Transform per-storm CSVs (concurrently if USE_PROCESS_POOL=true)
  3. Download wind GRIB files → create wind threshold envelope polygons
  4. Load all data to Snowflake
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import multiprocessing

# Add repo root to path
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

from pipeline_core import (
    BasePipelineConfig,
    PipelineStats as _BasePipelineStats,
    extract_tc_data_info,
    extract_tc_data_info_from_bufr,
    step1_download,
    step2_extract,
    step3_transform,
    step4_download_wind,
    step4b_download_gust,
    step5_process_wind,
    step5b_extract_gust_envelopes,
    step6_download_precip,
    cleanup_files,
)
from snowflake.snowflake_loader import get_snowflake_connection, load_csv_to_snowflake, load_precip_metadata_to_snowflake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('unicef_pipeline.log'),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class PipelineConfig(BasePipelineConfig):
    """Configuration for the SPCS concurrent pipeline (OAuth / private key auth)."""

    def __init__(self):
        super().__init__()
        # transformed_data_dir already defaults to 'tc_data_transformed' via
        # BasePipelineConfig.__init__ (same as github_actions), and already
        # respects an explicit TRANSFORMED_DATA_DIR override

        # Extended Snowflake auth options
        self.sf_private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        self.sf_private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'MY_WH')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE', 'MY_DB')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

        # SPCS OAuth mode
        self.spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
        self.spcs_token_path = os.getenv('SPCS_TOKEN_PATH', '/snowflake/session/token')

        # Concurrency settings
        self.max_workers = int(os.getenv('MAX_WORKERS', '0'))
        if self.max_workers == 0:
            self.max_workers = multiprocessing.cpu_count()
            logger.info(f"MAX_WORKERS=0 → auto-detected {self.max_workers} CPUs")

        max_concurrent_downloads_env = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '0'))
        self.max_concurrent_downloads = (
            min(self.max_workers, 10) if max_concurrent_downloads_env == 0
            else min(max_concurrent_downloads_env, 10)
        )

        self.use_process_pool = os.getenv('USE_PROCESS_POOL', 'true').lower() == 'true'

    def validate(self) -> bool:
        """
        Validate required configuration (supports SPCS OAuth, private key, or password auth).

        BLOB, SNOWFLAKE, and LOCAL are genuinely independent, matching the base class's own
        validate() (pipeline_core.py) and the GitHub Actions entry point (github_actions/main.py);
        this override previously required sf_account and (when PROCESS_MET=true)
        SNOWFLAKE_STAGE_NAME unconditionally for any non-LOCAL mode, including BLOB, which both
        contradicted the base class's documented independence and made a correctly-configured
        BLOB-only SPCS deployment fail validation before it could even run.
        """
        if self.data_pipeline_db == 'BLOB':
            blob_missing = [var for var, val in (
                ('ACCOUNT_URL', self.blob_account_url),
                ('SAS_TOKEN', self.blob_sas_token),
                ('CONTAINER_NAME', self.blob_container),
            ) if not val]
            if blob_missing:
                logger.error(f"Missing required Blob environment variables: {', '.join(blob_missing)}")
                return False
            return self._validate_run_time()

        if self.data_pipeline_db == 'LOCAL':
            logger.info("DATA_PIPELINE_DB=LOCAL -- Snowflake credentials not required")
            return self._validate_run_time()

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

        if self.process_met and self.data_pipeline_db == 'SNOWFLAKE' and not self.snowflake_stage_name:
            logger.error(
                "SNOWFLAKE_STAGE_NAME is required when PROCESS_MET=true and "
                "DATA_PIPELINE_DB=SNOWFLAKE. Add it to the SPCS service spec."
            )
            return False

        # Non-fatal, discoverability only: see BasePipelineConfig.validate()'s
        # own identical check (pipeline_core.py) for the full rationale.
        if self.publish_wind_raster and self.data_pipeline_db == 'SNOWFLAKE' and not self.snowflake_stage_name:
            logger.warning(
                "PUBLISH_WIND_RASTER=true but SNOWFLAKE_STAGE_NAME is not set: "
                "wind/gust rasters will be generated but never uploaded this run."
            )

        return self._validate_run_time()


# ---------------------------------------------------------------------------
# Stats (extends base with phase timing)
# ---------------------------------------------------------------------------

class PipelineStats(_BasePipelineStats):
    """Extended statistics tracker with per-phase timing for concurrent execution."""

    def __init__(self):
        super().__init__()
        self.phase_times: Dict[str, float] = {}
        # Separately track TC-only downloads (set before wind download runs)
        self.files_downloaded_tc = 0

    def log_phase_time(self, phase_name: str, duration: float):
        """Record and log the elapsed time for a phase."""
        self.phase_times[phase_name] = duration
        logger.info(f"  {phase_name} completed in {duration:.2f}s ({duration / 60:.1f} min)")

    def log_summary(self):
        """Log pipeline execution summary including phase timings."""
        total_duration = (datetime.now() - self.start_time).total_seconds()
        logger.info("=" * 70)
        logger.info("CONCURRENT PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total Duration: {total_duration:.2f}s ({total_duration / 60:.1f} min)")
        if self.phase_times:
            logger.info("Phase Timings:")
            for phase, duration in self.phase_times.items():
                logger.info(f"  {phase}: {duration:.2f}s ({duration / 60:.1f} min)")
        logger.info("Files Processed:")
        logger.info(f"  TC Files Downloaded:   {self.files_downloaded_tc}")
        logger.info(f"  Wind Files Downloaded: {self.files_wind_downloaded}")
        logger.info(f"  Files Extracted:       {self.files_extracted}")
        logger.info(f"  Files Transformed:     {self.files_transformed}")
        logger.info(f"  Wind Files Processed:  {self.files_wind_processed}")
        logger.info(f"  Files Loaded:          {self.files_loaded}")
        logger.info(f"  Total Rows Loaded:     {self.rows_loaded:,}")
        if self.errors:
            logger.error(f"Errors encountered: {len(self.errors)}")
            for error in self.errors:
                logger.error(f"  - {error}")
        else:
            logger.info("No errors encountered")
        logger.info("=" * 70)


# ---------------------------------------------------------------------------
# Pipeline phases
# ---------------------------------------------------------------------------

def _open_snowflake_conn(config: PipelineConfig):
    """Open a Snowflake connection using the auth mode active in config."""
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


def phase4_snowflake_loading(config: PipelineConfig, stats: PipelineStats,
                              transformed_files: List[Path],
                              envelope_files: List[Path],
                              precip_metadata: list = None,
                              raster_files: List[Path] = None):
    """Phase 4: Load all results. Real MERGE into Snowflake tables in
    SNOWFLAKE mode, plain CSV/raster upload to Blob in BLOB mode, or a no-op
    (files kept locally) in LOCAL mode.

    raster_files: wind/gust speed-field GeoTIFFs from step5_process_wind().
        Uploaded in both BLOB and SNOWFLAKE mode below, same shape as
        github_actions/main.py's step7_load() (this file's own GHA-entry-point
        sibling for the identical pipeline).
    """
    raster_files = raster_files or []
    logger.info("=" * 70)
    logger.info("PHASE 4: SNOWFLAKE LOADING")
    logger.info("=" * 70)
    phase_start = datetime.now()

    if config.data_pipeline_db == 'LOCAL':
        logger.info("DATA_PIPELINE_DB=LOCAL -- skipping Snowflake load, files kept locally")
        logger.info(f"  Transformed tracks : {config.transformed_data_dir}")
        logger.info(f"  Wind envelopes     : {config.wind_extracted_dir}")
        logger.info(f"  Wind rasters       : {len(raster_files)} file(s) in {config.wind_extracted_dir}")
        logger.info(f"  Met ZipStores      : {config.met_data_dir}")
        stats.files_loaded = len(transformed_files) + len(envelope_files) + len(raster_files)
        stats.rows_loaded = 0
        stats._local_mode = True
        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time("Phase 4: Skip (LOCAL mode)", duration)
        return

    if config.data_pipeline_db == 'BLOB':
        # Uploads the same track/envelope CSVs the SNOWFLAKE branch would load directly,
        # to the shared Blob container instead, under 'tracks/' and 'envelopes/' at the
        # container root, matching github_actions/main.py's step7_load() BLOB branch for
        # the identical pipeline (this file is the SPCS entry point for the same pipeline).
        # TC_TRACKS/TC_ENVELOPES_COMBINED/TC_GUST_ENVELOPES_* are not written from these
        # CSVs directly: a separate, already-built manual loader
        # (blob_to_snowflake_loader.py, not run automatically by any real scheduled
        # workflow today) reads these same Blob prefixes back and MERGEs them in.
        logger.info("DATA_PIPELINE_DB=BLOB -- uploading CSVs to Blob, TC_TRACKS/TC_ENVELOPES_COMBINED not updated")
        from ecmwf_met_downloader import upload_to_blob
        uploaded = 0
        for csv_file in transformed_files:
            if upload_to_blob(csv_file, config.blob_account_url, config.blob_sas_token,
                               config.blob_container, f'tracks/{csv_file.name}'):
                uploaded += 1
            else:
                stats.upload_failed_filenames.add(csv_file.name)
        for csv_file in envelope_files:
            if upload_to_blob(csv_file, config.blob_account_url, config.blob_sas_token,
                               config.blob_container, f'envelopes/{csv_file.name}'):
                uploaded += 1
            else:
                stats.upload_failed_filenames.add(csv_file.name)
        # Wind/gust rasters: own prefix per dataset, parallel to tracks/envelopes. See
        # github_actions/main.py's step7_load() BLOB branch for the full rationale (no
        # Snowflake pointer table needed, cleanly file-per-(storm, forecast_time,
        # lead_time), pure-filename-parseable the same way tracks/envelopes already
        # are). Prefix derived from the filename's own `{dataset_label}_raster_` token,
        # same as that sibling function, one real implementation to keep in sync,
        # not two drifting copies of the routing rule.
        raster_uploaded = 0
        for tif_file in raster_files:
            prefix = 'gust_raster' if 'gust_raster' in tif_file.name else 'wind_raster'
            if upload_to_blob(tif_file, config.blob_account_url, config.blob_sas_token,
                               config.blob_container, f'{prefix}/{tif_file.name}'):
                raster_uploaded += 1
            else:
                stats.upload_failed_filenames.add(tif_file.name)
        if raster_files and raster_uploaded < len(raster_files):
            logger.warning(f"Only {raster_uploaded}/{len(raster_files)} wind/gust raster(s) uploaded to Blob successfully")
        elif raster_files:
            logger.info(f"Uploaded {raster_uploaded} wind/gust raster(s) to Blob (wind_raster//gust_raster/)")
        stats.files_loaded = uploaded + raster_uploaded
        stats.rows_loaded = 0
        stats._local_mode = True
        if uploaded < len(transformed_files) + len(envelope_files):
            error_msg = (f"Only {uploaded}/{len(transformed_files) + len(envelope_files)} "
                         f"track/envelope CSVs uploaded to Blob successfully")
            logger.error(error_msg)
            stats.errors.append(error_msg)

        # MET_FORECASTS pointer rows: same best-effort write as github_actions/main.py's
        # step7_load() BLOB branch; see that function's own comment for the full
        # rationale (pointer tables always live in Snowflake regardless of where the bulk
        # data is). Uses this file's own _open_snowflake_conn() (supports SPCS OAuth/
        # private-key/password auth) rather than duplicating a simpler env-var-only
        # connect. sf_account is only a minimal "worth attempting" signal here; validate()
        # never required full Snowflake credentials for BLOB mode, so any partial/broken
        # config still fails safely inside the try/except below rather than crashing this
        # otherwise-successful Blob upload.
        if precip_metadata:
            if config.sf_account:
                met_conn = None
                try:
                    met_conn = _open_snowflake_conn(config)
                    rows = load_precip_metadata_to_snowflake(precip_metadata, met_conn)
                    stats.rows_loaded += rows
                    logger.info(f"Loaded {rows} metadata row(s) into MET_FORECASTS (BLOB mode)")
                except Exception as e:
                    error_msg = f"Could not write MET_FORECASTS pointer row(s) in BLOB mode: {e}"
                    logger.error(error_msg)
                    stats.errors.append(error_msg)
                finally:
                    if met_conn is not None:
                        met_conn.close()
            else:
                logger.warning(
                    "No Snowflake credentials configured -- MET_FORECASTS pointer row(s) not "
                    "written; the precip/runoff data is safely in Blob, but nothing in "
                    "Snowflake records where it is until a MET_FORECASTS row is written "
                    "separately"
                )

        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time("Phase 4: Blob Upload", duration)
        return

    try:
        conn = _open_snowflake_conn(config)

        def _set_context():
            c = conn.cursor()
            try:
                c.execute(f"USE WAREHOUSE {config.sf_warehouse}")
                c.execute(f"USE DATABASE {config.sf_database}")
                c.execute(f"USE SCHEMA {config.sf_schema}")
            finally:
                c.close()

        try:
            _set_context()  # initial context, inside try/finally: conn.close()
            total_rows = 0

            def _load_and_track(csv_file, table_type):
                """load_csv_to_snowflake wrapper that tells a real failure
                (None) apart from a legitimate empty file (0), a None means
                the load itself broke and must be recorded as a real error,
                not silently treated as zero rows loaded."""
                nonlocal total_rows
                rows = load_csv_to_snowflake(csv_file, conn, table_type=table_type)
                if rows is None:
                    error_msg = f"Failed to load {csv_file.name} into {table_type} -- see error above"
                    logger.error(error_msg)
                    stats.errors.append(error_msg)
                    stats.upload_failed_filenames.add(csv_file.name)
                    return
                total_rows += rows

            for csv_file in transformed_files:
                _set_context()
                _load_and_track(csv_file, 'TC_TRACKS')

            for csv_file in envelope_files:
                _set_context()
                if 'gust_envelopes_individual' in csv_file.name:
                    table_type = 'TC_GUST_ENVELOPES_INDIVIDUAL'
                elif 'gust_envelopes_combined' in csv_file.name:
                    table_type = 'TC_GUST_ENVELOPES_COMBINED'
                elif 'individual' in csv_file.name:
                    table_type = 'TC_ENVELOPES_INDIVIDUAL'
                elif 'combined' in csv_file.name:
                    table_type = 'TC_ENVELOPES_COMBINED'
                else:
                    logger.warning(f"Unknown envelope file type: {csv_file.name}")
                    continue
                _load_and_track(csv_file, table_type)

            if precip_metadata:
                _set_context()
                total_rows += load_precip_metadata_to_snowflake(precip_metadata, conn)

            # Wind/gust rasters: same generic binary-file-to-stage upload as
            # github_actions/main.py's step7_load() SNOWFLAKE branch (this
            # file's own GHA-entry-point sibling for the identical pipeline).
            # Reuses upload_to_snowflake_stage() (already used for precip's
            # own Zarr uploads elsewhere in this pipeline).
            raster_uploaded = 0
            if raster_files and config.snowflake_stage_name:
                from ecmwf_met_downloader import upload_to_snowflake_stage
                _set_context()
                for tif_file in raster_files:
                    prefix = 'gust_raster' if 'gust_raster' in tif_file.name else 'wind_raster'
                    if upload_to_snowflake_stage(tif_file, config.snowflake_stage_name,
                                                  f'{prefix}/{tif_file.name}', conn):
                        raster_uploaded += 1
                    else:
                        stats.upload_failed_filenames.add(tif_file.name)
                if raster_uploaded < len(raster_files):
                    logger.warning(f"Only {raster_uploaded}/{len(raster_files)} wind/gust raster(s) "
                                    f"uploaded to Snowflake stage successfully")
                else:
                    logger.info(f"Uploaded {raster_uploaded} wind/gust raster(s) to Snowflake stage "
                                f"(wind_raster//gust_raster/)")
            elif raster_files:
                logger.warning("SNOWFLAKE_STAGE_NAME not configured, wind/gust raster(s) not "
                                "uploaded to Snowflake stage")
                stats.upload_failed_filenames.update(f.name for f in raster_files)

            def _safe_table_count(cursor, table_name):
                """COUNT(*) for a diagnostic-only summary log line. Returns
                None (logged as 'unavailable') instead of raising on failure,
                this is a visibility check for the newer gust/precip
                tables, not load-bearing: a failure here (missing table,
                transient permission/network issue) must never fail the
                whole run or mask the wind/track data that already loaded
                and committed successfully earlier in this function."""
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    return cursor.fetchone()[0]
                except Exception as e:
                    logger.warning(f"Could not read diagnostic count for {table_name}: {e}")
                    return None

            def _fmt_count(n):
                return f"{n:,}" if n is not None else "unavailable"

            cursor = conn.cursor()
            try:
                _set_context()
                # TC_TRACKS/TC_ENVELOPES_* are the already-proven core wind
                # path, a failure reading their own counts stays a hard
                # error (matches this block's original behavior). Only the
                # newer gust/precip tables get the fault-tolerant treatment.
                cursor.execute("SELECT COUNT(*) FROM TC_TRACKS")
                tracks_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_INDIVIDUAL")
                individual_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_COMBINED")
                combined_count = cursor.fetchone()[0]
                gust_individual_count = _safe_table_count(cursor, "TC_GUST_ENVELOPES_INDIVIDUAL")
                gust_combined_count = _safe_table_count(cursor, "TC_GUST_ENVELOPES_COMBINED")
                met_count = _safe_table_count(cursor, "MET_FORECASTS")
            finally:
                cursor.close()

            logger.info("Total records in database:")
            logger.info(f"  TC_TRACKS:                     {tracks_count:,}")
            logger.info(f"  TC_ENVELOPES_INDIVIDUAL:       {individual_count:,}")
            logger.info(f"  TC_ENVELOPES_COMBINED:         {combined_count:,}")
            logger.info(f"  TC_GUST_ENVELOPES_INDIVIDUAL:  {_fmt_count(gust_individual_count)}")
            logger.info(f"  TC_GUST_ENVELOPES_COMBINED:    {_fmt_count(gust_combined_count)}")
            logger.info(f"  MET_FORECASTS:                 {_fmt_count(met_count)}")

            stats.files_loaded = len(transformed_files) + len(envelope_files) + raster_uploaded
            stats.rows_loaded = total_rows
            logger.info(f"Loaded {total_rows:,} rows from {stats.files_loaded} files")

        finally:
            conn.close()
            logger.info("Snowflake connection closed")

        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time("Phase 4: Snowflake Loading", duration)

    except Exception as e:
        error_msg = f"Phase 4 failed: {e}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Run the concurrent pipeline."""
    logger.info("=" * 70)
    logger.info("TC ECMWF FORECAST PIPELINE -- CONCURRENT EXECUTION")
    logger.info("=" * 70)
    logger.info(f"Start: {datetime.now().isoformat()}")

    config = PipelineConfig()
    stats = PipelineStats()

    if not config.validate():
        logger.error("Configuration validation failed. Exiting.")
        sys.exit(1)

    config.create_directories()

    pool_type = "ProcessPool (CPU-optimized)" if config.use_process_pool else "Sequential"
    if config.spcs_run:
        auth_method = f"SPCS OAuth ({config.spcs_token_path})"
    elif config.sf_private_key_path:
        auth_method = f"Private key ({config.sf_private_key_path})"
    else:
        auth_method = "Password"

    logger.info(
        f"Workers: {config.max_workers} ({pool_type}), "
        f"max concurrent downloads: {config.max_concurrent_downloads}"
    )
    logger.info(f"Snowflake: {config.sf_database}.{config.sf_schema} ({auth_method})")
    if os.getenv('SNOWFLAKE_INSECURE_MODE', 'false').lower() == 'true':
        logger.warning("SSL mode: INSECURE (certificate validation disabled)")
    if config.download_date:
        logger.info(f"Download: {config.download_date} {config.run_time or 'any'}Z")
    else:
        logger.info(f"Download: latest {config.download_limit} forecast(s)")

    try:
        phase_start = datetime.now()
        bufr_files = step1_download(config, stats)
        stats.files_downloaded_tc = stats.files_downloaded
        if not bufr_files:
            logger.warning("No TC files downloaded. Exiting.")
            sys.exit(0)

        csv_files = step2_extract(config, stats, bufr_files)
        if not csv_files:
            logger.warning("No named storms found in BUFR data -- skipping wind processing.")
            if config.process_met:
                logger.info("PROCESS_MET=true -- running met download anyway.")
                tc_data_info = extract_tc_data_info_from_bufr(bufr_files)
                _precip_conn = None
                if config.data_pipeline_db == 'SNOWFLAKE':
                    _precip_conn = _open_snowflake_conn(config)
                try:
                    precip_metadata = step6_download_precip(
                        config, stats, tc_data_info,
                        snowflake_conn=_precip_conn,
                        max_workers=config.max_concurrent_downloads,
                    )
                finally:
                    if _precip_conn:
                        _precip_conn.close()
                phase4_snowflake_loading(config, stats, [], [], precip_metadata)
                cleanup_files(config, stats.upload_failed_filenames)
            # No _local_mode bookkeeping needed here unlike github_actions/main.py's
            # own equivalent branch: this file's own PipelineStats.log_summary()
            # override (above) uses one plain, mode-agnostic "Files Loaded:" label,
            # it never branches on _local_mode at all.
            stats.log_summary()
            sys.exit(1 if stats.errors else 0)

        tc_data_info = extract_tc_data_info(csv_files)
        stats.log_phase_time("Phase 1: Download & Extract", (datetime.now() - phase_start).total_seconds())

        phase_start = datetime.now()
        transformed_files = step3_transform(
            config, stats, csv_files,
            use_process_pool=config.use_process_pool,
            max_workers=config.max_workers,
        )
        stats.log_phase_time("Phase 2: Transformation", (datetime.now() - phase_start).total_seconds())

        phase_start = datetime.now()
        step4_download_wind(config, stats, tc_data_info,
                            max_workers=config.max_concurrent_downloads)
        step4b_download_gust(config, stats, tc_data_info,
                             max_workers=config.max_concurrent_downloads)
        envelope_files, raster_files = step5_process_wind(
            config, stats,
            use_process_pool=config.use_process_pool,
            max_workers=config.max_workers,
        )
        gust_files, gust_raster_files = step5b_extract_gust_envelopes(config, stats)

        # Open a connection for the precip stage PUT (SNOWFLAKE mode only)
        _precip_conn = None
        if config.data_pipeline_db == 'SNOWFLAKE':
            _precip_conn = _open_snowflake_conn(config)

        try:
            precip_metadata = step6_download_precip(
                config, stats, tc_data_info,
                snowflake_conn=_precip_conn,
                max_workers=config.max_concurrent_downloads,
            )
        finally:
            if _precip_conn:
                _precip_conn.close()

        stats.log_phase_time("Phase 3: Wind, Envelopes & Precip", (datetime.now() - phase_start).total_seconds())

        phase4_snowflake_loading(config, stats, transformed_files, envelope_files + gust_files, precip_metadata,
                                  raster_files=raster_files + gust_raster_files)

        cleanup_files(config, stats.upload_failed_filenames)
        stats.log_summary()

        if stats.errors:
            logger.warning("Pipeline completed with errors")
            sys.exit(1)
        else:
            logger.info("Pipeline completed successfully!")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        stats.log_summary()
        sys.exit(1)


if __name__ == "__main__":
    main()