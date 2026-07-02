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

SPCS entry point — concurrent pipeline execution with SPCS OAuth / private-key auth.

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
    step5_process_wind,
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

        # SPCS-specific directory default (differs from github_actions default)
        if not os.getenv('TRANSFORMED_DATA_DIR'):
            self.transformed_data_dir = Path('tc_transformed')

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
        """Validate required configuration (supports SPCS OAuth, private key, or password auth)."""
        if self.data_pipeline_db == 'LOCAL':
            logger.info("DATA_PIPELINE_DB=LOCAL — Snowflake credentials not required")
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

        if self.process_met and not self.snowflake_stage_name:
            logger.error(
                "SNOWFLAKE_STAGE_NAME is required when PROCESS_MET=true and "
                "DATA_PIPELINE_DB=SNOWFLAKE. Add it to the SPCS service spec."
            )
            return False

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
                              precip_metadata: list = None):
    """Phase 4: Load all results to Snowflake, or skip if DATA_PIPELINE_DB=LOCAL."""
    logger.info("=" * 70)
    logger.info("PHASE 4: SNOWFLAKE LOADING")
    logger.info("=" * 70)
    phase_start = datetime.now()

    if config.data_pipeline_db == 'LOCAL':
        logger.info("DATA_PIPELINE_DB=LOCAL — skipping Snowflake load, files kept locally")
        logger.info(f"  Transformed tracks : {config.transformed_data_dir}")
        logger.info(f"  Wind envelopes     : {config.wind_extracted_dir}")
        logger.info(f"  Met ZipStores      : {config.met_data_dir}")
        stats.files_loaded = len(transformed_files) + len(envelope_files)
        stats.rows_loaded = 0
        stats._local_mode = True
        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time("Phase 4: Skip (LOCAL mode)", duration)
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
            _set_context()  # initial context — inside try/finally: conn.close()
            total_rows = 0
            for csv_file in transformed_files:
                _set_context()
                total_rows += load_csv_to_snowflake(csv_file, conn, table_type='TC_TRACKS')

            for csv_file in envelope_files:
                _set_context()
                if 'individual' in csv_file.name:
                    table_type = 'TC_ENVELOPES_INDIVIDUAL'
                elif 'combined' in csv_file.name:
                    table_type = 'TC_ENVELOPES_COMBINED'
                else:
                    logger.warning(f"Unknown envelope file type: {csv_file.name}")
                    continue
                total_rows += load_csv_to_snowflake(csv_file, conn, table_type=table_type)

            if precip_metadata:
                _set_context()
                total_rows += load_precip_metadata_to_snowflake(precip_metadata, conn)

            cursor = conn.cursor()
            try:
                _set_context()
                cursor.execute("SELECT COUNT(*) FROM TC_TRACKS")
                tracks_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_INDIVIDUAL")
                individual_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_COMBINED")
                combined_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM MET_FORECASTS")
                met_count = cursor.fetchone()[0]
            finally:
                cursor.close()

            logger.info("Total records in database:")
            logger.info(f"  TC_TRACKS:                {tracks_count:,}")
            logger.info(f"  TC_ENVELOPES_INDIVIDUAL:  {individual_count:,}")
            logger.info(f"  TC_ENVELOPES_COMBINED:    {combined_count:,}")
            logger.info(f"  MET_FORECASTS:         {met_count:,}")

            stats.files_loaded = len(transformed_files) + len(envelope_files)
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
    logger.info("TC ECMWF FORECAST PIPELINE — CONCURRENT EXECUTION")
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
            logger.warning("No named storms found in BUFR data — skipping wind processing.")
            if config.process_met:
                logger.info("PROCESS_MET=true — running met download anyway.")
                tc_data_info = extract_tc_data_info_from_bufr(bufr_files)
                _precip_conn = None
                if config.data_pipeline_db == 'SNOWFLAKE':
                    _precip_conn = _open_snowflake_conn(config)
                try:
                    precip_metadata = step6_download_precip(
                        config, stats, tc_data_info, [],
                        snowflake_conn=_precip_conn,
                        max_workers=config.max_concurrent_downloads,
                    )
                finally:
                    if _precip_conn:
                        _precip_conn.close()
                phase4_snowflake_loading(config, stats, [], [], precip_metadata)
                cleanup_files(config)
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
        envelope_files = step5_process_wind(
            config, stats,
            use_process_pool=config.use_process_pool,
            max_workers=config.max_workers,
        )
        storm_ids = [f.stem.split('_storm_')[-1].split('_extracted')[0]
                     for f in csv_files if '_storm_' in f.stem]

        # Open a connection for the precip stage PUT (SNOWFLAKE mode only)
        _precip_conn = None
        if config.data_pipeline_db == 'SNOWFLAKE':
            _precip_conn = _open_snowflake_conn(config)

        try:
            precip_metadata = step6_download_precip(
                config, stats, tc_data_info, storm_ids,
                snowflake_conn=_precip_conn,
                max_workers=config.max_concurrent_downloads,
            )
        finally:
            if _precip_conn:
                _precip_conn.close()

        stats.log_phase_time("Phase 3: Wind, Envelopes & Precip", (datetime.now() - phase_start).total_seconds())

        phase4_snowflake_loading(config, stats, transformed_files, envelope_files, precip_metadata)

        cleanup_files(config)
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