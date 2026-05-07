#!/usr/bin/env python3
"""
GitHub Actions entry point — sequential pipeline execution with password auth.

Orchestrates steps 1–5 from pipeline_core, then loads the results to Snowflake
using the github_actions/snowflake_loader (password-based connection).
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List

# Add parent directory to path to import core modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline_core import (
    BasePipelineConfig,
    PipelineStats,
    extract_tc_data_info,
    step1_download,
    step2_extract,
    step3_transform,
    step4_download_wind,
    step5_process_wind,
    cleanup_files,
)
from snowflake_loader import get_snowflake_connection, load_csv_to_snowflake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log'),
    ],
)
logger = logging.getLogger(__name__)


class PipelineConfig(BasePipelineConfig):
    """Configuration for the GitHub Actions pipeline (password auth)."""


def step6_load(config: PipelineConfig, stats: PipelineStats,
               transformed_files: List[Path], envelope_files: List[Path]):
    """Step 6: Load transformed data to Snowflake, or skip if DATA_PIPELINE_DB=LOCAL."""
    logger.info("=" * 70)
    logger.info("STEP 6: Loading data...")
    logger.info("=" * 70)

    if config.data_pipeline_db == 'LOCAL':
        logger.info("DATA_PIPELINE_DB=LOCAL — skipping Snowflake load, files kept locally")
        logger.info(f"  Transformed tracks : {config.transformed_data_dir}")
        logger.info(f"  Wind envelopes     : {config.wind_extracted_dir}")
        stats.files_loaded = len(transformed_files) + len(envelope_files)
        stats.rows_loaded = 0
        return

    try:
        os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
        os.environ['SNOWFLAKE_USER'] = config.sf_user
        os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
        os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
        os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
        os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema

        conn = get_snowflake_connection()
        try:
            total_rows = 0

            for csv_file in transformed_files:
                total_rows += load_csv_to_snowflake(csv_file, conn, table_type='TC_TRACKS')

            for csv_file in envelope_files:
                if 'individual' in csv_file.name:
                    table_type = 'TC_ENVELOPES_INDIVIDUAL'
                elif 'combined' in csv_file.name:
                    table_type = 'TC_ENVELOPES_COMBINED'
                else:
                    logger.warning(f"Unknown envelope file type: {csv_file.name}")
                    continue
                total_rows += load_csv_to_snowflake(csv_file, conn, table_type=table_type)

            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM TC_TRACKS")
            tracks_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_INDIVIDUAL")
            individual_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_COMBINED")
            combined_count = cursor.fetchone()[0]
            cursor.close()

            logger.info("Total records in database:")
            logger.info(f"  TC_TRACKS:                {tracks_count:,}")
            logger.info(f"  TC_ENVELOPES_INDIVIDUAL:  {individual_count:,}")
            logger.info(f"  TC_ENVELOPES_COMBINED:    {combined_count:,}")

            stats.files_loaded = len(transformed_files) + len(envelope_files)
            stats.rows_loaded = total_rows
            logger.info(f"Loaded {total_rows:,} rows from {stats.files_loaded} files")

        finally:
            conn.close()
            logger.info("Snowflake connection closed")

    except Exception as e:
        error_msg = f"Snowflake load failed: {e}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def main():
    """Run the sequential pipeline."""
    logger.info("=" * 70)
    logger.info("ECMWF TC FORECAST PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Start: {datetime.now().isoformat()}")

    config = PipelineConfig()
    stats = PipelineStats()

    if not config.validate():
        logger.error("Configuration validation failed. Exiting.")
        sys.exit(1)

    config.create_directories()
    logger.info(
        f"Dirs: raw={config.raw_data_dir}, "
        f"transformed={config.transformed_data_dir}, "
        f"wind={config.wind_data_dir}, "
        f"envelopes={config.wind_extracted_dir}"
    )
    logger.info(f"Snowflake: {config.sf_database}.{config.sf_schema}")
    logger.info(f"Wind processing: {config.process_wind_data}")
    if config.download_date:
        logger.info(f"Download date: {config.download_date} {config.run_time or 'any'}Z")
    else:
        logger.info(f"Download mode: latest {config.download_limit} forecast(s)")

    try:
        bufr_files = step1_download(config, stats)
        if not bufr_files:
            logger.warning("No BUFR files to process. Exiting.")
            sys.exit(0)

        csv_files = step2_extract(config, stats, bufr_files)
        transformed_files = step3_transform(config, stats, csv_files)

        tc_data_info = extract_tc_data_info(csv_files)
        logger.info(f"TC data info: {tc_data_info}")

        step4_download_wind(config, stats, tc_data_info)
        envelope_files = step5_process_wind(config, stats)
        step6_load(config, stats, transformed_files, envelope_files)

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
