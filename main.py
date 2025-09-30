#!/usr/bin/env python3
"""
ECMWF TC Forecast Pipeline
Automates: Download → Extract → Transform → Load to Snowflake
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Import our pipeline modules
from ecmwf_tc_data_downloader import download_tc_data
from ecmwf_tc_data_extractor import extract_tc_data
from ecmwf_tc_data_transformer import transform_tc_data
from snowflake_loader import get_snowflake_connection, load_csv_to_snowflake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class PipelineConfig:
    """Configuration for the pipeline from environment variables"""

    def __init__(self):
        # Directories
        self.raw_data_dir = Path(os.getenv('RAW_DATA_DIR', 'tc_data'))
        self.transformed_data_dir = Path(os.getenv('TRANSFORMED_DATA_DIR', 'tc_data_transformed'))

        # Snowflake credentials (from GitHub Secrets)
        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'TC_WH')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE', 'TC_FORECASTS')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

        # Pipeline options
        self.cleanup_after_load = os.getenv('CLEANUP_AFTER_LOAD', 'true').lower() == 'true'
        self.skip_existing = os.getenv('SKIP_EXISTING', 'true').lower() == 'true'

        # Download options
        self.download_date = os.getenv('DOWNLOAD_DATE')  # e.g., "20250929"
        self.download_limit = int(os.getenv('DOWNLOAD_LIMIT', '1'))

    def validate(self) -> bool:
        """Validate that all required config is present"""
        missing = []

        if not self.sf_account:
            missing.append('SNOWFLAKE_ACCOUNT')
        if not self.sf_user:
            missing.append('SNOWFLAKE_USER')
        if not self.sf_password:
            missing.append('SNOWFLAKE_PASSWORD')

        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            return False

        return True

    def create_directories(self):
        """Ensure all required directories exist"""
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.transformed_data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directories: {self.raw_data_dir}, {self.transformed_data_dir}")


class PipelineStats:
    """Track pipeline statistics"""

    def __init__(self):
        self.start_time = datetime.now()
        self.files_downloaded = 0
        self.files_extracted = 0
        self.files_transformed = 0
        self.files_loaded = 0
        self.rows_loaded = 0
        self.errors = []

    def log_summary(self):
        """Log pipeline execution summary"""
        duration = (datetime.now() - self.start_time).total_seconds()

        logger.info("=" * 70)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Duration: {duration:.2f} seconds ({duration / 60:.1f} minutes)")
        logger.info(f"Files Downloaded: {self.files_downloaded}")
        logger.info(f"Files Extracted: {self.files_extracted}")
        logger.info(f"Files Transformed: {self.files_transformed}")
        logger.info(f"Files Loaded to Snowflake: {self.files_loaded}")
        logger.info(f"Total Rows Loaded: {self.rows_loaded:,}")

        if self.errors:
            logger.error(f"Errors encountered: {len(self.errors)}")
            for error in self.errors:
                logger.error(f"  - {error}")
        else:
            logger.info("No errors encountered")

        logger.info("=" * 70)


def step1_download(config: PipelineConfig, stats: PipelineStats) -> List[Path]:
    """Step 1: Download BUFR files from ECMWF"""
    logger.info("=" * 70)
    logger.info("STEP 1: Downloading TC forecast files...")
    logger.info("=" * 70)

    try:
        download_kwargs = {
            'output_dir': str(config.raw_data_dir)
        }

        if config.download_date:
            logger.info(f"Downloading for specific date: {config.download_date}")
            download_kwargs['date'] = config.download_date
        else:
            logger.info(f"Downloading latest {config.download_limit} forecast(s)")
            download_kwargs['limit'] = config.download_limit

        result = download_tc_data(**download_kwargs)

        stats.files_downloaded = result['downloaded']
        logger.info(f"Downloaded {stats.files_downloaded} files")

        # Get list of downloaded BUFR files
        bufr_files = list(config.raw_data_dir.glob("*.bin"))
        return bufr_files

    except Exception as e:
        error_msg = f"Download failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def step2_extract(config: PipelineConfig, stats: PipelineStats, bufr_files: List[Path]) -> List[Path]:
    """Step 2: Extract BUFR files to CSV"""
    logger.info("=" * 70)
    logger.info("STEP 2: Extracting BUFR files to CSV...")
    logger.info("=" * 70)

    extracted_files = []

    for bufr_file in bufr_files:
        try:
            csv_file = config.raw_data_dir / f"{bufr_file.stem}.csv"

            # Skip if already extracted
            if csv_file.exists() and config.skip_existing:
                logger.info(f"Skipping already extracted: {csv_file.name}")
                extracted_files.append(csv_file)
                continue

            logger.info(f"Extracting: {bufr_file.name}")

            # Extract data
            df = extract_tc_data(str(bufr_file), verbose=False)

            if df.empty:
                logger.warning(f"  No data extracted from {bufr_file.name}")
                continue

            # Save to CSV
            df.to_csv(csv_file, index=False)

            extracted_files.append(csv_file)
            stats.files_extracted += 1

            logger.info(f"  Extracted {len(df)} records to {csv_file.name}")

        except Exception as e:
            error_msg = f"Extraction failed for {bufr_file.name}: {str(e)}"
            logger.error(error_msg)
            stats.errors.append(error_msg)
            # Continue with other files

    logger.info(f"Extracted {stats.files_extracted} files")
    return extracted_files


def step3_transform(config: PipelineConfig, stats: PipelineStats, csv_files: List[Path]) -> List[Path]:
    """Step 3: Transform CSV files for Snowflake"""
    logger.info("=" * 70)
    logger.info("STEP 3: Transforming CSV files...")
    logger.info("=" * 70)

    transformed_files = []

    for csv_file in csv_files:
        try:
            output_file = config.transformed_data_dir / f"transformed_{csv_file.name}"

            # Skip if already transformed
            if output_file.exists() and config.skip_existing:
                logger.info(f"Skipping already transformed: {output_file.name}")
                transformed_files.append(output_file)
                continue

            logger.info(f"Transforming: {csv_file.name}")

            # Extract storm name from filename if possible
            import re
            match = re.search(r'tropical_cyclone_track_([A-Z0-9]+)', csv_file.stem)
            storm_name = match.group(1) if match else None

            # Transform data
            result = transform_tc_data(
                str(csv_file),
                str(output_file),
                storm_name=storm_name,
                verbose=False
            )

            if result['success']:
                transformed_files.append(output_file)
                stats.files_transformed += 1
                logger.info(f"  Transformed {result['records']} records")
            else:
                logger.error(f"  Failed to transform {csv_file.name}")

        except Exception as e:
            error_msg = f"Transformation failed for {csv_file.name}: {str(e)}"
            logger.error(error_msg)
            stats.errors.append(error_msg)
            # Continue with other files

    logger.info(f"Transformed {stats.files_transformed} files")
    return transformed_files


def step4_load(config: PipelineConfig, stats: PipelineStats, transformed_files: List[Path]):
    """Step 4: Load transformed data to Snowflake"""
    logger.info("=" * 70)
    logger.info("STEP 4: Loading data to Snowflake...")
    logger.info("=" * 70)

    try:
        # Set environment variables for get_snowflake_connection()
        os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
        os.environ['SNOWFLAKE_USER'] = config.sf_user
        os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
        os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
        os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
        os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema

        # Connect to Snowflake
        conn = get_snowflake_connection()

        try:
            # Load each CSV file
            total_rows = 0
            for csv_file in transformed_files:
                rows = load_csv_to_snowflake(csv_file, conn)
                total_rows += rows

            # Verify final count
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM TC_FORECASTS")
            total_in_db = cursor.fetchone()[0]
            cursor.close()

            logger.info(f"Total records in database: {total_in_db:,}")

            stats.files_loaded = len(transformed_files)
            stats.rows_loaded = total_rows
            logger.info(f"Loaded {total_rows:,} rows from {stats.files_loaded} files")

        finally:
            conn.close()
            logger.info("Connection closed")

    except Exception as e:
        error_msg = f"Snowflake load failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def cleanup_files(config: PipelineConfig):
    """Clean up temporary files after successful load"""
    if not config.cleanup_after_load:
        logger.info("Cleanup skipped (CLEANUP_AFTER_LOAD=false)")
        return

    logger.info("=" * 70)
    logger.info("Cleaning up temporary files...")
    logger.info("=" * 70)

    try:
        removed_count = 0

        # Remove raw BUFR files
        for bufr_file in config.raw_data_dir.glob("*.bin"):
            bufr_file.unlink()
            removed_count += 1

        # Remove extracted CSVs
        for csv_file in config.raw_data_dir.glob("*.csv"):
            csv_file.unlink()
            removed_count += 1

        # Remove transformed CSVs
        for csv_file in config.transformed_data_dir.glob("*.csv"):
            csv_file.unlink()
            removed_count += 1

        logger.info(f"Removed {removed_count} temporary files")

    except Exception as e:
        logger.warning(f"Cleanup failed (non-critical): {str(e)}")


def main():
    """Main pipeline execution"""
    logger.info("=" * 70)
    logger.info("ECMWF TC FORECAST PIPELINE - PRODUCTION")
    logger.info("=" * 70)
    logger.info(f"Pipeline start time: {datetime.now().isoformat()}")

    # Initialize configuration and stats
    config = PipelineConfig()
    stats = PipelineStats()

    # Validate configuration
    if not config.validate():
        logger.error("Configuration validation failed. Exiting.")
        sys.exit(1)

    # Create directories
    config.create_directories()

    # Log configuration (without sensitive data)
    logger.info(f"Configuration:")
    logger.info(f"  Raw data directory: {config.raw_data_dir}")
    logger.info(f"  Transformed data directory: {config.transformed_data_dir}")
    logger.info(f"  Snowflake database: {config.sf_database}.{config.sf_schema}")
    logger.info(f"  Cleanup after load: {config.cleanup_after_load}")
    logger.info(f"  Skip existing files: {config.skip_existing}")

    try:
        # Execute pipeline steps
        bufr_files = step1_download(config, stats)

        if not bufr_files:
            logger.warning("No BUFR files to process. Exiting.")
            sys.exit(0)

        csv_files = step2_extract(config, stats, bufr_files)
        transformed_files = step3_transform(config, stats, csv_files)
        step4_load(config, stats, transformed_files)

        # Clean up if successful
        cleanup_files(config)

        # Log summary
        stats.log_summary()

        # Exit with appropriate code
        if stats.errors:
            logger.warning("Pipeline completed with errors")
            sys.exit(1)
        else:
            logger.info("Pipeline completed successfully!")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        stats.log_summary()
        sys.exit(1)


if __name__ == "__main__":
    main()