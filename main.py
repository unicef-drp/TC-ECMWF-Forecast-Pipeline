#!/usr/bin/env python3
"""
ECMWF TC Forecast Pipeline
Automates: Download → Extract → Transform → Wind Download → Wind Processing → Load to Snowflake

The pipeline downloads wind forecast data to match the tropical cyclone forecast run time.
Wind forecast hours are every 6 hours from 0 to 144 hours (0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60,
66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144).

The wind data download automatically matches:
- The same forecast date as the TC data
- The same run time (00Z, 06Z, 12Z, 18Z) as the TC data
- All ensemble members (50 members)
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict
import pandas as pd

from ecmwf_tc_data_downloader import download_tc_data
from ecmwf_tc_data_extractor import extract_tc_data
from ecmwf_tc_data_transformer import transform_tc_data
from ecmwf_wind_data_downloader import download_ensemble_wind
from ecmwf_tc_wind_combination import process_wind_combination, analyze_required_forecast_hours
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
        self.wind_data_dir = Path(os.getenv('WIND_DATA_DIR', 'wind_data'))
        self.wind_extracted_dir = Path(os.getenv('WIND_EXTRACTED_DIR', 'wind_extracted'))

        # Snowflake credentials (from GitHub Secrets)
        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'TC_WH')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE', 'AOTS')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

        # Pipeline options
        self.cleanup_after_load = os.getenv('CLEANUP_AFTER_LOAD', 'true').lower() == 'true'
        self.skip_existing = os.getenv('SKIP_EXISTING', 'true').lower() == 'true'

        # Download options
        self.download_date = os.getenv('DOWNLOAD_DATE')  # e.g., "20250929"
        self.download_limit = int(os.getenv('DOWNLOAD_LIMIT', '1'))

        # Wind processing options
        self.process_wind_data = os.getenv('PROCESS_WIND_DATA', 'true').lower() == 'true'
        self.max_ensemble_members = 50  # Fixed: All ensemble members

        # Wind forecast configuration - Every 6 hours from 0 to 144 hours
        self.wind_forecast_hours = list(range(0, 145,
                                              6))  # [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144]

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
        self.wind_data_dir.mkdir(parents=True, exist_ok=True)
        self.wind_extracted_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"Created directories: {self.raw_data_dir}, {self.transformed_data_dir}, {self.wind_data_dir}, {self.wind_extracted_dir}")


class PipelineStats:
    """Track pipeline statistics"""

    def __init__(self):
        self.start_time = datetime.now()
        self.files_downloaded = 0
        self.files_extracted = 0
        self.files_transformed = 0
        self.files_wind_downloaded = 0
        self.files_wind_processed = 0
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
        logger.info(f"Wind Files Downloaded: {self.files_wind_downloaded}")
        logger.info(f"Wind Files Processed: {self.files_wind_processed}")
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


def extract_tc_data_info(csv_files: List[Path]) -> Dict:
    """Extract run time and date information from TC data files"""
    if not csv_files:
        return {}

    try:
        # Read the first CSV file to get forecast metadata
        first_csv = csv_files[0]
        df = pd.read_csv(first_csv)

        if df.empty:
            return {}

        # Get datetime from the first row (this is the valid time)
        datetime_str = df['datetime'].iloc[0]
        valid_time = pd.to_datetime(datetime_str)

        # Get step from the first row
        step = df['step'].iloc[0]

        # Calculate forecast time: valid_time - step hours
        forecast_time = valid_time - pd.Timedelta(hours=int(step))

        # Extract run time (hour) and date
        run_time = forecast_time.hour
        date = forecast_time.strftime('%Y-%m-%d')

        logger.info(f"Extracted TC data info: {date} {run_time:02d}Z (from valid_time: {valid_time}, step: {step}h)")

        return {
            'run_time': run_time,
            'date': date,
            'forecast_time': forecast_time.strftime('%Y-%m-%d %H:%M:%S')
        }

    except Exception as e:
        logger.warning(f"Could not extract TC data info: {e}")
        return {}


def step3_transform(config: PipelineConfig, stats: PipelineStats, csv_files: List[Path]) -> List[Path]:
    """Step 3: Transform CSV files for Snowflake"""
    logger.info("=" * 70)
    logger.info("STEP 3: Transforming CSV files...")
    logger.info("=" * 70)

    transformed_files = []

    for csv_file in csv_files:
        try:
            # Create output file path (without extension first)
            output_base = config.transformed_data_dir / f"transformed_{csv_file.stem}"
            output_file = output_base.with_suffix('.csv')

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
                str(output_base),  # Pass base path without extension
                storm_name=storm_name,
                verbose=False
            )

            if result['success']:
                # Use the actual file path returned by the transformer
                actual_file = Path(result.get('csv_file', str(output_file)))
                transformed_files.append(actual_file)
                stats.files_transformed += 1
                logger.info(f"  Transformed {result['records']} records")
                logger.info(f"  Saved to: {actual_file.name}")
            else:
                logger.error(f"  Failed to transform {csv_file.name}")

        except Exception as e:
            error_msg = f"Transformation failed for {csv_file.name}: {str(e)}"
            logger.error(error_msg)
            stats.errors.append(error_msg)
            # Continue with other files

    logger.info(f"Transformed {stats.files_transformed} files")

    # Debug: List actual files in transformed directory
    actual_files = list(config.transformed_data_dir.glob("*.csv"))
    logger.info(f"Actual files in {config.transformed_data_dir}: {[f.name for f in actual_files]}")

    return transformed_files


def step4_download_wind(config: PipelineConfig, stats: PipelineStats, tc_data_info: Dict,
                        transformed_files: List[Path]) -> List[Path]:
    """Step 4: Download wind forecast data matching TC forecast run time"""
    if not config.process_wind_data:
        logger.info("Wind data processing disabled, skipping wind download")
        return []

    logger.info("=" * 70)
    logger.info("STEP 4: Downloading wind forecast data...")
    logger.info("=" * 70)

    try:
        # Extract run time from TC data
        tc_run_time = tc_data_info.get('run_time')
        tc_date = tc_data_info.get('date')

        if tc_run_time is None or tc_date is None:
            logger.warning("Could not determine TC run time or date, skipping wind download")
            return []

        # Analyze TC data to determine required forecast hours using combination script
        max_forecast_hour = analyze_required_forecast_hours(config.transformed_data_dir, verbose=True)

        # Generate forecast hours list (every 6 hours from 0 to max_hour)
        required_forecast_hours = list(range(0, max_forecast_hour + 1, 6))

        logger.info(f"TC forecast run time: {tc_run_time:02d}Z")
        logger.info(f"TC forecast date: {tc_date}")
        logger.info(f"Required wind forecast hours: {required_forecast_hours}")
        logger.info(
            f"Total wind files to download: {len(required_forecast_hours)} (vs {len(config.wind_forecast_hours)} if using full range)")

        # Download wind data for the same run time as TC data
        result = download_ensemble_wind(
            date=tc_date,
            run_time=tc_run_time,
            forecast_hours=required_forecast_hours,
            output_dir=str(config.wind_data_dir),
            verbose=False
        )

        if result['success']:
            stats.files_wind_downloaded = result['files_downloaded']
            logger.info(f"Downloaded {stats.files_wind_downloaded} wind files")
            return result['downloaded_files']
        else:
            logger.warning(f"Wind download failed: {result.get('error', 'Unknown error')}")
            return []

    except Exception as e:
        error_msg = f"Wind download failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        return []


def step5_process_wind(config: PipelineConfig, stats: PipelineStats, wind_files: List[Path]) -> List[Path]:
    """Step 5: Process wind data with TC tracks"""
    if not config.process_wind_data or not wind_files:
        logger.info("Wind data processing disabled or no wind files, skipping wind processing")
        return []

    logger.info("=" * 70)
    logger.info("STEP 5: Processing wind data with TC tracks...")
    logger.info("=" * 70)

    try:
        # Process wind combination
        result = process_wind_combination(
            tc_data_dir=config.transformed_data_dir,
            wind_data_dir=config.wind_data_dir,
            output_dir=config.wind_extracted_dir,
            buffer_radius_km=500,
            max_ensemble_members=config.max_ensemble_members,
            verbose=False
        )

        if result['processed_storms'] > 0:
            stats.files_wind_processed = result['processed_storms']
            logger.info(f"Processed wind data for {result['processed_storms']} storms")

            # Get list of generated envelope files
            envelope_files = list(config.wind_extracted_dir.glob("*_envelopes_*.csv"))
            logger.info(f"Found envelope files: {[f.name for f in envelope_files]}")
            return envelope_files
        else:
            logger.warning("No wind envelope files generated")
            return []

    except Exception as e:
        error_msg = f"Wind processing failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        return []


def step6_load(config: PipelineConfig, stats: PipelineStats, transformed_files: List[Path], envelope_files: List[Path]):
    """Step 6: Load transformed data to Snowflake"""
    logger.info("=" * 70)
    logger.info("STEP 6: Loading data to Snowflake...")
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
            # Load TC track data
            total_rows = 0
            for csv_file in transformed_files:
                rows = load_csv_to_snowflake(csv_file, conn, table_type='TC_TRACKS')
                total_rows += rows

            # Load envelope data
            for csv_file in envelope_files:
                if 'individual' in csv_file.name:
                    rows = load_csv_to_snowflake(csv_file, conn, table_type='TC_ENVELOPES_INDIVIDUAL')
                elif 'combined' in csv_file.name:
                    rows = load_csv_to_snowflake(csv_file, conn, table_type='TC_ENVELOPES_COMBINED')
                else:
                    logger.warning(f"Unknown envelope file type: {csv_file.name}")
                    continue
                total_rows += rows

            # Verify final counts
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM TC_TRACKS")
            tracks_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_INDIVIDUAL")
            individual_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_COMBINED")
            combined_count = cursor.fetchone()[0]
            cursor.close()

            logger.info(f"Total records in database:")
            logger.info(f"  TC_TRACKS: {tracks_count:,}")
            logger.info(f"  TC_ENVELOPES_INDIVIDUAL: {individual_count:,}")
            logger.info(f"  TC_ENVELOPES_COMBINED: {combined_count:,}")

            stats.files_loaded = len(transformed_files) + len(envelope_files)
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

        # Remove wind data files
        for wind_file in config.wind_data_dir.glob("*.grib2"):
            wind_file.unlink()
            removed_count += 1

        # Remove wind envelope files
        for envelope_file in config.wind_extracted_dir.glob("*.csv"):
            envelope_file.unlink()
            removed_count += 1

        logger.info(f"Removed {removed_count} temporary files")

    except Exception as e:
        logger.warning(f"Cleanup failed (non-critical): {str(e)}")


def main():
    """Main pipeline execution"""
    logger.info("=" * 70)
    logger.info("ECMWF TC FORECAST PIPELINE")
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

    # Log configuration
    logger.info(f"Configuration:")
    logger.info(f"  Raw data directory: {config.raw_data_dir}")
    logger.info(f"  Transformed data directory: {config.transformed_data_dir}")
    logger.info(f"  Wind data directory: {config.wind_data_dir}")
    logger.info(f"  Wind extracted directory: {config.wind_extracted_dir}")
    logger.info(f"  Snowflake database: {config.sf_database}.{config.sf_schema}")
    logger.info(f"  Process wind data: {config.process_wind_data}")
    logger.info(f"  Ensemble members: {config.max_ensemble_members}")
    logger.info(f"  Wind forecast hours: {config.wind_forecast_hours}")
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

        # Extract TC data info for wind download
        tc_data_info = extract_tc_data_info(csv_files)
        logger.info(f"TC data info extracted: {tc_data_info}")
        wind_files = step4_download_wind(config, stats, tc_data_info, transformed_files)
        envelope_files = step5_process_wind(config, stats, wind_files)
        step6_load(config, stats, transformed_files, envelope_files)

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