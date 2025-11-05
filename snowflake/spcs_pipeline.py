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

UNICEF Snowflake Pipeline - Concurrent Execution
Optimized pipeline that executes downloads and processing concurrently for maximum performance.

Execution Flow:
1. Phase 1: Download TC data and Wind data CONCURRENTLY
2. Phase 2: Extract and Transform TC data CONCURRENTLY (after TC download completes)
3. Phase 3: Process wind combination (after TC transformation completes)
4. Phase 4: Load all data to Snowflake

This program uses concurrent.futures to maximize download and processing performance
while respecting data dependencies.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, wait
import pandas as pd
import multiprocessing

# Add parent directory to path to import core modules
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

# Import pipeline modules from repo root
from ecmwf_tc_data_downloader import download_tc_data
from ecmwf_tc_data_extractor import extract_tc_data
from ecmwf_tc_data_transformer import transform_tc_data
from ecmwf_wind_data_downloader import download_ensemble_wind
from ecmwf_tc_wind_combination import (
    process_wind_combination, 
    analyze_required_forecast_hours
)
from snowflake.snowflake_loader import (
    get_snowflake_connection, 
    load_csv_to_snowflake
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('unicef_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class PipelineConfig:
    """Configuration for the concurrent pipeline"""
    
    def __init__(self):
        # Directories
        self.raw_data_dir = Path(os.getenv('RAW_DATA_DIR', 'tc_data'))
        self.transformed_data_dir = Path(os.getenv('TRANSFORMED_DATA_DIR', 'tc_transformed'))
        self.wind_data_dir = Path(os.getenv('WIND_DATA_DIR', 'wind_data'))
        self.wind_extracted_dir = Path(os.getenv('WIND_EXTRACTED_DIR', 'wind_extracted'))
        
        # Snowflake credentials
        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        self.sf_private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'MY_WH')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE', 'MY_DB')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        
        # SPCS (Snowflake Container Services) OAuth mode
        self.spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
        self.spcs_token_path = os.getenv('SPCS_TOKEN_PATH', '/snowflake/session/token')
        
        # Pipeline options
        self.cleanup_after_load = os.getenv('CLEANUP_AFTER_LOAD', 'true').lower() == 'true'
        self.skip_existing = os.getenv('SKIP_EXISTING', 'true').lower() == 'true'
        
        # Download options
        self.download_date = os.getenv('DOWNLOAD_DATE')
        self.run_time = os.getenv('RUN_TIME')
        self.download_limit = int(os.getenv('DOWNLOAD_LIMIT', '1'))
        
        # Wind processing options
        self.process_wind_data = os.getenv('PROCESS_WIND_DATA', 'true').lower() == 'true'
        self.max_ensemble_members = 50
        
        # Storm filtering options
        self.named_storms_only = os.getenv('NAMED_STORMS_ONLY', 'true').lower() == 'true'
        
        # Concurrency settings
        self.max_workers = int(os.getenv('MAX_WORKERS', '0'))
        
        # Auto-detect CPU count if MAX_WORKERS is 0
        if self.max_workers == 0:
            self.max_workers = multiprocessing.cpu_count()
            logger.info(f"MAX_WORKERS set to 0, auto-detected {self.max_workers} CPUs")
        
        # Download concurrency (capped at 10 to avoid overwhelming servers)
        max_concurrent_downloads_env = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '0'))
        if max_concurrent_downloads_env == 0:
            # Default to MAX_WORKERS if not specified
            self.max_concurrent_downloads = min(self.max_workers, 10)
        else:
            # Use specified value but cap at 10
            self.max_concurrent_downloads = min(max_concurrent_downloads_env, 10)
        
        # Use ProcessPoolExecutor for CPU-intensive tasks (extraction/transformation)
        # Use ThreadPoolExecutor for I/O-intensive tasks (downloads)
        self.use_process_pool = os.getenv('USE_PROCESS_POOL', 'true').lower() == 'true'
    
    def validate(self) -> bool:
        """Validate required configuration"""
        missing = []
        
        if not self.sf_account:
            missing.append('SNOWFLAKE_ACCOUNT')
        
        # Check for authentication method (SPCS OAuth, password, or private key)
        if self.spcs_run:
            # SPCS mode: validate token file exists
            # Note: SNOWFLAKE_USER not required in SPCS mode (OAuth handles identity)
            token_file = Path(self.spcs_token_path)
            if not token_file.exists():
                logger.error(f"SPCS token file not found: {self.spcs_token_path}")
                return False
            if not token_file.is_file():
                logger.error(f"SPCS token path is not a file: {self.spcs_token_path}")
                return False
        else:
            # Non-SPCS mode: require user, and either password or private key
            if not self.sf_user:
                missing.append('SNOWFLAKE_USER')
            
            if not self.sf_password and not self.sf_private_key_path:
                logger.error("Missing authentication: Set either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
                return False
            
            # If using private key, validate the file exists
            if self.sf_private_key_path:
                private_key_file = Path(self.sf_private_key_path)
                if not private_key_file.exists():
                    logger.error(f"Private key file not found: {self.sf_private_key_path}")
                    return False
                if not private_key_file.is_file():
                    logger.error(f"Private key path is not a file: {self.sf_private_key_path}")
                    return False
        
        # RUN_TIME is optional when DOWNLOAD_DATE is specified
        # If not specified, all run times for that date will be downloaded
        if self.run_time and self.run_time not in ['00', '06', '12', '18']:
            logger.error(f"Invalid RUN_TIME: {self.run_time}. Must be 00, 06, 12, or 18")
            return False
        
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
        logger.info(f"Created directories: {self.raw_data_dir}, {self.transformed_data_dir}, "
                   f"{self.wind_data_dir}, {self.wind_extracted_dir}")


class PipelineStats:
    """Track pipeline statistics"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.phase_times = {}
        self.files_downloaded_tc = 0
        self.files_downloaded_wind = 0
        self.files_extracted = 0
        self.files_transformed = 0
        self.files_wind_processed = 0
        self.files_loaded = 0
        self.rows_loaded = 0
        self.errors = []
    
    def log_phase_time(self, phase_name: str, duration: float):
        """Log time taken for a phase"""
        self.phase_times[phase_name] = duration
        logger.info(f"⏱️  {phase_name} completed in {duration:.2f} seconds ({duration/60:.1f} minutes)")
    
    def log_summary(self):
        """Log pipeline execution summary"""
        total_duration = (datetime.now() - self.start_time).total_seconds()
        
        logger.info("=" * 70)
        logger.info("CONCURRENT PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total Duration: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
        logger.info("")
        logger.info("Phase Timings:")
        for phase, duration in self.phase_times.items():
            logger.info(f"  {phase}: {duration:.2f}s ({duration/60:.1f}m)")
        logger.info("")
        logger.info("Files Processed:")
        logger.info(f"  TC Files Downloaded: {self.files_downloaded_tc}")
        logger.info(f"  Wind Files Downloaded: {self.files_downloaded_wind}")
        logger.info(f"  Files Extracted: {self.files_extracted}")
        logger.info(f"  Files Transformed: {self.files_transformed}")
        logger.info(f"  Wind Files Processed: {self.files_wind_processed}")
        logger.info(f"  Files Loaded to Snowflake: {self.files_loaded}")
        logger.info(f"  Total Rows Loaded: {self.rows_loaded:,}")
        
        if self.errors:
            logger.error(f"\nErrors encountered: {len(self.errors)}")
            for error in self.errors:
                logger.error(f"  - {error}")
        else:
            logger.info("\n✓ No errors encountered")
        
        logger.info("=" * 70)


def is_named_storm(storm_name: str) -> bool:
    """
    Check if a storm is a "named storm" based on its name.
    
    A named storm contains multiple letters and no numbers.
    Examples:
        - "MELISSA" -> True (named storm)
        - "CHENGE" -> True (named storm)  
        - "92W" -> False (contains numbers)
        - "AL14" -> False (contains numbers)
        - "A" -> False (single letter)
        - "" -> False (empty)
    
    Args:
        storm_name: The storm name to check
    
    Returns:
        True if the storm is a named storm, False otherwise
    """
    if not storm_name:
        return False
    
    # Check if the name contains any numbers
    if any(char.isdigit() for char in storm_name):
        return False
    
    # Check if the name contains multiple letters
    letter_count = sum(1 for char in storm_name if char.isalpha())
    
    return letter_count > 1


def extract_storm_name_from_filename(filename: str) -> Optional[str]:
    """
    Extract the storm name from a TC data filename.
    
    Example filename:
    A_JSXX01ECEP300600_C_ECMP_20251030060000_tropical_cyclone_track_MELISSA_-75p3degW_22p1degN_bufr4.bin
    
    Returns: "MELISSA"
    """
    import re
    match = re.search(r'tropical_cyclone_track_([A-Z0-9]+)', filename)
    return match.group(1) if match else None


def download_tc_data_wrapper(config: PipelineConfig) -> Tuple[int, List[Path]]:
    """
    Wrapper for TC data download to be executed concurrently.
    Returns: (count_downloaded, list_of_files)
    """
    logger.info("🚀 Starting TC data download...")
    
    try:
        download_kwargs = {
            'output_dir': str(config.raw_data_dir),
            'max_workers': config.max_concurrent_downloads  # Pass max_concurrent_downloads for connection pooling
        }
        
        if config.download_date:
            logger.info(f"Downloading TC data for date: {config.download_date}")
            download_kwargs['date'] = config.download_date
            if config.run_time:
                logger.info(f"Filtering for run time: {config.run_time}Z")
                download_kwargs['run_time'] = config.run_time
            else:
                logger.info(f"Downloading all run times (00Z, 06Z, 12Z, 18Z) for {config.download_date}")
        else:
            logger.info(f"Downloading latest {config.download_limit} TC forecast(s)")
            download_kwargs['limit'] = config.download_limit
        
        result = download_tc_data(**download_kwargs)
        
        # Get list of downloaded BUFR files
        bufr_files = list(config.raw_data_dir.glob("*.bin"))
        
        # Filter for named storms only if enabled
        if config.named_storms_only:
            original_count = len(bufr_files)
            filtered_files = []
            removed_files = []
            
            for bufr_file in bufr_files:
                storm_name = extract_storm_name_from_filename(bufr_file.name)
                
                if storm_name and is_named_storm(storm_name):
                    filtered_files.append(bufr_file)
                    logger.info(f"✓ Keeping named storm: {storm_name} ({bufr_file.name})")
                else:
                    removed_files.append(bufr_file)
                    logger.info(f"✗ Removing non-named storm: {storm_name or 'UNKNOWN'} ({bufr_file.name})")
                    # Delete the non-named storm file
                    bufr_file.unlink()
                    # Also delete associated CSV if it exists
                    csv_file = bufr_file.with_suffix('.csv')
                    if csv_file.exists():
                        csv_file.unlink()
            
            bufr_files = filtered_files
            logger.info(f"Named storms filter: kept {len(bufr_files)}/{original_count} files")
        
        logger.info(f"✓ TC download complete: {result['downloaded']} files")
        return result['downloaded'], bufr_files
        
    except Exception as e:
        error_msg = f"TC download failed: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def download_wind_data_wrapper(config: PipelineConfig, tc_data_info: Dict) -> Tuple[int, List[Path]]:
    """
    Wrapper for wind data download to be executed concurrently.
    Returns: (count_downloaded, list_of_files)
    """
    if not config.process_wind_data:
        logger.info("Wind data processing disabled, skipping wind download")
        return 0, []
    
    logger.info("🚀 Starting wind data download...")
    
    try:
        # Extract run time from TC data
        tc_run_time = tc_data_info.get('run_time')
        tc_date = tc_data_info.get('date')
        
        if tc_run_time is None or tc_date is None:
            logger.warning("Could not determine TC run time or date, skipping wind download")
            return 0, []
        
        # Analyze TC data to determine required forecast hours
        # First try transformed directory (if files exist from previous run)
        # Otherwise check raw data directory for extracted CSV files
        max_forecast_hour = analyze_required_forecast_hours(config.transformed_data_dir, verbose=False)
        
        # If no transformed files found, analyze extracted files in raw_data_dir
        if max_forecast_hour == 144:
            # Create a temporary function to analyze extracted files
            extracted_files = list(config.raw_data_dir.glob("*.csv"))
            if extracted_files:
                max_hour = 0
                for csv_file in extracted_files:
                    try:
                        df = pd.read_csv(csv_file)
                        if not df.empty and 'step' in df.columns:
                            max_hour = max(max_hour, df['step'].max())
                    except:
                        continue
                if max_hour > 0:
                    rounded_hour = ((max_hour + 5) // 6) * 6
                    max_forecast_hour = min(rounded_hour + 12, 144)
                    logger.info(f"Analyzed extracted TC data: max step = {max_hour}h, using forecast hours up to {max_forecast_hour}h")
        
        required_forecast_hours = list(range(0, max_forecast_hour + 1, 6))
        
        logger.info(f"TC forecast run time: {tc_run_time:02d}Z")
        logger.info(f"TC forecast date: {tc_date}")
        logger.info(f"Required wind forecast hours: {required_forecast_hours}")
        
        # Download wind data with concurrent downloads
        result = download_ensemble_wind(
            date=tc_date,
            run_time=tc_run_time,
            forecast_hours=required_forecast_hours,
            output_dir=str(config.wind_data_dir),
            verbose=False,
            max_workers=config.max_concurrent_downloads
        )
        
        if result['success']:
            logger.info(f"✓ Wind download complete: {result['files_downloaded']} files")
            return result['files_downloaded'], result['downloaded_files']
        else:
            error_msg = result.get('error', 'Unknown error')
            files_downloaded = result.get('files_downloaded', 0)
            files_failed = result.get('files_failed', 0)
            
            logger.error(f"Wind download failed: {error_msg}")
            logger.error(f"  Files downloaded: {files_downloaded}")
            logger.error(f"  Files failed: {files_failed}")
            logger.error(f"  Full result: {result}")
            
            # Check if it's a date/data availability issue
            if files_failed == 0 and files_downloaded == 0:
                logger.error(f"  Possible cause: No wind data available for {tc_date} at {tc_run_time:02d}Z")
                logger.error(f"  Note: ECMWF open data may only have recent forecasts available")
            
            return 0, []
        
    except Exception as e:
        error_msg = f"Wind download failed: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def _extract_and_transform_worker(args: Tuple[str, str, str, bool]) -> Tuple[bool, Optional[str], str]:
    """
    Worker function for ProcessPoolExecutor - must be picklable (top-level function).
    Takes simple types that can be pickled.
    
    Args:
        args: (bufr_file_path, raw_data_dir, transformed_data_dir, skip_existing)
    
    Returns:
        (success, transformed_file_path, log_message)
    """
    import sys
    from pathlib import Path
    import re
    import pandas as pd
    
    # Add the TC pipeline directory to the path (needed in subprocess)
    pipeline_dir = Path(__file__).parent / 'TC-ECMWF-Forecast-Pipeline-main'
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))
    
    from ecmwf_tc_data_extractor import extract_tc_data
    from ecmwf_tc_data_transformer import transform_tc_data
    
    bufr_file_path, raw_data_dir, transformed_data_dir, skip_existing = args
    
    bufr_file = Path(bufr_file_path)
    raw_data_dir = Path(raw_data_dir)
    transformed_data_dir = Path(transformed_data_dir)
    
    log_messages = []
    
    try:
        # Extract BUFR to CSV
        csv_file = raw_data_dir / f"{bufr_file.stem}.csv"
        
        if csv_file.exists() and skip_existing:
            log_messages.append(f"Skipping already extracted: {csv_file.name}")
        else:
            log_messages.append(f"Extracting: {bufr_file.name}")
            df = extract_tc_data(str(bufr_file), verbose=False)
            
            if df.empty:
                return False, None, f"No data extracted from {bufr_file.name}"
            
            df.to_csv(csv_file, index=False)
            log_messages.append(f"✓ Extracted {len(df)} records to {csv_file.name}")
        
        # Transform CSV
        output_base = transformed_data_dir / f"transformed_{csv_file.stem}"
        output_file = output_base.with_suffix('.csv')
        
        if output_file.exists() and skip_existing:
            log_messages.append(f"Skipping already transformed: {output_file.name}")
            return True, str(output_file), "\n".join(log_messages)
        
        log_messages.append(f"Transforming: {csv_file.name}")
        
        # Extract storm name from filename
        match = re.search(r'tropical_cyclone_track_([A-Z0-9]+)', csv_file.stem)
        storm_name = match.group(1) if match else None
        
        # Transform data
        result = transform_tc_data(
            str(csv_file),
            str(output_base),
            storm_name=storm_name,
            verbose=False
        )
        
        if result['success']:
            actual_file = result.get('csv_file', str(output_file))
            log_messages.append(f"✓ Transformed {result['records']} records -> {Path(actual_file).name}")
            return True, actual_file, "\n".join(log_messages)
        else:
            return False, None, f"Failed to transform {csv_file.name}"
        
    except Exception as e:
        return False, None, f"Error processing {bufr_file.name}: {str(e)}"


def extract_and_transform_tc_file(bufr_file: Path, config: PipelineConfig) -> Tuple[bool, Optional[Path]]:
    """
    Extract and transform a single TC BUFR file (ThreadPool version).
    Returns: (success, transformed_file_path)
    """
    try:
        # Extract BUFR to CSV
        csv_file = config.raw_data_dir / f"{bufr_file.stem}.csv"
        
        if csv_file.exists() and config.skip_existing:
            logger.info(f"Skipping already extracted: {csv_file.name}")
        else:
            logger.info(f"Extracting: {bufr_file.name}")
            df = extract_tc_data(str(bufr_file), verbose=False)
            
            if df.empty:
                logger.warning(f"No data extracted from {bufr_file.name}")
                return False, None
            
            df.to_csv(csv_file, index=False)
            logger.info(f"✓ Extracted {len(df)} records to {csv_file.name}")
        
        # Transform CSV
        output_base = config.transformed_data_dir / f"transformed_{csv_file.stem}"
        output_file = output_base.with_suffix('.csv')
        
        if output_file.exists() and config.skip_existing:
            logger.info(f"Skipping already transformed: {output_file.name}")
            return True, output_file
        
        logger.info(f"Transforming: {csv_file.name}")
        
        # Extract storm name from filename
        import re
        match = re.search(r'tropical_cyclone_track_([A-Z0-9]+)', csv_file.stem)
        storm_name = match.group(1) if match else None
        
        # Transform data
        result = transform_tc_data(
            str(csv_file),
            str(output_base),
            storm_name=storm_name,
            verbose=False
        )
        
        if result['success']:
            actual_file = Path(result.get('csv_file', str(output_file)))
            logger.info(f"✓ Transformed {result['records']} records -> {actual_file.name}")
            return True, actual_file
        else:
            logger.error(f"Failed to transform {csv_file.name}")
            return False, None
        
    except Exception as e:
        logger.error(f"Error processing {bufr_file.name}: {str(e)}")
        return False, None


def extract_tc_data_info_from_csv(csv_files: List[Path]) -> Dict:
    """Extract run time and date information from TC data files"""
    if not csv_files:
        return {}
    
    try:
        first_csv = csv_files[0]
        df = pd.read_csv(first_csv)
        
        if df.empty:
            return {}
        
        datetime_str = df['datetime'].iloc[0]
        valid_time = pd.to_datetime(datetime_str)
        step = df['step'].iloc[0]
        
        forecast_time = valid_time - pd.Timedelta(hours=int(step))
        run_time = forecast_time.hour
        date = forecast_time.strftime('%Y-%m-%d')
        
        logger.info(f"Extracted TC data info: {date} {run_time:02d}Z")
        
        return {
            'run_time': run_time,
            'date': date,
            'forecast_time': forecast_time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        logger.warning(f"Could not extract TC data info: {e}")
        return {}


def phase1_concurrent_downloads(config: PipelineConfig, stats: PipelineStats) -> Tuple[List[Path], Dict]:
    """
    PHASE 1: Download TC and Wind data CONCURRENTLY
    Returns: (list_of_bufr_files, tc_data_info)
    """
    logger.info("=" * 70)
    logger.info("PHASE 1: CONCURRENT DOWNLOADS")
    logger.info("=" * 70)
    
    phase_start = datetime.now()
    bufr_files = []
    tc_data_info = {}
    
    try:
        # First, download TC data to extract metadata for wind download
        tc_count, bufr_files = download_tc_data_wrapper(config)
        stats.files_downloaded_tc = tc_count
        
        if not bufr_files:
            logger.warning("No TC files downloaded")
            return [], {}
        
        # Extract TC data info from one file to get run time for wind download
        # We need to do a quick extraction to get the metadata
        first_csv = config.raw_data_dir / f"{bufr_files[0].stem}.csv"
        if not first_csv.exists():
            df = extract_tc_data(str(bufr_files[0]), verbose=False)
            df.to_csv(first_csv, index=False)
        
        tc_data_info = extract_tc_data_info_from_csv([first_csv])
        
        # Now download wind data if enabled
        if config.process_wind_data and tc_data_info:
            wind_count, wind_files = download_wind_data_wrapper(config, tc_data_info)
            stats.files_downloaded_wind = wind_count
        
        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time("Phase 1: Downloads", duration)
        
        return bufr_files, tc_data_info
        
    except Exception as e:
        error_msg = f"Phase 1 failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def phase2_concurrent_extraction_transformation(config: PipelineConfig, stats: PipelineStats, 
                                               bufr_files: List[Path]) -> List[Path]:
    """
    PHASE 2: Extract and Transform TC data files CONCURRENTLY
    Uses ProcessPoolExecutor for CPU-intensive work (extraction/transformation)
    or ThreadPoolExecutor for I/O-intensive work.
    Returns: list_of_transformed_files
    """
    pool_type = "ProcessPool" if config.use_process_pool else "ThreadPool"
    logger.info("=" * 70)
    logger.info(f"PHASE 2: CONCURRENT EXTRACTION & TRANSFORMATION ({pool_type})")
    logger.info("=" * 70)
    
    phase_start = datetime.now()
    transformed_files = []
    
    try:
        if config.use_process_pool:
            # Use ProcessPoolExecutor for better CPU utilization (avoids GIL)
            # Better for CPU-intensive tasks like BUFR decoding and data transformation
            logger.info(f"Using ProcessPoolExecutor with {config.max_workers} workers for CPU-intensive tasks")
            
            with ProcessPoolExecutor(max_workers=config.max_workers) as executor:
                # Prepare arguments for worker function (must be picklable)
                work_args = [
                    (str(bufr_file), str(config.raw_data_dir), 
                     str(config.transformed_data_dir), config.skip_existing)
                    for bufr_file in bufr_files
                ]
                
                # Submit all tasks
                futures = {
                    executor.submit(_extract_and_transform_worker, args): bufr_files[i]
                    for i, args in enumerate(work_args)
                }
                
                # Process results as they complete
                for future in as_completed(futures):
                    bufr_file = futures[future]
                    try:
                        success, transformed_file_path, log_msg = future.result()
                        
                        # Log the messages from the worker process
                        if log_msg:
                            for line in log_msg.split('\n'):
                                if line:
                                    logger.info(f"  [{bufr_file.name}] {line}")
                        
                        if success:
                            stats.files_extracted += 1
                            stats.files_transformed += 1
                            if transformed_file_path:
                                transformed_files.append(Path(transformed_file_path))
                    except Exception as e:
                        error_msg = f"Processing failed for {bufr_file.name}: {str(e)}"
                        logger.error(error_msg)
                        stats.errors.append(error_msg)
        else:
            # Use ThreadPoolExecutor (original behavior)
            # Better for I/O-bound tasks or when pickling is problematic
            logger.info(f"Using ThreadPoolExecutor with {config.max_workers} workers")
            
            with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
                futures = {
                    executor.submit(extract_and_transform_tc_file, bufr_file, config): bufr_file 
                    for bufr_file in bufr_files
                }
                
                for future in as_completed(futures):
                    bufr_file = futures[future]
                    try:
                        success, transformed_file = future.result()
                        if success:
                            stats.files_extracted += 1
                            stats.files_transformed += 1
                            if transformed_file:
                                transformed_files.append(transformed_file)
                    except Exception as e:
                        error_msg = f"Processing failed for {bufr_file.name}: {str(e)}"
                        logger.error(error_msg)
                        stats.errors.append(error_msg)
        
        logger.info(f"✓ Processed {len(transformed_files)} TC files concurrently using {pool_type}")
        
        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time(f"Phase 2: Extraction & Transformation ({pool_type})", duration)
        
        return transformed_files
        
    except Exception as e:
        error_msg = f"Phase 2 failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def phase3_wind_combination(config: PipelineConfig, stats: PipelineStats) -> List[Path]:
    """
    PHASE 3: Process wind combination (depends on transformed TC data)
    Returns: list_of_envelope_files
    """
    if not config.process_wind_data:
        logger.info("Wind data processing disabled, skipping combination")
        return []
    
    logger.info("=" * 70)
    logger.info("PHASE 3: WIND COMBINATION PROCESSING")
    logger.info("=" * 70)
    
    phase_start = datetime.now()
    envelope_files = []
    
    try:
        result = process_wind_combination(
            tc_data_dir=config.transformed_data_dir,
            wind_data_dir=config.wind_data_dir,
            output_dir=config.wind_extracted_dir,
            buffer_radius_km=500,
            max_ensemble_members=config.max_ensemble_members,
            verbose=False,
            use_process_pool=config.use_process_pool,
            max_workers=config.max_workers
        )
        
        if result['processed_storms'] > 0:
            stats.files_wind_processed = result['processed_storms']
            logger.info(f"✓ Processed wind data for {result['processed_storms']} storms")
            
            # Get list of generated envelope files
            envelope_files = list(config.wind_extracted_dir.glob("*_envelopes_*.csv"))
            logger.info(f"Found envelope files: {[f.name for f in envelope_files]}")
        else:
            logger.warning("No wind envelope files generated")
        
        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time("Phase 3: Wind Combination", duration)
        
        return envelope_files
        
    except Exception as e:
        error_msg = f"Phase 3 failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        return []


def phase4_snowflake_loading(config: PipelineConfig, stats: PipelineStats, 
                             transformed_files: List[Path], envelope_files: List[Path]):
    """
    PHASE 4: Load all data to Snowflake
    """
    logger.info("=" * 70)
    logger.info("PHASE 4: SNOWFLAKE LOADING")
    logger.info("=" * 70)
    
    phase_start = datetime.now()
    
    try:
        # Set environment variables for Snowflake connection
        os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
        os.environ['SNOWFLAKE_USER'] = config.sf_user
        os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
        os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
        os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema
        
        # Set authentication credentials
        if config.spcs_run:
            # SPCS OAuth mode
            os.environ['SPCS_RUN'] = 'true'
            os.environ['SPCS_TOKEN_PATH'] = config.spcs_token_path
            logger.info("Connecting to Snowflake with SPCS OAuth authentication...")
            # Clear other authentication methods
            if 'SNOWFLAKE_PASSWORD' in os.environ:
                del os.environ['SNOWFLAKE_PASSWORD']
            if 'SNOWFLAKE_PRIVATE_KEY_PATH' in os.environ:
                del os.environ['SNOWFLAKE_PRIVATE_KEY_PATH']
            if 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE' in os.environ:
                del os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
        elif config.sf_private_key_path:
            os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'] = config.sf_private_key_path
            if config.sf_private_key_passphrase:
                os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'] = config.sf_private_key_passphrase
            # Clear password if using private key
            if 'SNOWFLAKE_PASSWORD' in os.environ:
                del os.environ['SNOWFLAKE_PASSWORD']
        elif config.sf_password:
            os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
            # Clear private key if using password
            if 'SNOWFLAKE_PRIVATE_KEY_PATH' in os.environ:
                del os.environ['SNOWFLAKE_PRIVATE_KEY_PATH']
            if 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE' in os.environ:
                del os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
        
        # Connect to Snowflake
        conn = get_snowflake_connection()
        
        # Ensure warehouse is active and set database/schema context
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE WAREHOUSE {config.sf_warehouse}")
            cursor.execute(f"USE DATABASE {config.sf_database}")
            cursor.execute(f"USE SCHEMA {config.sf_schema}")
            logger.info(f"Using: {config.sf_warehouse} / {config.sf_database}.{config.sf_schema}")
        finally:
            cursor.close()
        
        try:
            total_rows = 0
            
            # Helper function to ensure context is set before each load
            def ensure_context():
                """Ensure warehouse, database, and schema context are set"""
                cursor = conn.cursor()
                try:
                    cursor.execute(f"USE WAREHOUSE {config.sf_warehouse}")
                    cursor.execute(f"USE DATABASE {config.sf_database}")
                    cursor.execute(f"USE SCHEMA {config.sf_schema}")
                finally:
                    cursor.close()
            
            # Load TC track data
            logger.info(f"Loading {len(transformed_files)} TC track files...")
            for csv_file in transformed_files:
                ensure_context()  # Ensure context before each load
                rows = load_csv_to_snowflake(csv_file, conn, table_type='TC_TRACKS')
                total_rows += rows
            
            # Load envelope data
            logger.info(f"Loading {len(envelope_files)} envelope files...")
            for csv_file in envelope_files:
                ensure_context()  # Ensure context before each load
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
            try:
                # Ensure warehouse is still active for verification queries
                cursor.execute(f"USE WAREHOUSE {config.sf_warehouse}")
                # Ensure we're in the correct database and schema context
                cursor.execute(f"USE DATABASE {config.sf_database}")
                cursor.execute(f"USE SCHEMA {config.sf_schema}")
                
                cursor.execute("SELECT COUNT(*) FROM TC_TRACKS")
                tracks_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_INDIVIDUAL")
                individual_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_COMBINED")
                combined_count = cursor.fetchone()[0]
            finally:
                cursor.close()
            
            logger.info("Total records in database:")
            logger.info(f"  TC_TRACKS: {tracks_count:,}")
            logger.info(f"  TC_ENVELOPES_INDIVIDUAL: {individual_count:,}")
            logger.info(f"  TC_ENVELOPES_COMBINED: {combined_count:,}")
            
            stats.files_loaded = len(transformed_files) + len(envelope_files)
            stats.rows_loaded = total_rows
            logger.info(f"✓ Loaded {total_rows:,} rows from {stats.files_loaded} files")
            
        finally:
            conn.close()
            logger.info("Connection closed")
        
        duration = (datetime.now() - phase_start).total_seconds()
        stats.log_phase_time("Phase 4: Snowflake Loading", duration)
        
    except Exception as e:
        error_msg = f"Phase 4 failed: {str(e)}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def cleanup_files(config: PipelineConfig):
    """Clean up temporary files after successful load"""
    if not config.cleanup_after_load:
        logger.info("Cleanup skipped (CLEANUP_AFTER_LOAD=false)")
        return
    
    logger.info("=" * 70)
    logger.info("CLEANING UP TEMPORARY FILES")
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
        
        logger.info(f"✓ Removed {removed_count} temporary files")
        
    except Exception as e:
        logger.warning(f"Cleanup failed (non-critical): {str(e)}")


def main():
    """Main pipeline execution with concurrent processing"""
    logger.info("=" * 70)
    logger.info("TC ECMWF Forecast Pipeline - CONCURRENT EXECUTION")
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
    logger.info("Configuration:")
    logger.info(f"  Max concurrent workers: {config.max_workers}")
    logger.info(f"  Max concurrent downloads: {config.max_concurrent_downloads}")
    pool_type = "ProcessPool (CPU-optimized)" if config.use_process_pool else "ThreadPool (I/O-optimized)"
    logger.info(f"  Executor type: {pool_type}")
    logger.info(f"  Raw data directory: {config.raw_data_dir}")
    logger.info(f"  Transformed data directory: {config.transformed_data_dir}")
    logger.info(f"  Wind data directory: {config.wind_data_dir}")
    logger.info(f"  Wind extracted directory: {config.wind_extracted_dir}")
    logger.info(f"  Snowflake database: {config.sf_database}.{config.sf_schema}")
    
    # Log download date/time settings
    download_date_env = os.getenv('DOWNLOAD_DATE', 'Not set')
    run_time_env = os.getenv('RUN_TIME', 'Not set')
    logger.info(f"  DOWNLOAD_DATE: {download_date_env}")
    logger.info(f"  RUN_TIME: {run_time_env}")
    
    # Log authentication method
    if config.spcs_run:
        auth_method = f"SPCS OAuth (token: {config.spcs_token_path})"
    elif config.sf_private_key_path:
        auth_method = f"Private key ({config.sf_private_key_path})"
    else:
        auth_method = "Password"
    logger.info(f"  Snowflake authentication: {auth_method}")
    
    # Log SSL mode if insecure mode is enabled
    if os.getenv('SNOWFLAKE_INSECURE_MODE', 'false').lower() == 'true':
        logger.warning("  SSL mode: INSECURE (certificate validation disabled)")
    else:
        logger.info("  SSL mode: Secure (certificate validation enabled)")
    
    logger.info(f"  Process wind data: {config.process_wind_data}")
    logger.info(f"  Named storms only: {config.named_storms_only}")
    logger.info(f"  Cleanup after load: {config.cleanup_after_load}")
    
    # Log download configuration
    if config.download_date:
        if config.run_time:
            logger.info(f"  Download mode: Specific date and time ({config.download_date} {config.run_time}Z)")
        else:
            logger.info(f"  Download mode: All run times for date ({config.download_date})")
    else:
        logger.info(f"  Download mode: Latest {config.download_limit} forecast(s)")
    
    try:
        # PHASE 1: Concurrent Downloads
        bufr_files, tc_data_info = phase1_concurrent_downloads(config, stats)
        
        if not bufr_files:
            logger.warning("No BUFR files to process. Exiting.")
            sys.exit(0)
        
        # PHASE 2: Concurrent Extraction & Transformation
        transformed_files = phase2_concurrent_extraction_transformation(config, stats, bufr_files)
        
        # PHASE 3: Wind Combination (depends on Phase 2)
        envelope_files = phase3_wind_combination(config, stats)
        
        # PHASE 4: Snowflake Loading
        phase4_snowflake_loading(config, stats, transformed_files, envelope_files)
        
        # Clean up if successful
        cleanup_files(config)
        
        # Log summary
        stats.log_summary()
        
        # Exit with appropriate code
        if stats.errors:
            logger.warning("Pipeline completed with errors")
            sys.exit(1)
        else:
            logger.info("✓ Pipeline completed successfully!")
            sys.exit(0)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        stats.log_summary()
        sys.exit(1)


if __name__ == "__main__":
    main()

