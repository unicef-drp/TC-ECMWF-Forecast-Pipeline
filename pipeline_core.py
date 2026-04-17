#!/usr/bin/env python3
"""
Shared pipeline core: configuration, statistics, and processing steps 1–5.

Both entry points import from here:
  - github_actions/main.py  — sequential execution, password auth
  - snowflake/spcs_pipeline.py — concurrent execution, SPCS / private-key auth

Each entry point adds:
  - PipelineConfig(BasePipelineConfig) for deployment-specific settings
  - Step 6: Snowflake loading (different connectors for each deployment)
  - main() orchestration
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import pandas as pd

from ecmwf_tc_data_downloader import download_tc_data
from ecmwf_tc_data_extractor import extract_tc_data, filter_tc_data, save_per_storm_csvs
from ecmwf_wind_data_downloader import download_ensemble_wind
from ecmwf_tc_wind_combination import process_wind_combination, analyze_required_forecast_hours

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration base class
# ---------------------------------------------------------------------------

class BasePipelineConfig:
    """
    Shared configuration fields and helpers for both pipeline entry points.

    Subclasses call super().__init__() then add their own fields.
    Override validate() when authentication requirements differ.
    """

    def __init__(self):
        # Working directories
        self.raw_data_dir = Path(os.getenv('RAW_DATA_DIR', 'tc_data'))
        self.transformed_data_dir = Path(os.getenv('TRANSFORMED_DATA_DIR', 'tc_data_transformed'))
        self.wind_data_dir = Path(os.getenv('WIND_DATA_DIR', 'wind_data'))
        self.wind_extracted_dir = Path(os.getenv('WIND_EXTRACTED_DIR', 'wind_extracted'))

        # Snowflake credentials
        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA')

        # Pipeline options
        self.cleanup_after_load = os.getenv('CLEANUP_AFTER_LOAD', 'true').lower() == 'true'
        self.skip_existing = os.getenv('SKIP_EXISTING', 'true').lower() == 'true'

        # Download options
        self.download_date = os.getenv('DOWNLOAD_DATE')    # YYYYMMDD or None → latest
        self.run_time = os.getenv('RUN_TIME')               # 00/06/12/18 or None
        self.download_limit = int(os.getenv('DOWNLOAD_LIMIT', '1'))

        # Wind processing
        self.process_wind_data = os.getenv('PROCESS_WIND_DATA', 'true').lower() == 'true'
        self.named_storms_only = os.getenv('NAMED_STORMS_ONLY', 'true').lower() == 'true'
        self.max_ensemble_members = 51  # Fixed: 50 perturbed + 1 control

    def _validate_run_time(self) -> bool:
        """Validate run_time format and required-when-date-specified rule."""
        if self.download_date and not self.run_time:
            logger.error("RUN_TIME is required when DOWNLOAD_DATE is specified")
            return False
        if self.run_time and self.run_time not in ['00', '06', '12', '18']:
            logger.error(f"Invalid RUN_TIME: {self.run_time}. Must be 00, 06, 12, or 18")
            return False
        return True

    def validate(self) -> bool:
        """
        Validate required Snowflake credentials and download options.

        Returns True if valid, False otherwise (errors are logged).
        Subclasses with different auth requirements should override this.
        """
        missing = []
        for var in ('sf_account', 'sf_user', 'sf_password',
                    'sf_warehouse', 'sf_database', 'sf_schema'):
            if not getattr(self, var):
                missing.append(var.replace('sf_', 'SNOWFLAKE_').upper())

        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            return False

        return self._validate_run_time()

    def create_directories(self):
        """Ensure all required working directories exist."""
        for d in (self.raw_data_dir, self.transformed_data_dir,
                  self.wind_data_dir, self.wind_extracted_dir):
            d.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"Directories ready: {self.raw_data_dir}, {self.transformed_data_dir}, "
            f"{self.wind_data_dir}, {self.wind_extracted_dir}"
        )


# ---------------------------------------------------------------------------
# Statistics tracker
# ---------------------------------------------------------------------------

class PipelineStats:
    """Track pipeline execution statistics."""

    def __init__(self):
        self.start_time = datetime.now()
        self.files_downloaded = 0
        self.files_extracted = 0
        self.files_transformed = 0
        self.files_wind_downloaded = 0
        self.files_wind_processed = 0
        self.files_loaded = 0
        self.rows_loaded = 0
        self.errors: List[str] = []

    def log_summary(self):
        """Log pipeline execution summary."""
        duration = (datetime.now() - self.start_time).total_seconds()
        logger.info("=" * 70)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Duration: {duration:.2f}s ({duration / 60:.1f} min)")
        logger.info(f"Files Downloaded:           {self.files_downloaded}")
        logger.info(f"Files Extracted:            {self.files_extracted}")
        logger.info(f"Files Transformed:          {self.files_transformed}")
        logger.info(f"Wind Files Downloaded:      {self.files_wind_downloaded}")
        logger.info(f"Wind Files Processed:       {self.files_wind_processed}")
        logger.info(f"Files Loaded to Snowflake:  {self.files_loaded}")
        logger.info(f"Total Rows Loaded:          {self.rows_loaded:,}")
        if self.errors:
            logger.error(f"Errors encountered: {len(self.errors)}")
            for error in self.errors:
                logger.error(f"  - {error}")
        else:
            logger.info("No errors encountered")
        logger.info("=" * 70)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def extract_tc_data_info(csv_files: List[Path]) -> Dict:
    """Derive forecast run date and hour from the first per-storm CSV file."""
    if not csv_files:
        return {}
    try:
        df = pd.read_csv(csv_files[0])
        if df.empty:
            return {}
        valid_time = pd.to_datetime(df['datetime'].iloc[0])
        step = int(df['step'].iloc[0])
        forecast_time = valid_time - pd.Timedelta(hours=step)
        run_time = forecast_time.hour
        date = forecast_time.strftime('%Y-%m-%d')
        logger.info(
            f"TC data info: {date} {run_time:02d}Z "
            f"(valid_time={valid_time}, step={step}h)"
        )
        return {
            'run_time': run_time,
            'date': date,
            'forecast_time': forecast_time.strftime('%Y-%m-%d %H:%M:%S'),
        }
    except Exception as e:
        logger.warning(f"Could not extract TC data info: {e}")
        return {}


# ---------------------------------------------------------------------------
# Pipeline steps 1–5
# ---------------------------------------------------------------------------

def step1_download(config: BasePipelineConfig, stats: PipelineStats) -> List[Path]:
    """Step 1: Download the combined TC track BUFR file from ECMWF Open Data."""
    logger.info("=" * 70)
    logger.info("STEP 1: Downloading TC forecast files...")
    logger.info("=" * 70)
    try:
        download_kwargs: Dict = {'output_dir': str(config.raw_data_dir)}
        if config.download_date:
            logger.info(f"Downloading for specific date: {config.download_date}")
            download_kwargs['date'] = config.download_date
            if config.run_time:
                logger.info(f"Run time: {config.run_time}Z")
                download_kwargs['run_time'] = config.run_time
        else:
            logger.info(f"Downloading latest {config.download_limit} forecast(s)")
            download_kwargs['limit'] = config.download_limit

        result = download_tc_data(**download_kwargs)
        stats.files_downloaded = result['downloaded']
        logger.info(f"Downloaded {stats.files_downloaded} file(s)")

        # Support both .bufr4 (ecmwf-opendata) and .bin (DISS) files
        bufr_files = (list(config.raw_data_dir.glob("*.bufr4"))
                      + list(config.raw_data_dir.glob("*.bin")))
        return bufr_files

    except Exception as e:
        error_msg = f"Download failed: {e}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def step2_extract(config: BasePipelineConfig, stats: PipelineStats,
                  bufr_files: List[Path]) -> List[Path]:
    """Step 2: Parse the combined BUFR file and write one CSV per named storm.

    The ecmwf-opendata API delivers a single .bufr4 file per forecast run
    containing all active storms and all 51 ensemble members.  Named-storm
    filtering and per-storm splitting happen here so that downstream steps
    are unchanged (one CSV in → one transformed CSV out).
    """
    logger.info("=" * 70)
    logger.info("STEP 2: Extracting BUFR files to per-storm CSVs...")
    logger.info("=" * 70)
    extracted_files: List[Path] = []

    for bufr_file in bufr_files:
        try:
            logger.info(f"Extracting: {bufr_file.name}")
            df = extract_tc_data(str(bufr_file), verbose=False)

            if df.empty:
                logger.warning(f"  No data extracted from {bufr_file.name}")
                continue

            df = filter_tc_data(df, named_storms_only=config.named_storms_only)
            if df.empty:
                logger.warning(f"  No named storms found in {bufr_file.name}")
                continue

            storms = df['storm_id'].unique()
            logger.info(f"  Found {len(storms)} named storm(s): {list(storms)}")

            csv_paths = save_per_storm_csvs(
                df, str(config.raw_data_dir), bufr_file.name, verbose=True
            )
            for p in csv_paths:
                extracted_files.append(Path(p))
                stats.files_extracted += 1

        except Exception as e:
            error_msg = f"Extraction failed for {bufr_file.name}: {e}"
            logger.error(error_msg)
            stats.errors.append(error_msg)

    logger.info(f"Extracted {stats.files_extracted} per-storm CSV(s)")
    return extracted_files


def _transform_worker(args: tuple) -> tuple:
    """Transform a single per-storm CSV. Top-level so it is picklable by ProcessPoolExecutor."""
    import sys as _sys
    from pathlib import Path as _Path
    _repo_root = _Path(__file__).parent
    if str(_repo_root) not in _sys.path:
        _sys.path.insert(0, str(_repo_root))
    from ecmwf_tc_data_transformer import transform_tc_data as _transform
    csv_file_path, transformed_data_dir, skip_existing = args
    csv_file = _Path(csv_file_path)
    output_base = _Path(transformed_data_dir) / f"transformed_{csv_file.stem}"
    output_file = output_base.with_suffix('.csv')
    if output_file.exists() and skip_existing:
        return True, str(output_file), f"Skipping: {output_file.name}"
    import re as _re
    match = _re.search(r'_storm_([A-Z0-9-]+)_', csv_file.stem)
    storm_name = match.group(1) if match else None
    try:
        result = _transform(str(csv_file), str(output_base), storm_name=storm_name, verbose=False)
        if result['success']:
            actual = result.get('csv_file', str(output_file))
            return True, actual, f"Transformed {result['records']} records → {_Path(actual).name}"
        return False, None, f"Failed to transform {csv_file.name}"
    except Exception as exc:
        return False, None, f"Error transforming {csv_file.name}: {exc}"


def step3_transform(config: BasePipelineConfig, stats: PipelineStats,
                    csv_files: List[Path],
                    use_process_pool: bool = False,
                    max_workers: int = 1) -> List[Path]:
    """Step 3: Standardise units, compute wind radii, and create WKT polygons.

    Args:
        use_process_pool: If True use ProcessPoolExecutor (SPCS); if False run sequentially (GHA).
        max_workers: Worker count, only used when use_process_pool=True.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed
    pool_label = f"ProcessPool({max_workers})" if use_process_pool else "Sequential"
    logger.info("=" * 70)
    logger.info(f"STEP 3: Transforming CSV files… ({pool_label})")
    logger.info("=" * 70)
    transformed_files: List[Path] = []

    work_args = [
        (str(f), str(config.transformed_data_dir), config.skip_existing)
        for f in csv_files
    ]

    if use_process_pool and len(csv_files) > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_transform_worker, a): csv_files[i]
                       for i, a in enumerate(work_args)}
            for future in as_completed(futures):
                csv_file = futures[future]
                try:
                    success, path, msg = future.result()
                    if msg:
                        logger.info(f"  [{csv_file.name}] {msg}")
                    if success and path:
                        transformed_files.append(Path(path))
                        stats.files_transformed += 1
                except Exception as exc:
                    error_msg = f"Transformation failed for {csv_file.name}: {exc}"
                    logger.error(error_msg)
                    stats.errors.append(error_msg)
    else:
        for args in work_args:
            success, path, msg = _transform_worker(args)
            if msg:
                logger.info(f"  {msg}")
            if success and path:
                transformed_files.append(Path(path))
                stats.files_transformed += 1
            elif not success:
                stats.errors.append(msg)

    logger.info(f"Transformed {len(transformed_files)} file(s)")
    return transformed_files


def step4_download_wind(config: BasePipelineConfig, stats: PipelineStats,
                        tc_data_info: Dict, max_workers: int = 4) -> List[Path]:
    """Step 4: Download ensemble 10 m wind GRIB files matching the TC forecast run.

    Args:
        max_workers: Concurrent download workers passed to download_ensemble_wind (default 4).
    """
    if not config.process_wind_data:
        logger.info("Wind data processing disabled — skipping wind download")
        return []

    logger.info("=" * 70)
    logger.info("STEP 4: Downloading wind forecast data...")
    logger.info("=" * 70)
    try:
        tc_run_time = tc_data_info.get('run_time')
        tc_date = tc_data_info.get('date')
        if tc_run_time is None or tc_date is None:
            logger.warning("Cannot determine TC run time or date — skipping wind download")
            return []

        max_forecast_hour = analyze_required_forecast_hours(
            config.transformed_data_dir, verbose=True
        )
        required_forecast_hours = list(range(0, max_forecast_hour + 1, 6))
        logger.info(f"TC forecast: {tc_date} {tc_run_time:02d}Z")
        logger.info(
            f"Wind steps: 0–{max_forecast_hour}h every 6h "
            f"({len(required_forecast_hours)} files)"
        )

        result = download_ensemble_wind(
            date=tc_date,
            run_time=tc_run_time,
            forecast_hours=required_forecast_hours,
            output_dir=str(config.wind_data_dir),
            verbose=False,
            max_workers=max_workers,
        )
        if result['success']:
            stats.files_wind_downloaded = result['files_downloaded']
            logger.info(f"Downloaded {stats.files_wind_downloaded} wind file(s)")
            return result['downloaded_files']
        else:
            logger.warning(f"Wind download failed: {result.get('error', 'unknown error')}")
            return []

    except Exception as e:
        error_msg = f"Wind download failed: {e}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        return []


def step5_process_wind(config: BasePipelineConfig, stats: PipelineStats,
                       use_process_pool: bool = False,
                       max_workers: int = 1) -> List[Path]:
    """Step 5: Extract wind-threshold contours and union across forecast steps.

    Globs wind_data_dir for .grib2 files so the early-exit check is always
    based on what is actually on disk (not just what was downloaded this run).

    Args:
        use_process_pool: If True use ProcessPoolExecutor (SPCS); if False run sequentially (GHA).
        max_workers: Worker count, only used when use_process_pool=True.
    """
    wind_files = list(config.wind_data_dir.glob("*.grib2"))
    if not config.process_wind_data or not wind_files:
        logger.info("Wind processing disabled or no wind files — skipping")
        return []

    pool_label = f"ProcessPool({max_workers})" if use_process_pool else "Sequential"
    logger.info("=" * 70)
    logger.info(f"STEP 5: Processing wind data with TC tracks… ({pool_label})")
    logger.info("=" * 70)
    try:
        result = process_wind_combination(
            tc_data_dir=config.transformed_data_dir,
            wind_data_dir=config.wind_data_dir,
            output_dir=config.wind_extracted_dir,
            buffer_radius_km=500,
            max_ensemble_members=config.max_ensemble_members,
            verbose=False,
            use_process_pool=use_process_pool,
            max_workers=max_workers,
        )
        if result['processed_storms'] > 0:
            stats.files_wind_processed = result['processed_storms']
            logger.info(f"Processed wind data for {result['processed_storms']} storm(s)")
            envelope_files = list(config.wind_extracted_dir.glob("*_envelopes_*.csv"))
            logger.info(f"Envelope files: {[f.name for f in envelope_files]}")
            return envelope_files
        else:
            logger.warning("No wind envelope files generated")
            return []

    except Exception as e:
        error_msg = f"Wind processing failed: {e}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        return []


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup_files(config: BasePipelineConfig):
    """Remove temporary data files after a successful pipeline run."""
    if not config.cleanup_after_load:
        logger.info("Cleanup skipped (CLEANUP_AFTER_LOAD=false)")
        return

    logger.info("Cleaning up temporary files...")
    try:
        removed = 0
        for pattern, directory in [
            ("*.bufr4", config.raw_data_dir),
            ("*.bin", config.raw_data_dir),
            ("*.csv", config.raw_data_dir),
            ("*.csv", config.transformed_data_dir),
            ("*.grib2", config.wind_data_dir),
            ("*.csv", config.wind_extracted_dir),
        ]:
            for f in directory.glob(pattern):
                f.unlink()
                removed += 1
        logger.info(f"Removed {removed} temporary file(s)")
    except Exception as e:
        logger.warning(f"Cleanup failed (non-critical): {e}")
