-- ============================================================================
-- Download Wind GRIB2 Files Stored Procedure
-- ============================================================================
-- Downloads ensemble 10m wind forecast GRIB2 files from ECMWF Open Data
-- and stores them in @TC_WIND_STAGE
-- ============================================================================
-- Prerequisites:
--   - Database: AOTS
--   - Schema: ECMWF_PIPELINE
--   - Role: AOTS_ROLE
--   - Warehouse: AOTS_X86_WH (Python execution)
--   - Stage: @TC_WIND_STAGE
--   - Table: FILE_PROCESSING_LOG
-- ============================================================================

USE ROLE AOTS_ROLE;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Create Stored Procedure: download_wind_grib_files
-- ============================================================================

CREATE OR REPLACE PROCEDURE download_wind_grib_files(
    FORECAST_DATE DATE,
    RUN_TIME VARCHAR,  -- '00', '06', '12', '18'
    FORECAST_HOURS ARRAY  -- Array of integers, e.g., [0, 6, 12, 18, 24, ..., 144]
)
RETURNS TABLE(
    FILE_PATH VARCHAR,
    FILE_SIZE_BYTES NUMBER,
    STATUS VARCHAR,
    MESSAGE VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
RESOURCE_CONSTRAINT = (architecture='x86')
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
EXTERNAL_ACCESS_INTEGRATIONS = (ecmwf_external_access)
PACKAGES = ('ecmwf-opendata', 'snowflake-snowpark-python')
HANDLER = 'download_wind_grib_files'
AS
$$
from datetime import datetime
from snowflake.snowpark import Session
import tempfile
import os

def download_wind_grib_files(session: Session, forecast_date, run_time, forecast_hours):
    """
    Download ensemble 10m wind forecast GRIB2 files from ECMWF Open Data.
    
    Args:
        session: Snowpark session
        forecast_date: Forecast date (DATE type)
        run_time: Run time as string ('00', '06', '12', '18')
        forecast_hours: Array of forecast hours (e.g., [0, 6, 12, 18, ..., 144])
    
    Returns:
        List of tuples: (file_path, file_size_bytes, status, message)
    """
    results = []
    
    try:
        # Import ecmwf-opendata client
        from ecmwf.opendata import Client
        
        # Convert date
        if isinstance(forecast_date, str):
            dt = datetime.strptime(forecast_date, '%Y-%m-%d')
        else:
            dt = forecast_date
        
        # Convert run_time to integer
        run_hour = int(run_time)
        
        # Validate run time
        if run_hour not in [0, 6, 12, 18]:
            results.append((
                None,
                0,
                'ERROR',
                f'Invalid run_time: {run_time}. Must be 00, 06, 12, or 18'
            ))
            # Convert results to Snowpark DataFrame
            schema = ['FILE_PATH', 'FILE_SIZE_BYTES', 'STATUS', 'MESSAGE']
            return session.create_dataframe(results, schema=schema)
        
        # Create stage directory path
        stage_dir = f"@TC_WIND_STAGE/{dt.strftime('%Y%m%d')}_{run_time.zfill(2)}"
        
        # Initialize ECMWF client
        client = Client(source="ecmwf")
        
        # Download each forecast hour - download both PF and CF files (matching Python)
        for step in forecast_hours:
            # PF (perturbed) file - members 1-50
            filename_pf = f"wind_ens_{dt.strftime('%Y-%m-%d')}_r{run_hour:02d}_f{step:03d}h_pf.grib2"
            stage_file_path_pf = f"{stage_dir}/{filename_pf}"
            
            # CF (control) file - member 51
            filename_cf = f"wind_ens_{dt.strftime('%Y-%m-%d')}_r{run_hour:02d}_f{step:03d}h_cf.grib2"
            stage_file_path_cf = f"{stage_dir}/{filename_cf}"
            
            # ========================================================================
            # Download PF file (perturbed members 1-50)
            # ========================================================================
            try:
                # Check if file already exists in FILE_PROCESSING_LOG
                existing_file_pf = session.sql(f"""
                    SELECT FILE_PATH 
                    FROM AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG 
                    WHERE FILE_PATH = '{stage_file_path_pf}'
                      AND FILE_TYPE = 'GRIB2'
                      AND PROCESSING_STATUS = 'COMPLETED'
                    LIMIT 1
                """).collect()
                
                if existing_file_pf:
                    # File already exists and was processed - skip download
                    results.append((
                        stage_file_path_pf,
                        0,
                        'SKIPPED',
                        f'Step +{step}h PF: Already exists and processed'
                    ))
                else:
                    # Create temporary file with correct filename
                    temp_dir = tempfile.mkdtemp()
                    temp_path_pf = os.path.join(temp_dir, filename_pf)
                    
                    try:
                        # Download PF wind data (u10 and v10 components, perturbed members 1-50)
                        client.retrieve(
                            date=dt,
                            time=run_hour,
                            stream="enfo",  # Ensemble forecast
                            type="pf",      # Perturbed forecast (members 1-50)
                            step=step,
                            param=["10u", "10v"],  # 10m u and v wind components
                            target=temp_path_pf
                        )
                        
                        # Verify file was created
                        if not os.path.exists(temp_path_pf):
                            results.append((
                                None,
                                0,
                                'ERROR',
                                f'Step +{step}h PF: File not created by ECMWF client'
                            ))
                        else:
                            file_size_pf = os.path.getsize(temp_path_pf)
                            
                            # PUT file to stage
                            put_result = session.file.put(
                                local_file_name=temp_path_pf,
                                stage_location=stage_dir,
                                auto_compress=False,
                                overwrite=True
                            )
                            
                            # Insert into FILE_PROCESSING_LOG
                            session.sql(f"""
                                INSERT INTO AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG (
                                    FILE_PATH,
                                    FILE_TYPE,
                                    FORECAST_DATE,
                                    RUN_TIME,
                                    FILE_SIZE_BYTES,
                                    PROCESSING_STATUS,
                                    CREATED_AT,
                                    UPDATED_AT
                                ) VALUES (
                                    '{stage_file_path_pf}',
                                    'GRIB2',
                                    '{forecast_date}',
                                    '{run_time.zfill(2)}',
                                    {file_size_pf},
                                    'PENDING',
                                    CURRENT_TIMESTAMP(),
                                    CURRENT_TIMESTAMP()
                                )
                            """).collect()
                            
                            results.append((
                                stage_file_path_pf,
                                file_size_pf,
                                'SUCCESS',
                                f'Downloaded step +{step}h PF ({file_size_pf / 1024 / 1024:.1f} MB)'
                            ))
                        
                    finally:
                        # Clean up temp file
                        if os.path.exists(temp_path_pf):
                            os.unlink(temp_path_pf)
                        if os.path.exists(temp_dir):
                            os.rmdir(temp_dir)
                
            except Exception as e:
                results.append((
                    None,
                    0,
                    'ERROR',
                    f'Step +{step}h PF: {str(e)}'
                ))
            
            # ========================================================================
            # Download CF file (control forecast, member 51)
            # ========================================================================
            try:
                # Check if file already exists in FILE_PROCESSING_LOG
                existing_file_cf = session.sql(f"""
                    SELECT FILE_PATH 
                    FROM AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG 
                    WHERE FILE_PATH = '{stage_file_path_cf}'
                      AND FILE_TYPE = 'GRIB2'
                      AND PROCESSING_STATUS = 'COMPLETED'
                    LIMIT 1
                """).collect()
                
                if existing_file_cf:
                    # File already exists and was processed - skip download
                    results.append((
                        stage_file_path_cf,
                        0,
                        'SKIPPED',
                        f'Step +{step}h CF: Already exists and processed'
                    ))
                else:
                    # Create temporary file with correct filename
                    temp_dir = tempfile.mkdtemp()
                    temp_path_cf = os.path.join(temp_dir, filename_cf)
                    
                    try:
                        # Download CF wind data (u10 and v10 components, control forecast)
                        client.retrieve(
                            date=dt,
                            time=run_hour,
                            stream="enfo",  # Ensemble forecast
                            type="cf",      # Control forecast (member 51)
                            step=step,
                            param=["10u", "10v"],  # 10m u and v wind components
                            target=temp_path_cf
                        )
                        
                        # Verify file was created
                        if not os.path.exists(temp_path_cf):
                            results.append((
                                None,
                                0,
                                'ERROR',
                                f'Step +{step}h CF: File not created by ECMWF client'
                            ))
                        else:
                            file_size_cf = os.path.getsize(temp_path_cf)
                            
                            # PUT file to stage
                            put_result = session.file.put(
                                local_file_name=temp_path_cf,
                                stage_location=stage_dir,
                                auto_compress=False,
                                overwrite=True
                            )
                            
                            # Insert into FILE_PROCESSING_LOG
                            session.sql(f"""
                                INSERT INTO AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG (
                                    FILE_PATH,
                                    FILE_TYPE,
                                    FORECAST_DATE,
                                    RUN_TIME,
                                    FILE_SIZE_BYTES,
                                    PROCESSING_STATUS,
                                    CREATED_AT,
                                    UPDATED_AT
                                ) VALUES (
                                    '{stage_file_path_cf}',
                                    'GRIB2',
                                    '{forecast_date}',
                                    '{run_time.zfill(2)}',
                                    {file_size_cf},
                                    'PENDING',
                                    CURRENT_TIMESTAMP(),
                                    CURRENT_TIMESTAMP()
                                )
                            """).collect()
                            
                            results.append((
                                stage_file_path_cf,
                                file_size_cf,
                                'SUCCESS',
                                f'Downloaded step +{step}h CF ({file_size_cf / 1024 / 1024:.1f} MB)'
                            ))
                        
                    finally:
                        # Clean up temp file
                        if os.path.exists(temp_path_cf):
                            os.unlink(temp_path_cf)
                        if os.path.exists(temp_dir):
                            os.rmdir(temp_dir)
                
            except Exception as e:
                results.append((
                    None,
                    0,
                    'ERROR',
                    f'Step +{step}h CF: {str(e)}'
                ))
        
        # Convert results to Snowpark DataFrame
        schema = ['FILE_PATH', 'FILE_SIZE_BYTES', 'STATUS', 'MESSAGE']
        return session.create_dataframe(results, schema=schema)
        
    except Exception as e:
        results.append((
            None,
            0,
            'ERROR',
            f'Failed to download wind files: {str(e)}'
        ))
        # Convert results to Snowpark DataFrame
        schema = ['FILE_PATH', 'FILE_SIZE_BYTES', 'STATUS', 'MESSAGE']
        return session.create_dataframe(results, schema=schema)
$$;

-- ============================================================================
-- Verify procedure created
-- ============================================================================

SHOW PROCEDURES LIKE 'download_wind_grib_files';

DESC PROCEDURE download_wind_grib_files(DATE, VARCHAR, ARRAY);

SELECT 
    '✓ Procedure created successfully' as status,
    'download_wind_grib_files(DATE, VARCHAR, ARRAY)' as procedure_name,
    'Downloads wind GRIB2 files to @TC_WIND_STAGE' as purpose;

