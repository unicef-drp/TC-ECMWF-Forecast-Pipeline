-- ============================================================================
-- Download TC BUFR Files Stored Procedure
-- ============================================================================
-- Downloads tropical cyclone track BUFR files from ECMWF DISS system
-- and stores them in @TC_BUFR_STAGE
-- ============================================================================
-- Prerequisites:
--   - Database: AOTS
--   - Schema: ECMWF_PIPELINE
--   - Role: AOTS_ROLE
--   - Warehouse: AOTS_X86_WH (Python execution)
--   - Stage: @TC_BUFR_STAGE
--   - Table: FILE_PROCESSING_LOG
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Create Stored Procedure: download_tc_bufr_files
-- ============================================================================

CREATE OR REPLACE PROCEDURE download_tc_bufr_files(
    FORECAST_DATE DATE,
    RUN_TIME VARCHAR  -- '00', '06', '12', '18'
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
PACKAGES = ('requests', 'beautifulsoup4', 'snowflake-snowpark-python')
HANDLER = 'download_tc_bufr_files'
AS
$$
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.files import SnowflakeFile
import tempfile
import os

# Configuration
BASE_URL = "https://essential.ecmwf.int/"

def download_tc_bufr_files(session: Session, forecast_date, run_time):
    """
    Download tropical cyclone track BUFR files from ECMWF DISS system.
    
    Args:
        session: Snowpark session
        forecast_date: Forecast date (DATE type)
        run_time: Run time as string ('00', '06', '12', '18')
    
    Returns:
        List of tuples: (file_path, file_size_bytes, status, message)
    """
    results = []
    
    try:
        # Convert date and run_time to forecast time format (YYYYMMDDHHMMSS)
        if isinstance(forecast_date, str):
            dt = datetime.strptime(forecast_date, '%Y-%m-%d')
        else:
            dt = forecast_date
        
        # Format: YYYYMMDDHHMMSS (with run_time as HH and 00 for minutes and seconds)
        forecast_time = dt.strftime('%Y%m%d') + run_time.zfill(2) + '0000'
        
        # Create stage directory path
        stage_dir = f"@TC_BUFR_STAGE/{dt.strftime('%Y%m%d')}_{run_time.zfill(2)}"
        
        # Get list of TC files from ECMWF DISS
        forecast_url = f"{BASE_URL}/file/{forecast_time}/"
        
        response = requests.get(forecast_url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)
        
        # Extract .bin files that are tropical cyclone tracks
        tc_files = []
        for link in links:
            text = link.get_text().strip()
            if text and text.endswith('.bin') and 'tropical_cyclone_track' in text.lower():
                # Check if it's from EP (ensemble prediction)
                if text[10:12] == 'EP':
                    # Filter for named storms only (not numbered invest systems)
                    match = re.search(r'tropical_cyclone_track_([^_]+)_', text)
                    if match:
                        storm_part = match.group(1)
                        # Check if it's a proper name (not just numbers like "70U", "73E")
                        if len(storm_part) > 2 and not (storm_part[:2].isdigit() and len(storm_part) == 3):
                            tc_files.append(text)
        
        if not tc_files:
            results.append((
                None,
                0,
                'NO_FILES',
                f'No TC track files found for {forecast_date} {run_time}Z'
            ))
            # Convert results to Snowpark DataFrame
            schema = ['FILE_PATH', 'FILE_SIZE_BYTES', 'STATUS', 'MESSAGE']
            return session.create_dataframe(results, schema=schema)
        
        # Download each file
        for filename in tc_files:
            try:
                file_url = f"{BASE_URL}/file/{forecast_time}/{filename}"
                file_response = requests.get(file_url, timeout=60)
                file_response.raise_for_status()
                
                # Validate BUFR format
                is_valid_bufr = file_response.content[:4] == b'BUFR'
                
                if not is_valid_bufr:
                    results.append((
                        None,
                        len(file_response.content),
                        'INVALID',
                        f'{filename}: Not a valid BUFR file'
                    ))
                    continue
                
                # Write to temporary file with correct filename
                # Create temp directory and write file with original name
                temp_dir = tempfile.mkdtemp()
                temp_path = os.path.join(temp_dir, filename)
                
                with open(temp_path, 'wb') as temp_file:
                    temp_file.write(file_response.content)
                
                try:
                    # PUT file to stage (will use the filename from temp_path)
                    put_result = session.file.put(
                        local_file_name=temp_path,
                        stage_location=stage_dir,
                        auto_compress=False,
                        overwrite=True
                    )
                    
                    # Build stage file path
                    stage_file_path = f"{stage_dir}/{filename}"
                    file_size = len(file_response.content)
                    
                    # Insert into FILE_PROCESSING_LOG
                    session.sql(f"""
                        INSERT INTO FILE_PROCESSING_LOG (
                            FILE_PATH,
                            FILE_TYPE,
                            FORECAST_DATE,
                            RUN_TIME,
                            FILE_SIZE_BYTES,
                            PROCESSING_STATUS,
                            CREATED_AT,
                            UPDATED_AT
                        ) VALUES (
                            '{stage_file_path}',
                            'BUFR',
                            '{forecast_date}',
                            '{run_time.zfill(2)}',
                            {file_size},
                            'PENDING',
                            CURRENT_TIMESTAMP(),
                            CURRENT_TIMESTAMP()
                        )
                    """).collect()
                    
                    results.append((
                        stage_file_path,
                        file_size,
                        'SUCCESS',
                        f'Downloaded and staged: {filename}'
                    ))
                    
                finally:
                    # Clean up temp file and directory
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                    if os.path.exists(temp_dir):
                        os.rmdir(temp_dir)
                    
            except Exception as e:
                results.append((
                    None,
                    0,
                    'ERROR',
                    f'{filename}: {str(e)}'
                ))
        
        # Convert results to Snowpark DataFrame
        schema = ['FILE_PATH', 'FILE_SIZE_BYTES', 'STATUS', 'MESSAGE']
        return session.create_dataframe(results, schema=schema)
        
    except Exception as e:
        results.append((
            None,
            0,
            'ERROR',
            f'Failed to download TC files: {str(e)}'
        ))
        # Convert results to Snowpark DataFrame
        schema = ['FILE_PATH', 'FILE_SIZE_BYTES', 'STATUS', 'MESSAGE']
        return session.create_dataframe(results, schema=schema)
$$;

-- ============================================================================
-- Verify procedure created
-- ============================================================================

SHOW PROCEDURES LIKE 'download_tc_bufr_files';

DESC PROCEDURE download_tc_bufr_files(DATE, VARCHAR);

SELECT 
    '✓ Procedure created successfully' as status,
    'download_tc_bufr_files(DATE, VARCHAR)' as procedure_name,
    'Downloads TC BUFR files to @TC_BUFR_STAGE' as purpose;

