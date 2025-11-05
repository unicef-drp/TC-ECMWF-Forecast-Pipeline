-- ============================================================================
-- Stored Procedure: process_pending_bufr_files
-- ============================================================================
-- Processes all BUFR files with PROCESSING_STATUS = 'PENDING'
-- This is the stored procedure version of process_bufr_files.ipynb
-- Designed to be called by Snowflake Tasks for automated processing
-- ============================================================================

USE ROLE AOTS_ROLE;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

CREATE OR REPLACE PROCEDURE process_pending_bufr_files()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
RESOURCE_CONSTRAINT = (architecture='x86')
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_pending_bufr_files'
AS
$$
def process_pending_bufr_files(session):
    """
    Process all PENDING BUFR files from FILE_PROCESSING_LOG.
    
    Returns:
        Summary string with processing results
    """
    from datetime import datetime
    
    # Find unprocessed BUFR files (using fully qualified names)
    unprocessed_files = session.sql("""
        SELECT 
            FILE_PATH,
            FILE_TYPE,
            FORECAST_DATE,
            RUN_TIME,
            CREATED_AT
        FROM AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
        WHERE FILE_TYPE = 'BUFR'
          AND PROCESSING_STATUS = 'PENDING'
        ORDER BY FORECAST_DATE, RUN_TIME, FILE_PATH
    """).collect()
    
    if not unprocessed_files:
        return "No unprocessed BUFR files found"
    
    processed_count = 0
    failed_count = 0
    total_records = 0
    errors = []
    
    for file_info in unprocessed_files:
        file_path = file_info['FILE_PATH']
        forecast_date = file_info['FORECAST_DATE']
        run_time = file_info['RUN_TIME']
        
        filename = file_path.split('/')[-1]
        
        try:
            # Update status to PROCESSING
            session.sql(f"""
                UPDATE AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
                SET PROCESSING_STATUS = 'PROCESSING',
                    PROCESSING_START_TIME = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE FILE_PATH = '{file_path}'
            """).collect()
            
            # Extract BUFR data (LONG format - one row per threshold/quadrant)
            extract_sql = f"""
                SELECT 
                    SOURCE_FILE,
                    FORECAST_TIME,
                    STORM_IDENTIFIER,
                    ENSEMBLE_MEMBER,
                    STEP,
                    DATETIME,
                    LATITUDE,
                    LONGITUDE,
                    PRESSURE,
                    WIND_SPEED,
                    WLATITUDE,
                    WLONGITUDE,
                    WIND_THRESHOLD,
                    QUADRANT,
                    WIND_RADIUS_M,
                    BEARING_START,
                    BEARING_END
                FROM TABLE(AOTS.ECMWF_PIPELINE.extract_bufr_file('{file_path}'))
            """
            
            extracted_data = session.sql(extract_sql).collect()
            
            # Check for errors in extraction
            if extracted_data and extracted_data[0]['STORM_IDENTIFIER'].startswith('ERROR'):
                error_msg = extracted_data[0]['STORM_IDENTIFIER']
                raise Exception(f"Extraction failed: {error_msg}")
            
            if not extracted_data:
                raise Exception("No data extracted from file")
            
            record_count = len(extracted_data)
            
            # Insert into staging table (TC_RAW_EXTRACTED in LONG format)
            insert_sql = f"""
                INSERT INTO AOTS.ECMWF_PIPELINE.TC_RAW_EXTRACTED (
                    SOURCE_FILE,
                    FORECAST_TIME,
                    STORM_IDENTIFIER,
                    ENSEMBLE_MEMBER,
                    STEP,
                    DATETIME,
                    LATITUDE,
                    LONGITUDE,
                    PRESSURE,
                    WIND_SPEED,
                    WLATITUDE,
                    WLONGITUDE,
                    WIND_THRESHOLD,
                    QUADRANT,
                    WIND_RADIUS_M,
                    BEARING_START,
                    BEARING_END
                )
                SELECT 
                    SOURCE_FILE,
                    FORECAST_TIME,
                    STORM_IDENTIFIER,
                    ENSEMBLE_MEMBER,
                    STEP,
                    DATETIME,
                    LATITUDE,
                    LONGITUDE,
                    PRESSURE,
                    WIND_SPEED,
                    WLATITUDE,
                    WLONGITUDE,
                    WIND_THRESHOLD,
                    QUADRANT,
                    WIND_RADIUS_M,
                    BEARING_START,
                    BEARING_END
                FROM TABLE(AOTS.ECMWF_PIPELINE.extract_bufr_file('{file_path}'))
            """
            
            session.sql(insert_sql).collect()
            
            # Update status to COMPLETED
            session.sql(f"""
                UPDATE AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
                SET PROCESSING_STATUS = 'COMPLETED',
                    PROCESSING_END_TIME = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    RECORDS_EXTRACTED = {record_count},
                    ERROR_MESSAGE = NULL
                WHERE FILE_PATH = '{file_path}'
            """).collect()
            
            processed_count += 1
            total_records += record_count
            
        except Exception as e:
            error_msg = str(e)[:500]
            errors.append(f"{filename}: {error_msg}")
            
            # Update status to FAILED
            session.sql(f"""
                UPDATE AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
                SET PROCESSING_STATUS = 'FAILED',
                    PROCESSING_END_TIME = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    ERROR_MESSAGE = '{error_msg.replace("'", "''")}'
                WHERE FILE_PATH = '{file_path}'
            """).collect()
            
            failed_count += 1
    
    # Build summary
    summary = f"Processed {processed_count}/{len(unprocessed_files)} files, {total_records} records extracted"
    if failed_count > 0:
        summary += f" ({failed_count} failed)"
    
    return summary
$$;

-- Test the procedure
SELECT '✓ Stored procedure created' as status,
       'process_pending_bufr_files()' as procedure_name,
       'Ready to be called by tasks' as usage;

-- Test execution (uncomment to test)
-- CALL process_pending_bufr_files();

