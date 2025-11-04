-- ============================================================================
-- Stored Procedure: process_pending_grib_files
-- ============================================================================
-- Processes all GRIB2 files with PROCESSING_STATUS = 'PENDING'
-- Extracts wind data and loads into WIND_RAW_EXTRACTED table
-- Designed to be called by Snowflake Tasks for automated processing
-- ============================================================================

USE ROLE AOTS_ROLE;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

CREATE OR REPLACE PROCEDURE process_pending_grib_files()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
RESOURCE_CONSTRAINT = (architecture='x86')
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_pending_grib_files'
AS
$$
def process_pending_grib_files(session):
    """
    Process all PENDING GRIB2 files from FILE_PROCESSING_LOG.
    
    Returns:
        Summary string with processing results
    """
    from datetime import datetime
    
    # Find unprocessed GRIB2 files
    unprocessed_files = session.sql("""
        SELECT 
            FILE_PATH,
            FILE_TYPE,
            FORECAST_DATE,
            RUN_TIME,
            CREATED_AT
        FROM FILE_PROCESSING_LOG
        WHERE FILE_TYPE = 'GRIB2'
          AND PROCESSING_STATUS = 'PENDING'
        ORDER BY FORECAST_DATE, RUN_TIME, FILE_PATH
    """).collect()
    
    if not unprocessed_files:
        return "No unprocessed GRIB2 files found"
    
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
                UPDATE FILE_PROCESSING_LOG
                SET PROCESSING_STATUS = 'PROCESSING',
                    PROCESSING_START_TIME = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE FILE_PATH = '{file_path}'
            """).collect()
            
            # Extract GRIB2 wind data
            extract_sql = f"""
                SELECT 
                    SOURCE_FILE,
                    FORECAST_TIME,
                    VALID_TIME,
                    LEAD_TIME,
                    ENSEMBLE_MEMBER,
                    LATITUDE,
                    LONGITUDE,
                    WIND_U_COMPONENT,
                    WIND_V_COMPONENT,
                    WIND_SPEED_10M
                FROM TABLE(extract_wind_grib_file('{file_path}'))
            """
            
            extracted_data = session.sql(extract_sql).collect()
            
            # Check for errors in extraction
            if extracted_data and extracted_data[0]['FORECAST_TIME'] is None:
                raise Exception("Extraction failed - no valid data")
            
            if not extracted_data:
                raise Exception("No data extracted from file")
            
            record_count = len(extracted_data)
            
            # Insert into staging table (WIND_RAW_EXTRACTED)
            insert_sql = f"""
                INSERT INTO WIND_RAW_EXTRACTED (
                    SOURCE_FILE,
                    FORECAST_TIME,
                    VALID_TIME,
                    LEAD_TIME,
                    ENSEMBLE_MEMBER,
                    LATITUDE,
                    LONGITUDE,
                    WIND_SPEED_10M,
                    WIND_U_COMPONENT,
                    WIND_V_COMPONENT
                )
                SELECT 
                    SOURCE_FILE,
                    FORECAST_TIME,
                    VALID_TIME,
                    LEAD_TIME,
                    ENSEMBLE_MEMBER,
                    LATITUDE,
                    LONGITUDE,
                    WIND_SPEED_10M,
                    WIND_U_COMPONENT,
                    WIND_V_COMPONENT
                FROM TABLE(extract_wind_grib_file('{file_path}'))
            """
            
            session.sql(insert_sql).collect()
            
            # Update status to COMPLETED
            session.sql(f"""
                UPDATE FILE_PROCESSING_LOG
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
                UPDATE FILE_PROCESSING_LOG
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
       'process_pending_grib_files()' as procedure_name,
       'Ready to be called by tasks' as usage;

-- Test execution (uncomment to test)
-- CALL process_pending_grib_files();

