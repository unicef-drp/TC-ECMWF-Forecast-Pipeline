-- ============================================================================
-- Stored Procedure: process_pending_grib_files
-- ============================================================================
-- Marks GRIB2 files as COMPLETED (ready for on-demand processing)
-- Wind data is now processed on-demand in create_wind_envelopes procedure
-- No raw wind data is stored - matching Python approach
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
    Mark PENDING GRIB2 files as COMPLETED (ready for on-demand processing).
    
    Wind data is now processed on-demand in create_wind_envelopes procedure.
    No raw wind data is stored - matching Python approach.
    
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
        FROM AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
        WHERE FILE_TYPE = 'GRIB2'
          AND PROCESSING_STATUS = 'PENDING'
        ORDER BY FORECAST_DATE, RUN_TIME, FILE_PATH
    """).collect()
    
    if not unprocessed_files:
        return "No unprocessed GRIB2 files found"
    
    processed_count = 0
    
    for file_info in unprocessed_files:
        file_path = file_info['FILE_PATH']
        
        try:
            # Mark file as COMPLETED (ready for on-demand processing in create_wind_envelopes)
            # No extraction needed - wind data will be loaded on-demand when creating envelopes
            session.sql(f"""
                UPDATE AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
                SET PROCESSING_STATUS = 'COMPLETED',
                    PROCESSING_START_TIME = CURRENT_TIMESTAMP(),
                    PROCESSING_END_TIME = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    RECORDS_EXTRACTED = NULL,
                    ERROR_MESSAGE = NULL
                WHERE FILE_PATH = '{file_path}'
            """).collect()
            
            processed_count += 1
            
        except Exception as e:
            error_msg = str(e)[:500]
            
            # Update status to FAILED
            session.sql(f"""
                UPDATE AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
                SET PROCESSING_STATUS = 'FAILED',
                    PROCESSING_END_TIME = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    ERROR_MESSAGE = 'Error marking file as ready: {error_msg.replace("'", "''")}'
                WHERE FILE_PATH = '{file_path}'
            """).collect()
    
    # Build summary
    summary = f"Marked {processed_count}/{len(unprocessed_files)} GRIB2 files as ready for on-demand processing"
    
    return summary
$$;

-- Test the procedure
SELECT '✓ Stored procedure created' as status,
       'process_pending_grib_files()' as procedure_name,
       'Ready to be called by tasks' as usage;

-- Test execution (uncomment to test)
-- CALL process_pending_grib_files();

