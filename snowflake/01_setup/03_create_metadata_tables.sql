-- ============================================================================
-- 03: Create Metadata Tables for TC-ECMWF Forecast Pipeline
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Table: FILE_PROCESSING_LOG
-- ============================================================================
-- Tracks all files downloaded and processed through the pipeline
-- Enables incremental loading by recording processing status
-- Primary key: FILE_PATH (unique identifier for each file)

CREATE TABLE IF NOT EXISTS FILE_PROCESSING_LOG (
    -- File identification
    FILE_PATH VARCHAR PRIMARY KEY
        COMMENT 'Full path to file in stage (e.g., @TC_BUFR_STAGE/20251015_18/filename.bin)',
    
    FILE_TYPE VARCHAR NOT NULL
        COMMENT 'Type of file: BUFR (TC tracks) or GRIB2 (wind data)',
    
    FORECAST_DATE DATE
        COMMENT 'Forecast initialization date (YYYY-MM-DD)',
    
    RUN_TIME VARCHAR(2)
        COMMENT 'Forecast run time: 00, 06, 12, or 18 (UTC)',
    
    FILE_SIZE_BYTES NUMBER
        COMMENT 'Size of file in bytes',
    
    -- Processing status tracking
    PROCESSING_STATUS VARCHAR NOT NULL
        COMMENT 'Current status: PENDING, PROCESSING, COMPLETED, FAILED',
    
    PROCESSING_START_TIME TIMESTAMP_NTZ
        COMMENT 'When processing started for this file',
    
    PROCESSING_END_TIME TIMESTAMP_NTZ
        COMMENT 'When processing completed (or failed) for this file',
    
    ERROR_MESSAGE VARCHAR
        COMMENT 'Error message if processing failed',
    
    -- Processing metrics
    RECORDS_EXTRACTED NUMBER
        COMMENT 'Number of records extracted from this file',
    
    RECORDS_TRANSFORMED NUMBER
        COMMENT 'Number of records successfully transformed',
    
    -- Audit fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this record was created',
    
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this record was last updated'
    
    -- Note: Snowflake does not support CHECK constraints
    -- Validation for FILE_TYPE (BUFR, GRIB2), RUN_TIME (00, 06, 12, 18), 
    -- and PROCESSING_STATUS (PENDING, PROCESSING, COMPLETED, FAILED) 
    -- should be enforced in application logic
)
COMMENT = 'Logs all files downloaded and processed. Used for incremental loading and error tracking.';

-- ============================================================================
-- Verify table created
-- ============================================================================

DESC TABLE FILE_PROCESSING_LOG;

-- Show table structure with comments
SELECT 
    column_name,
    data_type,
    comment
FROM AOTS.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'ECMWF_PIPELINE'
  AND table_name = 'FILE_PROCESSING_LOG'
ORDER BY ordinal_position;

SELECT 
    '✓ Metadata table created successfully' as status,
    'FILE_PROCESSING_LOG' as table_name,
    'Ready to track file processing' as next_step;