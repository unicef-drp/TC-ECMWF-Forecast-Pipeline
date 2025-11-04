-- ============================================================================
-- Cost-Optimized Pipeline Monitoring
-- ============================================================================
-- Creates a lightweight summary table that gets updated periodically
-- instead of running expensive queries on-demand
-- ============================================================================

USE ROLE AOTS_ROLE;
USE WAREHOUSE AOTS_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Create Summary Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS PIPELINE_MONITORING_SUMMARY (
    SNAPSHOT_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- File processing status
    BUFR_PENDING INTEGER,
    BUFR_PROCESSING INTEGER,
    BUFR_COMPLETED INTEGER,
    BUFR_FAILED INTEGER,
    GRIB_PENDING INTEGER,
    GRIB_PROCESSING INTEGER,
    GRIB_COMPLETED INTEGER,
    GRIB_FAILED INTEGER,
    
    -- Timestamps
    LATEST_FORECAST_TIME_RAW TIMESTAMP_NTZ,
    LATEST_FORECAST_TIME_FINAL TIMESTAMP_NTZ,
    LATEST_FILE_ADDED TIMESTAMP_NTZ,
    LATEST_FILE_PROCESSED TIMESTAMP_NTZ,
    
    -- Calculated metrics
    HOURS_SINCE_LATEST_FORECAST INTEGER,
    HOURS_SINCE_LAST_PROCESSED INTEGER,
    
    -- Status indicators
    DATA_FRESHNESS_STATUS VARCHAR,  -- 'FRESH', 'WARNING', 'STALE'
    PROCESSING_HEALTH_STATUS VARCHAR  -- 'HEALTHY', 'WARNING', 'CRITICAL'
)
COMMENT = 'Summary table for pipeline monitoring - updated periodically';

-- Cluster by snapshot time for fast queries on latest data
ALTER TABLE PIPELINE_MONITORING_SUMMARY CLUSTER BY (SNAPSHOT_TIME);

-- ============================================================================
-- Procedure: Refresh Monitoring Summary
-- ============================================================================
-- This procedure updates the summary table with current metrics
-- Run this periodically (e.g., every 15-30 minutes) instead of querying views

CREATE OR REPLACE PROCEDURE refresh_monitoring_summary()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert new snapshot
    INSERT INTO PIPELINE_MONITORING_SUMMARY (
        BUFR_PENDING,
        BUFR_PROCESSING,
        BUFR_COMPLETED,
        BUFR_FAILED,
        GRIB_PENDING,
        GRIB_PROCESSING,
        GRIB_COMPLETED,
        GRIB_FAILED,
        LATEST_FORECAST_TIME_RAW,
        LATEST_FORECAST_TIME_FINAL,
        LATEST_FILE_ADDED,
        LATEST_FILE_PROCESSED,
        HOURS_SINCE_LATEST_FORECAST,
        HOURS_SINCE_LAST_PROCESSED,
        DATA_FRESHNESS_STATUS,
        PROCESSING_HEALTH_STATUS
    )
    SELECT 
        -- File counts from FILE_PROCESSING_LOG
        COUNT(CASE WHEN FILE_TYPE = 'BUFR' AND PROCESSING_STATUS = 'PENDING' THEN 1 END),
        COUNT(CASE WHEN FILE_TYPE = 'BUFR' AND PROCESSING_STATUS = 'PROCESSING' THEN 1 END),
        COUNT(CASE WHEN FILE_TYPE = 'BUFR' AND PROCESSING_STATUS = 'COMPLETED' THEN 1 END),
        COUNT(CASE WHEN FILE_TYPE = 'BUFR' AND PROCESSING_STATUS = 'FAILED' THEN 1 END),
        COUNT(CASE WHEN FILE_TYPE = 'GRIB2' AND PROCESSING_STATUS = 'PENDING' THEN 1 END),
        COUNT(CASE WHEN FILE_TYPE = 'GRIB2' AND PROCESSING_STATUS = 'PROCESSING' THEN 1 END),
        COUNT(CASE WHEN FILE_TYPE = 'GRIB2' AND PROCESSING_STATUS = 'COMPLETED' THEN 1 END),
        COUNT(CASE WHEN FILE_TYPE = 'GRIB2' AND PROCESSING_STATUS = 'FAILED' THEN 1 END),
        
        -- Timestamps
        (SELECT MAX(FORECAST_TIME) FROM TC_RAW_EXTRACTED),
        (SELECT MAX(FORECAST_TIME) FROM TC_TRACKS),
        (SELECT MAX(CREATED_AT) FROM FILE_PROCESSING_LOG),
        (SELECT MAX(PROCESSING_END_TIME) FROM FILE_PROCESSING_LOG WHERE PROCESSING_STATUS = 'COMPLETED'),
        
        -- Calculated metrics
        DATEDIFF('hour', (SELECT MAX(FORECAST_TIME) FROM TC_TRACKS), CURRENT_TIMESTAMP()),
        DATEDIFF('hour', (SELECT MAX(PROCESSING_END_TIME) FROM FILE_PROCESSING_LOG WHERE PROCESSING_STATUS = 'COMPLETED'), CURRENT_TIMESTAMP()),
        
        -- Status indicators
        CASE 
            WHEN DATEDIFF('hour', (SELECT MAX(FORECAST_TIME) FROM TC_TRACKS), CURRENT_TIMESTAMP()) > 24 THEN 'STALE'
            WHEN DATEDIFF('hour', (SELECT MAX(FORECAST_TIME) FROM TC_TRACKS), CURRENT_TIMESTAMP()) > 12 THEN 'WARNING'
            ELSE 'FRESH'
        END,
        CASE 
            WHEN COUNT(CASE WHEN PROCESSING_STATUS = 'FAILED' THEN 1 END) > 5 THEN 'CRITICAL'
            WHEN COUNT(CASE WHEN PROCESSING_STATUS = 'PENDING' THEN 1 END) > 10 THEN 'WARNING'
            ELSE 'HEALTHY'
        END
    FROM FILE_PROCESSING_LOG;
    
    -- Keep only last 7 days of snapshots (optional cleanup)
    DELETE FROM PIPELINE_MONITORING_SUMMARY 
    WHERE SNAPSHOT_TIME < DATEADD('day', -7, CURRENT_TIMESTAMP());
    
    RETURN 'Monitoring summary refreshed successfully';
END;
$$;

COMMENT ON PROCEDURE refresh_monitoring_summary() IS 'Refreshes pipeline monitoring summary table with current metrics';

-- ============================================================================
-- Views for Easy Querying
-- ============================================================================

-- Latest summary snapshot
CREATE OR REPLACE VIEW V_PIPELINE_MONITORING_LATEST AS
SELECT * FROM PIPELINE_MONITORING_SUMMARY
WHERE SNAPSHOT_TIME = (SELECT MAX(SNAPSHOT_TIME) FROM PIPELINE_MONITORING_SUMMARY);

COMMENT ON VIEW V_PIPELINE_MONITORING_LATEST IS 'Latest pipeline monitoring snapshot - fast and cheap to query';

-- Processing errors
CREATE OR REPLACE VIEW V_PROCESSING_ERRORS AS
SELECT 
    FILE_TYPE,
    FILE_PATH,
    FORECAST_DATE,
    RUN_TIME,
    PROCESSING_STATUS,
    ERROR_MESSAGE,
    PROCESSING_START_TIME,
    PROCESSING_END_TIME,
    DATEDIFF('second', PROCESSING_START_TIME, PROCESSING_END_TIME) as processing_duration_seconds,
    UPDATED_AT
FROM FILE_PROCESSING_LOG
WHERE PROCESSING_STATUS = 'FAILED'
   OR ERROR_MESSAGE IS NOT NULL
ORDER BY UPDATED_AT DESC;

COMMENT ON VIEW V_PROCESSING_ERRORS IS 'All failed file processing attempts with error details';

-- ============================================================================
-- Optional: Comprehensive Health Check Procedure
-- ============================================================================
-- NOTE: This procedure does expensive queries - use sparingly (on-demand)
-- For routine monitoring, use V_PIPELINE_MONITORING_LATEST instead

CREATE OR REPLACE PROCEDURE check_pipeline_health()
RETURNS TABLE(
    check_category VARCHAR,
    check_name VARCHAR,
    status VARCHAR,
    value VARCHAR,
    message VARCHAR
)
LANGUAGE SQL
AS
$$
BEGIN
    CREATE OR REPLACE TEMP TABLE health_check_results (
        check_category VARCHAR,
        check_name VARCHAR,
        status VARCHAR,
        value VARCHAR,
        message VARCHAR
    );
    
    -- File processing status
    INSERT INTO health_check_results
    SELECT 
        'FILE_PROCESSING' as check_category,
        'BUFR Files Status' as check_name,
        CASE 
            WHEN SUM(CASE WHEN PROCESSING_STATUS = 'FAILED' THEN 1 ELSE 0 END) > 5 THEN 'CRITICAL'
            WHEN SUM(CASE WHEN PROCESSING_STATUS = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'WARNING'
            WHEN SUM(CASE WHEN PROCESSING_STATUS = 'PENDING' THEN 1 ELSE 0 END) > 10 THEN 'BACKLOG'
            ELSE 'HEALTHY'
        END as status,
        CONCAT(
            SUM(CASE WHEN PROCESSING_STATUS = 'COMPLETED' THEN 1 ELSE 0 END), ' completed, ',
            SUM(CASE WHEN PROCESSING_STATUS = 'PENDING' THEN 1 ELSE 0 END), ' pending, ',
            SUM(CASE WHEN PROCESSING_STATUS = 'FAILED' THEN 1 ELSE 0 END), ' failed'
        ) as value,
        CASE 
            WHEN SUM(CASE WHEN PROCESSING_STATUS = 'FAILED' THEN 1 ELSE 0 END) > 0 
            THEN 'Some BUFR files failed to process'
            WHEN SUM(CASE WHEN PROCESSING_STATUS = 'PENDING' THEN 1 ELSE 0 END) > 10 
            THEN 'Large backlog of pending files'
            ELSE 'All BUFR files processing normally'
        END as message
    FROM FILE_PROCESSING_LOG
    WHERE FILE_TYPE = 'BUFR';
    
    -- Data freshness
    INSERT INTO health_check_results
    SELECT 
        'DATA_FRESHNESS' as check_category,
        'Latest Forecast Data' as check_name,
        CASE 
            WHEN MAX(FORECAST_TIME) IS NULL THEN 'CRITICAL'
            WHEN DATEDIFF('hour', MAX(FORECAST_TIME), CURRENT_TIMESTAMP()) > 24 THEN 'STALE'
            WHEN DATEDIFF('hour', MAX(FORECAST_TIME), CURRENT_TIMESTAMP()) > 12 THEN 'WARNING'
            ELSE 'FRESH'
        END as status,
        COALESCE(TO_VARCHAR(MAX(FORECAST_TIME), 'YYYY-MM-DD HH24:MI'), 'NO DATA') as value,
        CASE 
            WHEN MAX(FORECAST_TIME) IS NULL THEN 'No forecast data found in system'
            WHEN DATEDIFF('hour', MAX(FORECAST_TIME), CURRENT_TIMESTAMP()) > 24 
            THEN 'Forecast data is over 24 hours old'
            WHEN DATEDIFF('hour', MAX(FORECAST_TIME), CURRENT_TIMESTAMP()) > 12 
            THEN 'Forecast data is over 12 hours old'
            ELSE CONCAT('Latest forecast is ', DATEDIFF('hour', MAX(FORECAST_TIME), CURRENT_TIMESTAMP()), ' hours old')
        END as message
    FROM TC_TRACKS;
    
    -- Recent errors
    INSERT INTO health_check_results
    SELECT 
        'ERROR_TRACKING' as check_category,
        'Recent Processing Errors' as check_name,
        CASE 
            WHEN COUNT(*) > 10 THEN 'CRITICAL'
            WHEN COUNT(*) > 3 THEN 'WARNING'
            WHEN COUNT(*) > 0 THEN 'MINOR'
            ELSE 'CLEAN'
        END as status,
        CONCAT(COUNT(*), ' errors in last 24 hours') as value,
        CASE 
            WHEN COUNT(*) > 10 THEN 'High error rate - immediate attention needed'
            WHEN COUNT(*) > 3 THEN 'Multiple errors detected - investigate'
            WHEN COUNT(*) > 0 THEN 'Some errors detected - monitor'
            ELSE 'No recent errors'
        END as message
    FROM FILE_PROCESSING_LOG
    WHERE PROCESSING_STATUS = 'FAILED'
      AND UPDATED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP());
    
    LET result_cursor CURSOR FOR 
        SELECT * FROM health_check_results ORDER BY 
            CASE check_category
                WHEN 'FILE_PROCESSING' THEN 1
                WHEN 'DATA_FRESHNESS' THEN 2
                WHEN 'ERROR_TRACKING' THEN 3
            END,
            check_name;
    
    OPEN result_cursor;
    RETURN TABLE(result_cursor);
END;
$$;

COMMENT ON PROCEDURE check_pipeline_health() IS 'Comprehensive health check - use sparingly as it does expensive queries';

-- ============================================================================
-- Optional: Task to Auto-Refresh Summary
-- ============================================================================
-- Uncomment to create a task that refreshes the summary every 15 minutes
-- Note: The CRON schedule uses "every 15 minutes" syntax

-- CREATE OR REPLACE TASK refresh_monitoring_summary_task
-- WAREHOUSE = AOTS_WH
-- SCHEDULE = 'USING CRON 0,15,30,45 * * * * UTC'  -- Every 15 minutes (at :00, :15, :30, :45)
-- COMMENT = 'Refreshes pipeline monitoring summary table periodically'
-- AS
--     CALL refresh_monitoring_summary();
-- 
-- -- Enable the task
-- ALTER TASK refresh_monitoring_summary_task RESUME;


-- ============================================================================
-- Verification
-- ============================================================================

SELECT 'Monitoring setup complete' as status,
       'Summary table created - refresh with CALL refresh_monitoring_summary()' as note,
       'Query V_PIPELINE_MONITORING_LATEST for fast, cheap monitoring' as usage;

-- ============================================================================
-- Usage Examples
-- ============================================================================
/*
-- Get latest monitoring snapshot (cheap - recommended for frequent queries)
SELECT * FROM V_PIPELINE_MONITORING_LATEST;

-- Manually refresh summary
CALL refresh_monitoring_summary();

-- Check processing errors (cheap)
SELECT * FROM V_PROCESSING_ERRORS LIMIT 10;

-- Comprehensive health check (expensive - use sparingly)
CALL check_pipeline_health();

-- View historical snapshots
SELECT * FROM PIPELINE_MONITORING_SUMMARY 
ORDER BY SNAPSHOT_TIME DESC 
LIMIT 10;
*/

