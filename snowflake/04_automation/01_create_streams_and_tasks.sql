-- ============================================================================
-- 07: Create Streams and Tasks for Event-Driven Processing
-- ============================================================================
-- Sets up event-driven processing using Snowflake Streams and Tasks
-- 
-- Architecture:
--   1. Stream captures new files added to FILE_PROCESSING_LOG
--   2. Task monitors stream and triggers processing notebook
--   3. Notebook processes all PENDING files
--   4. Stream is consumed after successful processing
--
-- This enables automatic processing whenever new files are downloaded
-- ============================================================================
-- Prerequisites:
--   - Database: AOTS
--   - Schema: ECMWF_PIPELINE
--   - Role: SYSADMIN (to create streams and tasks)
--   - Warehouse: AOTS_X86_WH
--   - Tasks will execute using AOTS_ROLE (set in task definition)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- STREAM: new_bufr_files_stream
-- ============================================================================
-- Captures INSERT operations on FILE_PROCESSING_LOG for BUFR files
-- This stream acts as a change data capture (CDC) mechanism

CREATE OR REPLACE STREAM new_bufr_files_stream
ON TABLE FILE_PROCESSING_LOG
APPEND_ONLY = TRUE  -- Only track inserts, not updates/deletes
COMMENT = 'Captures new BUFR files added to FILE_PROCESSING_LOG for event-driven processing';

-- Verify stream created
SELECT 
    '✓ Stream created' as status,
    'new_bufr_files_stream' as stream_name,
    'Watching FILE_PROCESSING_LOG for new BUFR files' as description;

-- ============================================================================
-- TASK: process_new_bufr_files_task
-- ============================================================================
-- Monitors the stream and executes processing notebook when new files arrive
-- Runs every 2 minutes to check for new files

CREATE OR REPLACE TASK process_new_bufr_files_task
WAREHOUSE = AOTS_X86_WH
SCHEDULE = 'USING CRON */2 * * * * UTC'  -- Every 2 minutes
COMMENT = 'Event-driven task: processes BUFR files when new entries detected in stream'
WHEN
    -- Only run if stream has new data (new files were added)
    SYSTEM$STREAM_HAS_DATA('new_bufr_files_stream')
AS
    -- Call the processing stored procedure
    -- Note: The procedure processes ALL PENDING files, not just the new ones
    -- This ensures any missed files are also picked up
    -- Task executes with SYSADMIN privileges (creator role)
    CALL process_pending_bufr_files();

-- Note: Task is created in SUSPENDED state
-- Enable with: ALTER TASK process_new_bufr_files_task RESUME;

-- ============================================================================
-- Optional: Create task execution log table
-- ============================================================================
-- Tracks when the task runs for monitoring purposes

CREATE TABLE IF NOT EXISTS TASK_EXECUTION_LOG (
    TASK_NAME VARCHAR,
    EXECUTION_TIME TIMESTAMP_NTZ,
    STATUS VARCHAR,
    MESSAGE VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Logs task executions for monitoring and debugging';

-- ============================================================================
-- Verification and Management Queries
-- ============================================================================

-- Check stream status
SELECT 
    'Stream Status' as info,
    SYSTEM$STREAM_HAS_DATA('new_bufr_files_stream') as has_data,
    (SELECT COUNT(*) FROM new_bufr_files_stream) as records_in_stream;

-- Preview what's in the stream
SELECT 
    FILE_PATH,
    FILE_TYPE,
    FORECAST_DATE,
    RUN_TIME,
    PROCESSING_STATUS,
    METADATA$ACTION as stream_action,
    METADATA$ISUPDATE as is_update
FROM new_bufr_files_stream
WHERE FILE_TYPE = 'BUFR'
LIMIT 10;

-- Check task status
SHOW TASKS LIKE 'process_new_bufr_files_task';

-- View task definition
DESC TASK process_new_bufr_files_task;

-- ============================================================================
-- Task Management Commands
-- ============================================================================

-- Enable the task (run this after uploading the notebook)
-- ALTER TASK process_new_bufr_files_task RESUME;

-- Disable the task
-- ALTER TASK process_new_bufr_files_task SUSPEND;

-- Manually trigger the task (for testing)
-- EXECUTE TASK process_new_bufr_files_task;

-- View task run history
-- SELECT * 
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'PROCESS_NEW_BUFR_FILES_TASK',
--     SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- View task execution log
-- SELECT * 
-- FROM TASK_EXECUTION_LOG 
-- ORDER BY EXECUTION_TIME DESC 
-- LIMIT 20;

-- ============================================================================
-- Stream Management
-- ============================================================================

-- Reset stream (consume all current data without processing)
-- Useful if you want to clear the backlog
/*
BEGIN
    -- This query consumes the stream without doing anything
    SELECT COUNT(*) FROM new_bufr_files_stream;
END;
*/

-- Check stream lag (how far behind is it?)
/*
SELECT 
    'new_bufr_files_stream' as stream_name,
    SYSTEM$STREAM_GET_TABLE_TIMESTAMP('new_bufr_files_stream') as stream_position,
    CURRENT_TIMESTAMP() as current_time;
*/

-- ============================================================================
-- Setup Complete
-- ============================================================================

SELECT 
    '✓ Event-driven processing setup complete!' as status,
    'Stream: new_bufr_files_stream' as stream_name,
    'Task: process_new_bufr_files_task (SUSPENDED)' as task_name,
    'Next: Upload process_bufr_files.ipynb and RESUME task' as next_step;

-- ============================================================================
-- How It Works
-- ============================================================================
/*
Event Flow:
1. Download procedure inserts new files into FILE_PROCESSING_LOG
2. Stream captures these INSERT operations
3. Task checks stream every 2 minutes
4. If stream has data (new files), task triggers notebook
5. Notebook processes ALL PENDING BUFR files
6. Files are marked COMPLETED in FILE_PROCESSING_LOG
7. Stream is consumed (delta is cleared)
8. Process repeats for next batch of files

Benefits:
- Automatic processing when files arrive
- No fixed schedule needed
- Efficient (only runs when there's work to do)
- Handles batch arrivals well
- Resilient to failures (pending files will be retried)

Monitoring:
- Check TASK_EXECUTION_LOG for trigger history
- Check TASK_HISTORY for task run status
- Check stream with SYSTEM$STREAM_HAS_DATA()
- Check FILE_PROCESSING_LOG for processing status
*/

-- ============================================================================
-- GRIB2 WIND FILE PROCESSING
-- ============================================================================

-- ============================================================================
-- STREAM: new_grib_files_stream
-- ============================================================================
-- Captures INSERT operations on FILE_PROCESSING_LOG for GRIB2 files

CREATE OR REPLACE STREAM new_grib_files_stream
ON TABLE FILE_PROCESSING_LOG
APPEND_ONLY = TRUE
COMMENT = 'Captures new GRIB2 files added to FILE_PROCESSING_LOG for event-driven processing';

-- Verify stream created
SELECT 
    '✓ GRIB2 stream created' as status,
    'new_grib_files_stream' as stream_name,
    'Watching FILE_PROCESSING_LOG for new GRIB2 files' as description;

-- ============================================================================
-- TASK: process_new_grib_files_task
-- ============================================================================
-- Monitors the stream and processes GRIB2 files when new entries arrive
-- Runs every 2 minutes to check for new files

CREATE OR REPLACE TASK process_new_grib_files_task
WAREHOUSE = AOTS_X86_WH
SCHEDULE = 'USING CRON */2 * * * * UTC'  -- Every 2 minutes
COMMENT = 'Event-driven task: processes GRIB2 files when new entries detected in stream'
WHEN
    -- Only run if stream has new GRIB2 data
    SYSTEM$STREAM_HAS_DATA('new_grib_files_stream')
AS
    -- Call the processing stored procedure
    -- Task executes with SYSADMIN privileges (creator role)
    CALL process_pending_grib_files();

-- Note: Task is created in SUSPENDED state
-- Enable with: ALTER TASK process_new_grib_files_task RESUME;

SELECT 
    '✓ GRIB2 task created' as status,
    'process_new_grib_files_task (SUSPENDED)' as task_name,
    'Enable with: ALTER TASK process_new_grib_files_task RESUME' as next_step;

-- ============================================================================
-- Summary of Event-Driven Processing
-- ============================================================================

SELECT 
    '✓ Event-driven processing setup complete!' as status,
    'BUFR: new_bufr_files_stream → process_new_bufr_files_task' as bufr_flow,
    'GRIB2: new_grib_files_stream → process_new_grib_files_task' as grib_flow,
    'Both tasks check every 2 minutes when stream has data' as schedule;