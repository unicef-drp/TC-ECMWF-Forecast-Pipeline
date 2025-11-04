-- ============================================================================
-- 02: Create Stages for TC-ECMWF Forecast Pipeline
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Stage 1: TC BUFR Files
-- ============================================================================
-- Stores BUFR files downloaded from ECMWF containing tropical cyclone tracks
-- Files organized by date and run time: @TC_BUFR_STAGE/YYYYMMDD_HH/filename.bin
-- No file format needed - binary files stored as-is

CREATE STAGE IF NOT EXISTS TC_BUFR_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for BUFR files containing TC track forecasts from ECMWF. Files organized by forecast date and run time (00Z, 06Z, 12Z, 18Z).';

-- ============================================================================
-- Stage 2: Wind GRIB2 Files
-- ============================================================================
-- Stores GRIB2 files downloaded from ECMWF containing ensemble wind forecasts
-- Files organized by date and run time: @TC_WIND_STAGE/YYYYMMDD_HH/filename.grib2
-- No file format needed - binary files stored as-is

CREATE STAGE IF NOT EXISTS TC_WIND_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for GRIB2 files containing ensemble wind forecasts from ECMWF. Files organized by forecast date and run time.';

-- ============================================================================
-- Verify stages created
-- ============================================================================

SHOW STAGES LIKE 'TC_%_STAGE';

-- Display stage information
SELECT 
    'TC_BUFR_STAGE' as stage_name,
    'BUFR files (TC tracks)' as purpose,
    '@TC_BUFR_STAGE/YYYYMMDD_HH/*.bin' as path_pattern
UNION ALL
SELECT 
    'TC_WIND_STAGE' as stage_name,
    'GRIB2 files (ensemble wind)' as purpose,
    '@TC_WIND_STAGE/YYYYMMDD_HH/*.grib2' as path_pattern;

-- Test stage access (should return empty result if no files yet)
LIST @TC_BUFR_STAGE;
LIST @TC_WIND_STAGE;

SELECT 
    '✓ Stages created successfully' as status,
    '@TC_BUFR_STAGE and @TC_WIND_STAGE' as stage_names,
    'Ready for file uploads' as next_step;

