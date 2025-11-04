-- ============================================================================
-- Populate ALL Wind Envelopes from TC_ENVELOPES_INDIVIDUAL
-- ============================================================================
-- Creates combined wind envelopes for all storms, forecasts, and ensembles
-- Uses ST_COLLECT to create geometry collections (works with ST_INTERSECTS)
-- ============================================================================

USE ROLE AOTS_ROLE;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Check Current State
-- ============================================================================

SELECT 'Current envelope status...' as status;

SELECT 
    'TC_ENVELOPES_INDIVIDUAL' as table_name,
    COUNT(*) as total_envelopes,
    COUNT(DISTINCT TRACK_ID) as storms,
    COUNT(DISTINCT FORECAST_TIME) as forecast_times,
    COUNT(DISTINCT ENSEMBLE_MEMBER) as ensemble_members
FROM TC_ENVELOPES_INDIVIDUAL;

SELECT 
    'TC_ENVELOPES_COMBINED' as table_name,
    COUNT(*) as total_combined,
    COUNT(DISTINCT TRACK_ID) as storms,
    COUNT(DISTINCT FORECAST_TIME) as forecast_times,
    COUNT(DISTINCT ENSEMBLE_MEMBER) as ensemble_members
FROM TC_ENVELOPES_COMBINED;

-- ============================================================================
-- Clean Existing Combined Envelopes
-- ============================================================================

SELECT 'Cleaning TC_ENVELOPES_COMBINED...' as status;

TRUNCATE TABLE TC_ENVELOPES_COMBINED;

-- ============================================================================
-- Populate Combined Envelopes for ALL storms/forecasts/ensembles
-- ============================================================================

SELECT 'Populating combined envelopes for all data...' as status;
SELECT 'This may take a few minutes...' as info;

INSERT INTO TC_ENVELOPES_COMBINED (
    FORECAST_TIME,
    TRACK_ID,
    ENSEMBLE_MEMBER,
    LEAD_TIME_RANGE,
    WIND_THRESHOLD,
    ENVELOPE_REGION
)
SELECT 
    FORECAST_TIME,
    TRACK_ID,
    ENSEMBLE_MEMBER,
    MIN(LEAD_TIME) as LEAD_TIME_RANGE,  -- Use minimum lead time as INTEGER (matches loader behavior)
    WIND_THRESHOLD,
    ST_COLLECT(ENVELOPE_REGION) as ENVELOPE_REGION
FROM TC_ENVELOPES_INDIVIDUAL
WHERE ENVELOPE_REGION IS NOT NULL
GROUP BY 
    FORECAST_TIME,
    TRACK_ID,
    ENSEMBLE_MEMBER,
    WIND_THRESHOLD;

-- ============================================================================
-- Verification
-- ============================================================================

SELECT '========== RESULTS ==========' as title;

-- Summary by storm
SELECT 
    TRACK_ID,
    COUNT(DISTINCT FORECAST_TIME) as forecast_runs,
    COUNT(DISTINCT ENSEMBLE_MEMBER) as ensemble_members,
    COUNT(DISTINCT WIND_THRESHOLD) as thresholds,
    COUNT(*) as total_combined_envelopes
FROM TC_ENVELOPES_COMBINED
GROUP BY TRACK_ID
ORDER BY TRACK_ID;

-- Overall summary
SELECT 
    COUNT(*) as total_envelopes,
    COUNT(DISTINCT TRACK_ID) as storms,
    COUNT(DISTINCT FORECAST_TIME) as forecast_times,
    COUNT(DISTINCT ENSEMBLE_MEMBER) as ensemble_members,
    COUNT(DISTINCT WIND_THRESHOLD) as thresholds
FROM TC_ENVELOPES_COMBINED;


SELECT 
    TRACK_ID,
    TO_VARCHAR(FORECAST_TIME, 'YYYY-MM-DD HH24:MI') as FORECAST,
    ENSEMBLE_MEMBER,
    WIND_THRESHOLD,
    LEAD_TIME_RANGE
FROM TC_ENVELOPES_COMBINED
WHERE TRACK_ID IN (SELECT DISTINCT TRACK_ID FROM TC_ENVELOPES_COMBINED LIMIT 1)  -- Use any available storm
  AND ENSEMBLE_MEMBER IN (1, 25, 50)
ORDER BY TRACK_ID, FORECAST_TIME DESC, ENSEMBLE_MEMBER, WIND_THRESHOLD
LIMIT 20;

SELECT 'All wind envelopes populated' as status,
       '' as result;


