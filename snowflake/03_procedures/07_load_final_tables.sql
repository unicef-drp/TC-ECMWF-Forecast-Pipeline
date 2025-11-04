-- ============================================================================
-- Stored Procedure: load_final_tables
-- ============================================================================
-- Loads transformed TC data from staging tables to final production tables
-- 
-- Target Tables:
-- TC_TRACKS - Main track data with all attributes
-- TC_ENVELOPES_INDIVIDUAL - Wind threshold envelopes per timestep
-- TC_ENVELOPES_COMBINED - Wind threshold envelopes combined across timesteps
-- 
-- Note: Envelopes can be populated either:
--       1. From WIND_ENVELOPES_STAGING (via this procedure)
--       2. Directly from create_wind_envelopes procedure (bypasses staging)
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_WH;  -- Use standard warehouse for data loading
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

CREATE OR REPLACE PROCEDURE load_final_tables()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- ========================================================================
    -- 1. Load TC_TRACKS
    -- ========================================================================
    
    -- Merge track data from staging (handles duplicates via primary key)
    MERGE INTO TC_TRACKS t
    USING TC_TRANSFORMED_STAGING s
    ON t.TRACK_ID = s.TRACK_ID 
        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
        AND t.FORECAST_TIME = s.FORECAST_TIME
        AND t.LEAD_TIME = s.LEAD_TIME
    WHEN NOT MATCHED THEN INSERT (
        FORECAST_TIME,
        TRACK_ID,
        ENSEMBLE_MEMBER,
        VALID_TIME,
        LEAD_TIME,
        LATITUDE,
        LONGITUDE,
        PRESSURE_HPA,
        WIND_SPEED_KNOTS,
        RADIUS_OF_MAXIMUM_WINDS_KM,
        RADIUS_34_KNOT_WINDS_NE_KM,
        RADIUS_34_KNOT_WINDS_SE_KM,
        RADIUS_34_KNOT_WINDS_SW_KM,
        RADIUS_34_KNOT_WINDS_NW_KM,
        RADIUS_50_KNOT_WINDS_NE_KM,
        RADIUS_50_KNOT_WINDS_SE_KM,
        RADIUS_50_KNOT_WINDS_SW_KM,
        RADIUS_50_KNOT_WINDS_NW_KM,
        RADIUS_64_KNOT_WINDS_NE_KM,
        RADIUS_64_KNOT_WINDS_SE_KM,
        RADIUS_64_KNOT_WINDS_SW_KM,
        RADIUS_64_KNOT_WINDS_NW_KM,
        WIND_FIELD_POLYGON_34KT,
        WIND_FIELD_POLYGON_50KT,
        WIND_FIELD_POLYGON_64KT
    ) VALUES (
        s.FORECAST_TIME,
        s.TRACK_ID,
        s.ENSEMBLE_MEMBER,
        s.VALID_TIME,
        s.LEAD_TIME,
        s.LATITUDE,
        s.LONGITUDE,
        s.PRESSURE_HPA,
        s.WIND_SPEED_KNOTS,
        s.RADIUS_OF_MAXIMUM_WINDS_KM,
        s.RADIUS_34_KNOT_WINDS_NE_KM,
        s.RADIUS_34_KNOT_WINDS_SE_KM,
        s.RADIUS_34_KNOT_WINDS_SW_KM,
        s.RADIUS_34_KNOT_WINDS_NW_KM,
        s.RADIUS_50_KNOT_WINDS_NE_KM,
        s.RADIUS_50_KNOT_WINDS_SE_KM,
        s.RADIUS_50_KNOT_WINDS_SW_KM,
        s.RADIUS_50_KNOT_WINDS_NW_KM,
        s.RADIUS_64_KNOT_WINDS_NE_KM,
        s.RADIUS_64_KNOT_WINDS_SE_KM,
        s.RADIUS_64_KNOT_WINDS_SW_KM,
        s.RADIUS_64_KNOT_WINDS_NW_KM,
        s.WIND_FIELD_POLYGON_34KT,
        s.WIND_FIELD_POLYGON_50KT,
        s.WIND_FIELD_POLYGON_64KT
    );
    
    LET tracks_loaded NUMBER := SQLROWCOUNT;
    
    -- ========================================================================
    -- 2. Load TC_ENVELOPES_INDIVIDUAL from WIND_ENVELOPES_STAGING
    -- ========================================================================
    
    MERGE INTO TC_ENVELOPES_INDIVIDUAL t
    USING (
        SELECT 
            FORECAST_TIME,
            TRACK_ID,
            ENSEMBLE_MEMBER,
            VALID_TIME,
            LEAD_TIME,
            WIND_THRESHOLD,
            CASE 
                WHEN ENVELOPE_REGION IS NOT NULL 
                     AND ENVELOPE_REGION != '' 
                     AND ENVELOPE_REGION != 'None'
                     AND ENVELOPE_REGION != 'null'
                THEN TRY_TO_GEOGRAPHY(ENVELOPE_REGION)
                ELSE NULL
            END AS ENVELOPE_REGION
        FROM WIND_ENVELOPES_STAGING
        WHERE ENVELOPE_TYPE = 'INDIVIDUAL'
    ) s
    ON t.TRACK_ID = s.TRACK_ID 
        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
        AND t.FORECAST_TIME = s.FORECAST_TIME
        AND t.LEAD_TIME = s.LEAD_TIME
        AND t.WIND_THRESHOLD = s.WIND_THRESHOLD
    WHEN NOT MATCHED THEN INSERT (
        FORECAST_TIME,
        TRACK_ID,
        ENSEMBLE_MEMBER,
        VALID_TIME,
        LEAD_TIME,
        WIND_THRESHOLD,
        ENVELOPE_REGION
    ) VALUES (
        s.FORECAST_TIME,
        s.TRACK_ID,
        s.ENSEMBLE_MEMBER,
        s.VALID_TIME,
        s.LEAD_TIME,
        s.WIND_THRESHOLD,
        s.ENVELOPE_REGION
    );
    
    LET individual_envelopes_loaded NUMBER := SQLROWCOUNT;
    
    -- ========================================================================
    -- 3. Load TC_ENVELOPES_COMBINED from WIND_ENVELOPES_STAGING
    -- ========================================================================
    
    MERGE INTO TC_ENVELOPES_COMBINED t
    USING (
        SELECT 
            c.FORECAST_TIME,
            c.TRACK_ID,
            c.ENSEMBLE_MEMBER,
            COALESCE(
                -- Try to parse LEAD_TIME_RANGE from VARCHAR (e.g., "0-144" -> 0)
                TRY_TO_NUMBER(SPLIT_PART(c.LEAD_TIME_RANGE, '-', 1)),
                -- Fallback: use MIN(LEAD_TIME) from individual envelopes
                (SELECT MIN(LEAD_TIME) 
                 FROM TC_ENVELOPES_INDIVIDUAL i
                 WHERE i.FORECAST_TIME = c.FORECAST_TIME
                   AND i.TRACK_ID = c.TRACK_ID
                   AND i.ENSEMBLE_MEMBER = c.ENSEMBLE_MEMBER
                   AND i.WIND_THRESHOLD = c.WIND_THRESHOLD),
                0
            ) as LEAD_TIME_RANGE,
            c.WIND_THRESHOLD,
            CASE 
                WHEN c.ENVELOPE_REGION IS NOT NULL 
                     AND c.ENVELOPE_REGION != '' 
                     AND c.ENVELOPE_REGION != 'None'
                     AND c.ENVELOPE_REGION != 'null'
                THEN TRY_TO_GEOGRAPHY(c.ENVELOPE_REGION)
                ELSE NULL
            END AS ENVELOPE_REGION
        FROM WIND_ENVELOPES_STAGING c
        WHERE c.ENVELOPE_TYPE = 'COMBINED'
    ) s
    ON t.TRACK_ID = s.TRACK_ID 
        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
        AND t.FORECAST_TIME = s.FORECAST_TIME
        AND t.WIND_THRESHOLD = s.WIND_THRESHOLD
    WHEN NOT MATCHED THEN INSERT (
        FORECAST_TIME,
        TRACK_ID,
        ENSEMBLE_MEMBER,
        LEAD_TIME_RANGE,
        WIND_THRESHOLD,
        ENVELOPE_REGION
    ) VALUES (
        s.FORECAST_TIME,
        s.TRACK_ID,
        s.ENSEMBLE_MEMBER,
        s.LEAD_TIME_RANGE,
        s.WIND_THRESHOLD,
        s.ENVELOPE_REGION
    );
    
    LET combined_envelopes_loaded NUMBER := SQLROWCOUNT;
    
    -- ========================================================================
    -- Summary
    -- ========================================================================
    
    RETURN 'Loaded ' || :tracks_loaded || ' tracks, ' 
           || :individual_envelopes_loaded || ' individual envelopes, '
           || :combined_envelopes_loaded || ' combined envelopes';
    
END;
$$;

-- Verify procedure created
SELECT '✓ Stored procedure created' as status,
       'load_final_tables()' as procedure_name,
       'Loads TC_TRANSFORMED_STAGING → TC_TRACKS, WIND_ENVELOPES_STAGING → TC_ENVELOPES_*' as description;

-- Test execution (uncomment to test)
-- CALL load_final_tables();

