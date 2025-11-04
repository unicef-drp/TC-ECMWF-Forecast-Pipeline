-- ============================================================================
-- 05: Create Final Tables for TC-ECMWF Forecast Pipeline
-- ============================================================================
-- Permanent tables for production data
-- These are the source of truth for TC tracks and wind envelopes
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Table: TC_TRACKS
-- ============================================================================
-- TC forecast tracks with wind radii and wind field polygons
-- One row per forecast point per ensemble member

CREATE TABLE IF NOT EXISTS TC_TRACKS (
    FORECAST_TIME TIMESTAMP_NTZ NOT NULL
        COMMENT 'Forecast initialization time',
    
    TRACK_ID VARCHAR NOT NULL
        COMMENT 'Storm name or identifier (e.g., LORENZO)',
    
    ENSEMBLE_MEMBER INTEGER NOT NULL
        COMMENT 'Ensemble member number (1-50)',
    
    VALID_TIME TIMESTAMP_NTZ NOT NULL
        COMMENT 'Valid time for this forecast point',
    
    LEAD_TIME INTEGER NOT NULL
        COMMENT 'Forecast lead time in hours',
    
    LATITUDE FLOAT NOT NULL
        COMMENT 'Latitude in degrees (-90 to 90)',
    
    LONGITUDE FLOAT NOT NULL
        COMMENT 'Longitude in degrees (-180 to 180)',
    
    PRESSURE_HPA FLOAT
        COMMENT 'Central pressure in hectopascals',
    
    WIND_SPEED_KNOTS FLOAT
        COMMENT 'Maximum wind speed in knots',
    
    RADIUS_OF_MAXIMUM_WINDS_KM FLOAT
        COMMENT 'Calculated radius of maximum winds in kilometers',
    
    -- Wind radii by quadrant (34 knot threshold)
    RADIUS_34_KNOT_WINDS_NE_KM FLOAT
        COMMENT 'Radius to 34kt winds in NE quadrant (km)',
    
    RADIUS_34_KNOT_WINDS_SE_KM FLOAT
        COMMENT 'Radius to 34kt winds in SE quadrant (km)',
    
    RADIUS_34_KNOT_WINDS_SW_KM FLOAT
        COMMENT 'Radius to 34kt winds in SW quadrant (km)',
    
    RADIUS_34_KNOT_WINDS_NW_KM FLOAT
        COMMENT 'Radius to 34kt winds in NW quadrant (km)',
    
    -- Wind radii by quadrant (50 knot threshold)
    RADIUS_50_KNOT_WINDS_NE_KM FLOAT
        COMMENT 'Radius to 50kt winds in NE quadrant (km)',
    
    RADIUS_50_KNOT_WINDS_SE_KM FLOAT
        COMMENT 'Radius to 50kt winds in SE quadrant (km)',
    
    RADIUS_50_KNOT_WINDS_SW_KM FLOAT
        COMMENT 'Radius to 50kt winds in SW quadrant (km)',
    
    RADIUS_50_KNOT_WINDS_NW_KM FLOAT
        COMMENT 'Radius to 50kt winds in NW quadrant (km)',
    
    -- Wind radii by quadrant (64 knot threshold)
    RADIUS_64_KNOT_WINDS_NE_KM FLOAT
        COMMENT 'Radius to 64kt winds in NE quadrant (km)',
    
    RADIUS_64_KNOT_WINDS_SE_KM FLOAT
        COMMENT 'Radius to 64kt winds in SE quadrant (km)',
    
    RADIUS_64_KNOT_WINDS_SW_KM FLOAT
        COMMENT 'Radius to 64kt winds in SW quadrant (km)',
    
    RADIUS_64_KNOT_WINDS_NW_KM FLOAT
        COMMENT 'Radius to 64kt winds in NW quadrant (km)',
    
    -- Wind field polygons (WKT format - will be converted to GEOGRAPHY)
    WIND_FIELD_POLYGON_34KT VARCHAR
        COMMENT 'WKT polygon for 34kt wind field',
    
    WIND_FIELD_POLYGON_50KT VARCHAR
        COMMENT 'WKT polygon for 50kt wind field',
    
    WIND_FIELD_POLYGON_64KT VARCHAR
        COMMENT 'WKT polygon for 64kt wind field',
    
    -- Primary key for deduplication
    CONSTRAINT pk_tc_tracks PRIMARY KEY (TRACK_ID, ENSEMBLE_MEMBER, FORECAST_TIME, LEAD_TIME)
)
COMMENT = 'TC forecast tracks with wind radii and wind field polygons. Source of truth for TC data.';

-- Note: Snowflake does not support traditional indexes
-- Instead, use clustering for query optimization

-- Cluster table by forecast time for better query performance
ALTER TABLE TC_TRACKS CLUSTER BY (FORECAST_TIME, TRACK_ID);

-- ============================================================================
-- Table: TC_ENVELOPES_INDIVIDUAL
-- ============================================================================
-- Wind threshold envelopes for individual timesteps
-- One row per storm per ensemble member per timestep per wind threshold

CREATE TABLE IF NOT EXISTS TC_ENVELOPES_INDIVIDUAL (
    FORECAST_TIME TIMESTAMP_NTZ NOT NULL
        COMMENT 'Forecast initialization time',
    
    TRACK_ID VARCHAR NOT NULL
        COMMENT 'Storm name or identifier',
    
    ENSEMBLE_MEMBER INTEGER NOT NULL
        COMMENT 'Ensemble member number',
    
    VALID_TIME TIMESTAMP_NTZ NOT NULL
        COMMENT 'Valid time for this envelope',
    
    LEAD_TIME INTEGER NOT NULL
        COMMENT 'Forecast lead time in hours',
    
    WIND_THRESHOLD INTEGER NOT NULL
        COMMENT 'Wind speed threshold in knots (34, 50, or 64)',
    
    ENVELOPE_REGION GEOGRAPHY
        COMMENT 'Wind threshold envelope polygon (GEOGRAPHY type)',
    
    -- Primary key for deduplication
    CONSTRAINT pk_tc_envelopes_individual 
        PRIMARY KEY (TRACK_ID, ENSEMBLE_MEMBER, FORECAST_TIME, LEAD_TIME, WIND_THRESHOLD)
    
    -- Note: Snowflake does not support CHECK constraints
    -- Validation for WIND_THRESHOLD (34, 50, 64) should be enforced in application logic
)
COMMENT = 'Wind threshold envelopes for individual forecast timesteps. One polygon per timestep.';

-- Cluster by forecast time for query optimization
ALTER TABLE TC_ENVELOPES_INDIVIDUAL CLUSTER BY (FORECAST_TIME, TRACK_ID);

-- ============================================================================
-- Table: TC_ENVELOPES_COMBINED
-- ============================================================================
-- Wind threshold envelopes combined across multiple timesteps
-- One row per storm per ensemble member per wind threshold (union of all timesteps)

CREATE TABLE IF NOT EXISTS TC_ENVELOPES_COMBINED (
    FORECAST_TIME TIMESTAMP_NTZ NOT NULL
        COMMENT 'Forecast initialization time',
    
    TRACK_ID VARCHAR NOT NULL
        COMMENT 'Storm name or identifier',
    
    ENSEMBLE_MEMBER INTEGER NOT NULL
        COMMENT 'Ensemble member number',
    
    LEAD_TIME_RANGE INTEGER
        COMMENT 'Lead time range (parsed from VARCHAR, e.g., "0-144" becomes 0)',
    
    WIND_THRESHOLD INTEGER NOT NULL
        COMMENT 'Wind speed threshold in knots (34, 50, or 64)',
    
    ENVELOPE_REGION GEOGRAPHY
        COMMENT 'Combined wind threshold envelope polygon (union of all timesteps)',
    
    -- Primary key for deduplication
    CONSTRAINT pk_tc_envelopes_combined 
        PRIMARY KEY (TRACK_ID, ENSEMBLE_MEMBER, FORECAST_TIME, WIND_THRESHOLD)
    
    -- Note: Snowflake does not support CHECK constraints
    -- Validation for WIND_THRESHOLD (34, 50, 64) should be enforced in application logic
)
COMMENT = 'Wind threshold envelopes combined across all forecast timesteps. One polygon per storm/member/threshold.';

-- Cluster by forecast time for query optimization
ALTER TABLE TC_ENVELOPES_COMBINED CLUSTER BY (FORECAST_TIME, TRACK_ID);

-- ============================================================================
-- Verify tables created
-- ============================================================================

SHOW TABLES IN AOTS.ECMWF_PIPELINE;

SELECT 
    table_name,
    table_type,
    row_count,
    bytes,
    COMMENT
FROM AOTS.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'ECMWF_PIPELINE'
  AND table_type = 'BASE TABLE'
  AND table_name LIKE 'TC_%'
ORDER BY table_name;

-- Show all columns for TC_TRACKS
SELECT 
    column_name,
    data_type,
    comment
FROM AOTS.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'ECMWF_PIPELINE'
  AND table_name = 'TC_TRACKS'
ORDER BY ordinal_position;

-- Show clustering keys
SHOW TABLES LIKE 'TC_%' IN AOTS.ECMWF_PIPELINE;

SELECT 
    '✓ Final tables created successfully' as status,
    '3 production tables' as table_count,
    'TC_TRACKS, TC_ENVELOPES_INDIVIDUAL, TC_ENVELOPES_COMBINED' as table_names,
    'Tables clustered and indexed' as optimization,
    'Ready for UDF and procedure development' as next_step;