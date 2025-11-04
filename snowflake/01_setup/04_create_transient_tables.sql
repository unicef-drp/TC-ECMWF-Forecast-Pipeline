-- ============================================================================
-- 04: Create Transient Tables for TC-ECMWF Forecast Pipeline
-- ============================================================================
-- Transient tables store intermediate data during processing
-- They persist between runs for debugging but skip fail-safe (lower cost)
-- Can be converted to temporary tables later if needed
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Table: TC_RAW_EXTRACTED
-- ============================================================================
-- Raw data extracted from BUFR files (one row per forecast point)
-- Before transformation and unit conversion

CREATE TRANSIENT TABLE IF NOT EXISTS TC_RAW_EXTRACTED (
    SOURCE_FILE VARCHAR
        COMMENT 'Path to source BUFR file in stage',
    
    FORECAST_TIME TIMESTAMP_NTZ
        COMMENT 'Forecast initialization time',
    
    STORM_IDENTIFIER VARCHAR
        COMMENT 'Storm identifier from BUFR (e.g., 12L)',
    
    ENSEMBLE_MEMBER INTEGER
        COMMENT 'Ensemble member number (1-50)',
    
    STEP INTEGER
        COMMENT 'Forecast lead time in hours',
    
    DATETIME TIMESTAMP_NTZ
        COMMENT 'Valid time (forecast_time + step)',
    
    LATITUDE FLOAT
        COMMENT 'Latitude in degrees (-90 to 90)',
    
    LONGITUDE FLOAT
        COMMENT 'Longitude in degrees (-180 to 180)',
    
    PRESSURE FLOAT
        COMMENT 'Central pressure in Pascals (raw from BUFR)',
    
    WIND_SPEED FLOAT
        COMMENT 'Maximum wind speed in m/s (raw from BUFR)',
    
    WLATITUDE FLOAT
        COMMENT 'Latitude of maximum wind location (degrees)',
    
    WLONGITUDE FLOAT
        COMMENT 'Longitude of maximum wind location (degrees)',
    
    -- Wind radii in LONG format
    -- One row per threshold per quadrant (3 thresholds × 4 quadrants = 12 rows per forecast point)
    WIND_THRESHOLD INTEGER
        COMMENT 'Wind speed threshold in m/s (18, 26, or 33 m/s = 34, 50, or 64 knots)',
    
    QUADRANT INTEGER
        COMMENT 'Quadrant number (1=NE, 2=SE, 3=SW, 4=NW)',
    
    WIND_RADIUS_M FLOAT
        COMMENT 'Wind radius in meters (raw from BUFR)',
    
    BEARING_START FLOAT
        COMMENT 'Bearing start angle in degrees (quadrant boundary)',
    
    BEARING_END FLOAT
        COMMENT 'Bearing end angle in degrees (quadrant boundary)',
    
    PROCESSING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this record was inserted'
)
COMMENT = 'Raw BUFR data extracted from TC track files. LONG format - one row per threshold/quadrant. Before unit conversion and transformation.';

-- ============================================================================
-- Table: TC_TRANSFORMED_STAGING
-- ============================================================================
-- Transformed TC track data ready for loading to final table
-- After unit conversion and wind field polygon generation

CREATE TRANSIENT TABLE IF NOT EXISTS TC_TRANSFORMED_STAGING (
    SOURCE_FILE VARCHAR
        COMMENT 'Path to source BUFR file',
    
    FORECAST_TIME TIMESTAMP_NTZ
        COMMENT 'Forecast initialization time',
    
    TRACK_ID VARCHAR
        COMMENT 'Storm name or identifier (e.g., LORENZO)',
    
    ENSEMBLE_MEMBER INTEGER
        COMMENT 'Ensemble member number (1-50)',
    
    VALID_TIME TIMESTAMP_NTZ
        COMMENT 'Valid time for this forecast point',
    
    LEAD_TIME INTEGER
        COMMENT 'Forecast lead time in hours',
    
    LATITUDE FLOAT
        COMMENT 'Latitude in degrees',
    
    LONGITUDE FLOAT
        COMMENT 'Longitude in degrees',
    
    PRESSURE_HPA FLOAT
        COMMENT 'Central pressure in hectopascals (converted from Pa)',
    
    WIND_SPEED_KNOTS FLOAT
        COMMENT 'Maximum wind speed in knots (converted from m/s)',
    
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
    
    -- Wind field polygons (WKT format)
    WIND_FIELD_POLYGON_34KT VARCHAR
        COMMENT 'WKT polygon for 34kt wind field',
    
    WIND_FIELD_POLYGON_50KT VARCHAR
        COMMENT 'WKT polygon for 50kt wind field',
    
    WIND_FIELD_POLYGON_64KT VARCHAR
        COMMENT 'WKT polygon for 64kt wind field',
    
    PROCESSING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this record was inserted'
)
COMMENT = 'Transformed TC track data with wind radii and polygons. Ready for final table.';

-- ============================================================================
-- Table: WIND_RAW_EXTRACTED
-- ============================================================================
-- Raw wind data extracted from GRIB2 files
-- One row per grid point per ensemble member

CREATE TRANSIENT TABLE IF NOT EXISTS WIND_RAW_EXTRACTED (
    SOURCE_FILE VARCHAR
        COMMENT 'Path to source GRIB2 file',
    
    FORECAST_TIME TIMESTAMP_NTZ
        COMMENT 'Forecast initialization time',
    
    VALID_TIME TIMESTAMP_NTZ
        COMMENT 'Valid time for this forecast',
    
    LEAD_TIME INTEGER
        COMMENT 'Forecast lead time in hours',
    
    ENSEMBLE_MEMBER INTEGER
        COMMENT 'Ensemble member number (1-50)',
    
    LATITUDE FLOAT
        COMMENT 'Grid point latitude',
    
    LONGITUDE FLOAT
        COMMENT 'Grid point longitude',
    
    WIND_SPEED_10M FLOAT
        COMMENT '10m wind speed in m/s',
    
    WIND_U_COMPONENT FLOAT
        COMMENT 'U (eastward) component of wind in m/s',
    
    WIND_V_COMPONENT FLOAT
        COMMENT 'V (northward) component of wind in m/s',
    
    PROCESSING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this record was inserted'
)
COMMENT = 'Raw wind data extracted from GRIB2 files. Grid points within TC track buffer zones.';

-- ============================================================================
-- Table: WIND_ENVELOPES_STAGING
-- ============================================================================
-- Wind threshold envelopes (both individual and combined)
-- Ready for loading to final tables

CREATE TRANSIENT TABLE IF NOT EXISTS WIND_ENVELOPES_STAGING (
    SOURCE_FILES ARRAY
        COMMENT 'Array of source files used to create this envelope',
    
    ENVELOPE_TYPE VARCHAR
        COMMENT 'Type: INDIVIDUAL (per timestep) or COMBINED (multi-timestep union)',
    
    FORECAST_TIME TIMESTAMP_NTZ
        COMMENT 'Forecast initialization time',
    
    TRACK_ID VARCHAR
        COMMENT 'Storm name or identifier',
    
    ENSEMBLE_MEMBER INTEGER
        COMMENT 'Ensemble member number',
    
    VALID_TIME TIMESTAMP_NTZ
        COMMENT 'Valid time (for INDIVIDUAL) or representative time (for COMBINED)',
    
    LEAD_TIME INTEGER
        COMMENT 'Lead time in hours (for INDIVIDUAL)',
    
    LEAD_TIME_RANGE VARCHAR
        COMMENT 'Lead time range (for COMBINED, e.g., "0-144")',
    
    WIND_THRESHOLD INTEGER
        COMMENT 'Wind speed threshold in knots (34, 50, or 64)',
    
    ENVELOPE_REGION VARCHAR
        COMMENT 'WKT polygon representing wind threshold envelope',
    
    PROCESSING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this record was inserted'
    
    -- Note: Snowflake does not support CHECK constraints
    -- Validation for ENVELOPE_TYPE (INDIVIDUAL, COMBINED) 
    -- and WIND_THRESHOLD (34, 50, 64) should be enforced in application logic
)
COMMENT = 'Wind threshold envelopes (polygons). Staging for individual and combined envelope tables.';

-- ============================================================================
-- Verify tables created
-- ============================================================================

SHOW TABLES LIKE '%STAGING%';
SHOW TABLES LIKE '%EXTRACTED%';

SELECT 
    table_name,
    table_type,
    row_count,
    bytes,
    COMMENT
FROM AOTS.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'ECMWF_PIPELINE'
  AND table_type = 'TRANSIENT'
ORDER BY table_name;

SELECT 
    '✓ Transient tables created successfully' as status,
    '4 staging tables' as table_count,
    'TC_RAW_EXTRACTED, TC_TRANSFORMED_STAGING, WIND_RAW_EXTRACTED, WIND_ENVELOPES_STAGING' as table_names,
    'Ready for final tables' as next_step;