-- ============================================================================
-- Stored Procedure: transform_tc_tracks
-- ============================================================================
-- Transforms raw TC track data from TC_RAW_EXTRACTED to TC_TRANSFORMED_STAGING
-- 
-- Transformations:
-- 1. Unit conversions (Pa→hPa, m/s→knots)
-- 2. Calculate Radius of Maximum Winds (RMW) using Haversine formula
-- 3. Use wind radii data extracted from BUFR (34/50/64kt × NE/SE/SW/NW)
-- 4. Create wind field polygons for 34kt, 50kt, 64kt thresholds from BUFR radii
-- 5. Aggregate data by unique forecast points
-- 
-- Note: Wind radii are now extracted directly from BUFR files by extract_bufr_file UDTF
-- ============================================================================

USE ROLE AOTS_ROLE;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

CREATE OR REPLACE PROCEDURE transform_tc_tracks()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
RESOURCE_CONSTRAINT = (architecture='x86')
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy', 'shapely')
HANDLER = 'transform_tc_tracks'
AS
$$
import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2

def transform_tc_tracks(session):
    """
    Transform raw TC track data into standardized format.
    
    Returns:
        Summary string with transformation results
    """
    
    # ========================================================================
    # Helper Functions
    # ========================================================================
    
    def haversine_distance(lat1, lon1, lat2, lon2):
        """
        Calculate distance between two points using Haversine formula.
        Returns distance in kilometers.
        """
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
            return None
        
        R = 6371.0  # Earth radius in km
        
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return R * c
    
    def create_wind_polygon_wkt(lat, lon, r_ne, r_se, r_sw, r_nw):
        """
        Create wedge-shaped wind field polygon from quadrant radii.
        Creates 4 wedge/pie-slice polygons (one per quadrant) that accurately
        represent the asymmetric tropical cyclone wind field.
        
        Returns WKT MultiPolygon string or None.
        """
        # Check if any radius exists and is non-zero
        if not any([r_ne, r_se, r_sw, r_nw]) or all(pd.isna([r_ne, r_se, r_sw, r_nw])):
            return None
        
        # Replace NaN with 0
        r_ne = r_ne if not pd.isna(r_ne) else 0
        r_se = r_se if not pd.isna(r_se) else 0
        r_sw = r_sw if not pd.isna(r_sw) else 0
        r_nw = r_nw if not pd.isna(r_nw) else 0
        
        # Convert km to degrees (approximate at given latitude)
        deg_per_km_lat = 1.0 / 111.0
        deg_per_km_lon = 1.0 / (111.0 * np.cos(np.radians(lat)))
        
        # Number of points to use for each arc (more = smoother)
        arc_points = 20
        
        wedges = []
        
        # Define quadrants: (start_angle, end_angle, radius)
        # Angles in degrees: 0=East, 90=North, 180=West, 270=South
        quadrants = [
            (0, 90, r_ne, 'NE'),      # Northeast: 0° to 90°
            (270, 360, r_se, 'SE'),   # Southeast: 270° to 360° (or -90° to 0°)
            (180, 270, r_sw, 'SW'),   # Southwest: 180° to 270°
            (90, 180, r_nw, 'NW'),    # Northwest: 90° to 180°
        ]
        
        for start_angle, end_angle, radius, quadrant_name in quadrants:
            if radius <= 0:
                continue  # Skip if no radius for this quadrant
            
            # Create wedge polygon for this quadrant
            # Start at center, draw arc, return to center
            coords = [(lon, lat)]  # Start at center
            
            # Generate arc points
            angles = np.linspace(start_angle, end_angle, arc_points)
            for angle in angles:
                angle_rad = np.radians(angle)
                # Calculate point at radius distance in this direction
                point_lon = lon + radius * deg_per_km_lon * np.cos(angle_rad)
                point_lat = lat + radius * deg_per_km_lat * np.sin(angle_rad)
                coords.append((point_lon, point_lat))
            
            # Close the wedge back to center
            coords.append((lon, lat))
            
            # Format as WKT polygon
            coords_str = ', '.join([f"{c[0]} {c[1]}" for c in coords])
            wedge_wkt = f"(({coords_str}))"
            wedges.append(wedge_wkt)
        
        if not wedges:
            return None
        
        # Combine all wedges into a MultiPolygon
        if len(wedges) == 1:
            # Single polygon if only one quadrant has data
            return f"POLYGON{wedges[0]}"
        else:
            # MultiPolygon for multiple quadrants
            wedges_str = ', '.join(wedges)
            return f"MULTIPOLYGON({wedges_str})"
    
    # ========================================================================
    # Extract Raw Data
    # ========================================================================
    
    # Get all raw TC data in LONG format
    raw_data_df = session.sql("""
        SELECT 
            SOURCE_FILE,
            FORECAST_TIME,
            STORM_IDENTIFIER as TRACK_ID,
            ENSEMBLE_MEMBER,
            STEP as LEAD_TIME,
            DATETIME as VALID_TIME,
            LATITUDE,
            LONGITUDE,
            PRESSURE,
            WIND_SPEED,
            WLATITUDE,
            WLONGITUDE,
            WIND_THRESHOLD,
            QUADRANT,
            WIND_RADIUS_M,
            BEARING_START,
            BEARING_END
        FROM AOTS.ECMWF_PIPELINE.TC_RAW_EXTRACTED
        WHERE SOURCE_FILE NOT IN (
            SELECT DISTINCT SOURCE_FILE 
            FROM AOTS.ECMWF_PIPELINE.TC_TRANSFORMED_STAGING
        )
        ORDER BY FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, LEAD_TIME, WIND_THRESHOLD, QUADRANT
    """).to_pandas()
    
    if raw_data_df.empty:
        return "No new data to transform"
    
    # ========================================================================
    # Convert Wind Radii from LONG to WIDE format
    # ========================================================================
    
    # Separate base forecast data from wind radii data
    base_cols = ['SOURCE_FILE', 'FORECAST_TIME', 'TRACK_ID', 'ENSEMBLE_MEMBER', 
                 'LEAD_TIME', 'VALID_TIME', 'LATITUDE', 'LONGITUDE', 'PRESSURE', 
                 'WIND_SPEED', 'WLATITUDE', 'WLONGITUDE']
    
    # Get unique forecast points (one per member/timestep)
    forecasts_df = raw_data_df[base_cols].drop_duplicates()
    
    # Process wind radii data: LONG → WIDE
    # Map thresholds: 18 m/s = 34 knots, 26 m/s = 50 knots, 33 m/s = 64 knots
    threshold_map = {18: 34, 26: 50, 33: 64}
    
    # Map quadrants: 1=NE, 2=SE, 3=SW, 4=NW
    quadrant_map = {1: 'ne', 2: 'se', 3: 'sw', 4: 'nw'}
    
    # Create wind radii DataFrame
    wind_radii_df = raw_data_df[['FORECAST_TIME', 'TRACK_ID', 'ENSEMBLE_MEMBER', 'LEAD_TIME',
                                  'WIND_THRESHOLD', 'QUADRANT', 'WIND_RADIUS_M']].copy()
    
    # Convert threshold from m/s to knots
    wind_radii_df['threshold_knots'] = wind_radii_df['WIND_THRESHOLD'].map(threshold_map)
    
    # Convert quadrant number to direction
    wind_radii_df['direction'] = wind_radii_df['QUADRANT'].map(quadrant_map)
    
    # Convert radius from meters to kilometers
    wind_radii_df['radius_km'] = wind_radii_df['WIND_RADIUS_M'] / 1000.0
    
    # Create column names for pivot
    wind_radii_df['column_name'] = (
        'RADIUS_' + 
        wind_radii_df['threshold_knots'].astype(str) + 
        '_KNOT_WINDS_' + 
        wind_radii_df['direction'].str.upper() + 
        '_KM'
    )
    
    # Pivot to wide format
    wind_radii_wide = wind_radii_df.pivot_table(
        index=['FORECAST_TIME', 'TRACK_ID', 'ENSEMBLE_MEMBER', 'LEAD_TIME'],
        columns='column_name',
        values='radius_km',
        aggfunc='first'
    ).reset_index()
    
    # Ensure all expected columns exist (fill missing with None)
    expected_wind_cols = []
    for threshold in [34, 50, 64]:
        for direction in ['NE', 'SE', 'SW', 'NW']:
            col = f'RADIUS_{threshold}_KNOT_WINDS_{direction}_KM'
            expected_wind_cols.append(col)
            if col not in wind_radii_wide.columns:
                wind_radii_wide[col] = None
    
    # Merge wind radii back with base forecast data
    forecasts_df = forecasts_df.merge(
        wind_radii_wide,
        on=['FORECAST_TIME', 'TRACK_ID', 'ENSEMBLE_MEMBER', 'LEAD_TIME'],
        how='left'
    )
    
    # Unit conversions
    # Pressure: Pa to hPa (if values > 10000, they're in Pa)
    forecasts_df['PRESSURE_HPA'] = forecasts_df['PRESSURE'].apply(
        lambda x: x / 100.0 if x and x > 10000 else x
    )
    
    # Wind speed: m/s to knots
    forecasts_df['WIND_SPEED_KNOTS'] = forecasts_df['WIND_SPEED'] * 1.944
    
    # Calculate Radius of Maximum Winds (RMW)
    forecasts_df['RADIUS_OF_MAXIMUM_WINDS_KM'] = forecasts_df.apply(
        lambda row: haversine_distance(
            row['LATITUDE'], row['LONGITUDE'],
            row['WLATITUDE'], row['WLONGITUDE']
        ),
        axis=1
    )
    
    # ========================================================================
    # Wind Radii - Converted from LONG to WIDE format
    # ========================================================================
    
    # Wind radii were extracted in LONG format (one row per threshold/quadrant)
    # and have been pivoted to WIDE format (12 columns: RADIUS_*_KNOT_WINDS_*_KM)
    # Values are in kilometers, representing the radius to specified wind speeds in each quadrant
    # This matches the Python transformer's convert_wind_radii_wide() function
    
    # ========================================================================
    # Create Wind Field Polygons
    # ========================================================================
    
    # Create wind field polygons for each threshold using actual BUFR wind radii
    wind_thresholds = [34, 50, 64]
    
    for threshold in wind_thresholds:
        col_name = f'WIND_FIELD_POLYGON_{threshold}KT'
        
        forecasts_df[col_name] = forecasts_df.apply(
            lambda row: create_wind_polygon_wkt(
                row['LATITUDE'],
                row['LONGITUDE'],
                row.get(f'RADIUS_{threshold}_KNOT_WINDS_NE_KM', 0),
                row.get(f'RADIUS_{threshold}_KNOT_WINDS_SE_KM', 0),
                row.get(f'RADIUS_{threshold}_KNOT_WINDS_SW_KM', 0),
                row.get(f'RADIUS_{threshold}_KNOT_WINDS_NW_KM', 0)
            ),
            axis=1
        )
    
    # ========================================================================
    # Prepare Final DataFrame
    # ========================================================================
    
    # Add processing timestamp
    from datetime import datetime
    forecasts_df['PROCESSING_TIMESTAMP'] = datetime.now()
    
    # Select and order columns for staging table
    final_cols = [
        'SOURCE_FILE',
        'FORECAST_TIME',
        'TRACK_ID',
        'ENSEMBLE_MEMBER',
        'VALID_TIME',
        'LEAD_TIME',
        'LATITUDE',
        'LONGITUDE',
        'PRESSURE_HPA',
        'WIND_SPEED_KNOTS',
        'RADIUS_OF_MAXIMUM_WINDS_KM',
        'RADIUS_34_KNOT_WINDS_NE_KM',
        'RADIUS_34_KNOT_WINDS_SE_KM',
        'RADIUS_34_KNOT_WINDS_SW_KM',
        'RADIUS_34_KNOT_WINDS_NW_KM',
        'RADIUS_50_KNOT_WINDS_NE_KM',
        'RADIUS_50_KNOT_WINDS_SE_KM',
        'RADIUS_50_KNOT_WINDS_SW_KM',
        'RADIUS_50_KNOT_WINDS_NW_KM',
        'RADIUS_64_KNOT_WINDS_NE_KM',
        'RADIUS_64_KNOT_WINDS_SE_KM',
        'RADIUS_64_KNOT_WINDS_SW_KM',
        'RADIUS_64_KNOT_WINDS_NW_KM',
        'WIND_FIELD_POLYGON_34KT',
        'WIND_FIELD_POLYGON_50KT',
        'WIND_FIELD_POLYGON_64KT',
        'PROCESSING_TIMESTAMP'
    ]
    
    result_df = forecasts_df[final_cols]
    
    # ========================================================================
    # Load to Staging Table
    # ========================================================================
    
    # Create Snowpark DataFrame and write to staging
    from snowflake.snowpark.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType
    
    # Convert pandas DataFrame to Snowpark DataFrame
    staging_df = session.create_dataframe(result_df)
    
    # Write to staging table (using fully qualified name)
    staging_df.write.mode('append').save_as_table('AOTS.ECMWF_PIPELINE.TC_TRANSFORMED_STAGING')
    
    # ========================================================================
    # Summary
    # ========================================================================
    
    record_count = len(result_df)
    storm_count = result_df['TRACK_ID'].nunique()
    member_count = result_df['ENSEMBLE_MEMBER'].nunique()
    
    summary = (
        f"Transformed {record_count} records: "
        f"{storm_count} storms, {member_count} members"
    )
    
    return summary
$$;

-- Verify procedure created
SELECT '✓ Stored procedure created' as status,
       'transform_tc_tracks()' as procedure_name,
       'Transforms TC_RAW_EXTRACTED → TC_TRANSFORMED_STAGING' as description;

-- Test execution (uncomment to test)
-- CALL transform_tc_tracks();

