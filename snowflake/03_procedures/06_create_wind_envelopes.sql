-- ============================================================================
-- Stored Procedure: create_wind_envelopes
-- ============================================================================
-- Creates wind threshold envelope polygons by combining TC tracks and wind data
--
-- Process:
-- 1. Join TC tracks with wind data (by forecast_time, valid_time, member)
-- 2. Buffer TC tracks (500km radius)
-- 3. Filter wind grid points within buffer
-- 4. Create contour polygons for wind thresholds (34, 40, 50, 64, 83, 96, 113, 137 knots)
-- 5. Generate individual envelopes (per timestep)
-- 6. Generate combined envelopes (union across timesteps)
--
-- Output: WIND_ENVELOPES_STAGING table
-- ============================================================================

USE ROLE AOTS_ROLE;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

CREATE OR REPLACE PROCEDURE create_wind_envelopes(
    forecast_time_filter TIMESTAMP_NTZ,
    track_id_filter VARCHAR,
    ensemble_member_filter INTEGER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
RESOURCE_CONSTRAINT = (architecture='x86')
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy', 'matplotlib', 'shapely', 'scipy', 'pyarrow', 'xarray', 'cfgrib')
HANDLER = 'create_wind_envelopes'
AS
$$
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, Point, MultiPolygon
from shapely.ops import unary_union
from scipy.spatial import ConvexHull
import xarray as xr
import tempfile
import os
from datetime import datetime, timedelta
from snowflake.snowpark.files import SnowflakeFile

def create_wind_envelopes(session, forecast_time_filter=None, track_id_filter=None, ensemble_member_filter=None):
    """
    Create wind threshold envelope polygons from TC tracks and wind data.
    
    Args:
        session: Snowpark session
        forecast_time_filter: Optional filter for specific forecast time
        track_id_filter: Optional filter for specific storm
        ensemble_member_filter: Optional filter for specific ensemble member
        
    Returns:
        Summary string with envelope creation results
    """
    
    # ========================================================================
    # Helper Functions
    # ========================================================================
    
    def create_buffer_bounds(lat, lon, radius_km=500):
        """
        Create bounding box for buffer around a point.
        Returns (lat_min, lat_max, lon_min, lon_max)
        """
        # Convert km to approximate degrees
        deg_per_km = 1.0 / 111.0
        radius_deg = radius_km * deg_per_km
        
        return (
            lat - radius_deg,
            lat + radius_deg,
            lon - radius_deg,
            lon + radius_deg
        )
    
    def point_in_buffer(point_lat, point_lon, center_lat, center_lon, radius_km=500):
        """
        Check if point is within radius of center using simple distance.
        """
        # Approximate distance in km
        lat_diff = (point_lat - center_lat) * 111.0
        lon_diff = (point_lon - center_lon) * 111.0 * np.cos(np.radians(center_lat))
        distance = np.sqrt(lat_diff**2 + lon_diff**2)
        return distance <= radius_km
    
    def create_wind_contour_polygon(lats, lons, wind_speeds, threshold_ms, verbose=False):
        """
        Create polygon from wind data exceeding threshold using matplotlib contour.
        
        Args:
            lats: Array of latitudes
            lons: Array of longitudes
            wind_speeds: Array of wind speeds in m/s
            threshold_ms: Wind threshold in m/s
            verbose: Print debug info
            
        Returns:
            Shapely Polygon/MultiPolygon or None
        """
        if len(lats) < 4:
            return None
        
        try:
            # Create regular grid for contouring
            lat_min, lat_max = lats.min(), lats.max()
            lon_min, lon_max = lons.min(), lons.max()
            
            # Create grid
            grid_resolution = 50  # points per axis
            lat_grid = np.linspace(lat_min, lat_max, grid_resolution)
            lon_grid = np.linspace(lon_min, lon_max, grid_resolution)
            lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)
            
            # Interpolate wind speeds to grid using nearest neighbor
            from scipy.interpolate import griddata
            wind_grid = griddata(
                (lons, lats),
                wind_speeds,
                (lon_mesh, lat_mesh),
                method='nearest',
                fill_value=0.0
            )
            
            # Create contour at threshold
            fig, ax = plt.subplots(figsize=(8, 6))
            contours = ax.contour(lon_mesh, lat_mesh, wind_grid, levels=[threshold_ms])
            plt.close(fig)
            
            # Extract contour polygons (matching Python: use contours.allsegs[0])
            polygons = []
            try:
                # Python approach: if len(cs.allsegs) > 0 and len(cs.allsegs[0]) > 0:
                if len(contours.allsegs) > 0 and len(contours.allsegs[0]) > 0:
                    for segment in contours.allsegs[0]:
                        if len(segment) > 3:  # Python requires > 3 points
                            try:
                                poly = Polygon(segment)
                                if poly.is_valid and poly.area > 0:  # Python checks validity and area
                                    polygons.append(poly)
                            except:
                                continue
            except Exception:
                # Fallback: try collections approach if allsegs doesn't work
                for collection in contours.collections:
                    for path in collection.get_paths():
                        vertices = path.vertices
                        if len(vertices) > 3:  # Changed from >= 3 to > 3 to match Python
                            try:
                                poly = Polygon(vertices)
                                if poly.is_valid and poly.area > 0:
                                    polygons.append(poly)
                            except:
                                continue
            
            if not polygons:
                return None
            
            # Combine all polygons
            if len(polygons) == 1:
                return polygons[0]
            else:
                return unary_union(polygons)
                
        except Exception as e:
            if verbose:
                print(f"Contour error: {e}")
            return None
    
    def create_convex_hull_polygon(lats, lons):
        """
        Create convex hull polygon from points (fallback if contour fails).
        """
        if len(lats) < 3:
            return None
        
        try:
            points = np.column_stack([lons, lats])
            hull = ConvexHull(points)
            hull_points = points[hull.vertices]
            return Polygon(hull_points)
        except:
            return None
    
    def polygon_to_wkt(polygon):
        """Convert Shapely polygon to WKT string."""
        if polygon is None or polygon.is_empty:
            return None
        try:
            return polygon.wkt
        except:
            return None
    
    # ========================================================================
    # Build Query Filters
    # ========================================================================
    
    where_clauses = []
    if forecast_time_filter:
        where_clauses.append(f"FORECAST_TIME = '{forecast_time_filter}'")
    if track_id_filter:
        where_clauses.append(f"TRACK_ID = '{track_id_filter}'")
    if ensemble_member_filter:
        where_clauses.append(f"ENSEMBLE_MEMBER = {ensemble_member_filter}")
    
    where_sql = " AND " + " AND ".join(where_clauses) if where_clauses else ""
    
    # ========================================================================
    # Extract TC Track and Wind Data
    # ========================================================================
    
    # Get TC tracks
    tracks_query = f"""
        SELECT 
            FORECAST_TIME,
            TRACK_ID,
            ENSEMBLE_MEMBER,
            VALID_TIME,
            LEAD_TIME,
            LATITUDE,
            LONGITUDE
        FROM AOTS.ECMWF_PIPELINE.TC_TRANSFORMED_STAGING
        WHERE 1=1 {where_sql}
        ORDER BY FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME
    """
    
    tracks_df = session.sql(tracks_query).to_pandas()
    
    if tracks_df.empty:
        return "No TC track data found for specified filters"
    
    # Wind thresholds (knots to m/s conversion) - matching Python WIND_THRESHOLDS
    wind_thresholds = {
        34: 17.49,   # Tropical storm force
        40: 20.58,   # Strong tropical storm
        50: 25.72,   # Very strong tropical storm
        64: 32.92,   # Category 1 hurricane
        83: 42.70,   # Category 2 hurricane
        96: 49.39,   # Category 3 hurricane
        113: 58.12,  # Category 4 hurricane
        137: 70.48   # Category 5 hurricane
    }
    
    # ========================================================================
    # Process Each Track Point
    # ========================================================================
    
    envelopes = []
    processed_count = 0
    error_count = 0
    
    # Group by storm (TRACK_ID) first to calculate ONE bbox per storm (from all members, all time steps)
    # This matches Python: create_buffered_track_polygon(tc_data, buffer_radius_km) - uses ALL track points
    storm_groups = tracks_df.groupby('TRACK_ID')
    
    for track_id, storm_tracks in storm_groups:
        
        # Calculate ONE bounding box from ALL track points for this storm (all members, all time steps)
        # This matches Python: one bbox per storm, reused for all time steps and members
        track_lat_min = storm_tracks['LATITUDE'].min()
        track_lat_max = storm_tracks['LATITUDE'].max()
        track_lon_min = storm_tracks['LONGITUDE'].min()
        track_lon_max = storm_tracks['LONGITUDE'].max()
        
        # Add buffer (500km ≈ 4.5 degrees, plus 2.0 degree extra buffer like Python)
        buffer_deg = 5.0 + 2.0  # 500km buffer + 2 degree extra (matching Python's get_bounding_box(buffer=2.0))
        bbox_lat_min = track_lat_min - buffer_deg
        bbox_lat_max = track_lat_max + buffer_deg
        bbox_lon_min = track_lon_min - buffer_deg
        bbox_lon_max = track_lon_max + buffer_deg
        
        # Handle longitude wrap-around (if track crosses 180/-180)
        if bbox_lon_min < -180:
            bbox_lon_min = -180
        if bbox_lon_max > 180:
            bbox_lon_max = 180
        
        # Now process each (forecast_time, member) combination for this storm
        # Use the SAME bbox for all time steps and members (matching Python)
        member_groups = storm_tracks.groupby(['FORECAST_TIME', 'ENSEMBLE_MEMBER'])
        
        for (forecast_time, member), track_group in member_groups:
            
            # Store polygons for combined envelope (per member)
            # Initialize for all thresholds (matching Python)
            combined_polys_by_threshold = {threshold: [] for threshold in wind_thresholds.keys()}
            
            # Process each time step (matching Python approach: load wind data on-demand per time step)
            for _, track_point in track_group.iterrows():
                
                valid_time = track_point['VALID_TIME']
                center_lat = track_point['LATITUDE']
                center_lon = track_point['LONGITUDE']
                lead_time = track_point['LEAD_TIME']
                
                # Find matching wind GRIB file for this time step (like Python's find_wind_file_for_time)
                # Format: wind_ens_YYYY-MM-DD_rHH_fHHHh_pf.grib2 or _cf.grib2
                # Note: Must use new format with _pf/_cf suffix (old format without suffix not supported)
                forecast_date_str = forecast_time.strftime('%Y-%m-%d')
                run_hour = f"{forecast_time.hour:02d}"
                forecast_hour = f"f{lead_time:03d}h"
                
                # Choose PF vs CF based on ensemble member (matching Python logic)
                # Member 51 uses CF file, members 1-50 use PF file
                file_type = "_cf" if member == 51 else "_pf"
                
                # Find wind file in FILE_PROCESSING_LOG
                # Must use new format with _pf/_cf suffix (matching Python downloads)
                wind_file_query = f"""
                    SELECT FILE_PATH
                    FROM AOTS.ECMWF_PIPELINE.FILE_PROCESSING_LOG
                    WHERE FILE_TYPE = 'GRIB2'
                      AND FILE_PATH LIKE '%wind_ens_{forecast_date_str}_r{run_hour}_f{lead_time:03d}h{file_type}.grib2%'
                      AND PROCESSING_STATUS = 'COMPLETED'
                    LIMIT 1
                """
                
                wind_file_result = session.sql(wind_file_query).collect()
                
                if not wind_file_result or len(wind_file_result) == 0:
                    # No wind file found for this time step - skip
                    continue
                
                wind_file_path = wind_file_result[0]['FILE_PATH']
                
                # Load wind data directly from GRIB file using UDF (on-demand, matching Python approach)
                # Extract only the bbox region (not all grid points) - uses the SAME bbox for all time steps
                wind_extract_query = f"""
                    SELECT 
                        VALID_TIME,
                        LATITUDE,
                        LONGITUDE,
                        WIND_SPEED_10M
                    FROM TABLE(AOTS.ECMWF_PIPELINE.extract_wind_grib_file(
                        '{wind_file_path}',
                        {bbox_lat_min}::FLOAT,
                        {bbox_lat_max}::FLOAT,
                        {bbox_lon_min}::FLOAT,
                        {bbox_lon_max}::FLOAT
                    ))
                    WHERE ENSEMBLE_MEMBER = {member}
                """
                
                wind_df = session.sql(wind_extract_query).to_pandas()
                
                if wind_df.empty:
                    continue
                
                # Wind data is already filtered by storm-level bbox, but we need to filter by local buffer around this track point
                # Apply additional buffer filter (500km) around this specific track point
                lat_min, lat_max, lon_min, lon_max = create_buffer_bounds(center_lat, center_lon, 500)
                wind_buffered = wind_df[
                    (wind_df['LATITUDE'] >= lat_min) &
                    (wind_df['LATITUDE'] <= lat_max) &
                    (wind_df['LONGITUDE'] >= lon_min) &
                    (wind_df['LONGITUDE'] <= lon_max)
                ].copy()
                
                if wind_buffered.empty or len(wind_buffered) < 4:
                    continue
                
                # Match Python approach: Use ALL wind points to create grid, then contour (not filter by threshold first)
                # This allows contours even when sparse points exceed threshold
                all_lats = wind_buffered['LATITUDE'].values
                all_lons = wind_buffered['LONGITUDE'].values
                all_speeds = wind_buffered['WIND_SPEED_10M'].values
                
                # Create envelope for each threshold
                for threshold_knots, threshold_ms in wind_thresholds.items():
                    
                    # Create polygon from ALL wind data (matches Python: contours on grid, not filtered points)
                    # The contour will naturally find the threshold boundary
                    polygon = create_wind_contour_polygon(all_lats, all_lons, all_speeds, threshold_ms)
                    
                    # Only create envelope if polygon exists (contour found threshold boundary)
                    # Python only creates envelope if polygon is not None (no fallback)
                    if polygon is not None:
                        # Individual envelope
                        envelopes.append({
                            'ENVELOPE_TYPE': 'INDIVIDUAL',
                            'FORECAST_TIME': forecast_time,
                            'TRACK_ID': track_id,
                            'ENSEMBLE_MEMBER': member,
                            'VALID_TIME': valid_time,
                            'LEAD_TIME': lead_time,
                            'WIND_THRESHOLD': threshold_knots,
                            'ENVELOPE_REGION': polygon_to_wkt(polygon)
                        })
                        
                        # Store for combined envelope (per member)
                        combined_polys_by_threshold[threshold_knots].append(polygon)
            
            # Create combined envelopes for this member (union of all timesteps for this member)
            for threshold_knots in wind_thresholds.keys():
                polys = combined_polys_by_threshold[threshold_knots]
                if polys:
                    try:
                        combined_poly = unary_union(polys)
                        if combined_poly is not None and not combined_poly.is_empty:
                            envelopes.append({
                                'ENVELOPE_TYPE': 'COMBINED',
                                'FORECAST_TIME': forecast_time,
                                'TRACK_ID': track_id,
                                'ENSEMBLE_MEMBER': member,
                                'VALID_TIME': track_group['VALID_TIME'].max(),  # Latest time
                                'LEAD_TIME': None,
                                'WIND_THRESHOLD': threshold_knots,
                                'ENVELOPE_REGION': polygon_to_wkt(combined_poly)
                            })
                    except:
                        pass
            
            processed_count += 1
    
    # ========================================================================
    # Load to Final Tables (TC_ENVELOPES_INDIVIDUAL and TC_ENVELOPES_COMBINED)
    # ========================================================================
    
    if not envelopes:
        return f"No envelopes created. Processed {processed_count} tracks, {error_count} errors."
    
    # Separate individual and combined envelopes
    individual_envelopes = [e for e in envelopes if e['ENVELOPE_TYPE'] == 'INDIVIDUAL']
    combined_envelopes = [e for e in envelopes if e['ENVELOPE_TYPE'] == 'COMBINED']
    
    # Load individual envelopes
    if individual_envelopes:
        individual_df = pd.DataFrame(individual_envelopes)
        # Prepare for TC_ENVELOPES_INDIVIDUAL table
        individual_df = individual_df[[
            'FORECAST_TIME', 'TRACK_ID', 'ENSEMBLE_MEMBER', 
            'VALID_TIME', 'LEAD_TIME', 'WIND_THRESHOLD', 'ENVELOPE_REGION'
        ]].copy()
        
        # Convert to Snowpark DataFrame and merge into final table
        individual_sdf = session.create_dataframe(individual_df)
        
        # Use MERGE to handle duplicates
        individual_sdf.write.mode('overwrite').save_as_table('AOTS.ECMWF_PIPELINE.WIND_ENVELOPES_STAGING_INDIVIDUAL')
        
        # Merge into final table
        session.sql("""
            MERGE INTO AOTS.ECMWF_PIPELINE.TC_ENVELOPES_INDIVIDUAL t
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
                FROM AOTS.ECMWF_PIPELINE.WIND_ENVELOPES_STAGING_INDIVIDUAL
            ) s
            ON t.TRACK_ID = s.TRACK_ID 
                AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
                AND t.FORECAST_TIME = s.FORECAST_TIME
                AND t.LEAD_TIME = s.LEAD_TIME
                AND t.WIND_THRESHOLD = s.WIND_THRESHOLD
            WHEN NOT MATCHED THEN INSERT (
                FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME, LEAD_TIME,
                WIND_THRESHOLD, ENVELOPE_REGION
            ) VALUES (
                s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.VALID_TIME, s.LEAD_TIME,
                s.WIND_THRESHOLD, s.ENVELOPE_REGION
            )
        """).collect()
        
        # Clean up staging table
        session.sql("DROP TABLE IF EXISTS AOTS.ECMWF_PIPELINE.WIND_ENVELOPES_STAGING_INDIVIDUAL").collect()
    
    # Load combined envelopes
    if combined_envelopes:
        combined_df = pd.DataFrame(combined_envelopes)
        # Prepare for TC_ENVELOPES_COMBINED table
        # Extract LEAD_TIME_RANGE from VALID_TIME or use MIN(LEAD_TIME) approach
        combined_df = combined_df[[
            'FORECAST_TIME', 'TRACK_ID', 'ENSEMBLE_MEMBER', 
            'VALID_TIME', 'WIND_THRESHOLD', 'ENVELOPE_REGION'
        ]].copy()
        
        # Convert to Snowpark DataFrame and merge into final table
        combined_sdf = session.create_dataframe(combined_df)
        combined_sdf.write.mode('overwrite').save_as_table('AOTS.ECMWF_PIPELINE.WIND_ENVELOPES_STAGING_COMBINED')
        
        # Merge into final table (use MIN(LEAD_TIME) from individual envelopes for this forecast/track/member)
        session.sql("""
            MERGE INTO AOTS.ECMWF_PIPELINE.TC_ENVELOPES_COMBINED t
            USING (
                SELECT 
                    c.FORECAST_TIME,
                    c.TRACK_ID,
                    c.ENSEMBLE_MEMBER,
                    COALESCE(
                        (SELECT MIN(LEAD_TIME) 
                         FROM AOTS.ECMWF_PIPELINE.TC_ENVELOPES_INDIVIDUAL i
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
                FROM AOTS.ECMWF_PIPELINE.WIND_ENVELOPES_STAGING_COMBINED c
            ) s
            ON t.TRACK_ID = s.TRACK_ID 
                AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
                AND t.FORECAST_TIME = s.FORECAST_TIME
                AND t.WIND_THRESHOLD = s.WIND_THRESHOLD
            WHEN NOT MATCHED THEN INSERT (
                FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, LEAD_TIME_RANGE,
                WIND_THRESHOLD, ENVELOPE_REGION
            ) VALUES (
                s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.LEAD_TIME_RANGE,
                s.WIND_THRESHOLD, s.ENVELOPE_REGION
            )
        """).collect()
        
        # Clean up staging table
        session.sql("DROP TABLE IF EXISTS AOTS.ECMWF_PIPELINE.WIND_ENVELOPES_STAGING_COMBINED").collect()
    
    # ========================================================================
    # Summary
    # ========================================================================
    
    individual_count = len([e for e in envelopes if e['ENVELOPE_TYPE'] == 'INDIVIDUAL'])
    combined_count = len([e for e in envelopes if e['ENVELOPE_TYPE'] == 'COMBINED'])
    
    summary = (
        f"Created {len(envelopes)} envelopes: "
        f"{individual_count} individual, {combined_count} combined. "
        f"Processed {processed_count} tracks, {error_count} errors."
    )
    
    return summary
$$;

-- Verify procedure created
SELECT '✓ Stored procedure created' as status,
       'create_wind_envelopes()' as procedure_name,
       'Creates wind threshold envelope polygons from TC tracks + wind data' as description;

-- Usage examples (uncomment to test)
/*
-- Process all data
CALL create_wind_envelopes(NULL::TIMESTAMP_NTZ, NULL::VARCHAR, NULL::INTEGER);

-- Process specific storm (use proper storm name, not numbered identifiers like "13L" or "04S")
-- CALL create_wind_envelopes(NULL::TIMESTAMP_NTZ, 'KALMAEGI', NULL::INTEGER);

-- Process specific forecast and member
-- CALL create_wind_envelopes('2025-10-24 00:00:00'::TIMESTAMP_NTZ, NULL::VARCHAR, 1);

-- Test with one member of one storm (use proper storm name)
-- CALL create_wind_envelopes(NULL::TIMESTAMP_NTZ, 'MONTHA', 1);
*/

