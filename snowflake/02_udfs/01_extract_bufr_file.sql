-- ============================================================================
-- BUFR File Extraction UDF
-- ============================================================================
-- Extracts tropical cyclone track data from ECMWF BUFR files
-- Processes BUFR Template 316082 format with ensemble forecast data
-- ============================================================================
-- Prerequisites:
--   - Database: AOTS
--   - Schema: ECMWF_PIPELINE
--   - Warehouse: AOTS_X86_WH (x86 architecture required for eccodes)
--   - Table: TC_RAW_EXTRACTED
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Create UDF: extract_bufr_file
-- ============================================================================

CREATE OR REPLACE FUNCTION extract_bufr_file(
    file_path VARCHAR  -- Path to BUFR file in stage (e.g., '@TC_BUFR_STAGE/20241024_00/file.bin')
)
RETURNS TABLE(
    SOURCE_FILE VARCHAR,
    FORECAST_TIME TIMESTAMP_NTZ,
    STORM_IDENTIFIER VARCHAR,
    ENSEMBLE_MEMBER INTEGER,
    STEP INTEGER,
    DATETIME TIMESTAMP_NTZ,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    PRESSURE FLOAT,
    WIND_SPEED FLOAT,
    WLATITUDE FLOAT,
    WLONGITUDE FLOAT,
    WIND_THRESHOLD INTEGER,
    QUADRANT INTEGER,
    WIND_RADIUS_M FLOAT,
    BEARING_START FLOAT,
    BEARING_END FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
RESOURCE_CONSTRAINT = (architecture='x86')
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('eccodes', 'pandas', 'numpy', 'snowflake-snowpark-python')
HANDLER = 'BUFRExtractor'
AS
$$
from datetime import datetime, timedelta
from eccodes import *
import numpy as np
import pandas as pd

class BUFRExtractor:
    """
    UDTF for extracting tropical cyclone track data from ECMWF BUFR files.
    Processes BUFR Template 316082 format with ensemble forecasts.
    """
    
    def __init__(self):
        """Initialize the UDTF"""
        self.results = []
    
    def process(self, file_path):
        """
        Process a BUFR file and extract TC track data.
        
        Args:
            file_path (str): Path to BUFR file in Snowflake stage
        
        Yields:
            tuple: Extracted TC track data records
        """
        try:
            # Import SnowflakeFile for reading from stage
            from snowflake.snowpark.files import SnowflakeFile
            
            # Open BUFR file from stage
            # Note: require_scoped_url=False is needed for UDFs to access stage files
            with SnowflakeFile.open(file_path, 'rb', require_scoped_url=False) as f:
                file_content = f.read()
            
            # Process BUFR messages
            results = self._extract_bufr_data(file_content, file_path)
            
            # Yield each result row
            for row in results:
                yield row
                
        except Exception as e:
            # Yield error row with actual error message in STORM_IDENTIFIER (17 columns for LONG format)
            error_msg = f"ERROR: {type(e).__name__}: {str(e)}"
            yield (
                file_path,          # SOURCE_FILE
                None,               # FORECAST_TIME
                error_msg[:255],    # STORM_IDENTIFIER - show error
                None,               # ENSEMBLE_MEMBER
                None,               # STEP
                None,               # DATETIME
                None,               # LATITUDE
                None,               # LONGITUDE
                None,               # PRESSURE
                None,               # WIND_SPEED
                None,               # WLATITUDE
                None,               # WLONGITUDE
                None,               # WIND_THRESHOLD
                None,               # QUADRANT
                None,               # WIND_RADIUS_M
                None,               # BEARING_START
                None                # BEARING_END
            )
    
    def _extract_bufr_data(self, file_content, file_path):
        """
        Extract data from BUFR file content.
        
        Args:
            file_content (bytes): BUFR file content
            file_path (str): Original file path (for metadata)
        
        Returns:
            list: List of tuples with extracted data
        """
        import tempfile
        import os
        
        results = []
        
        # Write content to a temporary file (eccodes needs a real file)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.bufr') as tmp:
            tmp.write(file_content)
            temp_path = tmp.name
        
        try:
            # Open the temporary file for eccodes
            bufr_file = open(temp_path, 'rb')
            
            # Loop through messages in the file
            while True:
                # Get handle for message
                bufr = codes_bufr_new_from_file(bufr_file)
                if bufr is None:
                    break  # End of file reached
                
                try:
                    # Unpack the data
                    codes_set(bufr, 'unpack', 1)
                    
                    # Extract metadata
                    year = codes_get(bufr, "year")
                    month = codes_get(bufr, "month")
                    day = codes_get(bufr, "day")
                    hour = codes_get(bufr, "hour")
                    minute = codes_get(bufr, "minute")
                    
                    # Create forecast time
                    forecast_time = datetime(year, month, day, hour, minute)
                
                    # Get storm identifier
                    storm_id = codes_get(bufr, "stormIdentifier")
                
                    # Get ensemble member numbers
                    member_numbers = codes_get_array(bufr, "ensembleMemberNumber")
                
                    # Determine number of forecast periods
                    number_of_periods = 0
                    while True:
                        number_of_periods += 1
                        try:
                            codes_get_array(bufr, f"#{number_of_periods}#timePeriod")
                        except CodesInternalError:
                            break
                
                    # Extract maximum wind location data (Code 3)
                    lat_max_wind_0 = codes_get_array(bufr, '#3#latitude')
                    lon_max_wind_0 = codes_get_array(bufr, '#3#longitude')
                    wind_max_wind_0 = codes_get_array(bufr, '#1#windSpeedAt10M')
                
                    # Extract storm center analysis data (Codes 4 & 5)
                    lat_analysis = codes_get_array(bufr, '#2#latitude')
                    lon_analysis = codes_get_array(bufr, '#2#longitude')
                    press_analysis = codes_get_array(bufr, '#1#pressureReducedToMeanSeaLevel')
                
                    # Initialize data storage for all members and time steps
                    data = {}
                    for k in range(len(member_numbers)):
                        data[k] = {}
                        # Store analysis (time step 0)
                        data[k][0] = [
                            lat_analysis[k] if len(lat_analysis) == len(member_numbers) else lat_analysis[0],
                            lon_analysis[k] if len(lon_analysis) == len(member_numbers) else lon_analysis[0],
                            press_analysis[k] if len(press_analysis) == len(member_numbers) else press_analysis[0],
                            lat_max_wind_0[k] if len(lat_max_wind_0) == len(member_numbers) else lat_max_wind_0[0],
                            lon_max_wind_0[k] if len(lon_max_wind_0) == len(member_numbers) else lon_max_wind_0[0],
                            wind_max_wind_0[k] if len(wind_max_wind_0) == len(member_numbers) else wind_max_wind_0[0]
                        ]
                
                    # Extract forecast periods
                    time_periods = [0] * number_of_periods
                
                    for i in range(1, number_of_periods):
                        rank1 = i * 2 + 2  # Storm center
                        rank3 = i * 2 + 3  # Maximum wind
                    
                        # Get time period
                        ivalues = codes_get_array(bufr, f"#{i}#timePeriod")
                        if len(ivalues) == 1:
                            time_periods[i] = ivalues[0]
                        else:
                            for j in range(len(ivalues)):
                                if ivalues[j] != CODES_MISSING_LONG:
                                    time_periods[i] = ivalues[j]
                                    break
                    
                        # Extract storm center
                        lat = codes_get_array(bufr, f"#{rank1}#latitude")
                        lon = codes_get_array(bufr, f"#{rank1}#longitude")
                        press = codes_get_array(bufr, f"#{i + 1}#pressureReducedToMeanSeaLevel")
                    
                        # Extract maximum wind
                        lat_wind = codes_get_array(bufr, f"#{rank3}#latitude")
                        lon_wind = codes_get_array(bufr, f"#{rank3}#longitude")
                        wind_10m = codes_get_array(bufr, f"#{i + 1}#windSpeedAt10M")
                    
                        # Check if valid data exists
                        if len(lat) == 1 and (lat[0] == CODES_MISSING_DOUBLE or lat[0] == -1e+100):
                            continue  # Skip invalid data
                    
                        # Store forecast data
                        for k in range(len(member_numbers)):
                            data[k][i] = [
                                lat[k] if len(lat) == len(member_numbers) else lat[0],
                                lon[k] if len(lon) == len(member_numbers) else lon[0],
                                press[k] if len(press) == len(member_numbers) else press[0],
                                lat_wind[k] if len(lat_wind) == len(member_numbers) else lat_wind[0],
                                lon_wind[k] if len(lon_wind) == len(member_numbers) else lon_wind[0],
                                wind_10m[k] if len(wind_10m) == len(member_numbers) else wind_10m[0]
                            ]
                
                    # =========================================================
                    # EXTRACT WIND RADII DATA
                    # =========================================================
                    # ECMWF Tropical Cyclone Wind Radii product
                    # windSpeedThreshold: 18, 26, 33 m/s (34, 50, 64 knots)
                    # bearingOrAzimuth: quadrant boundaries (degrees)
                    # effectiveRadiusWithRespectToWindSpeedsAboveThreshold: radius in meters
                    
                    wind_radii_data = {}  # [member][timestep][threshold_ms] = {'ne': r, 'se': r, 'sw': r, 'nw': r}
                    
                    # Helper function to map bearing angle to quadrant direction
                    def bearing_to_quadrant(bearing_start, bearing_end):
                        """
                        Map bearing angle range to quadrant direction.
                        Quadrants: NE (0-90), SE (90-180), SW (180-270), NW (270-360)
                        """
                        # Normalize to 0-360 range
                        start = bearing_start % 360
                        end = bearing_end % 360
                        
                        # Determine quadrant based on start angle
                        if 0 <= start < 90 or (start >= 360 and end < 90):
                            return 'ne'  # Northeast (0-90)
                        elif 90 <= start < 180:
                            return 'se'  # Southeast (90-180)
                        elif 180 <= start < 270:
                            return 'sw'  # Southwest (180-270)
                        elif 270 <= start < 360:
                            return 'nw'  # Northwest (270-360)
                        else:
                            # Fallback: use midpoint
                            mid = (start + end) / 2.0 % 360
                            if 0 <= mid < 90:
                                return 'ne'
                            elif 90 <= mid < 180:
                                return 'se'
                            elif 180 <= mid < 270:
                                return 'sw'
                            else:
                                return 'nw'
                    
                    # Wind radii extraction - wrap in try/except as these keys may not exist in all BUFR files
                    try:
                        # Try to get wind radii keys - catch any eccodes errors
                        try:
                            wind_speed_threshold = codes_get_array(bufr, 'windSpeedThreshold')
                        except:
                            wind_speed_threshold = None
                        
                        try:
                            bearing_or_azimuth = codes_get_array(bufr, 'bearingOrAzimuth')
                        except:
                            bearing_or_azimuth = None
                        
                        try:
                            wind_radii = codes_get_array(bufr, 'effectiveRadiusWithRespectToWindSpeedsAboveThreshold')
                        except:
                            wind_radii = None
                        
                        # Only process if we got all three keys
                        if wind_speed_threshold is None or bearing_or_azimuth is None or wind_radii is None:
                            raise ValueError("Wind radii keys not available in BUFR file")
                        
                        # Structure: [member][timestep][threshold][direction]
                        n_thresholds = 3  # 18, 26, 33 m/s
                        n_quadrants = 4   # NE, SE, SW, NW
                        values_per_member_per_timestep = n_thresholds * n_quadrants
                        
                        for m in range(len(member_numbers)):
                            wind_radii_data[m] = {}
                            for t in range(number_of_periods):
                                wind_radii_data[m][t] = {}
                                
                                # Calculate base index for this member/timestep
                                base_idx = (m * number_of_periods * values_per_member_per_timestep +
                                           t * values_per_member_per_timestep)
                                
                                # Extract data for each threshold
                                for thresh_idx in range(n_thresholds):
                                    # Get threshold value
                                    if t * n_thresholds + thresh_idx < len(wind_speed_threshold):
                                        threshold_val = wind_speed_threshold[t * n_thresholds + thresh_idx]
                                    else:
                                        threshold_val = [18, 26, 33][thresh_idx]
                                    
                                    # Initialize quadrant dictionary for this threshold
                                    # Store as dict with radius (in meters), bearing_start, bearing_end
                                    quadrant_dict = {'ne': None, 'se': None, 'sw': None, 'nw': None}
                                    
                                    # Extract radii for all 4 quadrants with bearing information
                                    for quad_idx in range(n_quadrants):
                                        radius_idx = base_idx + thresh_idx * n_quadrants + quad_idx
                                        
                                        # Get bearing pair for this quadrant
                                        # Bearing calculation: (t * n_thresholds + thresh_idx) * n_quadrants * 2 + quad_idx * 2
                                        bearing_base = (t * n_thresholds + thresh_idx) * n_quadrants * 2 + quad_idx * 2
                                        
                                        if radius_idx < len(wind_radii):
                                            radius = wind_radii[radius_idx]
                                            
                                            # Get bearing information
                                            bearing_start = None
                                            bearing_end = None
                                            if bearing_base + 1 < len(bearing_or_azimuth):
                                                bearing_start = bearing_or_azimuth[bearing_base]
                                                bearing_end = bearing_or_azimuth[bearing_base + 1]
                                            
                                            # Convert missing values to None
                                            if radius == CODES_MISSING_DOUBLE or radius == -1e+100 or radius < 0:
                                                radius_value = None
                                            else:
                                                # Keep radius in METERS
                                                radius_value = radius
                                            
                                            # Map bearing to quadrant direction
                                            if bearing_start is not None and bearing_end is not None:
                                                direction = bearing_to_quadrant(bearing_start, bearing_end)
                                            else:
                                                # No bearing info available - use assumed order (fallback)
                                                # Order: [NE, SE, SW, NW] based on quad_idx
                                                direction_map = ['ne', 'se', 'sw', 'nw']
                                                direction = direction_map[quad_idx]
                                            
                                            # Store radius with bearing info in correct quadrant
                                            quadrant_dict[direction] = {
                                                'radius': radius_value,
                                                'bearing_start': bearing_start,
                                                'bearing_end': bearing_end
                                            }
                                    
                                    # Store quadrant dictionary for this threshold
                                    wind_radii_data[m][t][threshold_val] = quadrant_dict
                    
                    except Exception as e:
                        # Wind radii data might not be available for all files
                        # Initialize empty structure
                        for m in range(len(member_numbers)):
                            wind_radii_data[m] = {}
                            for t in range(number_of_periods):
                                wind_radii_data[m][t] = {
                                    18: {'ne': None, 'se': None, 'sw': None, 'nw': None},
                                    26: {'ne': None, 'se': None, 'sw': None, 'nw': None},
                                    33: {'ne': None, 'se': None, 'sw': None, 'nw': None}
                                }
                
                    # Convert to result records
                    for m in range(len(member_numbers)):
                        member_num = int(member_numbers[m])
                    
                        # Filter out HRES member (52) - keep only ensemble members (1-51)
                        if member_num == 52:
                            continue
                    
                        for s in data[m].keys():
                            step_hours = time_periods[s]
                            valid_time = forecast_time + timedelta(hours=int(step_hours))
                        
                            # Extract values
                            lat, lon, press, w_lat, w_lon, wind = data[m][s]
                        
                            # Skip missing values
                            if lat == CODES_MISSING_DOUBLE or lat == -1e+100:
                                continue
                            
                            # Create base row data (repeated for each threshold/quadrant)
                            base_data = (
                                file_path,
                                forecast_time,
                                storm_id,
                                member_num,
                                int(step_hours),
                                valid_time,
                                float(lat),
                                float(lon),
                                float(press) if press != CODES_MISSING_DOUBLE else None,
                                float(wind) if wind != CODES_MISSING_DOUBLE else None,
                                float(w_lat) if w_lat != CODES_MISSING_DOUBLE else None,
                                float(w_lon) if w_lon != CODES_MISSING_DOUBLE else None
                            )
                            
                            # Extract wind radii for this member/timestep in LONG format
                            # Create one row per threshold per quadrant (matching Python extractor)
                            # Thresholds: 18 m/s (34kt), 26 m/s (50kt), 33 m/s (64kt)
                            # Quadrants: 1=NE, 2=SE, 3=SW, 4=NW
                            for threshold_ms in [18, 26, 33]:
                                threshold_data = wind_radii_data.get(m, {}).get(s, {}).get(threshold_ms, {})
                                
                                # Map quadrant names to numbers (matching Python: 1=NE, 2=SE, 3=SW, 4=NW)
                                quadrant_map = {'ne': 1, 'se': 2, 'sw': 3, 'nw': 4}
                                
                                for quadrant_name, quadrant_num in quadrant_map.items():
                                    quadrant_data = threshold_data.get(quadrant_name)
                                    
                                    # Handle both dict format (with bearing) and direct value format (legacy)
                                    if isinstance(quadrant_data, dict):
                                        radius_m = quadrant_data.get('radius')
                                        bearing_start = quadrant_data.get('bearing_start')
                                        bearing_end = quadrant_data.get('bearing_end')
                                    else:
                                        # Legacy format: just a value
                                        radius_m = quadrant_data
                                        bearing_start = None
                                        bearing_end = None
                                    
                                    # Create one row per threshold/quadrant combination
                                    results.append(
                                        base_data + (
                                            threshold_ms,  # WIND_THRESHOLD in m/s
                                            quadrant_num,  # QUADRANT (1-4)
                                            float(radius_m) if radius_m is not None and not (isinstance(radius_m, float) and np.isnan(radius_m)) else None,  # WIND_RADIUS_M
                                            float(bearing_start) if bearing_start is not None and not (isinstance(bearing_start, float) and np.isnan(bearing_start)) else None,  # BEARING_START
                                            float(bearing_end) if bearing_end is not None and not (isinstance(bearing_end, float) and np.isnan(bearing_end)) else None  # BEARING_END
                                        )
                                    )
                    
                finally:
                    # Release the BUFR message
                    codes_release(bufr)
            
            # Close the file (after processing all messages)
            bufr_file.close()
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        
        return results
$$;

-- ============================================================================
-- Test the UDF
-- ============================================================================

-- Example: Extract data from a single BUFR file
-- SELECT * FROM TABLE(extract_bufr_file('@TC_BUFR_STAGE/20241024_00/Z__C_ECEP_20241024000000_tropical_cyclone_track_FRANCINE_eps_global_20241024000000.bin'));

-- Example: Get summary of extraction
-- SELECT 
--     SOURCE_FILE,
--     STORM_IDENTIFIER,
--     COUNT(*) as record_count,
--     COUNT(DISTINCT ENSEMBLE_MEMBER) as member_count,
--     MIN(STEP) as min_step,
--     MAX(STEP) as max_step
-- FROM TABLE(extract_bufr_file('@TC_BUFR_STAGE/20241024_00/file.bin'))
-- GROUP BY SOURCE_FILE, STORM_IDENTIFIER;

SELECT '✓ UDF created successfully' as status,
       'extract_bufr_file(VARCHAR)' as function_name,
       'Extracts TC track data from BUFR files in stages' as purpose;

