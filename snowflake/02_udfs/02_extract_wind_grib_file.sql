-- ============================================================================
-- UDTF: extract_wind_grib_file
-- ============================================================================
-- Extracts wind data from ECMWF ensemble wind GRIB2 files
-- Reads 10m wind components (u10, v10) and calculates wind speed
-- 
-- Input: File path to GRIB2 file in stage
-- Output: Table with wind data per grid point per ensemble member
--
-- File Types:
--   - *_pf.grib2: Perturbed forecast (GRIB numbers 1-50) → TC members 1-50
--   - *_cf.grib2: Control forecast (GRIB number 0 or no ensemble) → TC member 51
--
-- Note: This is a simplified extraction that outputs raw grid data
--       Buffering and contouring will be done in subsequent UDFs
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_X86_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

CREATE OR REPLACE FUNCTION extract_wind_grib_file(
    file_path VARCHAR
)
RETURNS TABLE(
    SOURCE_FILE VARCHAR,
    FORECAST_TIME TIMESTAMP_NTZ,
    VALID_TIME TIMESTAMP_NTZ,
    LEAD_TIME INTEGER,
    ENSEMBLE_MEMBER INTEGER,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    WIND_U_COMPONENT FLOAT,
    WIND_V_COMPONENT FLOAT,
    WIND_SPEED_10M FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
RESOURCE_CONSTRAINT = (architecture='x86')
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('cfgrib', 'xarray', 'pandas', 'numpy', 'snowflake-snowpark-python')
HANDLER = 'WindGribExtractor'
AS
$$
import tempfile
import os
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
import pandas as pd

class WindGribExtractor:
    """
    Extracts wind data from GRIB2 files using cfgrib/xarray.
    
    Returns raw grid data with u/v components and calculated wind speed.
    """
    
    def __init__(self):
        self.results = []
    
    def process(self, file_path):
        """
        Process a single GRIB2 file and extract wind data.
        
        Args:
            file_path: Path to GRIB2 file in Snowflake stage
            
        Yields:
            tuple: Wind data records (one per grid point per ensemble member)
        """
        try:
            from snowflake.snowpark.files import SnowflakeFile
            
            # Read file from stage
            with SnowflakeFile.open(file_path, 'rb', require_scoped_url=False) as f:
                file_content = f.read()
            
            # Process GRIB2 data
            results = self._extract_wind_data(file_content, file_path)
            
            # Yield each result row
            for row in results:
                yield row
                
        except Exception as e:
            error_msg = f"ERROR: {type(e).__name__}: {str(e)}"
            # Return error record
            yield (file_path, None, None, None, None, None, None, None, None, None)
    
    def _extract_wind_data(self, file_content, file_path):
        """
        Extract wind data from GRIB2 file content.
        
        Args:
            file_content: Binary content of GRIB2 file
            file_path: Original file path (for metadata)
            
        Returns:
            list: List of tuples with wind data
        """
        results = []
        
        # Write content to temporary file (cfgrib needs a real file)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.grib2') as tmp:
            tmp.write(file_content)
            temp_path = tmp.name
        
        try:
            # Open GRIB2 file with cfgrib/xarray
            # Use filter_by_keys to get both u and v components
            ds = xr.open_dataset(
                temp_path,
                engine='cfgrib',
                backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround'}}
            )
            
            # Extract metadata
            # Forecast time (reference time)
            if 'time' in ds.coords:
                forecast_time = pd.to_datetime(ds['time'].values).to_pydatetime()
            else:
                forecast_time = None
            
            # Lead time (step)
            if 'step' in ds.coords:
                step_timedelta = pd.to_timedelta(ds['step'].values)
                lead_time_hours = int(step_timedelta.total_seconds() / 3600)
            else:
                lead_time_hours = None
            
            # Valid time
            if forecast_time and lead_time_hours is not None:
                valid_time = forecast_time + timedelta(hours=lead_time_hours)
            else:
                valid_time = None
            
            # Check for ensemble dimension
            has_ensemble = 'number' in ds.dims
            
            # Extract u10 and v10 components
            u10 = None
            v10 = None
            
            # Try to find u10 and v10 in the dataset
            if 'u10' in ds.variables:
                u10 = ds['u10']
            elif '10u' in ds.variables:
                u10 = ds['10u']
            
            if 'v10' in ds.variables:
                v10 = ds['v10']
            elif '10v' in ds.variables:
                v10 = ds['10v']
            
            if u10 is None or v10 is None:
                raise ValueError("Could not find u10/v10 wind components in GRIB2 file")
            
            # Get grid coordinates
            lats = ds['latitude'].values
            lons = ds['longitude'].values
            
            # Adjust longitudes to -180 to 180 range
            lons = np.where(lons > 180, lons - 360, lons)
            
            # Process ensemble members
            # Note: ECMWF downloads two separate file types:
            #   - PF files (*_pf.grib2): Contains GRIB numbers 1-50 (perturbed members)
            #   - CF files (*_cf.grib2): Contains GRIB number 0 (control forecast)
            #   - CF files may also have no ensemble dimension (deterministic)
            if has_ensemble:
                ensemble_numbers = ds['number'].values
                
                for grib_member_num in ensemble_numbers:
                    # Select data for this GRIB member
                    u_member = u10.sel(number=grib_member_num).values
                    v_member = v10.sel(number=grib_member_num).values
                    
                    # Calculate wind speed
                    wind_speed = np.sqrt(u_member**2 + v_member**2)
                    
                    # Map GRIB member number to TC member number
                    # This matches the Python extractor logic (ecmwf_wind_data_extractor.py):
                    #   - GRIB number 0 (from CF file) → TC control member 51
                    #   - GRIB number N (1-50, from PF file) → TC member N
                    if grib_member_num == 0:
                        tc_member_num = 51  # Control forecast (from CF file)
                    elif 1 <= grib_member_num <= 50:
                        tc_member_num = int(grib_member_num)  # Perturbed members (from PF file)
                    else:
                        # Unexpected GRIB number - skip
                        continue
                    
                    # Flatten arrays and create records
                    # Sample subset of grid points to reduce data volume
                    # (we'll filter to TC buffer zones in a later step)
                    lat_flat = lats.flatten()
                    lon_flat = lons.flatten()
                    u_flat = u_member.flatten()
                    v_flat = v_member.flatten()
                    speed_flat = wind_speed.flatten()
                    
                    # Sample every Nth point to reduce volume
                    # (full resolution is very large)
                    sample_rate = 4  # Take every 4th point
                    indices = np.arange(0, len(lat_flat), sample_rate)
                    
                    for idx in indices:
                        # Skip NaN values
                        if np.isnan(u_flat[idx]) or np.isnan(v_flat[idx]):
                            continue
                        
                        results.append((
                            file_path,
                            forecast_time,
                            valid_time,
                            lead_time_hours,
                            tc_member_num,  # Use TC member number (1-51) instead of GRIB number (0-50)
                            float(lat_flat[idx]),
                            float(lon_flat[idx]),
                            float(u_flat[idx]),
                            float(v_flat[idx]),
                            float(speed_flat[idx])
                        ))
            else:
                # No ensemble dimension - this is a deterministic/control forecast
                # Typically from CF files that don't have an ensemble dimension
                # Maps to TC control member 51 (matches Python extractor behavior)
                u_data = u10.values
                v_data = v10.values
                wind_speed = np.sqrt(u_data**2 + v_data**2)
                
                # Flatten and sample
                lat_flat = lats.flatten()
                lon_flat = lons.flatten()
                u_flat = u_data.flatten()
                v_flat = v_data.flatten()
                speed_flat = wind_speed.flatten()
                
                sample_rate = 4
                indices = np.arange(0, len(lat_flat), sample_rate)
                
                for idx in indices:
                    if np.isnan(u_flat[idx]) or np.isnan(v_flat[idx]):
                        continue
                    
                    results.append((
                        file_path,
                        forecast_time,
                        valid_time,
                        lead_time_hours,
                        51,  # Control forecast member 51 (from CF file without ensemble dimension)
                        float(lat_flat[idx]),
                        float(lon_flat[idx]),
                        float(u_flat[idx]),
                        float(v_flat[idx]),
                        float(speed_flat[idx])
                    ))
            
            ds.close()
            
        finally:
            # Clean up temp file
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        
        return results
$$;

-- Verify UDF created
SELECT 
    '✓ UDF created' as status,
    'extract_wind_grib_file(VARCHAR)' as function_name,
    'Extracts wind data from GRIB2 files' as description;


-- Test query (uncomment to test with a real file)
/*
SELECT 
    SOURCE_FILE,
    FORECAST_TIME,
    VALID_TIME,
    LEAD_TIME,
    COUNT(DISTINCT ENSEMBLE_MEMBER) as members,
    COUNT(*) as grid_points,
    AVG(WIND_SPEED_10M) as avg_wind_speed_ms,
    MAX(WIND_SPEED_10M) as max_wind_speed_ms
FROM TABLE(extract_wind_grib_file('@WIND_GRIB2_STAGE/20251024_00/wind_ens_2025-10-24_r00_f000h.grib2'))
GROUP BY SOURCE_FILE, FORECAST_TIME, VALID_TIME, LEAD_TIME;
*/

