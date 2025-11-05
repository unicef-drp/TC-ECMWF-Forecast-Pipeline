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
    file_path VARCHAR,
    bbox_lat_min FLOAT DEFAULT NULL,
    bbox_lat_max FLOAT DEFAULT NULL,
    bbox_lon_min FLOAT DEFAULT NULL,
    bbox_lon_max FLOAT DEFAULT NULL
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
    
    def process(self, file_path, bbox_lat_min=None, bbox_lat_max=None, bbox_lon_min=None, bbox_lon_max=None):
        """
        Process a single GRIB2 file and extract wind data.
        
        Args:
            file_path: Path to GRIB2 file in Snowflake stage
            bbox_lat_min: Optional minimum latitude for bounding box filtering
            bbox_lat_max: Optional maximum latitude for bounding box filtering
            bbox_lon_min: Optional minimum longitude for bounding box filtering
            bbox_lon_max: Optional maximum longitude for bounding box filtering
            
        Yields:
            tuple: Wind data records (one per grid point per ensemble member within bounding box)
        """
        try:
            from snowflake.snowpark.files import SnowflakeFile
            
            # Read file from stage
            with SnowflakeFile.open(file_path, 'rb', require_scoped_url=False) as f:
                file_content = f.read()
            
            # Process GRIB2 data with optional bounding box
            results = self._extract_wind_data(file_content, file_path, bbox_lat_min, bbox_lat_max, bbox_lon_min, bbox_lon_max)
            
            # Yield each result row
            for row in results:
                yield row
                
        except Exception as e:
            error_msg = f"ERROR: {type(e).__name__}: {str(e)}"
            # Return error record
            yield (file_path, None, None, None, None, None, None, None, None, None)
    
    def _extract_wind_data(self, file_content, file_path, bbox_lat_min=None, bbox_lat_max=None, bbox_lon_min=None, bbox_lon_max=None):
        """
        Extract wind data from GRIB2 file content, optionally filtered by bounding box.
        
        Args:
            file_content: Binary content of GRIB2 file
            file_path: Original file path (for metadata)
            bbox_lat_min: Optional minimum latitude for filtering
            bbox_lat_max: Optional maximum latitude for filtering
            bbox_lon_min: Optional minimum longitude for filtering
            bbox_lon_max: Optional maximum longitude for filtering
            
        Returns:
            list: List of tuples with wind data (filtered to bounding box if provided)
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
            
            # Get grid coordinates (before subsetting)
            # Note: xarray coordinates may be in 0-360 format for longitude
            lats = ds['latitude'].values
            lons_orig = ds['longitude'].values
            
            # Check if longitudes are in 0-360 format (ECMWF default)
            # We'll convert bbox to match the coordinate system
            lons_are_0_360 = np.any(lons_orig > 180)
            
            # For display/output, convert to -180 to 180 range
            lons = np.where(lons_orig > 180, lons_orig - 360, lons_orig)
            
            # Ensure we have a proper 2D grid structure
            if len(lats.shape) == 1 and len(lons.shape) == 1:
                # Create 2D meshgrid if we have 1D arrays
                lon_grid, lat_grid = np.meshgrid(lons, lats)
                lats = lat_grid
                lons = lon_grid
                # Also create 0-360 version for xarray subsetting
                if lons_are_0_360:
                    lon_grid_0_360, _ = np.meshgrid(lons_orig, lats)
                    lons_0_360 = lon_grid_0_360
                else:
                    lons_0_360 = lons
            else:
                lons_0_360 = lons_orig if lons_are_0_360 else lons
            
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
                    
                    # Get array dimensions
                    if len(u_member.shape) == 2:
                        n_lat, n_lon = u_member.shape
                    else:
                        # Fallback: flatten if shape is unexpected
                        n_lat = len(lats) if len(lats.shape) == 1 else lats.shape[0]
                        n_lon = len(lons) if len(lons.shape) == 1 else lons.shape[-1] if len(lons.shape) > 1 else len(lons)
                    
                    # Sample every Nth point in both dimensions to reduce volume
                    # This preserves spatial coverage across the entire globe
                    sample_rate = 4  # Take every 4th point in both dimensions
                    
                    # Use xarray's .sel() to subset by bounding box if provided (more efficient)
                    # This matches the Python extractor approach
                    if bbox_lat_min is not None and bbox_lat_max is not None and bbox_lon_min is not None and bbox_lon_max is not None:
                        # Convert bbox to match coordinate system (0-360 vs -180 to 180)
                        # If dataset uses 0-360, convert bbox longitudes
                        if lons_are_0_360:
                            # Convert bbox from -180/180 to 0/360
                            bbox_lon_min_sel = bbox_lon_min if bbox_lon_min >= 0 else bbox_lon_min + 360
                            bbox_lon_max_sel = bbox_lon_max if bbox_lon_max >= 0 else bbox_lon_max + 360
                        else:
                            bbox_lon_min_sel = bbox_lon_min
                            bbox_lon_max_sel = bbox_lon_max
                        
                        # Subset to bounding box using xarray (like Python extractor)
                        # Note: xarray uses (max, min) for latitude slice (descending order)
                        try:
                            u_subset = u10.sel(
                                number=grib_member_num,
                                latitude=slice(bbox_lat_max, bbox_lat_min),
                                longitude=slice(bbox_lon_min_sel, bbox_lon_max_sel)
                            )
                            v_subset = v10.sel(
                                number=grib_member_num,
                                latitude=slice(bbox_lat_max, bbox_lat_min),
                                longitude=slice(bbox_lon_min_sel, bbox_lon_max_sel)
                            )
                            u_member = u_subset.values
                            v_member = v_subset.values
                            wind_speed = np.sqrt(u_member**2 + v_member**2)
                            
                            # Get subset coordinates
                            if hasattr(u_subset, 'latitude'):
                                lats_subset = u_subset.latitude.values
                                lons_subset = u_subset.longitude.values
                                if len(lats_subset.shape) == 1 and len(lons_subset.shape) == 1:
                                    lon_grid, lat_grid = np.meshgrid(lons_subset, lats_subset)
                                    lats = lat_grid
                                    lons = lon_grid
                                else:
                                    lats = lats_subset
                                    lons = lons_subset
                            else:
                                # Fallback: use original coordinates and filter after
                                pass
                        except Exception as e:
                            # If subsetting fails, fall back to full grid with filtering
                            pass
                    
                    # Get array dimensions after possible subsetting
                    if len(u_member.shape) == 2:
                        n_lat, n_lon = u_member.shape
                    else:
                        n_lat = len(lats) if len(lats.shape) == 1 else lats.shape[0]
                        n_lon = len(lons) if len(lons.shape) == 1 else lons.shape[-1] if len(lons.shape) > 1 else len(lons)
                    
                    # Iterate over grid with sampling
                    for i in range(0, n_lat, sample_rate):
                        for j in range(0, n_lon, sample_rate):
                            # Get lat/lon values
                            if len(lats.shape) == 2:
                                lat_val = float(lats[i, j])
                                lon_val = float(lons[i, j])
                            elif len(lats.shape) == 1:
                                lat_val = float(lats[i])
                                lon_val = float(lons[j])
                            else:
                                # Fallback: use flattened arrays
                                lat_val = float(lats.flatten()[i * n_lon + j])
                                lon_val = float(lons.flatten()[i * n_lon + j])
                            
                            # Apply bounding box filter if not already done by xarray
                            if bbox_lat_min is not None and bbox_lat_max is not None and bbox_lon_min is not None and bbox_lon_max is not None:
                                if not (bbox_lat_min <= lat_val <= bbox_lat_max and bbox_lon_min <= lon_val <= bbox_lon_max):
                                    continue
                            
                            # Get wind values
                            u_val = float(u_member[i, j])
                            v_val = float(v_member[i, j])
                            speed_val = float(wind_speed[i, j])
                            
                            # Skip NaN values
                            if np.isnan(u_val) or np.isnan(v_val) or np.isnan(speed_val):
                                continue
                            
                            results.append((
                                file_path,
                                forecast_time,
                                valid_time,
                                lead_time_hours,
                                tc_member_num,  # Use TC member number (1-51) instead of GRIB number (0-50)
                                lat_val,
                                lon_val,
                                u_val,
                                v_val,
                                speed_val
                            ))
            else:
                # No ensemble dimension - this is a deterministic/control forecast
                # Typically from CF files that don't have an ensemble dimension
                # Maps to TC control member 51 (matches Python extractor behavior)
                u_data = u10.values
                v_data = v10.values
                wind_speed = np.sqrt(u_data**2 + v_data**2)
                
                # Get array dimensions and sample
                if len(u_data.shape) == 2:
                    n_lat, n_lon = u_data.shape
                else:
                    n_lat = len(lats) if len(lats.shape) == 1 else lats.shape[0]
                    n_lon = len(lons) if len(lons.shape) == 1 else lons.shape[-1] if len(lons.shape) > 1 else len(lons)
                
                sample_rate = 4
                
                for i in range(0, n_lat, sample_rate):
                    for j in range(0, n_lon, sample_rate):
                        # Get lat/lon values
                        if len(lats.shape) == 2:
                            lat_val = float(lats[i, j])
                            lon_val = float(lons[i, j])
                        elif len(lats.shape) == 1:
                            lat_val = float(lats[i])
                            lon_val = float(lons[j])
                        else:
                            lat_val = float(lats.flatten()[i * n_lon + j])
                            lon_val = float(lons.flatten()[i * n_lon + j])
                        
                        # Get wind values
                        u_val = float(u_data[i, j])
                        v_val = float(v_data[i, j])
                        speed_val = float(wind_speed[i, j])
                        
                        if np.isnan(u_val) or np.isnan(v_val) or np.isnan(speed_val):
                            continue
                        
                        results.append((
                            file_path,
                            forecast_time,
                            valid_time,
                            lead_time_hours,
                            51,  # Control forecast member 51 (from CF file without ensemble dimension)
                            lat_val,
                            lon_val,
                            u_val,
                            v_val,
                            speed_val
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

