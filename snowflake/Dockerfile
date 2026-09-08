# ==============================================================================
# License
# ==============================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# UNSUPPORTED BY SNOWFLAKE - CUSTOMER SUPPORTED ONLY
#
# ==============================================================================
# Dockerfile for ECMWF TC Forecast Pipeline (SPCS Deployment)
# ==============================================================================
# This container runs the complete TC forecast pipeline including:
# - TC data download from ECMWF
# - BUFR extraction and transformation
# - Wind data download and processing
# - Met data download and zarr storage
# - Snowflake data loading
#
# The container will execute snowflake/spcs_pipeline.py and terminate when complete.
# ==============================================================================

FROM python:3.11-slim

LABEL maintainer="TC Forecast Pipeline"
LABEL description="ECMWF Tropical Cyclone Forecast Pipeline with Snowflake Integration (SPCS)"
LABEL version="2.0"

# ==============================================================================
# Install system dependencies
# ==============================================================================

# Install system packages required for:
# - eccodes: BUFR file processing
# - GRIB: GRIB2 file processing
# - geospatial: Shapely and geopandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    # BUFR/GRIB processing
    libeccodes-dev \
    libeccodes-tools \
    # HDF5/NetCDF: required by zarr/cfgrib
    libhdf5-dev \
    libnetcdf-dev \
    # Geospatial libraries
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    pkg-config \
    # Compilation tools
    build-essential \
    gcc \
    g++ \
    gfortran \
    # Utilities
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# ==============================================================================
# Set environment variables
# ==============================================================================

# Python environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# Headless matplotlib backend: required for wind isoline extraction (ax.contour())
ENV MPLBACKEND=Agg

# Set working directory
WORKDIR /app

# ==============================================================================
# Copy application code
# ==============================================================================

# Copy all pipeline modules from repo root
COPY ecmwf_*.py /app/
COPY pipeline_core.py /app/

# Copy snowflake directory (contains spcs_pipeline.py and snowflake_loader.py)
COPY snowflake/ /app/snowflake/

# Copy requirements (lean pipeline-only set, no cartopy/scipy)
COPY requirements-ci.txt /app/requirements-ci.txt

# ==============================================================================
# Install Python dependencies
# ==============================================================================

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install Python packages
RUN pip install --no-cache-dir -r requirements-ci.txt

# ==============================================================================
# Create directories for data
# ==============================================================================

RUN mkdir -p /app/tc_data \
    /app/tc_transformed \
    /app/wind_data \
    /app/wind_extracted \
    /app/met_data/grib_tmp

# ==============================================================================
# Health check (optional - for monitoring)
# ==============================================================================

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# ==============================================================================
# Set default environment variables (can be overridden at runtime)
# ==============================================================================

# Pipeline configuration
ENV RAW_DATA_DIR=/app/tc_data
ENV TRANSFORMED_DATA_DIR=/app/tc_transformed
ENV WIND_DATA_DIR=/app/wind_data
ENV WIND_EXTRACTED_DIR=/app/wind_extracted

# Defaults (override these at runtime)
ENV DOWNLOAD_LIMIT=1
ENV MAX_WORKERS=4
ENV PROCESS_WIND_DATA=true
ENV PROCESS_MET=true
ENV MET_DATA_DIR=/app/met_data
ENV NAMED_STORMS_ONLY=true
ENV CLEANUP_AFTER_LOAD=true
ENV USE_PROCESS_POOL=false
# SNOWFLAKE_STAGE_NAME must be overridden at runtime when PROCESS_MET=true

# Snowflake defaults (MUST be overridden at runtime)
ENV SNOWFLAKE_WAREHOUSE=COMPUTE_WH
ENV SNOWFLAKE_DATABASE=WEATHER_DB
ENV SNOWFLAKE_SCHEMA=TC_DATA

# ==============================================================================
# Entrypoint and command
# ==============================================================================

# Create startup script to clean old index files
# Use printf so \n is expanded as real newlines (echo does not expand \n in /bin/sh)
RUN printf '#!/bin/bash\nrm -rf /app/wind_data/.idx_cache 2>/dev/null || true\nrm -f /app/wind_data/*.idx 2>/dev/null || true\necho "Cleaned old index files"\nexec python snowflake/spcs_pipeline.py\n' \
    > /app/start.sh && chmod +x /app/start.sh

# Run the SPCS pipeline with cleanup
CMD ["/app/start.sh"]

