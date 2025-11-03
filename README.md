# TC-ECMWF-Forecast-Pipeline – Data Pipeline Setup Guide

This repository contains the pipeline for processing ECMWF (European Centre for Medium-Range Weather Forecasts) tropical cyclone and wind forecast data. The pipeline downloads, extracts, transforms, and loads hurricane forecast data into Snowflake for use by downstream applications.

## Related Repositories

- **[Ahead-of-the-Storm](https://github.com/unicef-drp/Ahead-of-the-Storm)**: Dash web application for visualizing hurricane impact forecasts. The application displays interactive maps, probabilistic analysis, and impact reports based on pre-processed hurricane data

- **[Ahead-of-the-Storm-DATAPIPELINE](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE)**: Data processing pipeline for creating bounding boxes, initializing base data, and processing storm impact files that are read by the Ahead-of-the-Storm application

## Overview

The pipeline processes ECMWF ensemble tropical cyclone forecasts through the following steps:

1. **Download TC Data**: Downloads tropical cyclone track BUFR files from ECMWF's Dissemination (DISS) system
2. **Extract TC Data**: Extracts structured data from BUFR files using eccodes library
3. **Transform TC Data**: Converts raw data to standardized format with wind radii, polygons, and metadata
4. **Download Wind Data**: Downloads ensemble wind forecast GRIB files matching TC forecast run times
5. **Process Wind Combination**: Creates wind threshold envelope polygons by combining TC tracks with wind forecast data
6. **Load to Snowflake**: Loads processed data into Snowflake for querying and visualization

### Output Data

The pipeline produces three types of data:

- **TC Track Data** (`*_transformed.csv`): Individual forecast points with storm positions, wind speeds, pressure, wind radii, and wind field polygons
- **Individual Wind Envelopes** (`*_envelopes_individual.csv`): Wind threshold polygons for each forecast step and ensemble member
- **Combined Wind Envelopes** (`*_envelopes_combined.csv`): Combined wind threshold polygons across all forecast steps per member

## Prerequisites

1. **Python 3.11+** installed

2. **Virtual environment** activated (`.venv`)

3. **Environment variables** configured

   - Start from the provided example: `cp sample_env.txt .env`

   - Edit values to match your environment (Snowflake credentials)

4. **eccodes library** installed (required for BUFR file processing)

### Environment Setup

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Required Environment Variables

#### Snowflake Configuration (Required for GitHub Actions pipeline)

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

#### Optional Pipeline Configuration

- `DOWNLOAD_DATE` - Specific date to download (YYYYMMDD format, e.g., "20251015")
- `RUN_TIME` - Specific run time filter (00, 06, 12, or 18)
- `DOWNLOAD_LIMIT` - Number of latest forecasts to download (default: 1)
- `PROCESS_WIND_DATA` - Enable wind processing (default: true)
- `CLEANUP_AFTER_LOAD` - Clean up temporary files after load (default: true)
- `SKIP_EXISTING` - Skip already processed files (default: false in CI)

## Pipeline Components

The pipeline consists of several Python modules:

### Core Processing Modules

- **`ecmwf_tc_data_downloader.py`**: Downloads tropical cyclone BUFR files from ECMWF DISS system
- **`ecmwf_tc_data_extractor.py`**: Extracts structured data from BUFR files (Template 316082)
- **`ecmwf_tc_data_transformer.py`**: Transforms raw data to standardized CSV format with wind radii and polygons
- **`ecmwf_wind_data_downloader.py`**: Downloads ensemble wind forecast GRIB files from ECMWF Open Data
- **`ecmwf_wind_data_extractor.py`**: Extracts wind threshold polygons from GRIB files
- **`ecmwf_tc_wind_combination.py`**: Combines TC tracks with wind forecasts to create envelope polygons

### Infrastructure

- **`github_actions/main.py`**: Automated pipeline orchestrator for GitHub Actions
- **`github_actions/snowflake_loader.py`**: Snowflake database loader with staging table logic
- **`web/app.py`**: Dash web dashboard for visualizing forecast data from Snowflake
- **`visualization.py`**: Visualization utilities for TC tracks and wind envelopes

## Running the Pipeline

### Local Development

You can run individual pipeline steps interactively or use the Jupyter notebook for exploration:

```bash
# Interactive exploration
jupyter notebook pipeline_demonstration.ipynb
```

Or run individual steps:

```python
from ecmwf_tc_data_downloader import download_tc_data
from ecmwf_tc_data_extractor import extract_tc_data_from_file
from ecmwf_tc_data_transformer import transform_tc_data_from_file

# Step 1: Download TC data
download_tc_data(date="20251015", run_time="12", output_dir="tc_data")

# Step 2: Extract BUFR files
# (extract from downloaded .bin files)

# Step 3: Transform data
# (transform extracted CSV files)
```

### GitHub Actions Pipeline

The pipeline runs automatically on a schedule:

**Pipeline Schedule**

ECMWF issues new forecasts at **00, 06, 12, and 18 UTC**, but the data is typically not published until around **07:41, 11:40, 19:41, and 23:40 UTC**.

To align with these publication times, the pipeline is scheduled to run at:

- **09:00 UTC** (after 00Z forecast published)
- **13:00 UTC** (after 06Z forecast published)
- **21:00 UTC** (after 12Z forecast published)
- **01:00 UTC** (after 18Z forecast published)

This ensures the forecasts are available before the pipeline starts.

**Manual Trigger**

You can manually trigger the workflow with parameters:

- `download_date` (optional): Specific date in YYYYMMDD format
- `run_time` (optional): Forecast run time (00, 06, 12, or 18)
- `cleanup` (optional): Clean up temporary files after load (default: true)

**Setup for GitHub Actions**

1. Configure GitHub Secrets in your repository settings:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`

2. The workflow will automatically:
   - Download latest TC forecast data
   - Extract and transform data
   - Download matching wind forecast data
   - Process wind envelopes
   - Load all data to Snowflake

### Web Dashboard

The web dashboard provides interactive visualization of forecast data stored in Snowflake:

```bash
# Set Snowflake environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"

# Run the dashboard
python web/app.py
```

The dashboard will be available at `http://localhost:10000`

## Data Flow

```
ECMWF DISS System (BUFR files)
    ↓
[Download TC Data]
    ↓
BUFR Files (.bin)
    ↓
[Extract TC Data]
    ↓
Raw CSV Files
    ↓
[Transform TC Data]
    ↓
Transformed CSV Files (*_transformed.csv)
    ↓
ECMWF Open Data (GRIB files)
    ↓
[Download Wind Data]
    ↓
Wind GRIB Files (.grib2)
    ↓
[Process Wind Combination]
    ↓
Envelope CSV Files (*_envelopes_*.csv)
    ↓
[Load to Snowflake]
    ↓
Snowflake Tables (TC_TRACKS, TC_ENVELOPES_INDIVIDUAL, TC_ENVELOPES_COMBINED)
```

## Troubleshooting

### "eccodes library not found" error

- Install eccodes library (see Prerequisites section)
- On macOS: `brew install eccodes`

### "No BUFR files downloaded" error

- Check ECMWF data availability for the specified date/time
- Verify network connectivity to `https://essential.ecmwf.int/`
- Ensure forecast has been published (check publication times in schedule)

### "Snowflake connection error"

- Verify all `SNOWFLAKE_*` environment variables are set correctly
- Check network connectivity to Snowflake
- Ensure Snowflake credentials have proper permissions
- Verify warehouse is running

### "Wind file not found" error

- Ensure wind data download completed successfully
- Check that wind forecast run time matches TC forecast run time
- Verify forecast hours are available (wind data downloads every 6 hours from 0-144h)

### "Import errors" or "Module not found"

- Ensure you're in the virtual environment: `source .venv/bin/activate`
- Reinstall dependencies: `pip install -r requirements.txt`
- Check that all core modules are in the repository root

### Pipeline fails at transformation step

- Verify BUFR files were extracted correctly
- Check that extracted CSV files contain valid data
- Review error logs for specific column or data type issues

### GitHub Actions pipeline fails

- Verify all GitHub Secrets are set correctly
- Check GitHub Actions logs for specific error messages
- Ensure workflow file (`ecmwf-tc-pipline.yml`) exists and is configured correctly

### Web dashboard shows no data

- Verify Snowflake connection credentials
- Check that data has been loaded by the pipeline
- Verify database/schema names match
- Query Snowflake directly to confirm data exists

## Data Storage Locations

### Local Development

- **Raw TC data**: `tc_data/` (BUFR files and extracted CSVs)
- **Transformed TC data**: `tc_data_transformed/` (or `TRANSFORMED_DATA_DIR` env var)
- **Wind data**: `wind_data/` (GRIB files)
- **Wind envelopes**: `wind_extracted/` (envelope CSV files)

### Snowflake Tables

- **`TC_TRACKS`**: Individual forecast points with storm positions, wind speeds, pressure, and wind radii
- **`TC_ENVELOPES_INDIVIDUAL`**: Wind threshold polygons per forecast step
- **`TC_ENVELOPES_COMBINED`**: Combined wind threshold polygons across all forecast steps

### GitHub Actions

Temporary files are cleaned up after successful load (unless `CLEANUP_AFTER_LOAD=false`).

## Architecture

### Data Processing

- **BUFR Processing**: Uses eccodes library to parse ECMWF BUFR Template 316082
- **GRIB Processing**: Uses xarray and cfgrib to process ensemble wind forecast data
- **Geospatial Processing**: Uses Shapely and GeoPandas for polygon operations
- **Data Transformation**: Pandas for data manipulation and standardization

### Pipeline Execution

- **Local**: Python scripts can be run individually or via Jupyter notebook
- **Automated**: GitHub Actions workflow orchestrates all steps
- **Containerized**: Docker support for consistent execution environments

### Data Storage

- **Input**: ECMWF DISS system (BUFR) and ECMWF Open Data (GRIB)
- **Processing**: Local filesystem (temporary)
- **Output**: Snowflake data warehouse
- **Visualization**: Dash web application reading from Snowflake

## Quick Start Summary

```bash
# 1. Set up environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment variables
cp sample_env.txt .env
# Edit .env with your Snowflake credentials

# 3. Test locally (optional)
jupyter notebook pipeline_demonstration.ipynb

# 4. For automated pipeline: Configure GitHub Secrets and enable workflow
# The pipeline will run automatically on schedule
```

## References

- [ECMWF Dissemination System](https://essential.ecmwf.int/)
- [ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)
- [ECMWF BUFR Format Documentation](https://confluence.ecmwf.int/display/ECC/BUFR+examples)
- [eccodes Library](https://pypi.org/project/eccodes/)