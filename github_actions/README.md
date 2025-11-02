# GitHub Actions Pipeline

This directory contains the GitHub Actions-specific implementation of the ECMWF TC Forecast Pipeline.

## Overview

The GitHub Actions pipeline automates the complete data processing workflow:
1. Downloads TC forecast data from ECMWF
2. Extracts and transforms the data
3. Downloads matching wind forecast data
4. Processes wind envelopes
5. Loads all data to Snowflake

## Files

- `main.py` - Pipeline orchestrator that runs all steps and loads to Snowflake
- `snowflake_loader.py` - Snowflake database loader with staging table logic
- `Dockerfile` - Docker configuration for containerized execution

## Setup

### GitHub Secrets

Configure the following secrets in your GitHub repository:

- `SNOWFLAKE_ACCOUNT` - Your Snowflake account identifier
- `SNOWFLAKE_USER` - Snowflake username
- `SNOWFLAKE_PASSWORD` - Snowflake password
- `SNOWFLAKE_WAREHOUSE` - Snowflake warehouse name
- `SNOWFLAKE_DATABASE` - Snowflake database name
- `SNOWFLAKE_SCHEMA` - Snowflake schema name

### Workflow Configuration

The workflow runs automatically on a schedule:
- **09:00 UTC** (after 00Z forecast published)
- **13:00 UTC** (after 06Z forecast published)
- **21:00 UTC** (after 12Z forecast published)
- **01:00 UTC** (after 18Z forecast published)

### Manual Trigger

You can manually trigger the workflow with:
- `download_date` (optional): Specific date in YYYYMMDD format
- `run_time` (optional): Forecast run time (00, 06, 12, or 18)
- `cleanup` (optional): Clean up temporary files after load (default: true)

## Environment Variables

The pipeline reads the following environment variables:

### Required
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

### Optional
- `DOWNLOAD_DATE` - Specific date to download (YYYYMMDD)
- `RUN_TIME` - Specific run time (00, 06, 12, 18)
- `DOWNLOAD_LIMIT` - Number of latest forecasts (default: 1)
- `PROCESS_WIND_DATA` - Enable wind processing (default: true)
- `CLEANUP_AFTER_LOAD` - Clean up temp files (default: true)
- `SKIP_EXISTING` - Skip already processed files (default: false in CI)

## Local Testing

To test the GitHub Actions pipeline locally:

```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"

# Run the pipeline
python github_actions/main.py
```

## Troubleshooting

### Pipeline fails at Snowflake connection
- Verify all Snowflake secrets are set correctly
- Check that the Snowflake user has proper permissions
- Ensure the warehouse is running

### No data downloaded
- Check ECMWF data availability for the specified date/time
- Verify network connectivity in GitHub Actions

### Import errors
- Ensure core pipeline modules are in the repository root
- Check that `sys.path` is correctly set in `main.py`

