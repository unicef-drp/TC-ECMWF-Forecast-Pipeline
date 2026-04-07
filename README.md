[![TC Forecast Pipeline](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml/badge.svg?branch=main)](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml)

# TC-ECMWF-Forecast-Pipeline – Data Pipeline Setup Guide

This repository contains the pipeline for processing ECMWF tropical cyclone and wind forecast data. The pipeline downloads, extracts, transforms, and loads TC forecast data into Snowflake for use by downstream applications.

## Related Repositories

- **[Ahead-of-the-Storm](https://github.com/unicef-drp/Ahead-of-the-Storm)**: Dash web application for visualizing hurricane impact forecasts
- **[Ahead-of-the-Storm-DATAPIPELINE](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE)**: Data processing pipeline for creating bounding boxes and processing storm impact files

## Overview

The pipeline processes ECMWF ensemble tropical cyclone forecasts through the following steps:

1. **Download TC Data**: Downloads one combined BUFR4 file per forecast run (all storms, all 51 ensemble members) from ECMWF Open Data via the `ecmwf-opendata` client (`type="tf"`)
2. **Extract TC Data**: Parses BUFR Template 316082, filters to named storms, splits into per-storm CSVs
3. **Transform TC Data**: Standardises units, computes wind radii, creates WKT polygons for Snowflake
4. **Download Wind Data**: Downloads ensemble 10 m wind GRIB files matching the TC forecast run time
5. **Process Wind Combination**: Creates wind threshold envelope polygons by combining TC tracks with wind forecast data
6. **Load to Snowflake**: Loads processed data into Snowflake using a staging table → MERGE pattern

### Output Data

| File | Table | Contents |
|------|-------|----------|
| `*_transformed.csv` | `TC_TRACKS` | One row per member × step, with wind radii and WKT polygons |
| `*_envelopes_individual.csv` | `TC_ENVELOPES_INDIVIDUAL` | Wind threshold polygon per member × step |
| `*_envelopes_combined.csv` | `TC_ENVELOPES_COMBINED` | Unioned polygon per member × threshold |

## Prerequisites

1. **Python 3.11+** installed
2. **Virtual environment** activated (`.venv`)
3. **Environment variables** configured — start from `cp sample_env.txt .env`
4. **eccodes library** installed (required for BUFR file processing)
   - macOS: `brew install eccodes`

### Environment Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Required Environment Variables

#### Snowflake Configuration (required for GitHub Actions pipeline)

| Variable | Purpose |
|----------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name |
| `SNOWFLAKE_DATABASE` | Snowflake database name |
| `SNOWFLAKE_SCHEMA` | Snowflake schema name |

#### Optional Pipeline Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNLOAD_DATE` | latest | Specific date to download (YYYYMMDD) |
| `RUN_TIME` | latest | Forecast run time: 00, 06, 12, or 18 |
| `DOWNLOAD_LIMIT` | 1 | Number of latest forecast runs to download |
| `PROCESS_WIND_DATA` | true | Enable wind envelope processing |
| `CLEANUP_AFTER_LOAD` | true | Delete temporary files after successful load |
| `SKIP_EXISTING` | true | Skip already-processed files |

## Architecture

### Data Flow

```
ECMWF Open Data (ecmwf-opendata client, type="tf")
    ↓  one combined BUFR4 file per run (all storms, all 51 members)
ecmwf_tc_data_downloader.py   ← Step 1: download
    ↓
ecmwf_tc_data_extractor.py    ← Step 2: parse BUFR, filter named storms, split per-storm CSVs
    ↓
ecmwf_tc_data_transformer.py  ← Step 3: transform + WKT polygons

ECMWF Open Data (ecmwf-opendata client, type="pf"/"cf")
    ↓  ensemble 10m wind GRIB files
ecmwf_wind_data_downloader.py ← Step 4: download
    ↓
ecmwf_tc_wind_combination.py  ← Step 5: wind threshold contours + union

github_actions/snowflake_loader.py  ← Step 6: MERGE into Snowflake
```

### Key Files

| File | Purpose |
|------|---------|
| `pipeline_core.py` | Shared steps 1–5, `BasePipelineConfig`, `PipelineStats` |
| `github_actions/main.py` | Sequential entry point — imports from `pipeline_core`, password auth |
| `snowflake/spcs_pipeline.py` | Concurrent entry point — ProcessPool/SPCS OAuth |
| `github_actions/snowflake_loader.py` | Snowflake loader for GitHub Actions (password auth) |
| `snowflake/snowflake_loader.py` | Snowflake loader for SPCS (OAuth + private key support) |

### Snowflake Loading

Uses a **staging table → MERGE** pattern:
1. Reads transformed CSVs into pandas DataFrames
2. Bulk-uploads to a temp staging table via `write_pandas`
3. Executes MERGE into the production table using a compound key to skip duplicates

**Wind field polygons** are stored as VARCHAR (WKT strings) in `TC_TRACKS`. `TRY_TO_GEOGRAPHY()` is applied during MERGE for `TC_ENVELOPES_INDIVIDUAL` and `TC_ENVELOPES_COMBINED`.

## Running the Pipeline

### Local Development

```bash
# Interactive exploration via Jupyter notebook
jupyter notebook pipeline_demonstration.ipynb

# Full pipeline run (latest available forecast)
python github_actions/main.py

# Specific date and run time
DOWNLOAD_DATE=20250929 RUN_TIME=12 python github_actions/main.py
```

### GitHub Actions Pipeline

**Manual trigger only** (cron is commented out in the workflow). Trigger via the GitHub Actions UI with optional inputs:
- `download_date` (optional): Specific date in YYYYMMDD format
- `run_time` (optional): Forecast run time (00, 06, 12, or 18)
- `cleanup` (optional): Clean up temporary files after load (default: true)

**Publication schedule** — ECMWF publishes TC forecasts at 00, 06, 12, 18 UTC. Data is typically available 4–9 hours after the model run time.

**Setup:** Configure GitHub Secrets with the six `SNOWFLAKE_*` variables listed above.

### Containerized (SPCS)

See `snowflake/README.md` for full SPCS deployment instructions including Docker build, registry push, and `EXECUTE JOB SERVICE` SQL.

## Troubleshooting

### "eccodes library not found"
Install eccodes: `brew install eccodes` (macOS) or `apt-get install libeccodes-dev` (Linux)

### "No BUFR files downloaded"
- Check ECMWF data availability for the specified date/time
- Verify network connectivity
- Ensure the forecast has been published (data is available 4–9 hours after the model run)

### "Snowflake connection error"
- Verify all `SNOWFLAKE_*` environment variables are set correctly
- Ensure the warehouse is running and credentials have proper permissions

### "Wind file not found"
- Ensure wind data download completed successfully for the correct run time
- Wind data downloads forecast hours 0–144h in 6-hour increments

### Import errors
- Ensure you are in the virtual environment: `source .venv/bin/activate`
- Reinstall dependencies: `pip install -r requirements.txt`

## Data Storage

### Local (temporary)

| Directory | Contents |
|-----------|----------|
| `tc_data/` | Downloaded BUFR files and extracted per-storm CSVs |
| `tc_data_transformed/` | Transformed TC CSVs |
| `wind_data/` | Wind GRIB files |
| `wind_extracted/` | Envelope CSV files |

### Snowflake Tables

| Table | Contents |
|-------|----------|
| `TC_TRACKS` | Storm positions, wind speeds, pressure, wind radii |
| `TC_ENVELOPES_INDIVIDUAL` | Wind threshold polygons per forecast step |
| `TC_ENVELOPES_COMBINED` | Combined wind threshold polygons across all forecast steps |

## Quick Start

```bash
# 1. Set up environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
cp sample_env.txt .env
# Edit .env with your Snowflake credentials

# 3. Test locally
jupyter notebook pipeline_demonstration.ipynb

# 4. For automated pipeline: configure GitHub Secrets and trigger via Actions UI
```

## References

- [ecmwf-opendata Python client](https://pypi.org/project/ecmwf-opendata/)
- [ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)
- [ECMWF BUFR Format Documentation](https://confluence.ecmwf.int/display/ECC/BUFR+examples)
- [eccodes Library](https://pypi.org/project/eccodes/)