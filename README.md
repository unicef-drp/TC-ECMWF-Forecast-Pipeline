[![TC Forecast Pipeline](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml/badge.svg?branch=main)](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml)

# TC-ECMWF-Forecast-Pipeline – Data Pipeline Setup Guide

This repository contains the pipeline for processing ECMWF tropical cyclone, wind, and precipitation forecast data. The pipeline downloads, extracts, transforms, and loads TC forecast data into Snowflake for use by downstream applications.

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
6. **Download Precipitation Data** *(optional)*: Downloads global precipitation GRIB files (total, convective, large-scale precipitation), converts to Zarr, and uploads to a Snowflake internal stage. Runs regardless of whether named storms were found.
7. **Load to Snowflake** *(or keep locally)*: Loads processed data into Snowflake using a staging table → MERGE pattern, or skips the load entirely when `DATA_PIPELINE_DB=LOCAL`

> **No named storms**: If Step 2 finds no named storms, Steps 3–5 (wind) are skipped. Step 6 (precipitation) still runs when `PROCESS_PRECIP=true`, then the pipeline exits cleanly.

### Output Data

| File | Table | Contents |
|------|-------|----------|
| `*_transformed.csv` | `TC_TRACKS` | One row per member × step, with wind radii and WKT polygons |
| `*_envelopes_individual.csv` | `TC_ENVELOPES_INDIVIDUAL` | Wind threshold polygon per member × step |
| `*_envelopes_combined.csv` | `TC_ENVELOPES_COMBINED` | Unioned polygon per member × threshold |
| `precip_data/*.zarr.zip` | `PRECIP_FORECASTS` | One row per (forecast_time, param) pointing to the Zarr stage path |

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

#### Storage Backend

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_PIPELINE_DB` | `SNOWFLAKE` | `SNOWFLAKE` — push to Snowflake after processing; `LOCAL` — skip load, keep output files in `tc_data_transformed/` and `wind_extracted/` |

#### Snowflake Configuration (required when `DATA_PIPELINE_DB=SNOWFLAKE`)

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
| `NAMED_STORMS_ONLY` | true | Filter to named storms only (skips unnamed disturbances like "92W") |
| `PROCESS_WIND_DATA` | true | Enable wind envelope processing (Steps 4–5) |
| `PROCESS_PRECIP` | true | Enable precipitation download and stage upload (Step 6) |
| `SNOWFLAKE_STAGE_NAME` | — | Snowflake internal stage for Zarr upload — **required when `PROCESS_PRECIP=true`** |
| `PRECIP_DATA_DIR` | `precip_data` | Local directory for precipitation Zarr files |
| `CLEANUP_AFTER_LOAD` | true | Delete temporary files after successful load |
| `SKIP_EXISTING` | true | Skip already-processed files |

## Architecture

### Data Flow

```
ECMWF Open Data (ecmwf-opendata client, type="tf")
    ↓  one combined BUFR4 file per run (all storms, all 51 members)
ecmwf_tc_data_downloader.py      ← Step 1: download
    ↓
ecmwf_tc_data_extractor.py       ← Step 2: parse BUFR, filter named storms, split per-storm CSVs
    ↓ (if named storms found)
ecmwf_tc_data_transformer.py     ← Step 3: transform + WKT polygons

ECMWF Open Data (ecmwf-opendata client, type="pf"/"cf")
    ↓  ensemble 10m wind GRIB files
ecmwf_wind_data_downloader.py    ← Step 4: download  (skipped if no named storms)
    ↓
ecmwf_tc_wind_combination.py     ← Step 5: wind threshold contours + union  (skipped if no named storms)

ECMWF Open Data (ecmwf-opendata client, type="pf"/"cf")
    ↓  global precipitation GRIB files (tp)
ecmwf_precip_data_downloader.py  ← Step 6: download + convert to Zarr + PUT to Snowflake stage
                                     (always runs when PROCESS_PRECIP=true, even with no named storms)

github_actions/snowflake_loader.py  ← Step 7: MERGE into Snowflake
```

### Key Files

| File | Purpose |
|------|---------|
| `pipeline_core.py` | Shared steps 1–6, `BasePipelineConfig`, `PipelineStats` |
| `ecmwf_precip_data_downloader.py` | Precipitation GRIB download, Zarr conversion, stage upload |
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

**Precipitation** is stored as Zarr ZipStore files in a Snowflake internal stage. `PRECIP_FORECASTS` records one metadata row per `(FORECAST_TIME, PARAM)` pointing to the stage path. The Zarr is global (not per-storm), so there is no `TRACK_ID` in this table.

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

**Setup:** Configure GitHub Secrets with the six `SNOWFLAKE_*` variables plus `SNOWFLAKE_STAGE_NAME` (see `github_actions/README.md`).

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

### "SNOWFLAKE_STAGE_NAME is required"
- Set `SNOWFLAKE_STAGE_NAME` to your Snowflake internal stage name (e.g. `AOTS_ANALYSIS`)
- Required when `PROCESS_PRECIP=true` and `DATA_PIPELINE_DB=SNOWFLAKE`

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
| `precip_data/` | Precipitation Zarr ZipStore files |

### Snowflake Tables

| Table | Contents |
|-------|----------|
| `TC_TRACKS` | Storm positions, wind speeds, pressure, wind radii |
| `TC_ENVELOPES_INDIVIDUAL` | Wind threshold polygons per forecast step |
| `TC_ENVELOPES_COMBINED` | Combined wind threshold polygons across all forecast steps |
| `PRECIP_FORECASTS` | Metadata: one row per (forecast_time, param) with Zarr stage path |

## Quick Start

```bash
# 1. Set up environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
cp sample_env.txt .env

# 3. Test locally
jupyter notebook pipeline_demonstration.ipynb

# 4. For automated pipeline: configure GitHub Secrets and trigger via Actions UI
```

## References

- [ecmwf-opendata Python client](https://pypi.org/project/ecmwf-opendata/)
- [ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)
- [ECMWF BUFR Format Documentation](https://confluence.ecmwf.int/display/ECC/BUFR+examples)
- [eccodes Library](https://pypi.org/project/eccodes/)
