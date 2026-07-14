[![TC Forecast Pipeline](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml/badge.svg?branch=main)](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml)

# TC-ECMWF-Forecast-Pipeline – Data Pipeline Setup Guide

This repository contains the pipeline for processing ECMWF tropical cyclone, wind, and precipitation forecast data. The pipeline downloads, extracts, transforms, and loads TC forecast data into Snowflake for use by downstream applications. It also includes a fully separate, standalone GloFAS riverine discharge pipeline (see the dedicated section below).

## Related Repositories

- **[Ahead-of-the-Storm](https://github.com/unicef-drp/Ahead-of-the-Storm)**: Dash web application for visualizing hurricane impact forecasts
- **[Ahead-of-the-Storm-DATAPIPELINE](https://github.com/unicef-drp/Ahead-of-the-Storm-DATAPIPELINE)**: Data processing pipeline for creating bounding boxes and processing storm impact files

## Overview

The pipeline processes ECMWF ensemble tropical cyclone forecasts through the following steps:

1. **Download TC Data**: Downloads one combined BUFR4 file per forecast run (all storms, all 51 ensemble members) from ECMWF Open Data via the `ecmwf-opendata` client (`type="tf"`)
2. **Extract TC Data**: Parses BUFR Template 316082, filters to named storms, splits into per-storm CSVs
3. **Transform TC Data**: Standardises units, computes wind radii, creates WKT polygons for Snowflake
4. **Download Wind Data**: Downloads ensemble 10 m wind GRIB files (`u10`/`v10`) matching the TC forecast run time
4b. **Download Gust Data**: Downloads ensemble 10fg (maximum wind gust) GRIB files for the same run time and lead times as Step 4
5. **Process Wind Combination**: Creates wind threshold envelope polygons by combining TC tracks with wind forecast data
5b. **Extract Gust Envelopes**: Creates gust threshold envelope polygons from 10fg GRIB files (step 0 skipped — no accumulation period at T+0)
6. **Download Gridded Parameters** *(optional)*: Downloads global GRIB files for `tp` (total precipitation) and `ro` (total runoff), converts each to a Zarr ZipStore, and uploads to a Snowflake internal stage. Runs regardless of whether named storms were found.
7. **Load to Snowflake** *(or keep locally)*: Loads processed data into Snowflake using a staging table → MERGE pattern, or skips the load entirely when `DATA_PIPELINE_DB=LOCAL`

> **No named storms**: If Step 2 finds no named storms, Steps 3–5 (wind) are skipped. Step 6 (precipitation) still runs when `PROCESS_MET=true`, then the pipeline exits cleanly.

### Output Data

| File | Table | Contents |
|------|-------|----------|
| `*_transformed.csv` | `TC_TRACKS` | One row per member × step, with wind radii and WKT polygons |
| `*_envelopes_individual.csv` | `TC_ENVELOPES_INDIVIDUAL` | Wind threshold polygon per member × step |
| `*_envelopes_combined.csv` | `TC_ENVELOPES_COMBINED` | Unioned polygon per member × threshold |
| `*_gust_envelopes_individual.csv` | `TC_GUST_ENVELOPES_INDIVIDUAL` | Gust threshold polygon per member × step (10fg, m/s thresholds) |
| `*_gust_envelopes_combined.csv` | `TC_GUST_ENVELOPES_COMBINED` | Unioned gust polygon per member × threshold |
| `met_data/tp_*.zarr.zip` | `MET_FORECASTS` | One row per (forecast_time, param) — `tp` total precipitation (global) |
| `met_data/ro_*.zarr.zip` | `MET_FORECASTS` | One row per (forecast_time, param) — `ro` total runoff (land-only) |
| `glofas_data/river_*.zarr.zip` | `RIVER_FORECASTS` | One row per (forecast_time, param='dis24') — GloFAS riverine discharge, standalone pipeline, see below |
| `glofas/{date}/river_extent_rp{N}_bymember_*.parquet` | `RIVER_FORECASTS` | One row per (forecast_time, param='extent_rp{2,5,10,20,50,100}_bymember'), GloFAS x JRC v2.1 per-member flood extent (sparse table, one row per pixel/member/step flooded), same table as raw discharge, see below |

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
| `PROCESS_MET` | true | Enable met parameter download and stage upload (Step 6) |
| `SNOWFLAKE_STAGE_NAME` | — | Snowflake internal stage for Zarr upload — **required when `PROCESS_MET=true`** |
| `MET_DATA_DIR` | `met_data` | Local directory for met Zarr files |
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
    ↓  ensemble 10m wind GRIB files (u10/v10)
ecmwf_wind_data_downloader.py    ← Step 4: download  (skipped if no named storms)
    ↓
ecmwf_tc_wind_combination.py     ← Step 5: wind threshold contours + union  (skipped if no named storms)

ECMWF Open Data (ecmwf-opendata client, type="pf"/"cf")
    ↓  ensemble 10fg (max wind gust) GRIB files
ecmwf_wind_data_downloader.py    ← Step 4b: download gust files  (skipped if no named storms)
    ↓
ecmwf_gust_envelope_extractor.py ← Step 5b: gust threshold contours + union  (skipped if no named storms)
                                             produces: *_gust_envelopes_individual.csv + *_gust_envelopes_combined.csv

ECMWF Open Data (ecmwf-opendata client, type="pf"/"cf")
    ↓  global GRIB files — tp (total precipitation) and ro (total runoff)
ecmwf_met_downloader.py  ← Step 6: download + convert to Zarr + PUT to Snowflake stage
                                     (always runs when PROCESS_MET=true, even with no named storms)
                                     produces: tp_YYYYMMDD_HH.zarr.zip + ro_YYYYMMDD_HH.zarr.zip

github_actions/snowflake_loader.py  ← Step 7: MERGE into Snowflake
```

### Key Files

| File | Purpose |
|------|---------|
| `pipeline_core.py` | Shared steps 1–6, `BasePipelineConfig`, `PipelineStats` |
| `ecmwf_met_downloader.py` | GRIB download (`tp`, `ro`), Zarr conversion, stage upload |
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

**Precipitation** is stored as Zarr ZipStore files in a Snowflake internal stage. `MET_FORECASTS` records one metadata row per `(FORECAST_TIME, PARAM)` pointing to the stage path. The Zarr is global (not per-storm), so there is no `TRACK_ID` in this table.

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
- Required when `PROCESS_MET=true` and `DATA_PIPELINE_DB=SNOWFLAKE`

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
| `met_data/` | Precipitation Zarr ZipStore files |
| `glofas_data/` | GloFAS riverine discharge Zarr ZipStore files (standalone pipeline — see below) |

### Snowflake Tables

| Table | Contents |
|-------|----------|
| `TC_TRACKS` | Storm positions, wind speeds, pressure, wind radii |
| `TC_ENVELOPES_INDIVIDUAL` | Wind threshold polygons per forecast step |
| `TC_ENVELOPES_COMBINED` | Combined wind threshold polygons across all forecast steps |
| `TC_GUST_ENVELOPES_INDIVIDUAL` | Gust threshold polygons per forecast step (10fg, m/s) |
| `TC_GUST_ENVELOPES_COMBINED` | Combined gust threshold polygons across all forecast steps |
| `MET_FORECASTS` | Metadata: one row per (forecast_time, param) with Zarr stage path |
| `RIVER_FORECASTS` | Metadata: one row per (forecast_time, param) with stage path — `param='dis24'` for raw discharge, `param='extent_rp{2,5,10,20,50,100}_bymember'` (plus `IS_STANDIN`) for GloFAS x JRC per-member flood extent |

## GloFAS Riverine Discharge Pipeline (standalone)

A fully separate daily pipeline, **not** part of the TC forecast pipeline above.
GloFAS's own publication cadence (once per calendar day, driven by the 00Z IFS
ENS cycle) and its much longer full-global download time are why it runs as
its own independent job rather than a step inside the main pipeline.

- **Entry points:** `github_actions/glofas_pipeline.py` (own workflow,
  `.github/workflows/glofas.yml`, cron `0 15 * * *` UTC) and
  `snowflake/glofas_spcs_pipeline.py` (SPCS, triggered separately from the main
  SPCS job)
- **Core module:** `glofas_downloader.py` — downloads the GloFAS v4.0
  51-member ensemble discharge forecast via `cdsapi` (a different API from
  `ecmwf-opendata`), builds a sparse Zarr ZipStore filtered to cells that cross the
  RP2yr threshold, uploads to `@{stage}/glofas/{date}/river_{date}.zarr.zip`
- **One-time setup required first:** `setup_glofas_thresholds.py` — caches the
  official RP threshold files the sparse filter depends on; run manually once, not
  part of any recurring schedule
- **Extent masking (enabled by default):** `glofas_extent_masking.py` — combines the
  raw discharge-exceedance probability above with [JRC's Global River Flood Hazard
  Maps v2.1](https://data.jrc.ec.europa.eu/), a static per-return-period flood-extent
  raster, to correct for GloFAS's 0.05° cell overstating exposure across a whole
  cell rather than just its real channel/floodplain footprint. Requires its own
  one-time cache first: `setup_jrc_extents.py`, fetches directly from JRC's own
  file server, run manually once. Toggle
  with `GLOFAS_EXTENT_ENABLED`; output uploads to the same
  `@{stage}/glofas/{date}/` folder as the raw discharge Zarr.
- **Why separate from the main pipeline:** GloFAS's ~11h publication latency
  doesn't align with the main pipeline's 4 daily run slots, and a full global
  fetch can take far longer than the main pipeline's other steps — bundling them
  would force one pipeline's schedule/timeout onto the other
- **Demo notebook:** `pipeline_demonstration_glofas.ipynb` — download, sparse Zarr
  inspection, and visualizations (raster maps, RP exceedance hotspot zoom, single-gauge
  ensemble hydrograph, plus a GloFAS x JRC extent-masking demo using a small
  Bangladesh-scale cache)

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
- [CDS/EWDS API (cdsapi)](https://pypi.org/project/cdsapi/) — used by the standalone GloFAS pipeline
- [GloFAS — Global Flood Awareness System](https://global-flood.emergency.copernicus.eu/)
