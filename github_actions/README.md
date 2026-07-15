# GitHub Actions Pipeline

This directory contains the GitHub Actions entry point for the ECMWF TC Forecast Pipeline.

## Overview

`main.py` is a thin sequential orchestrator. All data processing logic lives in `pipeline_core.py` (repo root). This wrapper adds:

- `PipelineConfig(BasePipelineConfig)` — password auth (no additional fields)
- `step7_load()` — Snowflake loading via `snowflake_loader.py` (password auth)
- `main()` — sequential step orchestration (steps 1, 2, 3, 4, 4b, 5, 5b, 6, 7)
  - Step 4b downloads 10fg gust GRIB files; Step 5b extracts gust threshold envelope polygons
  - Both produce `TC_GUST_ENVELOPES_INDIVIDUAL` / `TC_GUST_ENVELOPES_COMBINED` rows loaded in Step 7

### No named storms — precipitation still runs

If Step 2 finds no named storms, steps 3–5 (wind) are skipped entirely. Step 6 (`tp` + `ro` download and Zarr upload) still runs when `PROCESS_MET=true`, using the date and run time parsed from the BUFR filename (`tc_tracks_YYYY-MM-DD_rHH.bufr4`). This ensures global gridded data is always captured even during quiet TC seasons.

## Files

| File | Purpose |
|------|---------|
| `main.py` | Sequential pipeline entry point (main TC forecast pipeline) |
| `glofas_pipeline.py` | Standalone GloFAS riverine discharge pipeline entry point — see GloFAS section below |
| `snowflake_loader.py` | Snowflake loader (staging table → MERGE, password auth) |
| `Dockerfile` | Docker image for containerised execution |

## GloFAS Riverine Discharge Pipeline (standalone)

`glofas_pipeline.py` is a **fully separate** entry point from `main.py`, with its own workflow
(`.github/workflows/glofas.yml`), TC-independent, once-daily cadence. See root `README.md` for
the full architecture.

- Shared config/orchestration lives in `glofas_pipeline_core.py` (repo root); this file adds
  only password-auth specifics, mirroring how `main.py` adds password auth on top of
  `pipeline_core.py`.
- Requires `setup_glofas_thresholds.py` to have been run once, manually, first (populates the
  RP threshold cache the sparse cell filter depends on).
- Extent masking (GloFAS x JRC v2.1 flood-extent, enabled by default via `GLOFAS_EXTENT_ENABLED`)
  additionally requires `setup_jrc_extents.py` to have been run once, manually, first (populates
  the JRC flood-extent + permanent-water cache).
- Installs from `requirements-glofas.txt`, not `requirements-ci.txt` — a deliberately lean,
  separate dependency set (no eccodes/geos/proj/gdal, which this pipeline never touches).

### CDS idle-wait-time cost fix — submit/process split

`.github/workflows/glofas.yml` has **two schedule triggers**, not two workflows: `cron: '0 15 * * *'`
(submit) and `cron: '40 15 * * *'` (process), both on the same job. Each cron entry starts its own
independent, short-lived runner — there is no runner staying alive across the 40-minute gap, and
nothing is billed during it.

- **submit** (15:00 UTC): fires the 2 real CDS requests (`wait_until_complete=False`) and exits in
  seconds, no CDS queue wait. Saves the returned request IDs to the `GLOFAS_CDS_REQUESTS`
  Snowflake table via `save_cds_request_ids()`. Skips submitting if a request was already saved
  for today or if today's data is already fully downloaded/staged (handles a
  late-running submit trigger after process already completed the day via its own fallback).
- **process** (15:40 UTC): loads any request IDs `submit` saved for today and resumes waiting on
  them via `resume_glofas_download()`, patiently, with wait/retry behavior. If nothing was saved 
  (submit never ran, failed, or a fresh deployment hasn't added the submit trigger yet), 
  falls back to submitting fresh and blocks through the full wait.
- `CDS_PROCESS_DELAY_MINUTES` (a plain constant in `glofas_downloader.py`, currently `40`) is the
  intended gap between the two triggers, a cost-optimization target only, not a correctness
  requirement, tune freely as more real queue-time data accumulates. Must match the second cron
  string in `glofas.yml` if changed.
- `GLOFAS_MODE` env var controls which mode a given run operates in. Set via the workflow's 
  `GLOFAS_MODE: ${{ github.event.inputs.mode ||
  (github.event.schedule == '0 15 * * *' && 'submit') || 'process' }}` expression, which also
  respects the manual `mode` dispatch input below.

### GitHub Secrets (GloFAS)

Same Snowflake secrets as the main pipeline, plus:

| Secret | Description |
|--------|-------------|
| `CDSAPI_URL` | CDS/EWDS API base URL (`https://ewds.climate.copernicus.eu/api`) |
| `CDSAPI_KEY` | CDS/EWDS API key for `cems-glofas-forecast` |

### Manual Trigger (GloFAS)

Trigger `.github/workflows/glofas.yml` from the GitHub Actions UI with optional inputs:

| Input | Description |
|-------|-------------|
| `download_date` | Specific date (YYYY-MM-DD, leave empty for today UTC) |
| `mode` | `GLOFAS_MODE` override, `submit` (fire CDS requests, exit) or `process` (resume/full run, waits for CDS if needed). Default: `process` |
| `cleanup` | Clean up temporary files after load (default: true) |

### Environment Variables (GloFAS)

| Variable | Default | Description |
|----------|---------|-------------|
| `GLOFAS_DATA_DIR` | `glofas_data` | Local directory for GloFAS forecast Zarr files |
| `GLOFAS_THRESHOLD_SOURCE` | `snowflake` | Where the RP threshold files are read from: `snowflake` or `local` |
| `GLOFAS_THRESHOLD_LOCAL_DIR` | `glofas_data/thresholds_cache` | Local dir for threshold files, used only when `GLOFAS_THRESHOLD_SOURCE=local` |
| `GLOFAS_EXTENT_ENABLED` | `true` | Whether the GloFAS x JRC extent-masking step runs after raw discharge |
| `GLOFAS_JRC_SOURCE` | `snowflake` | Where the cached JRC RP10/20/50/100 + permanent-water GeoTIFFs are read from: `snowflake` or `local` |
| `GLOFAS_JRC_LOCAL_DIR` | `glofas_data/jrc_extent_cache` | Local dir for JRC cache files, used only when `GLOFAS_JRC_SOURCE=local` |
| `GLOFAS_MODE` | `process` | `submit` (fire CDS requests, save request IDs, exit) or `process` (resume saved requests, or submit-and-block fresh if nothing saved) |
| `DOWNLOAD_DATE` | today (UTC) | Specific date (YYYY-MM-DD) — note the different format from the main pipeline's YYYYMMDD |
| `CLEANUP_AFTER_LOAD` | true | Delete temp files after load |

## Setup

### GitHub Secrets

Configure the following secrets in your GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name |
| `SNOWFLAKE_DATABASE` | Snowflake database name |
| `SNOWFLAKE_SCHEMA` | Snowflake schema name |
| `SNOWFLAKE_STAGE_NAME` | Internal stage for met Zarr upload (e.g. `AOTS_ANALYSIS`) — required when `PROCESS_MET=true` |

### Manual Trigger

Trigger the workflow from the GitHub Actions UI with optional inputs:

| Input | Description |
|-------|-------------|
| `download_date` | Specific date in YYYYMMDD format |
| `run_time` | Forecast run time: 00, 06, 12, or 18 |
| `cleanup` | Clean up temporary files after load (default: true) |

## Environment Variables

### Required
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

### Optional
| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNLOAD_DATE` | latest | Specific date (YYYYMMDD) |
| `RUN_TIME` | latest | Specific run time (00, 06, 12, 18) |
| `DOWNLOAD_LIMIT` | 1 | Number of latest forecast runs |
| `NAMED_STORMS_ONLY` | true | Filter to named storms; wind steps are skipped if none found |
| `PROCESS_WIND_DATA` | true | Enable wind envelope processing (steps 4–5) |
| `PROCESS_MET` | true | Enable met parameter download + Zarr upload (step 6) |
| `SNOWFLAKE_STAGE_NAME` | — | Internal stage for Zarr upload — required when `PROCESS_MET=true` |
| `MET_DATA_DIR` | `met_data` | Local directory for met Zarr files |
| `CLEANUP_AFTER_LOAD` | true | Delete temp files after load |
| `SKIP_EXISTING` | true | Skip already-processed files |

## Local Testing

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
export SNOWFLAKE_STAGE_NAME="your_stage"

python github_actions/main.py

# Or for a specific date and run time:
DOWNLOAD_DATE=20250929 RUN_TIME=12 python github_actions/main.py
```

## Troubleshooting

**Snowflake connection fails** — verify all `SNOWFLAKE_*` secrets, ensure warehouse is running.

**No data downloaded** — check ECMWF data availability (data is published 4–9 hours after the model run time).

**`SNOWFLAKE_STAGE_NAME` is required** — set this secret to your internal stage name; required when `PROCESS_MET=true`.

**Import errors** — ensure core modules are in the repo root and `sys.path` is correctly set.
