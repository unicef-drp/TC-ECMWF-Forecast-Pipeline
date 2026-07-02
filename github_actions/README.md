# GitHub Actions Pipeline

This directory contains the GitHub Actions entry point for the ECMWF TC Forecast Pipeline.

## Overview

`main.py` is a thin sequential orchestrator. All data processing logic lives in `pipeline_core.py` (repo root). This wrapper adds:

- `PipelineConfig(BasePipelineConfig)` — password auth (no additional fields)
- `step7_load()` — Snowflake loading via `snowflake_loader.py` (password auth)
- `main()` — sequential step orchestration (steps 1–7)

### No named storms — precipitation still runs

If Step 2 finds no named storms, steps 3–5 (wind) are skipped entirely. Step 6 (precipitation download and Zarr upload) still runs when `PROCESS_PRECIP=true`, using the date and run time parsed from the BUFR filename (`tc_tracks_YYYY-MM-DD_rHH.bufr4`). This ensures global precipitation data is always captured even during quiet TC seasons.

## Files

| File | Purpose |
|------|---------|
| `main.py` | Sequential pipeline entry point |
| `snowflake_loader.py` | Snowflake loader (staging table → MERGE, password auth) |
| `Dockerfile` | Docker image for containerised execution |

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
| `SNOWFLAKE_STAGE_NAME` | Internal stage for precipitation Zarr upload (e.g. `AOTS_ANALYSIS`) — required when `PROCESS_PRECIP=true` |

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
| `PROCESS_PRECIP` | true | Enable precipitation download + Zarr upload (step 6) |
| `SNOWFLAKE_STAGE_NAME` | — | Internal stage for Zarr upload — required when `PROCESS_PRECIP=true` |
| `PRECIP_DATA_DIR` | `precip_data` | Local directory for precipitation Zarr files |
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

**`SNOWFLAKE_STAGE_NAME` is required** — set this secret to your internal stage name; required when `PROCESS_PRECIP=true`.

**Import errors** — ensure core modules are in the repo root and `sys.path` is correctly set.
