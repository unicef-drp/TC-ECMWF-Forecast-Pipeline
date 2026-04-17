# GitHub Actions Pipeline

This directory contains the GitHub Actions entry point for the ECMWF TC Forecast Pipeline.

## Overview

`main.py` is a thin sequential orchestrator.  All data processing logic lives in `pipeline_core.py` (repo root).  This wrapper adds:

- `PipelineConfig(BasePipelineConfig)` — password auth (no additional fields)
- `step6_load()` — Snowflake loading via `snowflake_loader.py` (password auth)
- `main()` — sequential step orchestration (steps 1–6)

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
| `PROCESS_WIND_DATA` | true | Enable wind envelope processing |
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

python github_actions/main.py

# Or for a specific date and run time:
DOWNLOAD_DATE=20250929 RUN_TIME=12 python github_actions/main.py
```

## Troubleshooting

**Snowflake connection fails** — verify all `SNOWFLAKE_*` secrets, ensure warehouse is running.

**No data downloaded** — check ECMWF data availability (data is published 4–9 hours after the model run time).

**Import errors** — ensure core modules are in the repo root and `sys.path` is correctly set.
