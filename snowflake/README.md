# Snowflake Container Services (SPCS) Pipeline

This directory contains the SPCS entry point for the ECMWF TC Forecast Pipeline.

## Overview

`spcs_pipeline.py` is a concurrent execution wrapper around `pipeline_core.py`.  It adds:

- **Concurrent Transformation**: per-storm CSVs transformed in parallel via `ProcessPoolExecutor` (controlled by `USE_PROCESS_POOL`)
- **SPCS Integration**: Native Snowflake Container Services OAuth authentication
- **Flexible Authentication**: SPCS OAuth, private key, or password
- **Phase-level Timing**: per-phase timing logged to `unicef_pipeline.log`

All data processing steps (1–5) are shared with the GitHub Actions pipeline via `pipeline_core.py`.  This entry point adds only the SPCS-specific Snowflake loading (`phase4_snowflake_loading`) and passes concurrency parameters to `step3_transform` and `step5_process_wind`.

Execution sequence:
1. **Phase 1**: Download combined BUFR file → extract named storms → split per-storm CSVs
2. **Phase 2**: Transform per-storm CSVs (concurrently if `USE_PROCESS_POOL=true`)
3. **Phase 3**: Download wind GRIB files → create wind threshold envelope polygons
4. **Phase 4**: Load all results to Snowflake (SPCS OAuth / private key / password)

## Files

| File | Purpose |
|------|---------|
| `spcs_pipeline.py` | Concurrent pipeline entry point |
| `snowflake_loader.py` | Snowflake loader with SPCS OAuth + private key support |
| `Dockerfile` | Container image for SPCS deployment |

## Prerequisites

1. **Snowflake CLI** installed and configured
   ```bash
   pip install snowflake-cli-labs
   snow connection add <your-connection-name>
   ```

2. **Docker** installed and running

3. **Snowflake account** with SPCS enabled

4. **Network Security Configuration**
   
   The pipeline requires network connectivity to external systems (ECMWF servers). Configure network rules and external access integration:
   
   ```sql
   USE ROLE ACCOUNTADMIN;
   
   -- Create network rule for egress access (allows outbound HTTP/HTTPS)
   CREATE OR REPLACE NETWORK RULE tc_wind_data_egress_access
     MODE = EGRESS
     TYPE = HOST_PORT
     VALUE_LIST = ('0.0.0.0:80', '0.0.0.0:443');
   
   -- Create external access integration
   CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION tc_wind_data_egress_access_integration
     ALLOWED_NETWORK_RULES = (tc_wind_data_egress_access)
     ENABLED = true;
   ```
   
   **Note:** The network rule allows outbound HTTP (port 80) and HTTPS (port 443) to any destination (0.0.0.0). This is required for accessing ECMWF's services:
   - `https://data.ecmwf.int/` (ECMWF Open Data — TC tracks and wind forecasts)

5. **Compute Pool**
   
   Create a compute pool for running SPCS services:
   
   ```sql
   -- View available compute pool instance families
   SHOW COMPUTE POOL INSTANCE FAMILIES;
   
   -- Create compute pool (adjust instance family as needed)
   CREATE COMPUTE POOL IF NOT EXISTS my_compute_pool
     MIN_NODES = 1
     MAX_NODES = 1
     INSTANCE_FAMILY = CPU_X64_M;
   ```
   
   Adjust the `INSTANCE_FAMILY` based on your requirements and available options from `SHOW COMPUTE POOL INSTANCE FAMILIES`.

6. **Image Repository** created in Snowflake
   
   Before pushing images, you must create an image repository in your Snowflake database:
   ```sql
   CREATE OR REPLACE IMAGE REPOSITORY SERVICES;
   ```
   
   This should be run in the database and schema where you plan to deploy your SPCS service.

## Building the Docker Image

From the repository root directory:

```bash
docker build -f snowflake/Dockerfile -t tc-ecmwf-pipeline:latest . --platform=linux/amd64
```

This will:
- Install all system dependencies (eccodes, geospatial libraries)
- Install Python dependencies from `requirements.txt`
- Copy all pipeline modules
- Set up the container entrypoint

## Tagging the Image for Snowflake Registry

Before pushing to Snowflake, you need to tag the image with the correct registry path.

### Step 1: Get Registry URL

```bash
snow spcs image-registry url --connection default
```

This will output something like: `orgname-account.registry.snowflakecomputing.com`

### Step 2: Tag the Image

The image tag format is:
```
<registry-url>/<database>/<schema>/<service>/<image-name>:<tag>
```

**Important:** Database and schema names must be **lowercase** in the Docker tag (even if they use uppercase in Snowflake).

Example:
```bash
docker tag tc-ecmwf-pipeline:latest \
  orgname-account.registry.snowflakecomputing.com/mydatabase/myschema/myservice/tc-ecmwf-pipeline:latest
```

Where:
- `orgname-account` = your registry URL (from `snow spcs image-registry url`)
- `mydatabase` = your database name (lowercase)
- `myschema` = your schema name (lowercase)
- `myservice` = your service name (lowercase)
- `tc-ecmwf-pipeline` = your image name
- `latest` = tag/version

## Pushing to Snowflake Registry

### Step 1: Authenticate with Snowflake Registry

```bash
snow spcs image-registry login --connection default
```

This automatically logs Docker into your Snowflake image registry.

### Step 2: Push the Image

```bash
docker push orgname-account.registry.snowflakecomputing.com/mydatabase/myschema/myservice/tc-ecmwf-pipeline:latest
```


## Running the Pipeline

The pipeline can be run in two ways:

### 1. Running in Docker

```bash
docker run --rm \
  -e SNOWFLAKE_ACCOUNT='your-account' \
  -e SNOWFLAKE_USER='your_user' \
  -e SNOWFLAKE_PASSWORD='your_password' \
  -e SNOWFLAKE_WAREHOUSE='your_warehouse' \
  -e SNOWFLAKE_DATABASE='your_database' \
  -e SNOWFLAKE_SCHEMA='your_schema' \
  tc-ecmwf-pipeline:latest
```

### 2. Running in SPCS

Execute the pipeline as a SPCS job using `EXECUTE JOB SERVICE`:

```sql
EXECUTE JOB SERVICE
   IN COMPUTE POOL my_compute_pool
   NAME = tc_ecmwf_job
   ASYNC = TRUE
   EXTERNAL_ACCESS_INTEGRATIONS = (AOTS_EGRESS_ACCESS_INTEGRATION)
   FROM SPECIFICATION $$
   spec:
     containers:
     - name: tc-ecmwf-pipeline
       image: /your_database/your_schema/your_service/tc-ecmwf-pipeline:latest
       env:
          SPCS_RUN: true
          SNOWFLAKE_ACCOUNT: your-account
          SNOWFLAKE_USER: your_user
          SNOWFLAKE_WAREHOUSE: your_warehouse
          SNOWFLAKE_DATABASE: your_database
          SNOWFLAKE_SCHEMA: your_schema
          SNOWFLAKE_INSECURE_MODE: true
          USE_PROCESS_POOL: true
          MAX_WORKERS: 0
          MAX_CONCURRENT_DOWNLOADS: 8
          NAMED_STORMS_ONLY: true
          DOWNLOAD_DATE: YYYYMMDD
          RUN_TIME: 00
     platformMonitor:
       metricConfig:
         groups:
         - system 
         - network 
   $$;
```


## Configuration Options

### Key Concept: One File Per Forecast Run

The `ecmwf-opendata` client downloads a **single combined BUFR4 file per forecast run** containing all active storms and all 51 ensemble members.  The extractor then splits this into one CSV per named storm for downstream processing.

- `DOWNLOAD_LIMIT=1` → 1 combined BUFR4 file → N per-storm CSVs (one per active named storm)
- `DOWNLOAD_DATE=20251102 RUN_TIME=12` → 1 combined BUFR4 file for 12Z on Nov 2
- `DOWNLOAD_DATE=20251102` (no `RUN_TIME`) → not supported; `RUN_TIME` is required when `DOWNLOAD_DATE` is set

### Download Parameters

#### `DOWNLOAD_DATE`
- **Format**: `YYYYMMDD` (e.g., `20251102`)
- **Required**: No
- **Default**: Not set (uses latest forecasts instead)
- **Description**: Specific date to download forecasts for

#### `RUN_TIME`
- **Format**: `00`, `06`, `12`, or `18` (2 digits, no leading zero required)
- **Required**: No
- **Default**: Not set
- **Description**: Specific forecast run time (UTC) to download
- **Note**: Only used when `DOWNLOAD_DATE` is also set

#### `DOWNLOAD_LIMIT`
- **Format**: Integer
- **Required**: No
- **Default**: `1`
- **Description**: Number of latest **forecast runs** to download (only used when `DOWNLOAD_DATE` is not set)
- **Important**: `DOWNLOAD_LIMIT=1` downloads **one combined BUFR4 file** containing all active named storms for that run. The extractor then splits it into one CSV per storm. Increasing the limit downloads multiple consecutive forecast runs (e.g. `DOWNLOAD_LIMIT=2` downloads the two most recent runs).

### Processing Parameters

#### `PROCESS_WIND_DATA`
- **Format**: `true` or `false` (case-insensitive)
- **Default**: `true`
- **Description**: Enable/disable wind data download and processing

#### `NAMED_STORMS_ONLY`
- **Format**: `true` or `false` (case-insensitive)
- **Default**: `true`
- **Description**: Filter to only download named storms (excludes numbered storms like "92W", "AL14")

#### `CLEANUP_AFTER_LOAD`
- **Format**: `true` or `false` (case-insensitive)
- **Default**: `true`
- **Description**: Remove temporary files after successful Snowflake load

#### `SKIP_EXISTING`
- **Format**: `true` or `false` (case-insensitive)
- **Default**: `true`
- **Description**: Skip files that have already been extracted/transformed

### Concurrency Parameters

#### `MAX_WORKERS`
- **Format**: Integer
- **Default**: `0` (auto-detect CPU count)
- **Description**: Number of parallel workers for CPU-intensive tasks (extraction/transformation)
- **Note**: Set to `0` to auto-detect based on available CPUs

#### `MAX_CONCURRENT_DOWNLOADS`
- **Format**: Integer
- **Default**: `0` (uses `MAX_WORKERS`, capped at 10)
- **Description**: Maximum number of concurrent downloads
- **Note**: Automatically capped at 10 to avoid overwhelming ECMWF servers

#### `USE_PROCESS_POOL`
- **Format**: `true` or `false` (case-insensitive)
- **Default**: `true`
- **Description**: Use ProcessPoolExecutor (better for CPU tasks) vs ThreadPoolExecutor (better for I/O)

### Download Scenarios

#### Latest forecast (default)
```bash
docker run ... tc-ecmwf-pipeline:latest
# Downloads the most recent combined BUFR4 file (DOWNLOAD_LIMIT=1)
```

#### Specific date and run time
```bash
docker run -e DOWNLOAD_DATE=20251102 -e RUN_TIME=12 ... tc-ecmwf-pipeline:latest
# Downloads the 12Z forecast for November 2, 2025
```

**Notes**:
- `DOWNLOAD_DATE` requires `RUN_TIME` — the API retrieves a specific run, not a full day
- `RUN_TIME` must be `00`, `06`, `12`, or `18` (no `Z` suffix)
- Date format: `YYYYMMDD` (8 digits, no dashes)
- If data is unavailable for the requested date/time, the pipeline logs an error and exits

### Wind Data Download Logic

Wind data download depends on TC data:

1. **TC data must be downloaded first** to extract metadata (date and run time)
2. The pipeline extracts the run time and date from the first downloaded TC file
3. Wind data is then downloaded for:
   - The same date as the TC data
   - The same run time as the TC data
   - Forecast hours determined by analyzing the TC data (typically 0-144 hours in 6-hour increments)

**If TC download fails or returns no files**: Wind download is skipped with a warning

## Pipeline Phases

1. **Phase 1**: Download combined BUFR file → extract named storms → split per-storm CSVs
2. **Phase 2**: Transform per-storm CSVs into Snowflake-ready format (concurrent when `USE_PROCESS_POOL=true`)
3. **Phase 3**: Download wind GRIB files → create wind threshold envelope polygons
4. **Phase 4**: Load `TC_TRACKS`, `TC_ENVELOPES_INDIVIDUAL`, `TC_ENVELOPES_COMBINED` to Snowflake


---

## License

Copyright 2025 Snowflake Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

**UNSUPPORTED BY SNOWFLAKE - CUSTOMER SUPPORTED ONLY**

This applies to:
- `snowflake/README.md`
- `Dockerfile`
- `spcs_pipeline.py`

