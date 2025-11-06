# Snowflake Container Services (SPCS) Pipeline

This directory contains the Snowflake Container Services (SPCS) optimized implementation of the ECMWF TC Forecast Pipeline.

## Overview

The SPCS pipeline (`spcs_pipeline.py`) is a concurrent execution pipeline optimized for deployment in Snowflake Container Services. It provides:

- **Concurrent Downloads**: Parallel TC and wind data downloads using ThreadPoolExecutor
- **Parallel Processing**: CPU-intensive extraction and transformation using ProcessPoolExecutor
- **SPCS Integration**: Native support for Snowflake Container Services OAuth authentication
- **Flexible Authentication**: Supports SPCS OAuth, private key, and password authentication
- **Performance Optimized**: Designed for maximum throughput with configurable concurrency

## Files

- `spcs_pipeline.py`: Main concurrent pipeline orchestrator
- `snowflake_loader.py`: Enhanced Snowflake data loader with SPCS support
- `Dockerfile`: Container image for SPCS deployment

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
   - `https://essential.ecmwf.int/` (ECMWF DISS system for TC data)
   - `https://data.ecmwf.int/` (ECMWF Open Data for wind forecasts)

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

### 3. Running in SPCS

Execute the pipeline as a SPCS job using `EXECUTE JOB SERVICE`:

```sql
EXECUTE JOB SERVICE
   IN COMPUTE POOL my_compute_pool
   NAME = tc_ecmwf_job
   ASYNC = TRUE
   EXTERNAL_ACCESS_INTEGRATIONS = (TC_WIND_DATA_EGRESS_ACCESS_INTEGRATION)
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

### Key Concept: Forecast Runs vs. Files

**Important Distinction**: The download parameters control **forecast runs** (date/time combinations), not individual files.

- **One forecast run** = One specific date and time (e.g., `20251102120000` = Nov 2, 2025 at 12:00 UTC)
- **Each forecast run** can contain **multiple files** (one file per active tropical cyclone/storm)
- For example, if there are 5 active named storms, a single forecast run will download 5 files

**Examples**:
- `DOWNLOAD_LIMIT=1` → Downloads 1 forecast run → Could be 1 file or 10+ files (depending on active storms)
- `DOWNLOAD_DATE=20251102 RUN_TIME=12` → Downloads 1 forecast run (12Z on Nov 2) → Could be 1 file or 10+ files
- `DOWNLOAD_DATE=20251102` (no RUN_TIME) → Downloads 4 forecast runs (00Z, 06Z, 12Z, 18Z) → Could be 4 files or 40+ files

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
- **Important**: This refers to forecast runs (date/time combinations), not individual files. Each forecast run can contain multiple files (one per storm). For example, `DOWNLOAD_LIMIT=1` downloads all TC track files for the most recent forecast run, which could be 1 file or 10+ files depending on how many storms are active.

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

### Parameter Handling Logic

#### Scenario 1: No `DOWNLOAD_DATE` and No `RUN_TIME`

**Behavior**: Downloads the **latest N forecast runs** (where N = `DOWNLOAD_LIMIT`, default: 1)

**Process**:
1. Scrapes ECMWF DISS system for the most recent N forecast dates (sorted newest first)
2. For each forecast date/time, lists ALL TC track files available
3. Filters by `NAMED_STORMS_ONLY` if enabled
4. Downloads ALL TC track files for each of those forecast times

**Example**:
```bash
# Downloads latest 1 forecast run (could be 1 file or 10+ files depending on active storms)
docker run ... tc-ecmwf-pipeline:latest

# Downloads latest 3 forecast runs
docker run -e DOWNLOAD_LIMIT=3 ... tc-ecmwf-pipeline:latest
```

#### Scenario 2: `DOWNLOAD_DATE` Set, No `RUN_TIME`

**Behavior**: Downloads **all run times (00Z, 06Z, 12Z, 18Z)** for the specified date

**Important**: Each run time can contain multiple files (one per storm). So this could download 4 forecast runs × N storms = potentially many files.

**Process**:
1. Finds all available forecast dates matching the date (e.g., `20251102`)
2. This matches all run times for that date: `20251102000000`, `20251102060000`, `20251102120000`, `20251102180000`
3. For each matching forecast time, downloads ALL TC track files (one per storm)

**Example**:
```bash
# Downloads all run times for November 2, 2025
docker run -e DOWNLOAD_DATE=20251102 ... tc-ecmwf-pipeline:latest
```

#### Scenario 3: Both `DOWNLOAD_DATE` and `RUN_TIME` Set

**Behavior**: Downloads **only the specific forecast run** for that date and run time

**Important**: Even though it's a single forecast run, it can still contain multiple files (one per active storm).

**Process**:
1. Finds forecast dates matching `DOWNLOAD_DATE`
2. Further filters to dates where the hour matches `RUN_TIME`
3. Downloads ALL TC track files for that specific forecast time (one file per storm)

**Example**:
```bash
# Downloads only the 12Z forecast for November 2, 2025
docker run -e DOWNLOAD_DATE=20251102 -e RUN_TIME=12 ... tc-ecmwf-pipeline:latest
```

**Note**: 
- `RUN_TIME` can be specified as `12` or `12Z` - the code normalizes it to 2 digits
- Date format must be `YYYYMMDD` (8 digits, no dashes)
- If no forecasts are found for the specified date/time, the pipeline will log an error and exit

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

1. **Phase 1**: Concurrent TC and wind data downloads
2. **Phase 2**: Concurrent extraction and transformation of TC data
3. **Phase 3**: Wind combination processing (creates envelope files)
4. **Phase 4**: Load all data to Snowflake tables


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

