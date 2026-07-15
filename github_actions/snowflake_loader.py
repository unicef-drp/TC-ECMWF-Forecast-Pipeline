# snowflake_loader.py
"""
Snowflake Loader
Loads all transformed TC forecast CSVs into Snowflake database.
Designed to run in GitHub Actions with environment variables.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import timedelta
from typing import Optional
import pandas as pd
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_snowflake_connection():
    """Create Snowflake connection from environment variables."""
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    logger.info("✓ Connected to Snowflake")
    return conn


def prepare_dataframe(df):
    """Prepare DataFrame for Snowflake upload."""
    # Rename columns to match Snowflake table (uppercase)
    df_upload = df.copy()
    df_upload.columns = df_upload.columns.str.upper()

    # The GEOGRAPHY type can't be uploaded directly via pandas

    # Convert timestamp columns to proper datetime format
    # Handle NaT values explicitly before converting to string
    if 'FORECAST_TIME' in df_upload.columns:
        df_upload['FORECAST_TIME'] = pd.to_datetime(df_upload['FORECAST_TIME'], errors='coerce')
        df_upload['FORECAST_TIME'] = df_upload['FORECAST_TIME'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
        )

    if 'VALID_TIME' in df_upload.columns:
        df_upload['VALID_TIME'] = pd.to_datetime(df_upload['VALID_TIME'], errors='coerce')
        df_upload['VALID_TIME'] = df_upload['VALID_TIME'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
        )

    # Clean numeric columns - replace inf/-inf/NaN with None
    # Also ensure proper float formatting to avoid SQL syntax errors
    numeric_columns = df_upload.select_dtypes(include=['float64', 'float32']).columns
    for col in numeric_columns:
        # Replace infinity values with None
        df_upload[col] = df_upload[col].replace([np.inf, -np.inf], np.nan)
        # Round to reasonable precision to avoid formatting issues
        df_upload[col] = df_upload[col].apply(
            lambda x: round(float(x), 6) if pd.notna(x) and not np.isinf(x) else None
        )

    # Convert integer columns properly
    int_columns = df_upload.select_dtypes(include=['int64', 'int32']).columns
    for col in int_columns:
        df_upload[col] = df_upload[col].where(pd.notna(df_upload[col]), None)

    # Clean geography columns - ensure proper WKT format
    geography_columns = ['ENVELOPE_REGION', 'WIND_FIELD_POLYGON_34KT', 'WIND_FIELD_POLYGON_50KT',
                         'WIND_FIELD_POLYGON_64KT']
    for col in geography_columns:
        if col in df_upload.columns:
            # Replace None, empty strings, and 'None' strings with None
            df_upload[col] = df_upload[col].replace(['', 'None', 'null'], None)
            # Ensure proper WKT format
            df_upload[col] = df_upload[col].apply(
                lambda x: None if pd.isna(x) or x is None else str(x).strip()
            )

    return df_upload


def load_csv_to_snowflake(csv_file, conn, table_type='TC_TRACKS', use_staging=True):
    """
    Load CSV file into Snowflake using bulk operations.

    Args:
        csv_file: Path to CSV file
        conn: Snowflake connection
        table_type: Target table type ('TC_TRACKS', 'TC_ENVELOPES_INDIVIDUAL',
                    'TC_ENVELOPES_COMBINED', 'TC_GUST_ENVELOPES_INDIVIDUAL',
                    'TC_GUST_ENVELOPES_COMBINED')
        use_staging: If True, use staging table + MERGE (handles duplicates)
                     If False, direct INSERT (faster but no deduplication)
    """
    logger.info(f"Loading {csv_file.name}...")

    try:
        # Read CSV
        df = pd.read_csv(csv_file)
        logger.info(f"  Read {len(df)} records from CSV")

        if df.empty:
            logger.warning("  No data to load")
            return 0

        # Prepare data
        df_upload = prepare_dataframe(df)

        cursor = conn.cursor()

        if use_staging:
            # Method 1: Staging table + MERGE (handles duplicates, slightly slower)
            staging_table = f"{table_type}_STAGING"

            # Create staging table with proper column types for geography handling
            if table_type == 'TC_TRACKS':
                cursor.execute(f"""
                    CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
                        FORECAST_TIME TIMESTAMP_NTZ,
                        TRACK_ID VARCHAR,
                        ENSEMBLE_MEMBER INTEGER,
                        VALID_TIME TIMESTAMP_NTZ,
                        LEAD_TIME INTEGER,
                        LATITUDE FLOAT,
                        LONGITUDE FLOAT,
                        PRESSURE_HPA FLOAT,
                        WIND_SPEED_KNOTS FLOAT,
                        RADIUS_OF_MAXIMUM_WINDS_KM FLOAT,
                        RADIUS_34_KNOT_WINDS_NE_KM FLOAT,
                        RADIUS_34_KNOT_WINDS_SE_KM FLOAT,
                        RADIUS_34_KNOT_WINDS_SW_KM FLOAT,
                        RADIUS_34_KNOT_WINDS_NW_KM FLOAT,
                        RADIUS_50_KNOT_WINDS_NE_KM FLOAT,
                        RADIUS_50_KNOT_WINDS_SE_KM FLOAT,
                        RADIUS_50_KNOT_WINDS_SW_KM FLOAT,
                        RADIUS_50_KNOT_WINDS_NW_KM FLOAT,
                        RADIUS_64_KNOT_WINDS_NE_KM FLOAT,
                        RADIUS_64_KNOT_WINDS_SE_KM FLOAT,
                        RADIUS_64_KNOT_WINDS_SW_KM FLOAT,
                        RADIUS_64_KNOT_WINDS_NW_KM FLOAT,
                        WIND_FIELD_POLYGON_34KT VARCHAR,
                        WIND_FIELD_POLYGON_50KT VARCHAR,
                        WIND_FIELD_POLYGON_64KT VARCHAR
                    )
                """)
            elif table_type == 'TC_ENVELOPES_INDIVIDUAL':
                cursor.execute(f"""
                    CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
                        FORECAST_TIME TIMESTAMP_NTZ,
                        TRACK_ID VARCHAR,
                        ENSEMBLE_MEMBER INTEGER,
                        VALID_TIME TIMESTAMP_NTZ,
                        LEAD_TIME INTEGER,
                        WIND_THRESHOLD INTEGER,
                        ENVELOPE_REGION VARCHAR
                    )
                """)
            elif table_type == 'TC_ENVELOPES_COMBINED':
                cursor.execute(f"""
                    CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
                        FORECAST_TIME TIMESTAMP_NTZ,
                        TRACK_ID VARCHAR,
                        ENSEMBLE_MEMBER INTEGER,
                        LEAD_TIME VARCHAR,  -- Keep as VARCHAR to handle range format
                        WIND_THRESHOLD INTEGER,
                        ENVELOPE_REGION VARCHAR
                    )
                """)
            elif table_type == 'TC_GUST_ENVELOPES_INDIVIDUAL':
                cursor.execute(f"""
                    CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
                        FORECAST_TIME TIMESTAMP_NTZ,
                        TRACK_ID VARCHAR,
                        ENSEMBLE_MEMBER INTEGER,
                        VALID_TIME TIMESTAMP_NTZ,
                        LEAD_TIME INTEGER,
                        GUST_THRESHOLD INTEGER,
                        ENVELOPE_REGION VARCHAR
                    )
                """)
            elif table_type == 'TC_GUST_ENVELOPES_COMBINED':
                cursor.execute(f"""
                    CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
                        FORECAST_TIME TIMESTAMP_NTZ,
                        TRACK_ID VARCHAR,
                        ENSEMBLE_MEMBER INTEGER,
                        LEAD_TIME VARCHAR,  -- Keep as VARCHAR to handle range format
                        GUST_THRESHOLD INTEGER,
                        ENVELOPE_REGION VARCHAR
                    )
                """)
            logger.info(f"  Created staging table with proper column types")

            # Bulk upload to staging table
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_upload,
                table_name=staging_table,
                auto_create_table=False,
                quote_identifiers=False
            )

            if not success:
                logger.error(f"  Failed to write to staging table")
                cursor.close()
                return 0

            logger.info(f"  Uploaded {nrows} rows to staging table")

            # Handle different table types
            if table_type == 'TC_TRACKS':
                # No special geography processing needed for TC_TRACKS
                logger.info(f"  Processing TC_TRACKS data")

                # MERGE from staging to main table
                merge_sql = f"""
                    MERGE INTO TC_TRACKS t
                    USING {staging_table} s
                    ON t.TRACK_ID = s.TRACK_ID 
                        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
                        AND t.FORECAST_TIME = s.FORECAST_TIME
                        AND t.LEAD_TIME = s.LEAD_TIME
                    WHEN NOT MATCHED THEN INSERT (
                        FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME, LEAD_TIME,
                        LATITUDE, LONGITUDE, PRESSURE_HPA, WIND_SPEED_KNOTS,
                        RADIUS_OF_MAXIMUM_WINDS_KM,
                        RADIUS_34_KNOT_WINDS_NE_KM, RADIUS_34_KNOT_WINDS_SE_KM,
                        RADIUS_34_KNOT_WINDS_SW_KM, RADIUS_34_KNOT_WINDS_NW_KM,
                        RADIUS_50_KNOT_WINDS_NE_KM, RADIUS_50_KNOT_WINDS_SE_KM,
                        RADIUS_50_KNOT_WINDS_SW_KM, RADIUS_50_KNOT_WINDS_NW_KM,
                        RADIUS_64_KNOT_WINDS_NE_KM, RADIUS_64_KNOT_WINDS_SE_KM,
                        RADIUS_64_KNOT_WINDS_SW_KM, RADIUS_64_KNOT_WINDS_NW_KM,
                        WIND_FIELD_POLYGON_34KT, WIND_FIELD_POLYGON_50KT, WIND_FIELD_POLYGON_64KT,
                        WIND_FIELD_POLYGON_34KT_GEO, WIND_FIELD_POLYGON_50KT_GEO, WIND_FIELD_POLYGON_64KT_GEO
                    ) VALUES (
                        s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.VALID_TIME, s.LEAD_TIME,
                        s.LATITUDE, s.LONGITUDE, s.PRESSURE_HPA, s.WIND_SPEED_KNOTS,
                        s.RADIUS_OF_MAXIMUM_WINDS_KM,
                        s.RADIUS_34_KNOT_WINDS_NE_KM, s.RADIUS_34_KNOT_WINDS_SE_KM,
                        s.RADIUS_34_KNOT_WINDS_SW_KM, s.RADIUS_34_KNOT_WINDS_NW_KM,
                        s.RADIUS_50_KNOT_WINDS_NE_KM, s.RADIUS_50_KNOT_WINDS_SE_KM,
                        s.RADIUS_50_KNOT_WINDS_SW_KM, s.RADIUS_50_KNOT_WINDS_NW_KM,
                        s.RADIUS_64_KNOT_WINDS_NE_KM, s.RADIUS_64_KNOT_WINDS_SE_KM,
                        s.RADIUS_64_KNOT_WINDS_SW_KM, s.RADIUS_64_KNOT_WINDS_NW_KM,
                        s.WIND_FIELD_POLYGON_34KT, s.WIND_FIELD_POLYGON_50KT, s.WIND_FIELD_POLYGON_64KT,
                        TRY_TO_GEOGRAPHY(NULLIF(NULLIF(TRIM(s.WIND_FIELD_POLYGON_34KT), ''), 'None')),
                        TRY_TO_GEOGRAPHY(NULLIF(NULLIF(TRIM(s.WIND_FIELD_POLYGON_50KT), ''), 'None')),
                        TRY_TO_GEOGRAPHY(NULLIF(NULLIF(TRIM(s.WIND_FIELD_POLYGON_64KT), ''), 'None'))
                    )
                """

            elif table_type == 'TC_ENVELOPES_INDIVIDUAL':
                # No need to update geography in staging table - keep as VARCHAR
                # Geography conversion will happen during MERGE
                logger.info(f"  Keeping ENVELOPE_REGION as VARCHAR in staging table")

                # MERGE from staging to main table with geography conversion
                merge_sql = f"""
                    MERGE INTO TC_ENVELOPES_INDIVIDUAL t
                    USING (
                        SELECT 
                            FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME, LEAD_TIME,
                            WIND_THRESHOLD,
                            CASE 
                                WHEN ENVELOPE_REGION IS NOT NULL 
                                     AND ENVELOPE_REGION != '' 
                                     AND ENVELOPE_REGION != 'None'
                                     AND ENVELOPE_REGION != 'null'
                                THEN TRY_TO_GEOGRAPHY(ENVELOPE_REGION)
                                ELSE NULL
                            END AS ENVELOPE_REGION
                        FROM {staging_table}
                    ) s
                    ON t.TRACK_ID = s.TRACK_ID 
                        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
                        AND t.FORECAST_TIME = s.FORECAST_TIME
                        AND t.LEAD_TIME = s.LEAD_TIME
                        AND t.WIND_THRESHOLD = s.WIND_THRESHOLD
                    WHEN NOT MATCHED THEN INSERT (
                        FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME, LEAD_TIME,
                        WIND_THRESHOLD, ENVELOPE_REGION
                    ) VALUES (
                        s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.VALID_TIME, s.LEAD_TIME,
                        s.WIND_THRESHOLD, s.ENVELOPE_REGION
                    )
                """

            elif table_type == 'TC_ENVELOPES_COMBINED':
                # No need to update geography in staging table - keep as VARCHAR
                # Geography conversion will happen during MERGE
                logger.info(f"  Keeping ENVELOPE_REGION as VARCHAR in staging table")
                logger.info(f"  LEAD_TIME will be parsed from VARCHAR to INTEGER during MERGE")

                # MERGE from staging to main table with geography conversion and lead time range handling
                merge_sql = f"""
                    MERGE INTO TC_ENVELOPES_COMBINED t
                    USING (
                        SELECT
                            FORECAST_TIME,
                            TRACK_ID,
                            ENSEMBLE_MEMBER,
                            CASE
                                WHEN LEAD_TIME LIKE '%-%' THEN
                                    CAST(SPLIT_PART(LEAD_TIME, '-', 1) AS INTEGER)
                                ELSE CAST(LEAD_TIME AS INTEGER)
                            END AS LEAD_TIME_RANGE,
                            WIND_THRESHOLD,
                            CASE
                                WHEN ENVELOPE_REGION IS NOT NULL
                                     AND ENVELOPE_REGION != ''
                                     AND ENVELOPE_REGION != 'None'
                                     AND ENVELOPE_REGION != 'null'
                                THEN TRY_TO_GEOGRAPHY(ENVELOPE_REGION)
                                ELSE NULL
                            END AS ENVELOPE_REGION
                        FROM {staging_table}
                    ) s
                    ON t.TRACK_ID = s.TRACK_ID
                        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
                        AND t.FORECAST_TIME = s.FORECAST_TIME
                        AND t.WIND_THRESHOLD = s.WIND_THRESHOLD
                    WHEN MATCHED AND s.ENVELOPE_REGION IS NOT NULL THEN UPDATE SET
                        ENVELOPE_REGION = s.ENVELOPE_REGION,
                        LEAD_TIME_RANGE = s.LEAD_TIME_RANGE
                    WHEN NOT MATCHED THEN INSERT (
                        FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, LEAD_TIME_RANGE,
                        WIND_THRESHOLD, ENVELOPE_REGION
                    ) VALUES (
                        s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.LEAD_TIME_RANGE,
                        s.WIND_THRESHOLD, s.ENVELOPE_REGION
                    )
                """

            elif table_type == 'TC_GUST_ENVELOPES_INDIVIDUAL':
                logger.info(f"  Keeping ENVELOPE_REGION as VARCHAR in staging table")

                merge_sql = f"""
                    MERGE INTO TC_GUST_ENVELOPES_INDIVIDUAL t
                    USING (
                        SELECT
                            FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME, LEAD_TIME,
                            GUST_THRESHOLD,
                            CASE
                                WHEN ENVELOPE_REGION IS NOT NULL
                                     AND ENVELOPE_REGION != ''
                                     AND ENVELOPE_REGION != 'None'
                                     AND ENVELOPE_REGION != 'null'
                                THEN TRY_TO_GEOGRAPHY(ENVELOPE_REGION)
                                ELSE NULL
                            END AS ENVELOPE_REGION
                        FROM {staging_table}
                    ) s
                    ON t.TRACK_ID = s.TRACK_ID
                        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
                        AND t.FORECAST_TIME = s.FORECAST_TIME
                        AND t.LEAD_TIME = s.LEAD_TIME
                        AND t.GUST_THRESHOLD = s.GUST_THRESHOLD
                    WHEN NOT MATCHED THEN INSERT (
                        FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, VALID_TIME, LEAD_TIME,
                        GUST_THRESHOLD, ENVELOPE_REGION
                    ) VALUES (
                        s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.VALID_TIME, s.LEAD_TIME,
                        s.GUST_THRESHOLD, s.ENVELOPE_REGION
                    )
                """

            elif table_type == 'TC_GUST_ENVELOPES_COMBINED':
                logger.info(f"  Keeping ENVELOPE_REGION as VARCHAR in staging table")
                logger.info(f"  LEAD_TIME will be parsed from VARCHAR to INTEGER during MERGE")

                merge_sql = f"""
                    MERGE INTO TC_GUST_ENVELOPES_COMBINED t
                    USING (
                        SELECT
                            FORECAST_TIME,
                            TRACK_ID,
                            ENSEMBLE_MEMBER,
                            CASE
                                WHEN LEAD_TIME LIKE '%-%' THEN
                                    CAST(SPLIT_PART(LEAD_TIME, '-', 1) AS INTEGER)
                                ELSE CAST(LEAD_TIME AS INTEGER)
                            END AS LEAD_TIME_RANGE,
                            GUST_THRESHOLD,
                            CASE
                                WHEN ENVELOPE_REGION IS NOT NULL
                                     AND ENVELOPE_REGION != ''
                                     AND ENVELOPE_REGION != 'None'
                                     AND ENVELOPE_REGION != 'null'
                                THEN TRY_TO_GEOGRAPHY(ENVELOPE_REGION)
                                ELSE NULL
                            END AS ENVELOPE_REGION
                        FROM {staging_table}
                    ) s
                    ON t.TRACK_ID = s.TRACK_ID
                        AND t.ENSEMBLE_MEMBER = s.ENSEMBLE_MEMBER
                        AND t.FORECAST_TIME = s.FORECAST_TIME
                        AND t.GUST_THRESHOLD = s.GUST_THRESHOLD
                    WHEN MATCHED AND s.ENVELOPE_REGION IS NOT NULL THEN UPDATE SET
                        ENVELOPE_REGION = s.ENVELOPE_REGION,
                        LEAD_TIME_RANGE = s.LEAD_TIME_RANGE
                    WHEN NOT MATCHED THEN INSERT (
                        FORECAST_TIME, TRACK_ID, ENSEMBLE_MEMBER, LEAD_TIME_RANGE,
                        GUST_THRESHOLD, ENVELOPE_REGION
                    ) VALUES (
                        s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.LEAD_TIME_RANGE,
                        s.GUST_THRESHOLD, s.ENVELOPE_REGION
                    )
                """

            cursor.execute(merge_sql)
            rows_merged = cursor.rowcount
            logger.info(f"  Merged {rows_merged} rows into {table_type}")

            # Drop staging table
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

        else:
            # Method 2: Direct INSERT (fastest, but will fail on duplicates)
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_upload,
                table_name=table_type,
                auto_create_table=False,
                quote_identifiers=False
            )

            if not success:
                logger.error(f"  Failed to write to {table_type}")
                cursor.close()
                return 0

            logger.info(f"  Inserted {nrows} rows directly")
            rows_merged = nrows

        conn.commit()
        cursor.close()

        logger.info(f"✓ Loaded {rows_merged} records from {csv_file.name}")
        return rows_merged

    except Exception as e:
        logger.error(f"Error loading {csv_file.name}: {e}")
        conn.rollback()
        try:
            cursor.close()
        except Exception:
            pass
        return 0


def load_precip_metadata_to_snowflake(metadata_rows: list, conn) -> int:
    """
    Load met forecast metadata rows into MET_FORECASTS table.

    Creates the table if it does not exist. Uses staging + MERGE to deduplicate
    on (FORECAST_TIME, PARAM) — re-runs are safe.

    The zarr is a global file (one per model run), so there is one row per
    (FORECAST_TIME, PARAM), not one per storm.

    Args:
        metadata_rows: list of dicts with keys forecast_time, param, stage_path
        conn: active Snowflake connection

    Returns number of rows merged.
    """
    if not metadata_rows:
        return 0

    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MET_FORECASTS (
                FORECAST_TIME  TIMESTAMP_NTZ,
                PARAM          VARCHAR,
                STAGE_PATH     VARCHAR,
                CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE MET_FORECASTS_STAGING (
                FORECAST_TIME  TIMESTAMP_NTZ,
                PARAM          VARCHAR,
                STAGE_PATH     VARCHAR
            )
        """)

        df = pd.DataFrame(metadata_rows)
        df.columns = df.columns.str.upper()
        if 'FORECAST_TIME' in df.columns:
            df['FORECAST_TIME'] = pd.to_datetime(df['FORECAST_TIME'], errors='coerce').apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
            )

        success, _, _, _ = write_pandas(conn=conn, df=df, table_name='MET_FORECASTS_STAGING',
                                        auto_create_table=False, quote_identifiers=False)
        if not success:
            logger.error('  Failed to write precip metadata to staging table')
            return 0

        cursor.execute("""
            MERGE INTO MET_FORECASTS t
            USING MET_FORECASTS_STAGING s
              ON  t.FORECAST_TIME = s.FORECAST_TIME
              AND t.PARAM         = s.PARAM
            WHEN MATCHED THEN
                UPDATE SET t.STAGE_PATH = s.STAGE_PATH
            WHEN NOT MATCHED THEN
                INSERT (FORECAST_TIME, PARAM, STAGE_PATH)
                VALUES (s.FORECAST_TIME, s.PARAM, s.STAGE_PATH)
        """)
        rows_merged = cursor.rowcount
        conn.commit()
        logger.info(f'  Merged {rows_merged} rows into MET_FORECASTS')
        return rows_merged

    except Exception as e:
        logger.error(f'Error loading precip metadata: {e}')
        conn.rollback()
        return 0
    finally:
        cursor.close()


def load_riverine_metadata_to_snowflake(metadata_rows: list, conn) -> int:
    """
    Load GloFAS riverine metadata rows into RIVER_FORECASTS table; both raw
    discharge (PARAM='dis24') and JRC per-member flood-extent output
    (PARAM='extent_rp{N}_bymember', with IS_STANDIN true for the RP2/RP5
    stand-in tiers) share this one table.

    Args:
        metadata_rows: list of dicts with keys forecast_time, param, stage_path,
            and optionally is_standin (only meaningful for extent_rp2/extent_rp5 rows)
        conn: active Snowflake connection

    Returns number of rows merged.
    """
    if not metadata_rows:
        return 0

    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RIVER_FORECASTS (
                FORECAST_TIME  TIMESTAMP_NTZ,
                PARAM          VARCHAR,
                STAGE_PATH     VARCHAR,
                IS_STANDIN     BOOLEAN,
                CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("ALTER TABLE RIVER_FORECASTS ADD COLUMN IF NOT EXISTS IS_STANDIN BOOLEAN")

        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE RIVER_FORECASTS_STAGING (
                FORECAST_TIME  TIMESTAMP_NTZ,
                PARAM          VARCHAR,
                STAGE_PATH     VARCHAR,
                IS_STANDIN     BOOLEAN
            )
        """)

        df = pd.DataFrame(metadata_rows)
        df.columns = df.columns.str.upper()
        if 'IS_STANDIN' not in df.columns:
            df['IS_STANDIN'] = None
        if 'FORECAST_TIME' in df.columns:
            df['FORECAST_TIME'] = pd.to_datetime(df['FORECAST_TIME'], errors='coerce').apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
            )

        success, _, _, _ = write_pandas(conn=conn, df=df, table_name='RIVER_FORECASTS_STAGING',
                                        auto_create_table=False, quote_identifiers=False)
        if not success:
            logger.error('  Failed to write riverine metadata to staging table')
            return 0

        cursor.execute("""
            MERGE INTO RIVER_FORECASTS t
            USING RIVER_FORECASTS_STAGING s
              ON  t.FORECAST_TIME = s.FORECAST_TIME
              AND t.PARAM         = s.PARAM
            WHEN MATCHED THEN
                UPDATE SET t.STAGE_PATH = s.STAGE_PATH, t.IS_STANDIN = s.IS_STANDIN
            WHEN NOT MATCHED THEN
                INSERT (FORECAST_TIME, PARAM, STAGE_PATH, IS_STANDIN)
                VALUES (s.FORECAST_TIME, s.PARAM, s.STAGE_PATH, s.IS_STANDIN)
        """)
        rows_merged = cursor.rowcount
        conn.commit()
        logger.info(f'  Merged {rows_merged} rows into RIVER_FORECASTS')
        return rows_merged

    except Exception as e:
        logger.error(f'Error loading riverine metadata: {e}')
        conn.rollback()
        return 0
    finally:
        cursor.close()


def save_cds_request_ids(actual_date, requests: dict, conn) -> int:
    """
    Persist the request_id from a submit_glofas_requests() call (GLOFAS_MODE=submit,
    see glofas_pipeline_core.py) so a later, separate process step can resume
    waiting on it via resume_glofas_download() without paying compute for the CDS
    queue wait in between.

    Args:
        actual_date: datetime, the date submit_glofas_requests() actually
            resolved to (may lag the originally-requested date)
        requests: {product_type: request_id} as returned by submit_glofas_requests()
        conn: active Snowflake connection

    Returns number of rows merged (one per product type, normally 2).
    """
    if not requests:
        return 0

    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLOFAS_CDS_REQUESTS (
                FORECAST_DATE  DATE,
                PRODUCT_TYPE   VARCHAR,
                REQUEST_ID     VARCHAR,
                SUBMITTED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

        rows = [{'FORECAST_DATE': actual_date.strftime('%Y-%m-%d'),
                 'PRODUCT_TYPE': product_type, 'REQUEST_ID': request_id}
                for product_type, request_id in requests.items()]
        df = pd.DataFrame(rows)

        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE GLOFAS_CDS_REQUESTS_STAGING (
                FORECAST_DATE  DATE,
                PRODUCT_TYPE   VARCHAR,
                REQUEST_ID     VARCHAR
            )
        """)
        success, _, _, _ = write_pandas(conn=conn, df=df, table_name='GLOFAS_CDS_REQUESTS_STAGING',
                                        auto_create_table=False, quote_identifiers=False)
        if not success:
            logger.error('  Failed to write CDS request IDs to staging table')
            return 0

        cursor.execute("""
            MERGE INTO GLOFAS_CDS_REQUESTS t
            USING GLOFAS_CDS_REQUESTS_STAGING s
              ON  t.FORECAST_DATE = s.FORECAST_DATE
              AND t.PRODUCT_TYPE  = s.PRODUCT_TYPE
            WHEN MATCHED THEN
                UPDATE SET t.REQUEST_ID = s.REQUEST_ID, t.SUBMITTED_AT = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (FORECAST_DATE, PRODUCT_TYPE, REQUEST_ID)
                VALUES (s.FORECAST_DATE, s.PRODUCT_TYPE, s.REQUEST_ID)
        """)
        rows_merged = cursor.rowcount
        conn.commit()
        logger.info(f'  Merged {rows_merged} CDS request ID(s) into GLOFAS_CDS_REQUESTS')
        return rows_merged

    except Exception as e:
        logger.error(f'Error saving CDS request IDs: {e}')
        conn.rollback()
        return 0
    finally:
        cursor.close()


def load_cds_request_ids(forecast_date, conn, max_lag_days: int) -> Optional[dict]:
    """
    Look up previously-saved CDS request IDs (from save_cds_request_ids()) for
    GLOFAS_MODE=process to resume from. Searches forecast_date first, then earlier
    days up to max_lag_days, same day-fallback order submit_glofas_requests()
    itself already resolved against, so this finds whichever date actually has a
    saved submission, not necessarily the literal date requested.

    max_lag_days has no default deliberately: it must be MAX_PUBLICATION_LAG_DAYS
    (from glofas_downloader.py) passed explicitly by the caller, not a second,
    independently-hardcoded literal here that could silently drift out of sync
    with submit_glofas_requests()'s own fallback window.

    Returns {'actual_date': datetime, 'requests': {product_type: request_id}} for
    the most recent date with a complete (both product types) saved submission,
    or None if nothing usable was found, the caller (download_glofas_forecast())
    treats None exactly like "no pre_submitted given" and falls back to a fresh
    submit-and-block, so a missed/failed submit step never blocks the pipeline.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT TO_VARCHAR(FORECAST_DATE, 'YYYY-MM-DD'), PRODUCT_TYPE, REQUEST_ID
            FROM GLOFAS_CDS_REQUESTS
            WHERE FORECAST_DATE BETWEEN DATEADD(day, %s, %s) AND %s
            ORDER BY FORECAST_DATE DESC
        """, (-max_lag_days, forecast_date.strftime('%Y-%m-%d'), forecast_date.strftime('%Y-%m-%d')))
        rows = cursor.fetchall()
    except Exception as e:
        logger.warning(f'  Could not look up saved CDS request IDs (table may not exist yet): {e}')
        return None
    finally:
        cursor.close()

    if not rows:
        return None

    by_date = {}
    for date_str, product_type, request_id in rows:
        by_date.setdefault(date_str, {})[product_type] = request_id

    for lag in range(max_lag_days + 1):
        candidate = forecast_date - timedelta(days=lag)
        candidate_str = candidate.strftime('%Y-%m-%d')
        requests = by_date.get(candidate_str)
        if requests and {'ensemble_perturbed_forecasts', 'control_forecast'} <= requests.keys():
            logger.info(f'  Resuming from saved CDS request IDs for {candidate_str}: {requests}')
            return {'actual_date': candidate, 'requests': requests}

    return None


def main():
    """Main execution."""
    try:
        logger.info("=" * 60)
        logger.info("SNOWFLAKE LOADER (OPTIMIZED)")
        logger.info("=" * 60)

        # Find transformed CSV files
        transformed_dir = Path("tc_data_transformed")
        csv_files = list(transformed_dir.glob("transformed_*.csv"))

        if not csv_files:
            logger.warning("No transformed CSV files found")
            return

        logger.info(f"Found {len(csv_files)} CSV files to load\n")

        # Connect to Snowflake
        conn = get_snowflake_connection()

        try:
            # Load each CSV using bulk operations
            total_loaded = 0
            for csv_file in csv_files:
                loaded = load_csv_to_snowflake(csv_file, conn, table_type='TC_TRACKS', use_staging=True)
                total_loaded += loaded

            # Verify
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM TC_TRACKS")
            total_in_db = cursor.fetchone()[0]
            cursor.close()

            logger.info("\n" + "=" * 60)
            logger.info("LOADING SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Files processed: {len(csv_files)}")
            logger.info(f"Records loaded: {total_loaded}")
            logger.info(f"Total in database: {total_in_db}")
            logger.info("=" * 60)
            logger.info("LOADING COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)

        finally:
            conn.close()
            logger.info("Connection closed")

    except Exception as e:
        logger.error(f"Loading failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()