# snowflake_loader.py
"""
Snowflake Loader (SPCS Enhanced)
Loads all transformed TC forecast CSVs into Snowflake database.
Designed for Snowflake Container Services (SPCS) deployment.
Supports SPCS OAuth, private key, and password authentication.
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_private_key(private_key_path, passphrase=None):
    """
    Load and parse a private key from file.
    
    Args:
        private_key_path: Path to the private key file (PEM format)
        passphrase: Optional passphrase for encrypted private keys
    
    Returns:
        Private key bytes in DER format
    """
    with open(private_key_path, "rb") as key_file:
        private_key_data = key_file.read()
    
    # Parse the private key
    if passphrase:
        passphrase_bytes = passphrase.encode() if isinstance(passphrase, str) else passphrase
    else:
        passphrase_bytes = None
    
    private_key = serialization.load_pem_private_key(
        private_key_data,
        password=passphrase_bytes,
        backend=default_backend()
    )
    
    # Convert to DER format (required by Snowflake connector)
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return private_key_der


def get_snowflake_connection():
    """
    Create Snowflake connection from environment variables.
    Supports SPCS OAuth, private key, and password authentication.
    
    Authentication Methods:
    1. SPCS OAuth (for Snowflake Container Services):
       - Set SPCS_RUN=true
       - Token read from SPCS_TOKEN_PATH (default: /snowflake/session/token)
       
    2. Private Key (recommended):
       - Set SNOWFLAKE_PRIVATE_KEY_PATH to path of private key file
       - Optionally set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE if key is encrypted
       
    3. Password (legacy):
       - Set SNOWFLAKE_PASSWORD
    
    Required variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER (not required for SPCS OAuth)
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - SNOWFLAKE_SCHEMA
    - Authentication: SPCS_RUN=true OR SNOWFLAKE_PRIVATE_KEY_PATH OR SNOWFLAKE_PASSWORD
    """
    # Check for required variables
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    # Check for authentication method
    spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    
    if not spcs_run and not private_key_path and not password:
        raise ValueError(
            "Missing authentication: Set SPCS_RUN=true OR SNOWFLAKE_PRIVATE_KEY_PATH OR SNOWFLAKE_PASSWORD"
        )
    
    # Build connection parameters based on authentication method
    if spcs_run:
        # SPCS OAuth authentication using session token
        # Reference: https://medium.com/snowflake/connecting-to-snowflake-from-snowpark-container-services-cfc3a133480e
        logger.info("Connecting to Snowflake with SPCS OAuth authentication...")
        token_path = os.getenv('SPCS_TOKEN_PATH', '/snowflake/session/token')
        
        try:
            with open(token_path, 'r') as f:
                token = f.read().strip()
            
            # SPCS requires specific connection parameters for internal network
            conn_params = {
                'host': os.getenv('SNOWFLAKE_HOST'),
                'port': os.getenv('SNOWFLAKE_PORT'),
                'protocol': 'https',
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'authenticator': 'oauth',
                'token': token,
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA'),
                'client_session_keep_alive': True
            }
            logger.info(f"✓ Loaded OAuth token from {token_path}")
            if conn_params.get('host') and conn_params.get('port'):
                logger.info(f"✓ Using SPCS internal network: {conn_params['host']}:{conn_params['port']}")
        except Exception as e:
            raise ValueError(f"Failed to load OAuth token from {token_path}: {str(e)}")
    else:
        # Non-SPCS mode: use standard connection parameters
        if not os.getenv('SNOWFLAKE_USER'):
            raise ValueError("SNOWFLAKE_USER is required for non-SPCS authentication")
        
        conn_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
        
        if private_key_path:
            logger.info("Connecting to Snowflake with private key authentication...")
            passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
            
            try:
                private_key_der = load_private_key(private_key_path, passphrase)
                conn_params['private_key'] = private_key_der
                logger.info(f"✓ Loaded private key from {private_key_path}")
            except Exception as e:
                raise ValueError(f"Failed to load private key from {private_key_path}: {str(e)}")
        else:
            logger.info("Connecting to Snowflake with password authentication...")
            conn_params['password'] = password
    
    # Add connection options for SSL/OCSP handling
    # This helps avoid certificate validation issues with S3 stage uploads
    conn_params['ocsp_fail_open'] = True
    
    # Check if we should use insecure mode (for certificate issues)
    use_insecure = os.getenv('SNOWFLAKE_INSECURE_MODE', 'false').lower() == 'true'
    if use_insecure:
        logger.warning("Using insecure_mode - SSL certificate validation is disabled")
        conn_params['insecure_mode'] = True
    
    # Disable OCSP checks entirely via session parameters
    conn_params['session_parameters'] = {
        'OCSP_FAIL_OPEN': 'true'
    }
    
    # Connect
    try:
        conn = snowflake.connector.connect(**conn_params)
        logger.info("✓ Connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise


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
        table_type: Target table type ('TC_TRACKS', 'TC_ENVELOPES_INDIVIDUAL', 'TC_ENVELOPES_COMBINED')
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
    Load precipitation forecast metadata rows into PRECIP_FORECASTS table.

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
            CREATE TABLE IF NOT EXISTS PRECIP_FORECASTS (
                FORECAST_TIME  TIMESTAMP_NTZ,
                PARAM          VARCHAR,
                STAGE_PATH     VARCHAR,
                CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE PRECIP_FORECASTS_STAGING (
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

        success, _, _, _ = write_pandas(conn=conn, df=df, table_name='PRECIP_FORECASTS_STAGING',
                                        auto_create_table=False, quote_identifiers=False)
        if not success:
            logger.error('  Failed to write precip metadata to staging table')
            return 0

        cursor.execute("""
            MERGE INTO PRECIP_FORECASTS t
            USING PRECIP_FORECASTS_STAGING s
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
        logger.info(f'  Merged {rows_merged} rows into PRECIP_FORECASTS')
        return rows_merged

    except Exception as e:
        logger.error(f'Error loading precip metadata: {e}')
        conn.rollback()
        return 0
    finally:
        cursor.close()


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