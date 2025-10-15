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

    return df_upload


def load_csv_to_snowflake(csv_file, conn, use_staging=True):
    """
    Load CSV file into Snowflake using bulk operations.

    Args:
        csv_file: Path to CSV file
        conn: Snowflake connection
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
            staging_table = "TC_TRACKS_STAGING"

            # Create staging table (temporary) - use CREATE OR REPLACE to handle any existing table
            cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {staging_table} LIKE TC_TRACKS")
            logger.info(f"  Created staging table")

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
                return 0

            logger.info(f"  Uploaded {nrows} rows to staging table")

            # Update LOCATION column using ST_POINT (handle NULL coordinates)
            cursor.execute(f"""
                UPDATE {staging_table}
                SET LOCATION = ST_POINT(LONGITUDE, LATITUDE)
                WHERE LONGITUDE IS NOT NULL AND LATITUDE IS NOT NULL
            """)
            updated = cursor.rowcount
            logger.info(f"  Updated {updated} LOCATION geography points")

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
                    RADIUS_OF_MAXIMUM_WINDS_KM, LOCATION,
                    RADIUS_34_KNOT_WINDS_NE_KM, RADIUS_34_KNOT_WINDS_SE_KM,
                    RADIUS_34_KNOT_WINDS_SW_KM, RADIUS_34_KNOT_WINDS_NW_KM,
                    RADIUS_50_KNOT_WINDS_NE_KM, RADIUS_50_KNOT_WINDS_SE_KM,
                    RADIUS_50_KNOT_WINDS_SW_KM, RADIUS_50_KNOT_WINDS_NW_KM,
                    RADIUS_64_KNOT_WINDS_NE_KM, RADIUS_64_KNOT_WINDS_SE_KM,
                    RADIUS_64_KNOT_WINDS_SW_KM, RADIUS_64_KNOT_WINDS_NW_KM
                ) VALUES (
                    s.FORECAST_TIME, s.TRACK_ID, s.ENSEMBLE_MEMBER, s.VALID_TIME, s.LEAD_TIME,
                    s.LATITUDE, s.LONGITUDE, s.PRESSURE_HPA, s.WIND_SPEED_KNOTS,
                    s.RADIUS_OF_MAXIMUM_WINDS_KM, s.LOCATION,
                    s.RADIUS_34_KNOT_WINDS_NE_KM, s.RADIUS_34_KNOT_WINDS_SE_KM,
                    s.RADIUS_34_KNOT_WINDS_SW_KM, s.RADIUS_34_KNOT_WINDS_NW_KM,
                    s.RADIUS_50_KNOT_WINDS_NE_KM, s.RADIUS_50_KNOT_WINDS_SE_KM,
                    s.RADIUS_50_KNOT_WINDS_SW_KM, s.RADIUS_50_KNOT_WINDS_NW_KM,
                    s.RADIUS_64_KNOT_WINDS_NE_KM, s.RADIUS_64_KNOT_WINDS_SE_KM,
                    s.RADIUS_64_KNOT_WINDS_SW_KM, s.RADIUS_64_KNOT_WINDS_NW_KM
                )
            """
            cursor.execute(merge_sql)
            rows_merged = cursor.rowcount
            logger.info(f"  Merged {rows_merged} rows into TC_TRACKS")

            # Drop staging table
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

        else:
            # Method 2: Direct INSERT (fastest, but will fail on duplicates)
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_upload,
                table_name="TC_TRACKS",
                auto_create_table=False,
                quote_identifiers=False
            )

            if not success:
                logger.error(f"  Failed to write to TC_TRACKS")
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
        return 0


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
                loaded = load_csv_to_snowflake_bulk(csv_file, conn, use_staging=True)
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