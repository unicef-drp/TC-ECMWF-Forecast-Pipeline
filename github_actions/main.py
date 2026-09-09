#!/usr/bin/env python3
"""
GitHub Actions entry point: sequential pipeline execution with password auth.

Orchestrates steps 1–7 from pipeline_core, then loads the results to Snowflake
using the github_actions/snowflake_loader (password-based connection).
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List

# Add parent directory to path to import core modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline_core import (
    BasePipelineConfig,
    PipelineStats,
    extract_tc_data_info,
    extract_tc_data_info_from_bufr,
    step1_download,
    step2_extract,
    step3_transform,
    step4_download_wind,
    step4b_download_gust,
    step5_process_wind,
    step5b_extract_gust_envelopes,
    step6_download_precip,
    cleanup_files,
)
from snowflake_loader import get_snowflake_connection, load_csv_to_snowflake, load_precip_metadata_to_snowflake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log'),
    ],
)
logger = logging.getLogger(__name__)


class PipelineConfig(BasePipelineConfig):
    """Configuration for the GitHub Actions pipeline (password auth)."""


def step7_load(config: PipelineConfig, stats: PipelineStats,
               transformed_files: List[Path], envelope_files: List[Path],
               precip_metadata: list, raster_files: List[Path] = None):
    """Step 7: Load all data. Real MERGE into Snowflake tables in SNOWFLAKE
    mode, plain CSV/raster upload to Blob in BLOB mode, or a no-op (files
    kept locally) in LOCAL mode.

    raster_files: wind/gust speed-field GeoTIFFs from step5_process_wind().
        Optional (defaults to none) so existing callers that haven't been
        updated yet keep working. Uploaded via upload_to_blob() in BLOB mode
        (own prefix per dataset) and via upload_to_snowflake_stage() in
        SNOWFLAKE mode, in each case as a plain additive PUT alongside the
        existing CSV MERGE logic below, never able to fail the run.
    """
    raster_files = raster_files or []
    logger.info("=" * 70)
    logger.info("STEP 7: Loading data to Snowflake...")
    logger.info("=" * 70)

    if config.data_pipeline_db == 'LOCAL':
        logger.info("DATA_PIPELINE_DB=LOCAL. Skipping Snowflake load, files kept locally")
        logger.info(f"  Transformed tracks : {config.transformed_data_dir}")
        logger.info(f"  Wind envelopes     : {config.wind_extracted_dir}")
        logger.info(f"  Wind rasters       : {len(raster_files)} file(s) in {config.wind_extracted_dir}")
        logger.info(f"  Met ZipStores      : {config.met_data_dir}")
        stats.files_loaded = len(transformed_files) + len(envelope_files) + len(raster_files)
        stats.rows_loaded = 0
        stats._local_mode = True
        return

    if config.data_pipeline_db == 'BLOB':
        # Uploads the same track/envelope CSVs the SNOWFLAKE branch would MERGE
        # directly, to the shared Blob container instead, under 'tracks/' and
        # 'envelopes/' at the container root, parallel to 'met/' and 'glofas/'.
        # Immediately below, the same files just uploaded are also synced into
        # TC_TRACKS/TC_ENVELOPES_COMBINED/TC_ENVELOPES_INDIVIDUAL/TC_GUST_ENVELOPES_*
        # via load_tracks_envelopes_from_blob() (blob_to_snowflake_loader.py), which
        # MERGEs them using the same load_csv_to_snowflake() the legacy SNOWFLAKE
        # branch below already uses. A separate periodic workflow
        # (sync-tracks-to-snowflake.yml) is the safety net for a cycle whose sync
        # step here is skipped (no Snowflake creds) or fails mid-run.
        logger.info("DATA_PIPELINE_DB=BLOB. Uploading CSVs to Blob, then syncing TC_TRACKS/TC_ENVELOPES_* below")
        from ecmwf_met_downloader import upload_to_blob
        uploaded = 0
        uploaded_blob_paths = []
        for csv_file in transformed_files:
            blob_path = f'tracks/{csv_file.name}'
            if upload_to_blob(csv_file, config.blob_account_url, config.blob_sas_token,
                               config.blob_container, blob_path):
                uploaded += 1
                uploaded_blob_paths.append(blob_path)
            else:
                stats.upload_failed_filenames.add(csv_file.name)
        for csv_file in envelope_files:
            blob_path = f'envelopes/{csv_file.name}'
            if upload_to_blob(csv_file, config.blob_account_url, config.blob_sas_token,
                               config.blob_container, blob_path):
                uploaded += 1
                uploaded_blob_paths.append(blob_path)
            else:
                stats.upload_failed_filenames.add(csv_file.name)
        # Wind/gust rasters: own prefix per dataset, parallel to tracks/envelopes.
        # No Snowflake pointer table needed (unlike MET_FORECASTS/RIVER_FORECASTS),
        # since these are cleanly file-per-(storm, forecast_time, lead_time), the
        # same pure-filename-parseable shape tracks/envelopes already use (see
        # get_storms_local()/get_tracks_local() in the DATAPIPELINE repo, so a
        # downstream BLOB-mode reader can list either prefix directly, no Snowflake
        # dependency needed to discover these files). Prefix is derived from the
        # filename's own `{dataset_label}_raster_` token (see
        # _save_wind_rasters_for_lead_time()'s own docstring in
        # ecmwf_tc_wind_combination.py), not a separate parameter, so wind and gust
        # rasters can arrive in the same raster_files list without the caller
        # having to keep them apart.
        raster_uploaded = 0
        for tif_file in raster_files:
            prefix = 'gust_raster' if 'gust_raster' in tif_file.name else 'wind_raster'
            if upload_to_blob(tif_file, config.blob_account_url, config.blob_sas_token,
                               config.blob_container, f'{prefix}/{tif_file.name}'):
                raster_uploaded += 1
            else:
                stats.upload_failed_filenames.add(tif_file.name)
        if raster_files and raster_uploaded < len(raster_files):
            error_msg = f"Only {raster_uploaded}/{len(raster_files)} wind/gust raster(s) uploaded to Blob successfully"
            logger.warning(error_msg)  # warning, not error: additive output, must not fail an otherwise-successful run
        elif raster_files:
            logger.info(f"Uploaded {raster_uploaded} wind/gust raster(s) to Blob (wind_raster//gust_raster/)")
        stats.files_loaded = uploaded + raster_uploaded
        stats.rows_loaded = 0
        stats._local_mode = True
        if uploaded < len(transformed_files) + len(envelope_files):
            error_msg = (f"Only {uploaded}/{len(transformed_files) + len(envelope_files)} "
                         f"track/envelope CSVs uploaded to Blob successfully")
            logger.error(error_msg)
            stats.errors.append(error_msg)

        # TC_TRACKS/TC_ENVELOPES_* Snowflake sync: tightest possible coupling to the
        # Blob upload just above. TC_ENVELOPES_COMBINED is the table DATAPIPELINE's
        # windgust job queries to discover new storms (existence/candidate-country
        # check only, not the detailed geometry, which already reads correctly from
        # Blob) and the dashboard reads TC_TRACKS/TC_ENVELOPES_COMBINED directly from
        # Snowflake for map rendering, and both depended, until this, entirely on the
        # separate legacy SNOWFLAKE-mode workflow keeping these tables fresh in
        # parallel. Loads via load_tracks_envelopes_from_blob() (blob_to_snowflake_
        # loader.py), which calls the SAME, unchanged load_csv_to_snowflake() the
        # legacy SNOWFLAKE-mode branch below already uses, scoped to exactly the
        # files uploaded above (not a broader Blob listing) so this step's cost
        # tracks this run's own output size, not the full container.
        # Best-effort and never fatal to this otherwise-successful Blob upload, same
        # reasoning as the MET_FORECASTS pointer-row write below: a credential-less
        # BLOB-only deployment logs a warning and skips it. The periodic
        # sync-tracks-to-snowflake.yml workflow (see that workflow's own header
        # comment) is the safety net that catches a skipped or failed sync here.
        if uploaded_blob_paths:
            have_snowflake_creds = all([
                config.sf_account, config.sf_user, config.sf_password,
                config.sf_warehouse, config.sf_database, config.sf_schema,
            ])
            if have_snowflake_creds:
                conn = None
                try:
                    os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
                    os.environ['SNOWFLAKE_USER'] = config.sf_user
                    os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
                    os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
                    os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
                    os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema
                    conn = get_snowflake_connection()
                    from blob_to_snowflake_loader import load_tracks_envelopes_from_blob
                    sync_result = load_tracks_envelopes_from_blob(
                        config.blob_account_url, config.blob_sas_token, config.blob_container,
                        conn=conn, blob_paths=uploaded_blob_paths, execute=True,
                    )
                    stats.rows_loaded += sum(rows for _, _, rows in sync_result['loaded'])
                    if sync_result['errors']:
                        # Logged loudly, but deliberately NOT added to stats.errors: this
                        # sync is a best-effort enhancement layered on an otherwise-
                        # successful Blob upload (the real, valuable output of this run),
                        # and sync-tracks-to-snowflake.yml exists specifically to catch a
                        # failure here. Escalating this into a whole-run failure would fail
                        # a multi-hour pipeline run over a downstream step with its own
                        # dedicated safety net.
                        error_msg = (f"{len(sync_result['errors'])}/{len(uploaded_blob_paths)} "
                                     f"file(s) failed to sync into TC_TRACKS/TC_ENVELOPES_* "
                                     f"(BLOB mode): {sync_result['errors']}")
                        logger.error(error_msg)
                    else:
                        logger.info(f"Synced {len(sync_result['loaded'])} file(s) into "
                                    f"TC_TRACKS/TC_ENVELOPES_* (BLOB mode)")
                except Exception as e:
                    # Same reasoning as above: loud, not fatal to this run.
                    error_msg = f"Could not sync tracks/envelopes into Snowflake in BLOB mode: {e}"
                    logger.error(error_msg)
                finally:
                    if conn is not None:
                        conn.close()
            else:
                logger.warning(
                    "No Snowflake credentials configured. TC_TRACKS/TC_ENVELOPES_* not "
                    "synced from this run's Blob upload; the tracks/envelopes CSVs are safely "
                    "in Blob, relying on the periodic sync-tracks-to-snowflake.yml safety net "
                    "(or the separate legacy SNOWFLAKE-mode workflow) to catch up later"
                )

        # MET_FORECASTS pointer rows: unlike tracks/envelopes (synced directly into
        # their real data tables just above), MET_FORECASTS is a pointer table --
        # a real Snowflake table that anything downstream queries to find where a
        # forecast cycle's precip/runoff Zarr actually lives. step6_download_precip
        # already uploads the Zarr itself to Blob and builds precip_metadata with the
        # real Blob stage_path regardless of mode; this was previously never written
        # here, so the file existed in Blob with nothing in Snowflake ever pointing at
        # it. Best-effort and never fatal to this otherwise-successful Blob upload: a
        # mixed-mode deployment (real Snowflake creds configured alongside Blob, the
        # case in this repo's actual .env) writes it for real; a genuinely
        # credential-less BLOB-only deployment logs a warning and skips it, matching
        # how the sibling RIVER_FORECASTS pointer write is treated as optional in a
        # fully-independent BLOB glofas config (see github_actions/glofas_pipeline.py).
        if precip_metadata:
            have_snowflake_creds = all([
                config.sf_account, config.sf_user, config.sf_password,
                config.sf_warehouse, config.sf_database, config.sf_schema,
            ])
            if have_snowflake_creds:
                conn = None
                try:
                    os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
                    os.environ['SNOWFLAKE_USER'] = config.sf_user
                    os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
                    os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
                    os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
                    os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema
                    conn = get_snowflake_connection()
                    rows = load_precip_metadata_to_snowflake(precip_metadata, conn)
                    stats.rows_loaded += rows
                    logger.info(f"Loaded {rows} metadata row(s) into MET_FORECASTS (BLOB mode)")
                except Exception as e:
                    error_msg = f"Could not write MET_FORECASTS pointer row(s) in BLOB mode: {e}"
                    logger.error(error_msg)
                    stats.errors.append(error_msg)
                finally:
                    if conn is not None:
                        conn.close()
            else:
                logger.warning(
                    "No Snowflake credentials configured. MET_FORECASTS pointer row(s) not "
                    "written; the precip/runoff data is safely in Blob, but nothing in "
                    "Snowflake records where it is until a MET_FORECASTS row is written "
                    "separately"
                )
        return

    try:
        os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
        os.environ['SNOWFLAKE_USER'] = config.sf_user
        os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
        os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
        os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
        os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema

        conn = get_snowflake_connection()
        try:
            total_rows = 0

            def _load_and_track(csv_file, table_type):
                """load_csv_to_snowflake wrapper that tells a real failure
                (None) apart from a legitimate empty file (0): a None means
                the load itself broke and must be recorded as a real error,
                not silently treated as zero rows loaded."""
                nonlocal total_rows
                rows = load_csv_to_snowflake(csv_file, conn, table_type=table_type)
                if rows is None:
                    error_msg = f"Failed to load {csv_file.name} into {table_type}. See error above"
                    logger.error(error_msg)
                    stats.errors.append(error_msg)
                    stats.upload_failed_filenames.add(csv_file.name)
                    return
                total_rows += rows

            for csv_file in transformed_files:
                _load_and_track(csv_file, 'TC_TRACKS')

            for csv_file in envelope_files:
                if 'gust_envelopes_individual' in csv_file.name:
                    table_type = 'TC_GUST_ENVELOPES_INDIVIDUAL'
                elif 'gust_envelopes_combined' in csv_file.name:
                    table_type = 'TC_GUST_ENVELOPES_COMBINED'
                elif 'individual' in csv_file.name:
                    table_type = 'TC_ENVELOPES_INDIVIDUAL'
                elif 'combined' in csv_file.name:
                    table_type = 'TC_ENVELOPES_COMBINED'
                else:
                    logger.warning(f"Unknown envelope file type: {csv_file.name}")
                    continue
                _load_and_track(csv_file, table_type)

            if precip_metadata:
                total_rows += load_precip_metadata_to_snowflake(precip_metadata, conn)

            # Wind/gust rasters. SNOWFLAKE mode had no generic binary-file-to-stage
            # upload path in this function until now. Reuses upload_to_snowflake_stage() (already used
            # for precip's own Zarr uploads elsewhere in this pipeline, ecmwf_met_
            # downloader.py), a real PUT to config.snowflake_stage_name, same
            # wind_raster//gust_raster/ prefix convention as the BLOB branch above
            # (derived from the filename's own `{dataset_label}_raster_` token). No
            # Snowflake pointer-table row: same reasoning as BLOB mode, these files
            # are cleanly file-per-(storm, forecast_time, lead_time) and
            # pure-filename-parseable, a downstream SNOWFLAKE-mode reader can LIST
            # the stage prefix directly.
            raster_uploaded = 0
            if raster_files and config.snowflake_stage_name:
                from ecmwf_met_downloader import upload_to_snowflake_stage
                for tif_file in raster_files:
                    prefix = 'gust_raster' if 'gust_raster' in tif_file.name else 'wind_raster'
                    if upload_to_snowflake_stage(tif_file, config.snowflake_stage_name,
                                                  f'{prefix}/{tif_file.name}', conn):
                        raster_uploaded += 1
                    else:
                        stats.upload_failed_filenames.add(tif_file.name)
                if raster_uploaded < len(raster_files):
                    logger.warning(f"Only {raster_uploaded}/{len(raster_files)} wind/gust raster(s) "
                                    f"uploaded to Snowflake stage successfully")
                else:
                    logger.info(f"Uploaded {raster_uploaded} wind/gust raster(s) to Snowflake stage "
                                f"(wind_raster//gust_raster/)")
            elif raster_files:
                logger.warning("SNOWFLAKE_STAGE_NAME not configured, wind/gust raster(s) not "
                                "uploaded to Snowflake stage")
                stats.upload_failed_filenames.update(f.name for f in raster_files)

            def _safe_table_count(cursor, table_name):
                """COUNT(*) for a diagnostic-only summary log line. Returns
                None (logged as 'unavailable') instead of raising on failure:
                this is a visibility check for the newer gust/precip
                tables, not load-bearing: a failure here (missing table,
                transient permission/network issue) must never fail the
                whole run or mask the wind/track data that already loaded
                and committed successfully earlier in step7_load."""
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    return cursor.fetchone()[0]
                except Exception as e:
                    logger.warning(f"Could not read diagnostic count for {table_name}: {e}")
                    return None

            def _fmt_count(n):
                return f"{n:,}" if n is not None else "unavailable"

            cursor = conn.cursor()
            try:
                # TC_TRACKS/TC_ENVELOPES_* are the core wind/gust discovery
                # and dashboard rendering input, so a failure reading their
                # own counts stays a hard error (matches this block's
                # original behavior). Only the newer gust/precip tables get
                # the fault-tolerant treatment.
                cursor.execute("SELECT COUNT(*) FROM TC_TRACKS")
                tracks_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_INDIVIDUAL")
                individual_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM TC_ENVELOPES_COMBINED")
                combined_count = cursor.fetchone()[0]
                gust_individual_count = _safe_table_count(cursor, "TC_GUST_ENVELOPES_INDIVIDUAL")
                gust_combined_count = _safe_table_count(cursor, "TC_GUST_ENVELOPES_COMBINED")
                met_count = _safe_table_count(cursor, "MET_FORECASTS")
            finally:
                cursor.close()

            logger.info("Total records in database:")
            logger.info(f"  TC_TRACKS:                     {tracks_count:,}")
            logger.info(f"  TC_ENVELOPES_INDIVIDUAL:       {individual_count:,}")
            logger.info(f"  TC_ENVELOPES_COMBINED:         {combined_count:,}")
            logger.info(f"  TC_GUST_ENVELOPES_INDIVIDUAL:  {_fmt_count(gust_individual_count)}")
            logger.info(f"  TC_GUST_ENVELOPES_COMBINED:    {_fmt_count(gust_combined_count)}")
            logger.info(f"  MET_FORECASTS:                 {_fmt_count(met_count)}")

            stats.files_loaded = len(transformed_files) + len(envelope_files) + raster_uploaded
            stats.rows_loaded = total_rows
            logger.info(f"Loaded {total_rows:,} rows from {stats.files_loaded} files")

        finally:
            conn.close()
            logger.info("Snowflake connection closed")

    except Exception as e:
        error_msg = f"Snowflake load failed: {e}"
        logger.error(error_msg)
        stats.errors.append(error_msg)
        raise


def main():
    """Run the sequential pipeline."""
    logger.info("=" * 70)
    logger.info("ECMWF TC FORECAST PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Start: {datetime.now().isoformat()}")

    config = PipelineConfig()
    stats = PipelineStats()

    if not config.validate():
        logger.error("Configuration validation failed. Exiting.")
        sys.exit(1)

    config.create_directories()
    logger.info(
        f"Dirs: raw={config.raw_data_dir}, "
        f"transformed={config.transformed_data_dir}, "
        f"wind={config.wind_data_dir}, "
        f"envelopes={config.wind_extracted_dir}"
    )
    logger.info(f"Snowflake: {config.sf_database}.{config.sf_schema}")
    logger.info(f"Wind processing: {config.process_wind_data}")
    logger.info(f"Gust processing: {config.process_gust}")
    logger.info(f"Met (precip/runoff) processing: {config.process_met}")
    if config.download_date:
        logger.info(f"Download date: {config.download_date} {config.run_time or 'any'}Z")
    else:
        logger.info(f"Download mode: latest {config.download_limit} forecast(s)")

    try:
        bufr_files = step1_download(config, stats)
        if not bufr_files:
            logger.warning("No BUFR files to process. Exiting.")
            sys.exit(0)

        csv_files = step2_extract(config, stats, bufr_files)
        if not csv_files:
            logger.warning("No named storms found in BUFR data. Skipping wind processing.")
            if config.process_met:
                logger.info("PROCESS_MET=true. Running met download anyway.")
                tc_data_info = extract_tc_data_info_from_bufr(bufr_files)
                _precip_conn = None
                if config.data_pipeline_db == 'SNOWFLAKE':
                    os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
                    os.environ['SNOWFLAKE_USER'] = config.sf_user
                    os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
                    os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
                    os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
                    os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema
                    _precip_conn = get_snowflake_connection()
                try:
                    precip_metadata = step6_download_precip(config, stats, tc_data_info,
                                                             snowflake_conn=_precip_conn)
                finally:
                    if _precip_conn:
                        _precip_conn.close()
                step7_load(config, stats, [], [], precip_metadata)
                cleanup_files(config, stats.upload_failed_filenames)
            else:
                # step7_load() is what normally sets stats._local_mode (for
                # the summary log's own label below, True for LOCAL and BLOB
                # alike, only SNOWFLAKE leaves it False); PROCESS_MET=false
                # skips that call entirely on this no-named-storms path, so
                # set it directly here too. In an `else` specifically, not
                # unconditionally: the PROCESS_MET=true branch above already
                # ran step7_load() for real and set this correctly (True for
                # BLOB too), an unconditional assignment here would silently
                # overwrite that back to the wrong value for BLOB mode.
                stats._local_mode = config.data_pipeline_db in ('LOCAL', 'BLOB')
            stats.log_summary()
            sys.exit(1 if stats.errors else 0)

        transformed_files = step3_transform(config, stats, csv_files)

        tc_data_info = extract_tc_data_info(csv_files)
        logger.info(f"TC data info: {tc_data_info}")

        step4_download_wind(config, stats, tc_data_info)
        step4b_download_gust(config, stats, tc_data_info)
        envelope_files, raster_files = step5_process_wind(config, stats)
        gust_files, gust_raster_files = step5b_extract_gust_envelopes(config, stats)

        # Open a connection for the precip stage PUT (SNOWFLAKE mode only)
        _precip_conn = None
        if config.data_pipeline_db == 'SNOWFLAKE':
            os.environ['SNOWFLAKE_ACCOUNT'] = config.sf_account
            os.environ['SNOWFLAKE_USER'] = config.sf_user
            os.environ['SNOWFLAKE_PASSWORD'] = config.sf_password
            os.environ['SNOWFLAKE_WAREHOUSE'] = config.sf_warehouse
            os.environ['SNOWFLAKE_DATABASE'] = config.sf_database
            os.environ['SNOWFLAKE_SCHEMA'] = config.sf_schema
            _precip_conn = get_snowflake_connection()

        try:
            precip_metadata = step6_download_precip(config, stats, tc_data_info,
                                                     snowflake_conn=_precip_conn)
        finally:
            if _precip_conn:
                _precip_conn.close()

        step7_load(config, stats, transformed_files, envelope_files + gust_files, precip_metadata,
                   raster_files=raster_files + gust_raster_files)

        cleanup_files(config, stats.upload_failed_filenames)
        stats.log_summary()

        if stats.errors:
            logger.warning("Pipeline completed with errors")
            sys.exit(1)
        else:
            logger.info("Pipeline completed successfully!")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        stats.log_summary()
        sys.exit(1)


if __name__ == "__main__":
    main()
