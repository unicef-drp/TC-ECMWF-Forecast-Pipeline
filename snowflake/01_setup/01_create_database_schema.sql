-- ============================================================================
-- 01: Create Database and Schema for TC-ECMWF Forecast Pipeline
-- ============================================================================

-- Use existing role and warehouse
USE ROLE SYSADMIN;
USE WAREHOUSE AOTS_WH;

-- Create database for TC-ECMWF pipeline
CREATE DATABASE IF NOT EXISTS AOTS
    COMMENT = 'Database for TC-ECMWF Forecast Pipeline - Tropical cyclone forecasts and wind envelopes';

-- Create schema for pipeline objects
CREATE SCHEMA IF NOT EXISTS AOTS.ECMWF_PIPELINE
    COMMENT = 'Main schema for TC forecast pipeline - contains stages, tables, UDFs, and procedures';

-- Use the new schema
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- Verify setup
SELECT 
    CURRENT_ROLE() as current_role,
    CURRENT_WAREHOUSE() as current_warehouse,
    CURRENT_DATABASE() as current_database,
    CURRENT_SCHEMA() as current_schema;

-- Display success message
SELECT 
    '✓ Database and schema created successfully' as status,
    'AOTS.ECMWF_PIPELINE' as full_schema_name,
    'Ready for stages and tables' as next_step;

