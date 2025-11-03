-- ============================================================================
-- 06: Create External Access Integration for ECMWF Downloads
-- ============================================================================
-- Enables stored procedures to make outbound HTTPS requests to ECMWF servers
-- Required for download_tc_bufr_files and download_wind_grib_files procedures
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AOTS_WH;
USE DATABASE AOTS;
USE SCHEMA ECMWF_PIPELINE;

-- ============================================================================
-- Create Network Rules for ECMWF Services
-- ============================================================================

-- Network Rule 1: ECMWF DISS System (for TC BUFR files)
CREATE OR REPLACE NETWORK RULE ecmwf_diss_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('essential.ecmwf.int')
  COMMENT = 'Allow access to ECMWF DISS system for downloading TC track BUFR files';

-- Network Rule 2: ECMWF Open Data (for wind GRIB2 files)
CREATE OR REPLACE NETWORK RULE ecmwf_opendata_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('data.ecmwf.int')
  COMMENT = 'Allow access to ECMWF Open Data services for downloading wind forecast GRIB2 files';

-- Network Rule 3: General web access (for any redirects or CDNs)
CREATE OR REPLACE NETWORK RULE general_https_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('*.ecmwf.int')
  COMMENT = 'Allow access to any ECMWF subdomain';

-- ============================================================================
-- Create External Access Integration
-- ============================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ecmwf_external_access
  ALLOWED_NETWORK_RULES = (
    ecmwf_diss_network_rule,
    ecmwf_opendata_network_rule,
    general_https_network_rule
  )
  ENABLED = TRUE
  COMMENT = 'External access integration for downloading ECMWF forecast data (TC tracks and wind)';

-- ============================================================================
-- Grant Network Rules and Integration to AOTS_ROLE
-- ============================================================================

-- Grant usage on network rules
GRANT USAGE ON NETWORK RULE ecmwf_diss_network_rule TO ROLE AOTS_ROLE;
GRANT USAGE ON NETWORK RULE ecmwf_opendata_network_rule TO ROLE AOTS_ROLE;
GRANT USAGE ON NETWORK RULE general_https_network_rule TO ROLE AOTS_ROLE;

-- Grant usage on external access integration
GRANT USAGE ON INTEGRATION ecmwf_external_access TO ROLE AOTS_ROLE;

-- Grant create procedure privilege if not already granted
GRANT CREATE PROCEDURE ON SCHEMA AOTS.ECMWF_PIPELINE TO ROLE AOTS_ROLE;

-- ============================================================================
-- Verify setup
-- ============================================================================

-- Show network rules
SHOW NETWORK RULES LIKE '%ecmwf%';

-- Show external access integrations
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE '%ecmwf%';

-- Describe the integration
DESC INTEGRATION ecmwf_external_access;

SELECT 
    '✓ External access integration created successfully' as status,
    'ecmwf_external_access' as integration_name,
    'Allows HTTPS requests to ECMWF servers' as purpose,
    'AOTS_ROLE has been granted usage on all network rules and integration' as grants_status,
    'Now update stored procedures to use this integration' as next_step;

