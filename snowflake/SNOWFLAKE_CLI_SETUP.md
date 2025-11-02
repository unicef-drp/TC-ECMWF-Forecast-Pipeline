# Snowflake CLI Setup

## Quick Setup

### 1. Install CLI

```bash
pip3 install snowflake-cli-labs
```

### 2. Create .env File

Create a `.env` file with your credentials:

```bash
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

### 3. Setup Connection (One Time)

```bash
./setup_connection_from_env.sh
```

This creates a connection with authentication only. Warehouse/database/schema are set using `USE` statements in your SQL files.

### 4. Use SQL Files

Add `USE` statements at the top of your SQL files:

```sql
USE WAREHOUSE SP_X86_WH;
USE DATABASE ECMWF_TC;
USE SCHEMA PIPELINE;

-- Your SQL statements here...
```

Then run:

```bash
snow sql -f create_database_schema.sql
snow sql -q "USE WAREHOUSE COMPUTE_WH; SELECT 1;"
```

