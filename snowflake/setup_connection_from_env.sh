#!/bin/bash

# ============================================================================
# Setup Snowflake CLI Connection from .env file
# ============================================================================
# Loads credentials from .env and creates Snowflake CLI connection
# ============================================================================

set -e

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "════════════════════════════════════════════════════════════════"
echo "  Snowflake CLI Connection Setup from .env"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check if .env file exists
ENV_FILE=".env"
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}✗ .env file not found!${NC}"
    echo ""
    echo "Create .env with:"
    echo "  SNOWFLAKE_ACCOUNT=your_account"
    echo "  SNOWFLAKE_USER=your_user"
    echo "  SNOWFLAKE_PASSWORD=your_password"
    exit 1
fi

echo -e "${GREEN}✓ Found .env file${NC}"
echo ""

# Load .env file
echo -e "${BLUE}Loading credentials from .env...${NC}"
export $(grep -v '^#' .env | grep -v '^$' | xargs)
echo ""

# Check required variables
REQUIRED_VARS=("SNOWFLAKE_ACCOUNT" "SNOWFLAKE_USER" "SNOWFLAKE_PASSWORD")
MISSING_VARS=()

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
    echo -e "${RED}✗ Missing required variables in .env:${NC}"
    for var in "${MISSING_VARS[@]}"; do
        echo "  - $var"
    done
    exit 1
fi

echo -e "${GREEN}✓ All required credentials found${NC}"
echo ""

# Connection name (default to 'default' or use environment variable)
CONNECTION_NAME=${SNOWFLAKE_CONNECTION_NAME:-"default"}

echo -e "${BLUE}Setting up connection: ${CONNECTION_NAME}${NC}"

# Check if connection already exists
CONNECTION_EXISTS=$(snow connection list 2>&1 | grep -i "$CONNECTION_NAME" || true)

if [ ! -z "$CONNECTION_EXISTS" ] && ! echo "$CONNECTION_EXISTS" | grep -q "No data"; then
    echo -e "${YELLOW}⚠ Connection '$CONNECTION_NAME' already exists${NC}"
    read -p "Do you want to remove and recreate it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        snow connection remove "$CONNECTION_NAME" 2>/dev/null || true
        echo -e "${GREEN}✓ Removed existing connection${NC}"
    else
        echo "Keeping existing connection. Test it with: snow connection test"
        exit 0
    fi
fi

# Build connection add command with all parameters
echo ""
echo -e "${BLUE}Creating connection...${NC}"

# Create connection with only authentication (warehouse/database/schema set via USE in SQL)
CONNECTION_CMD="snow connection add --no-interactive --connection-name $CONNECTION_NAME"
CONNECTION_CMD="$CONNECTION_CMD --account ${SNOWFLAKE_ACCOUNT}"
CONNECTION_CMD="$CONNECTION_CMD --user ${SNOWFLAKE_USER}"
CONNECTION_CMD="$CONNECTION_CMD --password ${SNOWFLAKE_PASSWORD}"
CONNECTION_CMD="$CONNECTION_CMD --default"

# Execute the command
if eval "$CONNECTION_CMD"; then
    echo -e "${GREEN}✓ Connection '$CONNECTION_NAME' created successfully!${NC}"
    echo ""
    
    # Test the connection
    echo -e "${BLUE}Testing connection...${NC}"
    if snow connection test; then
        echo -e "${GREEN}✓ Connection test passed!${NC}"
        echo ""
        echo "Your Snowflake CLI is ready to use!"
        echo ""
        echo "Usage:"
        echo "  snow sql -f your_file.sql"
        echo "  snow sql -q \"SELECT 1;\""
    else
        echo -e "${YELLOW}⚠ Connection test failed, but connection was created.${NC}"
        echo "Check your credentials in .env file"
    fi
else
    echo -e "${RED}✗ Failed to create connection${NC}"
    exit 1
fi
