# Web Dashboard

Interactive Dash web application for visualizing ECMWF tropical cyclone forecast data from Snowflake.

## Overview

The dashboard provides:
- Interactive map with animated ensemble tracks
- Wind threshold envelope visualization
- Storm intensity charts (wind speed, pressure, intensity distribution)
- Filtering by forecast date, time, and storm

## Setup

### Environment Variables

The application requires Snowflake connection credentials:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
```

Or create a `.env` file (make sure it's in `.gitignore`):

```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Running Locally

```bash
# Install dependencies (if not already installed)
pip install -r requirements.txt

# Run the app
python web/app.py
```

The dashboard will be available at `http://localhost:10000`

## Features

- **Interactive Map**: Plotly-based map with animation controls
- **Ensemble Visualization**: See all 51 ensemble members' tracks
- **Wind Envelopes**: Visualize wind threshold polygons (34kt, 64kt, etc.)
- **Real-time Charts**: Wind speed, pressure, and intensity distribution over time
- **Filtering**: Filter by forecast date, run time, and specific storms

## Troubleshooting

### No data showing
- Verify Snowflake connection credentials
- Check that data has been loaded by the pipeline
- Verify database/schema names match

### Map not loading
- Check browser console for JavaScript errors
- Ensure Plotly.js is loading correctly
- Verify Mapbox/OpenStreetMap tile access

### Performance issues
- Large datasets may load slowly
- Consider filtering to specific date ranges
- Check Snowflake warehouse size/performance

