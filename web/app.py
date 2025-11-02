#!/usr/bin/env python3
"""
ECMWF TC Forecast Visualization Dashboard
Using Plotly's built-in animation features
"""

import os
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State
import snowflake.connector
import numpy as np
from shapely import wkt


# Snowflake connection
def get_snowflake_data():
    """Connect to Snowflake and fetch TC forecast data"""
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    metadata_query = """
                     SELECT DISTINCT TRACK_ID, \
                                     FORECAST_TIME, \
                                     COUNT(DISTINCT ENSEMBLE_MEMBER) as ensemble_count, \
                                     MAX(LEAD_TIME)                  as max_lead_time
                     FROM TC_TRACKS
                     GROUP BY TRACK_ID, FORECAST_TIME
                     ORDER BY FORECAST_TIME DESC, TRACK_ID \
                     """

    cursor = conn.cursor()
    cursor.execute(metadata_query)
    metadata_df = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()

    return metadata_df


def get_forecast_data(track_id, forecast_time):
    """Get forecast data for a specific storm and forecast time"""
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    query = f"""
    SELECT 
        ENSEMBLE_MEMBER,
        LEAD_TIME,
        VALID_TIME,
        LATITUDE,
        LONGITUDE,
        WIND_SPEED_KNOTS,
        PRESSURE_HPA
    FROM TC_TRACKS
    WHERE TRACK_ID = '{track_id}'
      AND FORECAST_TIME = '{forecast_time}'
    ORDER BY ENSEMBLE_MEMBER, LEAD_TIME
    """

    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()

    return df


def get_combined_envelopes(track_id, forecast_time):
    """Get combined wind envelopes for a specific storm and forecast time"""
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    query = f"""
    SELECT 
        ENSEMBLE_MEMBER,
        LEAD_TIME_RANGE,
        WIND_THRESHOLD,
        ST_ASWKT(ENVELOPE_REGION) AS ENVELOPE_REGION
    FROM TC_ENVELOPES_COMBINED
    WHERE TRACK_ID = '{track_id}'
      AND FORECAST_TIME = '{forecast_time}'
    ORDER BY ENSEMBLE_MEMBER, LEAD_TIME_RANGE, WIND_THRESHOLD
    """

    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()

    return df


# Initialize Dash app with optimizations
app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    update_title=None  # Prevent title updates for better performance
)
app.title = "TC Forecast Viewer"

# Load initial metadata
metadata_df = get_snowflake_data()

# Parse dates and times from metadata
metadata_df['DATE'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.date
metadata_df['TIME'] = pd.to_datetime(metadata_df['FORECAST_TIME']).dt.strftime('%H:%M')

# Get unique dates and times
unique_dates = sorted(metadata_df['DATE'].unique(), reverse=True)
unique_times = sorted(metadata_df['TIME'].unique())

# App layout
app.layout = html.Div([
    # Header
    html.Div([
        html.Div([
            html.H1("Tropical Cyclone Ensemble Forecasts",
                    style={'margin': 0, 'fontSize': '28px', 'fontWeight': '600', 'color': '#1a1a1a'}),
            html.P("ECMWF Ensemble Prediction System",
                   style={'margin': '5px 0 0 0', 'fontSize': '14px', 'color': '#666'})
        ], style={'flex': '1'}),
        html.Button("Charts ▶", id='sidebar-toggle', n_clicks=0,
                    style={
                        'padding': '10px 20px',
                        'backgroundColor': '#3b82f6',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '6px',
                        'cursor': 'pointer',
                        'fontSize': '14px',
                        'fontWeight': '500'
                    })
    ], style={
        'display': 'flex',
        'alignItems': 'center',
        'padding': '24px 32px',
        'backgroundColor': 'white',
        'borderBottom': '1px solid #e0e0e0'
    }),

    # Main content area
    html.Div([
        # Map area
        html.Div([
            # Controls
            html.Div([
                html.Div([
                    html.Label("Forecast Date",
                               style={'fontSize': '13px', 'fontWeight': '500', 'color': '#444', 'marginBottom': '8px'}),
                    dcc.Dropdown(
                        id='date-dropdown',
                        options=[{'label': str(d), 'value': str(d)} for d in unique_dates],
                        value=str(unique_dates[0]) if len(unique_dates) > 0 else None,
                        clearable=False,
                        style={'fontSize': '14px'}
                    ),
                ], style={'flex': '1', 'minWidth': '180px'}),

                html.Div([
                    html.Label("Forecast Time (UTC)",
                               style={'fontSize': '13px', 'fontWeight': '500', 'color': '#444', 'marginBottom': '8px'}),
                    dcc.Dropdown(
                        id='time-dropdown',
                        options=[],
                        clearable=False,
                        style={'fontSize': '14px'}
                    ),
                ], style={'flex': '1', 'minWidth': '140px'}),

                html.Div([
                    html.Label("Storm",
                               style={'fontSize': '13px', 'fontWeight': '500', 'color': '#444', 'marginBottom': '8px'}),
                    dcc.Dropdown(
                        id='storm-dropdown',
                        options=[],
                        clearable=False,
                        style={'fontSize': '14px'}
                    ),
                ], style={'flex': '1', 'minWidth': '180px'}),

                html.Div([
                    html.Label("Ensemble Opacity",
                               style={'fontSize': '13px', 'fontWeight': '500', 'color': '#444', 'marginBottom': '8px'}),
                    dcc.Slider(
                        id='opacity-slider',
                        min=0.1,
                        max=1.0,
                        step=0.1,
                        value=0.4,
                        marks={0.1: '10%', 0.5: '50%', 1.0: '100%'},
                        tooltip={"placement": "bottom"}
                    ),
                ], style={'flex': '1', 'minWidth': '200px'}),

                html.Div([
                    html.Label("Wind Envelopes",
                               style={'fontSize': '13px', 'fontWeight': '500', 'color': '#444', 'marginBottom': '8px'}),
                    dcc.RadioItems(
                        id='envelope-toggle',
                        options=[
                            {'label': 'None', 'value': 'none'},
                            {'label': 'Combined', 'value': 'combined'}
                        ],
                        value='none',
                        style={'fontSize': '14px'},
                        inputStyle={'marginRight': '8px'}
                    ),
                ], style={'flex': '1', 'minWidth': '200px'}),

                html.Div([
                    html.Label("Wind Thresholds",
                               style={'fontSize': '13px', 'fontWeight': '500', 'color': '#444', 'marginBottom': '8px'}),
                    dcc.Checklist(
                        id='threshold-filter',
                        options=[
                            {'label': html.Span(
                                ['34kt ', html.Span('●', style={'color': '#FFD700', 'fontSize': '16px'})]),
                             'value': 34},
                            {'label': html.Span(
                                ['40kt ', html.Span('●', style={'color': '#FFA500', 'fontSize': '16px'})]),
                             'value': 40},
                            {'label': html.Span(
                                ['50kt ', html.Span('●', style={'color': '#FF8C00', 'fontSize': '16px'})]),
                             'value': 50},
                            {'label': html.Span(
                                ['64kt ', html.Span('●', style={'color': '#FF0000', 'fontSize': '16px'})]), 'value': 64}
                        ],
                        value=[],  # Empty by default - will be populated by callback
                        style={'fontSize': '14px'},
                        inputStyle={'marginRight': '8px'}
                    ),
                    html.Div(id='threshold-note', style={'fontSize': '12px', 'color': '#666', 'marginTop': '6px'}),
                ], style={'flex': '1', 'minWidth': '250px'}),
            ], style={
                'display': 'flex',
                'gap': '20px',
                'padding': '20px 32px',
                'backgroundColor': 'white',
                'borderBottom': '1px solid #e0e0e0',
                'flexWrap': 'wrap',
                'zIndex': 1000,
                'position': 'relative'
            }),

            # Map with built-in animation
            dcc.Graph(
                id='tc-map',
                style={'height': 'calc(100vh - 220px)', 'width': '100%'},
                config={
                    'displayModeBar': True,
                    'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                    'displaylogo': False,
                    'scrollZoom': True
                }
            ),
        ], style={'flex': '1', 'display': 'flex', 'flexDirection': 'column', 'width': '100%', 'overflow': 'hidden'}),

        # Sidebar
        html.Div([
            html.Div([
                html.Div([
                    html.H3(id='sidebar-title', style={'margin': '0 0 20px 0', 'fontSize': '20px', 'fontWeight': '600',
                                                       'color': '#1a1a1a'}),

                    # Wind speed chart
                    html.Div([
                        html.H4("Max Wind Speed (knots)",
                                style={'fontSize': '14px', 'fontWeight': '600', 'color': '#3b82f6',
                                       'marginBottom': '12px'}),
                        dcc.Graph(id='wind-chart', config={'displayModeBar': False}, style={'height': '220px'})
                    ], style={'marginBottom': '30px'}),

                    # Intensity distribution
                    html.Div([
                        html.H4("Intensity Distribution",
                                style={'fontSize': '14px', 'fontWeight': '600', 'color': '#3b82f6',
                                       'marginBottom': '12px'}),
                        dcc.Graph(id='intensity-chart', config={'displayModeBar': False}, style={'height': '220px'})
                    ], style={'marginBottom': '30px'}),

                    # Pressure chart
                    html.Div([
                        html.H4("Sea Level Pressure (hPa)",
                                style={'fontSize': '14px', 'fontWeight': '600', 'color': '#3b82f6',
                                       'marginBottom': '12px'}),
                        dcc.Graph(id='pressure-chart', config={'displayModeBar': False}, style={'height': '220px'})
                    ], style={'marginBottom': '20px'}),

                ], style={'padding': '24px'})
            ], style={
                'height': 'calc(100vh - 100px)',
                'overflowY': 'auto',
                'overflowX': 'hidden',
                'backgroundColor': 'white'
            })
        ], id='sidebar', style={
            'width': '0',
            'minWidth': '0',
            'maxWidth': '450px',
            'transition': 'all 0.3s ease',
            'overflow': 'hidden',
            'borderLeft': '1px solid #e0e0e0',
            'height': 'calc(100vh - 100px)'
        }),
    ], style={'display': 'flex', 'height': 'calc(100vh - 100px)', 'width': '100%', 'overflow': 'hidden'}),

    dcc.Store(id='forecast-data-store'),
    dcc.Store(id='sidebar-open', data=False),
    dcc.Store(id='metadata-store', data=metadata_df.to_dict('records'))

], style={
    'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
    'backgroundColor': '#f8f9fa',
    'margin': 0,
    'padding': 0,
    'height': '100vh',
    'width': '100vw',
    'overflow': 'hidden'
})


@app.callback(
    Output('sidebar', 'style'),
    Output('sidebar-toggle', 'children'),
    Output('sidebar-open', 'data'),
    Input('sidebar-toggle', 'n_clicks'),
    State('sidebar-open', 'data')
)
def toggle_sidebar(n_clicks, is_open):
    """Toggle sidebar visibility"""
    if n_clicks > 0:
        is_open = not is_open

    if is_open:
        style = {
            'width': '450px',
            'minWidth': '450px',
            'maxWidth': '450px',
            'transition': 'all 0.3s ease',
            'overflow': 'hidden',
            'borderLeft': '1px solid #e0e0e0',
            'height': 'calc(100vh - 100px)'
        }
        button_text = "◀ Close"
    else:
        style = {
            'width': '0',
            'minWidth': '0',
            'maxWidth': '450px',
            'transition': 'all 0.3s ease',
            'overflow': 'hidden',
            'borderLeft': '1px solid #e0e0e0',
            'height': 'calc(100vh - 100px)'
        }
        button_text = "Charts ▶"

    return style, button_text, is_open


@app.callback(
    Output('time-dropdown', 'options'),
    Output('time-dropdown', 'value'),
    Input('date-dropdown', 'value'),
    State('metadata-store', 'data')
)
def update_time_dropdown(selected_date, metadata):
    """Update available forecast times for selected date"""
    if not selected_date or not metadata:
        return [], None

    df = pd.DataFrame(metadata)
    df['DATE'] = pd.to_datetime(df['FORECAST_TIME']).dt.date.astype(str)
    df['TIME'] = pd.to_datetime(df['FORECAST_TIME']).dt.strftime('%H:%M')

    # Filter for selected date
    filtered = df[df['DATE'] == selected_date]
    available_times = sorted(filtered['TIME'].unique())

    options = [{'label': f"{t} UTC", 'value': t} for t in available_times]
    default_value = available_times[0] if available_times else None

    return options, default_value


@app.callback(
    Output('storm-dropdown', 'options'),
    Output('storm-dropdown', 'value'),
    Input('date-dropdown', 'value'),
    Input('time-dropdown', 'value'),
    State('metadata-store', 'data')
)
def update_storm_dropdown(selected_date, selected_time, metadata):
    """Update available storms for selected date and time"""
    if not selected_date or not selected_time or not metadata:
        return [], None

    df = pd.DataFrame(metadata)
    df['DATE'] = pd.to_datetime(df['FORECAST_TIME']).dt.date.astype(str)
    df['TIME'] = pd.to_datetime(df['FORECAST_TIME']).dt.strftime('%H:%M')

    # Filter for selected date and time
    filtered = df[(df['DATE'] == selected_date) & (df['TIME'] == selected_time)]
    available_storms = sorted(filtered['TRACK_ID'].unique())

    options = [{'label': storm, 'value': storm} for storm in available_storms]
    default_value = available_storms[0] if available_storms else None

    return options, default_value


@app.callback(
    Output('forecast-data-store', 'data'),
    Input('date-dropdown', 'value'),
    Input('time-dropdown', 'value'),
    Input('storm-dropdown', 'value'),
    State('metadata-store', 'data')
)
def load_forecast_data(selected_date, selected_time, storm_id, metadata):
    """Load forecast data when selections are made"""
    if not selected_date or not selected_time or not storm_id or not metadata:
        return {}

    df = pd.DataFrame(metadata)
    df['DATE'] = pd.to_datetime(df['FORECAST_TIME']).dt.date.astype(str)
    df['TIME'] = pd.to_datetime(df['FORECAST_TIME']).dt.strftime('%H:%M')

    # Find the matching forecast time
    filtered = df[(df['DATE'] == selected_date) &
                  (df['TIME'] == selected_time) &
                  (df['TRACK_ID'] == storm_id)]

    if len(filtered) == 0:
        return {}

    forecast_time = filtered.iloc[0]['FORECAST_TIME']

    # Get the forecast data
    forecast_df = get_forecast_data(storm_id, forecast_time)

    # Get combined envelope data
    combined_envelopes_df = get_combined_envelopes(storm_id, forecast_time)

    return {
        'track_id': storm_id,
        'forecast_time': forecast_time,
        'data': forecast_df.to_dict('records'),
        'combined_envelopes': combined_envelopes_df.to_dict('records')
    }


@app.callback(
    Output('sidebar-title', 'children'),
    Input('forecast-data-store', 'data')
)
def update_sidebar_title(stored_data):
    """Update sidebar title with storm name"""
    if not stored_data:
        return "Storm Data"
    return f"Storm: {stored_data.get('track_id', 'Unknown')}"


@app.callback(
    [Output('threshold-filter', 'options'),
     Output('threshold-filter', 'value'),
     Output('threshold-note', 'children'),
     Output('envelope-toggle', 'options'),
     Output('envelope-toggle', 'value')],
    Input('forecast-data-store', 'data')
)
def update_threshold_options(stored_data):
    """Dynamically update threshold filter options and envelope toggle based on available data"""
    has_envelopes = stored_data and stored_data.get('combined_envelopes') and len(stored_data['combined_envelopes']) > 0

    if not has_envelopes:
        # No options when no data - empty list and informative note
        note_text = "No wind envelopes available for the current selection."
        envelope_options = [
            {'label': 'None', 'value': 'none'},
            {'label': 'Combined (Not Available)', 'value': 'combined', 'disabled': True}
        ]
        return [], [], note_text, envelope_options, 'none'

    # Get available thresholds from the data
    available_thresholds = set()
    for envelope in stored_data['combined_envelopes']:
        available_thresholds.add(envelope['WIND_THRESHOLD'])

    # Color mapping for visual indicators
    color_map = {
        34: '#FFD700',  # Gold/Yellow
        40: '#FFA500',  # Orange
        50: '#FF8C00',  # Dark Orange
        64: '#FF0000',  # Red
        74: '#CC0000',  # Dark Red (Category 3)
        96: '#990000',  # Darker Red (Category 4)
        113: '#660000'  # Darkest Red (Category 5)
    }

    # Create options for available thresholds
    options = []
    selected_values = []
    for threshold in sorted(available_thresholds):
        color = color_map.get(threshold, '#808080')  # Gray for unknown thresholds
        options.append({
            'label': html.Span([f'{threshold}kt ', html.Span('●', style={'color': color, 'fontSize': '16px'})]),
            'value': threshold
        })
        selected_values.append(threshold)  # Select all available thresholds by default

    # When envelopes are available, show normal options
    envelope_options = [
        {'label': 'None', 'value': 'none'},
        {'label': 'Combined', 'value': 'combined'}
    ]

    return options, selected_values, None, envelope_options, 'none'


@app.callback(
    Output('tc-map', 'figure'),
    Input('forecast-data-store', 'data'),
    Input('opacity-slider', 'value'),
    Input('envelope-toggle', 'value'),
    Input('threshold-filter', 'value')
)
def update_map(stored_data, opacity, envelope_mode, selected_thresholds):
    """Update map with TC tracks and wind envelopes using Plotly animation frames and Mapbox"""
    if not stored_data or not stored_data.get('data'):
        return go.Figure()

    df = pd.DataFrame(stored_data['data'])
    track_id = stored_data['track_id']

    # Get combined envelope data
    combined_envelopes = stored_data.get('combined_envelopes', [])

    # Get all unique time steps
    time_steps = sorted(df['LEAD_TIME'].unique())

    # Calculate center for map
    center_lat = df['LATITUDE'].mean()
    center_lon = df['LONGITUDE'].mean()

    # Create frames for animation - all members progress together
    frames = []

    for time_step in time_steps:
        frame_data = []

        # For each member, show track up to current time_step
        for member in sorted(df['ENSEMBLE_MEMBER'].unique()):
            member_data = df[df['ENSEMBLE_MEMBER'] == member].sort_values('LEAD_TIME')
            member_current = member_data[member_data['LEAD_TIME'] <= time_step]

            if len(member_current) > 0:
                # Determine color - highlight control members 51 and 52
                if member in [51, 52]:
                    color = f'rgba(220, 38, 38, {opacity})'  # Red for control members
                    width = 1.5
                else:
                    color = f'rgba(59, 130, 246, {opacity})'  # Blue for ensemble members
                    width = 1

                # === ADD DATELINE CROSSING DETECTION ===
                lons = member_current['LONGITUDE'].values
                lats = member_current['LATITUDE'].values
                lead_times = member_current['LEAD_TIME'].values

                # Split track at dateline crossings
                segments = []
                current_lons = [lons[0]]
                current_lats = [lats[0]]
                current_times = [lead_times[0]]

                for i in range(1, len(lons)):
                    if abs(lons[i] - lons[i - 1]) > 180:
                        # Dateline crossing - save current segment and start new one
                        segments.append((current_lons, current_lats, current_times))
                        current_lons = [lons[i]]
                        current_lats = [lats[i]]
                        current_times = [lead_times[i]]
                    else:
                        current_lons.append(lons[i])
                        current_lats.append(lats[i])
                        current_times.append(lead_times[i])

                segments.append((current_lons, current_lats, current_times))

                # Add each segment as a separate trace
                for seg_lons, seg_lats, seg_times in segments:
                    frame_data.append(go.Scattermap(
                        lon=seg_lons,
                        lat=seg_lats,
                        mode='lines+markers',
                        line=dict(width=width, color=color),
                        marker=dict(size=4, color=color),
                        showlegend=False,
                        name=f'Member {member}' + (' (Control)' if member in [51, 52] else ''),
                        hovertemplate=f'<b>Member {member}</b>' + (' (Control)' if member in [51, 52] else '') +
                                      '<br>Hour: %{text}<br>Lat: %{lat:.2f}<br>Lon: %{lon:.2f}<extra></extra>',
                        text=seg_times
                    ))

        # Add combined wind envelopes
        if envelope_mode == 'combined' and combined_envelopes:
            # Add combined envelopes - these represent the TOTAL area across ALL forecast steps
            # Show them for all time steps since they represent the total possible impact

            for envelope in combined_envelopes:
                # Filter by selected thresholds
                threshold = envelope['WIND_THRESHOLD']
                if threshold not in selected_thresholds:
                    continue

                try:
                    # Parse polygon data - could be WKT or JSON format
                    envelope_data = envelope['ENVELOPE_REGION']

                    if envelope_data and isinstance(envelope_data, str):
                        # Check if it's JSON format (from Snowflake GEOGRAPHY)
                        if envelope_data.startswith('{"coordinates"'):
                            import json
                            try:
                                # Parse JSON coordinates
                                geo_data = json.loads(envelope_data)
                                coords = geo_data['coordinates'][0][0]  # Get first ring of first polygon

                                # Convert to lon/lat arrays
                                lons_env = [coord[0] for coord in coords]
                                lats_env = [coord[1] for coord in coords]

                            except (json.JSONDecodeError, KeyError, IndexError):
                                continue

                        # Check if it's WKT format (POLYGON or MULTIPOLYGON)
                        elif envelope_data.startswith(('POLYGON', 'MULTIPOLYGON')):
                            try:
                                polygon = wkt.loads(envelope_data)
                                if polygon.is_valid:
                                    # Handle both POLYGON and MULTIPOLYGON
                                    if hasattr(polygon, 'geoms'):  # MULTIPOLYGON
                                        # For MULTIPOLYGON, use the first polygon
                                        first_polygon = polygon.geoms[0]
                                        exterior_coords = list(first_polygon.exterior.coords)
                                    else:  # POLYGON
                                        exterior_coords = list(polygon.exterior.coords)

                                    lons_env = [coord[0] for coord in exterior_coords]
                                    lats_env = [coord[1] for coord in exterior_coords]
                                else:
                                    continue
                            except Exception:
                                continue
                        else:
                            continue
                    else:
                        continue

                    # Determine envelope color based on wind threshold
                    member = envelope.get('ENSEMBLE_MEMBER', 'Unknown')

                    # Color mapping matching the UI indicators (expanded for all thresholds)
                    color_map = {
                        34: 'rgba(255, 215, 0, 0.3)',  # Gold/Yellow (#FFD700)
                        40: 'rgba(255, 165, 0, 0.3)',  # Orange (#FFA500)
                        50: 'rgba(255, 140, 0, 0.3)',  # Dark Orange (#FF8C00)
                        64: 'rgba(255, 0, 0, 0.3)',  # Red (#FF0000)
                        74: 'rgba(204, 0, 0, 0.3)',  # Dark Red (Category 3)
                        96: 'rgba(153, 0, 0, 0.3)',  # Darker Red (Category 4)
                        113: 'rgba(102, 0, 0, 0.3)'  # Darkest Red (Category 5)
                    }
                    env_color = color_map.get(threshold, 'rgba(128, 128, 128, 0.3)')  # Gray for others

                    frame_data.append(go.Scattermap(
                        lon=lons_env,
                        lat=lats_env,
                        mode='lines',
                        line=dict(width=1, color=env_color),
                        fill='toself',
                        fillcolor=env_color,
                        showlegend=False,
                        name=f"Combined - {threshold}kt - M{member}",
                        hovertemplate=f"<b>Combined Envelope</b><br>Member: {member}<br>{threshold}kt Wind Field<br>Total Forecast Period<extra></extra>"
                    ))

                except Exception:
                    continue

        frames.append(go.Frame(data=frame_data, name=str(time_step)))

    # Create initial figure with first frame
    fig = go.Figure(data=frames[0].data, frames=frames)

    # Update mapbox layout
    fig.update_layout(
        map=dict(
            style="open-street-map",  # Free tile provider, no token needed
            center=dict(lat=center_lat, lon=center_lon),
            zoom=4
        ),
        updatemenus=[{
            'type': 'buttons',
            'showactive': False,
            'buttons': [
                {
                    'label': '▶ Play',
                    'method': 'animate',
                    'args': [None, {
                        'frame': {'duration': 200, 'redraw': True},
                        'fromcurrent': True,
                        'mode': 'immediate',
                        'transition': {'duration': 100, 'easing': 'linear'}
                    }]
                },
                {
                    'label': '⏸ Pause',
                    'method': 'animate',
                    'args': [[None], {
                        'frame': {'duration': 0, 'redraw': False},
                        'mode': 'immediate',
                        'transition': {'duration': 0}
                    }]
                }
            ],
            'x': 0.02,
            'y': 0.98,
            'xanchor': 'left',
            'yanchor': 'top',
            'bgcolor': 'rgba(255, 255, 255, 0.9)',
            'borderwidth': 1,
            'bordercolor': '#e0e0e0'
        }],
        sliders=[{
            'active': 0,
            'steps': [
                {
                    'args': [[f.name], {
                        'frame': {'duration': 0, 'redraw': True},
                        'mode': 'immediate'
                    }],
                    'label': f"{int(f.name)}h",
                    'method': 'animate'
                }
                for f in frames
            ],
            'x': 0.02,
            'y': 0,
            'len': 0.96,
            'xanchor': 'left',
            'yanchor': 'bottom',
            'bgcolor': 'rgba(255, 255, 255, 0.9)',
            'borderwidth': 1,
            'bordercolor': '#e0e0e0',
            'pad': {'b': 5, 't': 5}
        }],
        margin=dict(l=0, r=0, t=0, b=40),
        showlegend=False,
        hovermode='closest',
        uirevision=track_id,
        height=None,
        autosize=True
    )

    return fig


@app.callback(
    Output('wind-chart', 'figure'),
    Input('forecast-data-store', 'data')
)
def update_wind_chart(stored_data):
    """Create wind speed spaghetti plot"""
    if not stored_data or not stored_data.get('data'):
        return go.Figure()

    df = pd.DataFrame(stored_data['data'])

    fig = go.Figure()

    for member in df['ENSEMBLE_MEMBER'].unique():
        member_data = df[df['ENSEMBLE_MEMBER'] == member].sort_values('LEAD_TIME')
        fig.add_trace(go.Scatter(
            x=member_data['LEAD_TIME'],
            y=member_data['WIND_SPEED_KNOTS'],
            mode='lines',
            line=dict(width=0.8, color='rgba(59, 130, 246, 0.4)'),
            showlegend=False,
            hoverinfo='skip'
        ))

    mean_data = df.groupby('LEAD_TIME')['WIND_SPEED_KNOTS'].mean().reset_index()
    fig.add_trace(go.Scatter(
        x=mean_data['LEAD_TIME'],
        y=mean_data['WIND_SPEED_KNOTS'],
        mode='lines',
        line=dict(width=3, color='rgb(220, 38, 38)'),
        showlegend=False
    ))

    categories = [(34, 'TS'), (64, 'Cat 1'), (83, 'Cat 2'), (96, 'Cat 3'), (113, 'Cat 4'), (137, 'Cat 5')]
    for wind, label in categories:
        fig.add_hline(y=wind, line_dash="dot", line_color="gray", opacity=0.3,
                      annotation_text=label, annotation_position="right")

    fig.update_layout(
        xaxis_title="Forecast Hour",
        yaxis_title="Wind Speed (kt)",
        margin=dict(l=50, r=50, t=10, b=40),
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(gridcolor='#f0f0f0'),
        yaxis=dict(gridcolor='#f0f0f0', range=[0, max(150, df['WIND_SPEED_KNOTS'].max() * 1.1)])
    )

    return fig


@app.callback(
    Output('intensity-chart', 'figure'),
    Input('forecast-data-store', 'data')
)
def update_intensity_chart(stored_data):
    """Create intensity distribution stacked area chart"""
    if not stored_data or not stored_data.get('data'):
        return go.Figure()

    df = pd.DataFrame(stored_data['data'])

    def categorize_wind(wind):
        if wind < 34:
            return 'TD'
        elif wind < 64:
            return 'TS'
        elif wind < 83:
            return 'Cat 1'
        elif wind < 96:
            return 'Cat 2'
        elif wind < 113:
            return 'Cat 3'
        elif wind < 137:
            return 'Cat 4'
        else:
            return 'Cat 5'

    df['category'] = df['WIND_SPEED_KNOTS'].apply(categorize_wind)
    category_counts = df.groupby(['LEAD_TIME', 'category']).size().reset_index(name='count')
    pivot = category_counts.pivot(index='LEAD_TIME', columns='category', values='count').fillna(0)

    for cat in ['TD', 'TS', 'Cat 1', 'Cat 2', 'Cat 3', 'Cat 4', 'Cat 5']:
        if cat not in pivot.columns:
            pivot[cat] = 0

    cat_order = ['Cat 5', 'Cat 4', 'Cat 3', 'Cat 2', 'Cat 1', 'TS', 'TD']
    pivot = pivot[[c for c in cat_order if c in pivot.columns]]

    fig = go.Figure()
    colors = {
        'Cat 5': '#a29bfe', 'Cat 4': '#fd79a8', 'Cat 3': '#d63031',
        'Cat 2': '#e17055', 'Cat 1': '#fdcb6e', 'TS': '#00b894', 'TD': '#74b9ff'
    }

    for cat in pivot.columns:
        fig.add_trace(go.Scatter(
            x=pivot.index, y=pivot[cat], mode='lines', stackgroup='one',
            fillcolor=colors.get(cat, '#cccccc'), line=dict(width=0), showlegend=False
        ))

    fig.update_layout(
        xaxis_title="Forecast Hour", yaxis_title="Member Count",
        margin=dict(l=50, r=50, t=10, b=40),
        plot_bgcolor='white', paper_bgcolor='white',
        xaxis=dict(gridcolor='#f0f0f0'), yaxis=dict(gridcolor='#f0f0f0')
    )

    return fig


@app.callback(
    Output('pressure-chart', 'figure'),
    Input('forecast-data-store', 'data')
)
def update_pressure_chart(stored_data):
    """Create pressure spaghetti plot"""
    if not stored_data or not stored_data.get('data'):
        return go.Figure()

    df = pd.DataFrame(stored_data['data'])
    fig = go.Figure()

    for member in df['ENSEMBLE_MEMBER'].unique():
        member_data = df[df['ENSEMBLE_MEMBER'] == member].sort_values('LEAD_TIME')
        fig.add_trace(go.Scatter(
            x=member_data['LEAD_TIME'], y=member_data['PRESSURE_HPA'],
            mode='lines', line=dict(width=0.8, color='rgba(59, 130, 246, 0.4)'),
            showlegend=False, hoverinfo='skip'
        ))

    mean_data = df.groupby('LEAD_TIME')['PRESSURE_HPA'].mean().reset_index()
    fig.add_trace(go.Scatter(
        x=mean_data['LEAD_TIME'], y=mean_data['PRESSURE_HPA'],
        mode='lines', line=dict(width=3, color='rgb(220, 38, 38)'), showlegend=False
    ))

    fig.update_layout(
        xaxis_title="Forecast Hour", yaxis_title="Pressure (hPa)",
        margin=dict(l=50, r=50, t=10, b=40),
        plot_bgcolor='white', paper_bgcolor='white',
        xaxis=dict(gridcolor='#f0f0f0'), yaxis=dict(gridcolor='#f0f0f0'),
        yaxis_autorange='reversed'
    )

    return fig


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=10000)