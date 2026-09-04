#!/usr/bin/env python3
"""
Visualization Script for ECMWF Tropical Cyclone Pipeline

1. TC Track Visualization - Show ensemble tracks
2. Track with Wind Polygons - Show tracks with wind field polygons
3. Individual Wind Envelopes - Show wind threshold polygons per forecast step
4. Combined Wind Envelopes - Show combined wind threshold polygons
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from shapely import wkt
from shapely.geometry import Polygon, MultiPolygon
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
import warnings
import os

warnings.filterwarnings('ignore')

# Configuration
DEFAULT_OUTPUT_DIR = "visualizations"
DPI = 150
FIGSIZE = (12, 10)

# Wind threshold colors (from light to dark, low to high intensity)
THRESHOLD_COLORS = {
    34: '#90EE90',  # Light green - Tropical storm
    50: '#FFD700',  # Gold - Very strong tropical storm
    64: '#FF8C00',  # Dark orange - Category 1 hurricane
    83: '#FF4500',  # Orange red - Category 2 hurricane
    96: '#DC143C',  # Crimson - Category 3 hurricane (major)
    113: '#8B0000',  # Dark red - Category 4 hurricane
    137: '#4B0082'  # Indigo - Category 5 hurricane
}

# Threshold labels
THRESHOLD_LABELS = {
    34: 'Tropical Storm (34 kt)',
    50: 'Very Strong Tropical Storm (50 kt)',
    64: 'Category 1 Hurricane (64 kt)',
    83: 'Category 2 Hurricane (83 kt)',
    96: 'Category 3 Hurricane (96 kt)',
    113: 'Category 4 Hurricane (113 kt)',
    137: 'Category 5 Hurricane (137 kt)'
}


def get_bounds_from_data(data, padding=3.0):
    """Get bounding box from data with padding."""
    if isinstance(data, pd.DataFrame):
        if 'longitude' in data.columns and 'latitude' in data.columns:
            lon_min, lon_max = data['longitude'].min(), data['longitude'].max()
            lat_min, lat_max = data['latitude'].min(), data['latitude'].max()
        else:
            return -70, -50, 10, 30  # Default Caribbean bounds
    elif isinstance(data, list):
        # List of polygons
        all_bounds = [polygon.bounds for polygon in data if hasattr(polygon, 'bounds')]
        if not all_bounds:
            return -70, -50, 10, 30
        lon_min = min(b[0] for b in all_bounds)
        lon_max = max(b[2] for b in all_bounds)
        lat_min = min(b[1] for b in all_bounds)
        lat_max = max(b[3] for b in all_bounds)
    else:
        return -70, -50, 10, 30  # Default bounds

    return lon_min - padding, lon_max + padding, lat_min - padding, lat_max + padding


def plot_polygon_on_map(ax, polygon, color, alpha=0.6, linewidth=1.5):
    """Plot a polygon on the map with specified styling."""
    if isinstance(polygon, MultiPolygon):
        for poly in polygon.geoms:
            coords = list(poly.exterior.coords)
            ax.plot(*zip(*coords), color='black', linewidth=linewidth,
                    transform=ccrs.PlateCarree())
            ax.fill(*zip(*coords), color=color, alpha=alpha,
                    transform=ccrs.PlateCarree())
    else:
        coords = list(polygon.exterior.coords)
        ax.plot(*zip(*coords), color='black', linewidth=linewidth,
                transform=ccrs.PlateCarree())
        ax.fill(*zip(*coords), color=color, alpha=alpha,
                transform=ccrs.PlateCarree())


def _tile_zoom(lon_min, lon_max):
    """Auto-select tile zoom level from longitude span."""
    span = lon_max - lon_min
    if span > 100:
        return 2
    elif span > 30:
        return 3
    elif span > 10:
        return 4
    else:
        return 5


def setup_map(ax, bounds, basemap=True):
    """Setup map with Carto Positron tile basemap and cartopy features."""
    import cartopy.io.img_tiles as cimgt

    lon_min, lon_max, lat_min, lat_max = bounds
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())

    # Tile basemap (Carto Positron)
    if basemap:
        class _Positron(cimgt.GoogleWTS):
            def _image_url(self, tile):
                x, y, z = tile
                return f'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'
        try:
            ax.add_image(_Positron(), _tile_zoom(lon_min, lon_max))
        except Exception:
            ax.add_feature(cfeature.OCEAN, color='#d0e8f5', alpha=1.0)
            ax.add_feature(cfeature.LAND,  color='#f0ede8', alpha=1.0)

    ax.add_feature(cfeature.COASTLINE, linewidth=0.6, alpha=0.7, zorder=3)
    ax.add_feature(cfeature.BORDERS,   linewidth=0.4, alpha=0.5, zorder=3)

    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=0.4, color='gray', alpha=0.4, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False
    if hasattr(gl, 'geo_labels'):
        gl.geo_labels = False


def visualize_tc_tracks(csv_file, output_dir=DEFAULT_OUTPUT_DIR, show_plot=True, save_plot=False):
    """
    Visualize tropical cyclone ensemble tracks.

    Args:
        csv_file (str): Path to transformed TC CSV file
        output_dir (str): Output directory
        show_plot (bool): Whether to display the plot
        save_plot (bool): Whether to save the plot

    Returns:
        str: Path to saved file (if saved)
    """
    print("=" * 60)
    print("TC TRACK VISUALIZATION")
    print("=" * 60)

    # Load data
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} track points from {Path(csv_file).name}")

    # Get storm info
    storm_name = df['track_id'].iloc[0]
    forecast_time = df['forecast_time'].iloc[0]
    members = sorted(df['ensemble_member'].unique())

    print(f"Storm: {storm_name}")
    print(f"Forecast: {forecast_time}")
    print(f"Ensemble members: {len(members)}")

    # Create figure
    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Get bounds
    bounds = get_bounds_from_data(df)
    setup_map(ax, bounds)

    # Plot tracks for each member
    colors = plt.cm.tab20(np.linspace(0, 1, len(members)))

    for i, member in enumerate(members):
        member_df = df[df['ensemble_member'] == member].sort_values('lead_time')

        # Plot track line
        ax.plot(member_df['longitude'], member_df['latitude'],
                color=colors[i], linewidth=2, alpha=0.7,
                transform=ccrs.PlateCarree(), label=f'Member {member}')

        # Plot start and end points
        if len(member_df) > 0:
            # Start point
            start = member_df.iloc[0]
            ax.scatter(start['longitude'], start['latitude'],
                       c=colors[i], s=50, alpha=0.8, marker='o',
                       transform=ccrs.PlateCarree(), edgecolors='black', linewidth=1)

            # End point
            end = member_df.iloc[-1]
            ax.scatter(end['longitude'], end['latitude'],
                       c=colors[i], s=50, alpha=0.8, marker='s',
                       transform=ccrs.PlateCarree(), edgecolors='black', linewidth=1)

    # Add title
    plt.title(f"Tropical Cyclone {storm_name} - Ensemble Tracks\nForecast: {forecast_time}",
              fontsize=16, fontweight='bold', pad=20)

    # Add legend (show only first 10 members to avoid clutter)
    if len(members) <= 10:
        ax.legend(loc='upper right', fontsize=9, framealpha=0.9)
    else:
        ax.text(0.02, 0.98, f"{len(members)} ensemble members",
                transform=ax.transAxes, fontsize=10, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    # Save and show
    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{storm_name}_ensemble_tracks.png"
        filepath = Path(output_dir) / filename
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_tracks_with_wind_polygons(csv_file, output_dir=DEFAULT_OUTPUT_DIR,
                                        show_plot=True, save_plot=False, max_members=3):
    """
    Visualize TC tracks with wind field polygons for all thresholds.

    Args:
        csv_file (str): Path to transformed TC CSV file
        output_dir (str): Output directory
        show_plot (bool): Whether to display the plot
        save_plot (bool): Whether to save the plot
        max_members (int): Maximum number of members to show

    Returns:
        str: Path to saved file (if saved)
    """
    print("=" * 60)
    print("TC TRACKS WITH WIND POLYGONS")
    print("=" * 60)

    # Load data
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} track points from {Path(csv_file).name}")

    # Get storm info
    storm_name = df['track_id'].iloc[0]
    forecast_time = df['forecast_time'].iloc[0]
    members = sorted(df['ensemble_member'].unique())[:max_members]

    print(f"Storm: {storm_name}")
    print(f"Forecast: {forecast_time}")
    print(f"Showing {len(members)} members")

    # Check for wind polygon columns
    polygon_cols = [col for col in df.columns if 'polygon' in col.lower()]
    if not polygon_cols:
        print("No wind polygon data found!")
        return None

    print(f"Wind polygon columns: {polygon_cols}")

    # Create figure
    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Get bounds
    bounds = get_bounds_from_data(df)
    setup_map(ax, bounds)

    # Plot tracks and wind polygons for each member
    colors = plt.cm.tab20(np.linspace(0, 1, len(members)))

    # Check which wind thresholds are available
    available_thresholds = []
    for threshold in [34, 50, 64, 83, 96, 113, 137]:
        col_name = f'wind_field_polygon_{threshold}kt'
        if col_name in df.columns:
            available_thresholds.append(threshold)

    print(f"Available wind thresholds: {available_thresholds}")

    for i, member in enumerate(members):
        member_df = df[df['ensemble_member'] == member].sort_values('lead_time')

        # Plot track line
        ax.plot(member_df['longitude'], member_df['latitude'],
                color=colors[i], linewidth=3, alpha=0.8,
                transform=ccrs.PlateCarree(), label=f'Member {member}')

        # Plot wind polygons for available thresholds only
        for threshold in available_thresholds:
            col_name = f'wind_field_polygon_{threshold}kt'
            for _, row in member_df.iterrows():
                if pd.notna(row[col_name]):
                    try:
                        polygon = wkt.loads(row[col_name])
                        if polygon and not polygon.is_empty:
                            plot_polygon_on_map(ax, polygon, THRESHOLD_COLORS[threshold],
                                                alpha=0.4, linewidth=1)
                    except:
                        continue

    # Add title
    plt.title(f"Tropical Cyclone {storm_name} - Tracks with Wind Polygons\nForecast: {forecast_time}",
              fontsize=16, fontweight='bold', pad=20)

    # Add track legend: store it before creating the threshold legend, which would replace it
    track_legend = ax.legend(loc='upper right', fontsize=9, framealpha=0.9)

    # Add threshold legend outside the map (second ax.legend() call replaces the first,
    # so we re-add the track legend as an artist afterwards)
    if available_thresholds:
        legend_patches = []
        for threshold in sorted(available_thresholds):
            legend_patches.append(plt.Rectangle((0, 0), 1, 1,
                                                facecolor=THRESHOLD_COLORS[threshold],
                                                label=f'{threshold} kt'))
        ax.legend(handles=legend_patches, loc='center left', fontsize=8,
                  framealpha=0.9, title='Wind Thresholds', bbox_to_anchor=(1.05, 0.5))
        ax.add_artist(track_legend)  # restore track legend after second ax.legend() replaced it

    # Save and show
    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{storm_name}_tracks_with_polygons.png"
        filepath = Path(output_dir) / filename
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_individual_wind_envelopes(csv_file, output_dir=DEFAULT_OUTPUT_DIR,
                                        show_plot=True, save_plot=False, member=1):
    """
    Visualize individual wind envelopes for one member across multiple time steps.

    Args:
        csv_file (str): Path to individual envelopes CSV file
        output_dir (str): Output directory
        show_plot (bool): Whether to display the plot
        save_plot (bool): Whether to save the plot

    Returns:
        str: Path to saved file (if saved)
    """
    print("=" * 60)
    print("INDIVIDUAL WIND ENVELOPES")
    print("=" * 60)

    # Load data
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} records from {Path(csv_file).name}")

    # Filter for records with polygons
    df_polygons = df[df['envelope_region'].notna() & (df['envelope_region'] != '')].copy()
    print(f"Found {len(df_polygons)} records with polygons")

    if df_polygons.empty:
        print("No polygons found to visualize!")
        return None

    # Get storm info
    storm_name = df_polygons['track_id'].iloc[0]
    forecast_time = df_polygons['forecast_time'].iloc[0]

    # Select member, falling back to first available if requested member has no data
    available_members = sorted(df_polygons['ensemble_member'].unique())
    if member not in available_members:
        print(f"Member {member} not in data (available: {available_members[:5]}{'...' if len(available_members) > 5 else ''}); using member {available_members[0]}")
        member = available_members[0]
    member_df = df_polygons[df_polygons['ensemble_member'] == member]

    # Get unique forecast steps - check for different possible column names
    if 'forecast_step' in member_df.columns:
        forecast_steps = sorted(member_df['forecast_step'].unique())
    elif 'lead_time' in member_df.columns:
        forecast_steps = sorted(member_df['lead_time'].unique())
    elif 'forecast_hour' in member_df.columns:
        forecast_steps = sorted(member_df['forecast_hour'].unique())
    else:
        print("Available columns:", list(member_df.columns))
        print("No forecast step column found! Using row index instead.")
        forecast_steps = list(range(len(member_df)))

    print(f"Storm: {storm_name}")
    print(f"Forecast: {forecast_time}")
    print(f"Member: {member}")
    print(f"Forecast steps: {len(forecast_steps)}")

    # Calculate grid size for time steps
    n_steps = len(forecast_steps)
    n_cols = min(4, n_steps)
    n_rows = (n_steps + n_cols - 1) // n_cols

    # Create figure with subplots
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 4, n_rows * 3),
                             subplot_kw={'projection': ccrs.PlateCarree()})

    # Handle single subplot case
    if n_steps == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = axes if n_steps > 1 else [axes]
    else:
        axes = axes.flatten()

    # Compute bounding box from the actual polygons for this member
    _polys = []
    for s in member_df['envelope_region'].dropna():
        try:
            p = wkt.loads(s)
            if p and not p.is_empty:
                _polys.append(p)
        except Exception:
            pass
    bounds = get_bounds_from_data(_polys) if _polys else get_bounds_from_data(member_df)

    # Plot each forecast step
    for i, step in enumerate(forecast_steps):
        ax = axes[i]
        setup_map(ax, bounds)

        # Get data for this forecast step - handle different column names
        if 'forecast_step' in member_df.columns:
            step_df = member_df[member_df['forecast_step'] == step]
        elif 'lead_time' in member_df.columns:
            step_df = member_df[member_df['lead_time'] == step]
        elif 'forecast_hour' in member_df.columns:
            step_df = member_df[member_df['forecast_hour'] == step]
        else:
            # Use row index if no time column found
            step_df = member_df.iloc[[i]] if i < len(member_df) else member_df.iloc[0:0]

        # Plot polygons for each threshold
        thresholds_present = []
        for _, row in step_df.iterrows():
            try:
                polygon = wkt.loads(row['envelope_region'])
                threshold = row['wind_threshold']
                if polygon and not polygon.is_empty:
                    plot_polygon_on_map(ax, polygon, THRESHOLD_COLORS[threshold], alpha=0.6)
                    thresholds_present.append(threshold)
            except:
                continue

        # Add step title
        ax.set_title(f"Step {step}h\n{len(thresholds_present)} thresholds",
                     fontsize=10, fontweight='bold')

    # Hide unused subplots
    for i in range(n_steps, len(axes)):
        axes[i].set_visible(False)

    # Add overall title
    fig.suptitle(f"Storm: {storm_name} - Member {member} Wind Envelopes\nForecast: {forecast_time}",
                 fontsize=14, fontweight='bold')

    # Add legend (only on the first subplot, outside the map)
    if n_steps > 0:
        legend_patches = []
        for threshold in sorted(THRESHOLD_COLORS.keys()):
            legend_patches.append(plt.Rectangle((0, 0), 1, 1,
                                                facecolor=THRESHOLD_COLORS[threshold],
                                                label=f'{threshold} kt'))

        legend = axes[0].legend(handles=legend_patches, loc='center left', fontsize=7,
                                framealpha=0.9, title='Wind Thresholds', bbox_to_anchor=(1.05, 0.5))
        legend.set_bbox_to_anchor((1.05, 0.5))

    # Save and show
    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{storm_name}_individual_envelopes.png"
        filepath = Path(output_dir) / filename
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_combined_wind_envelopes(csv_file, output_dir=DEFAULT_OUTPUT_DIR,
                                      show_plot=True, save_plot=False, max_members=3):
    """
    Visualize combined wind envelopes (polygons combined across all forecast steps).

    Args:
        csv_file (str): Path to combined envelopes CSV file
        output_dir (str): Output directory
        show_plot (bool): Whether to display the plot
        save_plot (bool): Whether to save the plot
        max_members (int): Maximum number of members to show

    Returns:
        str: Path to saved file (if saved)
    """
    print("=" * 60)
    print("COMBINED WIND ENVELOPES")
    print("=" * 60)

    # Load data
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} records from {Path(csv_file).name}")

    # Filter for records with polygons
    df_polygons = df[df['envelope_region'].notna() & (df['envelope_region'] != '')].copy()
    print(f"Found {len(df_polygons)} records with polygons")

    if df_polygons.empty:
        print("No polygons found to visualize!")
        return None

    # Get storm info
    storm_name = df_polygons['track_id'].iloc[0]
    forecast_time = df_polygons['forecast_time'].iloc[0]
    members = sorted(df_polygons['ensemble_member'].unique())[:max_members]

    print(f"Storm: {storm_name}")
    print(f"Forecast: {forecast_time}")
    print(f"Showing {len(members)} members")

    # Calculate grid size
    n_members = len(members)
    n_cols = min(3, n_members)
    n_rows = (n_members + n_cols - 1) // n_cols

    # Create figure with subplots
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 5, n_rows * 4),
                             subplot_kw={'projection': ccrs.PlateCarree()})

    # Handle single subplot case
    if n_members == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = axes if n_members > 1 else [axes]
    else:
        axes = axes.flatten()

    # Get overall bounds
    all_polygons = []
    for _, row in df_polygons.iterrows():
        try:
            polygon = wkt.loads(row['envelope_region'])
            if polygon and not polygon.is_empty:
                all_polygons.append(polygon)
        except:
            continue

    bounds = get_bounds_from_data(all_polygons)

    # Plot each member
    for i, member in enumerate(members):
        ax = axes[i]
        setup_map(ax, bounds)

        # Get data for this member
        member_df = df_polygons[df_polygons['ensemble_member'] == member]

        # Plot polygons for each threshold
        thresholds_present = []
        for _, row in member_df.iterrows():
            try:
                polygon = wkt.loads(row['envelope_region'])
                threshold = row['wind_threshold']
                if polygon and not polygon.is_empty:
                    plot_polygon_on_map(ax, polygon, THRESHOLD_COLORS[threshold], alpha=0.6)
                    thresholds_present.append(threshold)
            except:
                continue

        # Add member title
        ax.set_title(f"Member {member}\n{len(thresholds_present)} thresholds",
                     fontsize=12, fontweight='bold')

        # Add threshold info
        if thresholds_present:
            ax.text(0.02, 0.98, f"{min(thresholds_present)}-{max(thresholds_present)} kt",
                    transform=ax.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    # Hide unused subplots
    for i in range(n_members, len(axes)):
        axes[i].set_visible(False)

    # Add overall title
    fig.suptitle(f"Storm: {storm_name} - Combined Wind Envelopes\nForecast: {forecast_time}",
                 fontsize=16, fontweight='bold')

    # Add legend (only on the first subplot, outside the map)
    if n_members > 0:
        legend_patches = []
        for threshold in sorted(THRESHOLD_COLORS.keys()):
            legend_patches.append(plt.Rectangle((0, 0), 1, 1,
                                                facecolor=THRESHOLD_COLORS[threshold],
                                                label=THRESHOLD_LABELS[threshold]))

        legend = axes[0].legend(handles=legend_patches, loc='center left', fontsize=8,
                                framealpha=0.9, title='Wind Thresholds', bbox_to_anchor=(1.05, 0.5))
        legend.set_bbox_to_anchor((1.05, 0.5))

    # Save and show
    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{storm_name}_combined_envelopes.png"
        filepath = Path(output_dir) / filename
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def show_tracks(csv_file, output_dir=DEFAULT_OUTPUT_DIR):
    """Quick function to show TC tracks."""
    return visualize_tc_tracks(csv_file, output_dir, show_plot=True, save_plot=False)


def show_tracks_with_polygons(csv_file, output_dir=DEFAULT_OUTPUT_DIR):
    """Quick function to show tracks with wind polygons."""
    return visualize_tracks_with_wind_polygons(csv_file, output_dir, show_plot=True, save_plot=False)


def show_individual_envelopes(csv_file, output_dir=DEFAULT_OUTPUT_DIR, member=1):
    """Quick function to show individual wind envelopes."""
    return visualize_individual_wind_envelopes(csv_file, output_dir, show_plot=True, save_plot=False, member=member)


def show_combined_envelopes(csv_file, output_dir=DEFAULT_OUTPUT_DIR):
    """Quick function to show combined wind envelopes."""
    return visualize_combined_wind_envelopes(csv_file, output_dir, show_plot=True, save_plot=False)


# ─── Precipitation Visualizations ───────────────────────────────────────────

def _load_precip_zarr(zarr_path):
    """Load precip Zarr ZipStore, return data array + coordinate metadata."""
    import zarr as _zarr
    store = _zarr.storage.ZipStore(str(zarr_path), mode='r')
    try:
        root = _zarr.open_group(store=store, mode='r')
        z = root['data']
        attrs = dict(root.attrs)
        n_lat, n_lon = z.shape[2], z.shape[3]
        # Zarr rows run N→S (row 0 = lat_max). Build descending lat array to match.
        lats = np.linspace(attrs['lat_max'], attrs['lat_min'], n_lat)
        lons = np.linspace(attrs['lon_min'], attrs['lon_max'], n_lon)
        # Load data eagerly so the store can be closed immediately
        data = np.asarray(z)
    finally:
        store.close()
    return {
        'data': data,                         # shape (51, 25, n_lat, n_lon) float16
        'lats': lats,
        'lons': lons,
        'steps': attrs['steps'],              # [0, 6, 12, ..., 144]
        'member_numbers': attrs.get('member_numbers', list(range(1, 52))),
        'forecast_date': attrs.get('forecast_date', ''),
        'run_time': int(attrs.get('run_time', 0)),
    }


def _precip_bounds_from_track(track_csv, padding=5.0):
    """Return (lon_min, lon_max, lat_min, lat_max) from TC track CSV."""
    return get_bounds_from_data(pd.read_csv(track_csv), padding=padding)


def _step_index(steps, hour):
    """Return Zarr step-axis index for a given forecast hour."""
    arr = np.array(steps)
    hits = np.where(arr == hour)[0]
    if not len(hits):
        raise ValueError(f"Hour {hour} not in available steps {steps}")
    return int(hits[0])


def visualize_precip_ensemble_mean(zarr_path, step_h=72, track_csv=None,
                                   output_dir=DEFAULT_OUTPUT_DIR,
                                   show_plot=True, save_plot=False):
    """
    Map of ensemble mean accumulated precipitation at a given forecast hour.

    Args:
        zarr_path:  Path to tp_YYYYMMDD_HH.zarr.zip
        step_h:     Accumulated-total forecast hour shown (0–144, multiple of 6)
        track_csv:  Optional TC track CSV for spatial extent + overlay
        output_dir: Directory for saved PNG
        show_plot:  Display interactively
        save_plot:  Save PNG to output_dir
    """
    print("=" * 60)
    print("PRECIPITATION -- ENSEMBLE MEAN")
    print("=" * 60)

    p = _load_precip_zarr(zarr_path)
    sidx = _step_index(p['steps'], step_h)

    # (51, lat, lon) → ensemble mean (lat, lon)
    arr = np.asarray(p['data'][:, sidx, :, :]).astype(np.float32)
    mean_arr = arr.mean(axis=0)

    if track_csv:
        bounds = _precip_bounds_from_track(track_csv)
        df_track = pd.read_csv(track_csv)
        storm_name = df_track['track_id'].iloc[0] if 'track_id' in df_track.columns else ''
    else:
        bounds = (float(p['lons'][0]), float(p['lons'][-1]),
                  float(p['lats'][-1]), float(p['lats'][0]))  # lats is N→S, so [-1]=S [0]=N
        df_track = None
        storm_name = ''

    lon_min, lon_max, lat_min, lat_max = bounds
    lat_idx = np.where((p['lats'] >= lat_min) & (p['lats'] <= lat_max))[0]
    lon_idx = np.where((p['lons'] >= lon_min) & (p['lons'] <= lon_max))[0]
    plot_lons = p['lons'][lon_idx]
    plot_lats = p['lats'][lat_idx]
    plot_data = mean_arr[np.ix_(lat_idx, lon_idx)]
    vmax = max(float(np.percentile(plot_data, 99)), 1.0)
    vmin_precip = 0.5  # cells below 0.5 mm are transparent (show basemap)

    print(f"  Step: {step_h}h  |  Ensemble max mean: {plot_data.max():.1f}mm")

    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    ax = plt.axes(projection=ccrs.PlateCarree())
    setup_map(ax, bounds)

    _cmap = plt.cm.Blues.copy()
    _cmap.set_under('none')
    mesh = ax.pcolormesh(plot_lons, plot_lats, plot_data,
                         cmap=_cmap, vmin=vmin_precip, vmax=vmax,
                         transform=ccrs.PlateCarree(), zorder=2)
    plt.colorbar(mesh, ax=ax, orientation='vertical', pad=0.02,
                 label='Accumulated precipitation (mm)', shrink=0.8)

    if df_track is not None:
        for member in sorted(df_track['ensemble_member'].unique()):
            mdf = df_track[df_track['ensemble_member'] == member].sort_values('lead_time')
            ax.plot(mdf['longitude'], mdf['latitude'],
                    color='orange', linewidth=0.8, alpha=0.4,
                    transform=ccrs.PlateCarree())
        hres = df_track[df_track['ensemble_member'] == 51].sort_values('lead_time')
        if not hres.empty:
            ax.plot(hres['longitude'], hres['latitude'],
                    color='red', linewidth=2.5, alpha=0.95,
                    transform=ccrs.PlateCarree(), label='HRES (member 51)', zorder=5)
        ax.legend(loc='upper right', fontsize=9)

    title = f"Ensemble Mean Accumulated Precipitation -- {step_h}h"
    if storm_name:
        title = f"{storm_name} -- {title}"
    title += f"\nForecast: {p['forecast_date']} {p['run_time']:02d}Z | {len(p['member_numbers'])} members"
    plt.title(title, fontsize=14, fontweight='bold', pad=15)

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        tag = f"{storm_name}_" if storm_name else ''
        filepath = Path(output_dir) / f"{tag}precip_mean_{step_h}h.png"
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_precip_period_panels(zarr_path, track_csv=None, periods=None,
                                   output_dir=DEFAULT_OUTPUT_DIR,
                                   show_plot=True, save_plot=False):
    """
    4-panel map of ensemble mean period precipitation (default: 0-24h, 24-48h, 48-72h, 72-144h).

    Args:
        zarr_path:  Path to tp_YYYYMMDD_HH.zarr.zip
        track_csv:  Optional TC track CSV for spatial extent + overlay
        periods:    List of (start_h, end_h) pairs. Defaults to [(0,24),(24,48),(48,72),(72,144)]
        output_dir: Directory for saved PNG
        show_plot:  Display interactively
        save_plot:  Save PNG to output_dir
    """
    print("=" * 60)
    print("PRECIPITATION -- PERIOD ACCUMULATION PANELS")
    print("=" * 60)

    if periods is None:
        periods = [(0, 24), (24, 48), (48, 72), (72, 144)]

    p = _load_precip_zarr(zarr_path)

    if track_csv:
        bounds = _precip_bounds_from_track(track_csv)
        df_track = pd.read_csv(track_csv)
        storm_name = df_track['track_id'].iloc[0] if 'track_id' in df_track.columns else ''
    else:
        bounds = (float(p['lons'][0]), float(p['lons'][-1]),
                  float(p['lats'][-1]), float(p['lats'][0]))  # lats is N→S, so [-1]=S [0]=N
        df_track = None
        storm_name = ''

    lon_min, lon_max, lat_min, lat_max = bounds
    lat_idx = np.where((p['lats'] >= lat_min) & (p['lats'] <= lat_max))[0]
    lon_idx = np.where((p['lons'] >= lon_min) & (p['lons'] <= lon_max))[0]
    plot_lons = p['lons'][lon_idx]
    plot_lats = p['lats'][lat_idx]

    # Pre-load accumulated data at each period boundary
    needed_hours = sorted({h for pair in periods for h in pair})
    step_data = {}
    for h in needed_hours:
        sidx = _step_index(p['steps'], h)
        step_data[h] = np.asarray(p['data'][:, sidx, :, :]).astype(np.float32)  # (51, lat, lon)

    period_means = []
    for start_h, end_h in periods:
        delta = step_data[end_h] - step_data[start_h]
        mean = delta.mean(axis=0)
        period_means.append(mean[np.ix_(lat_idx, lon_idx)])

    vmax = max(max(float(np.percentile(pm, 99)) for pm in period_means), 1.0)
    vmin_precip = 0.5
    _cmap = plt.cm.Blues.copy()
    _cmap.set_under('none')

    n = len(periods)
    n_cols = min(2, n)
    n_rows = (n + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(n_cols * 7, n_rows * 5),
                             subplot_kw={'projection': ccrs.PlateCarree()})
    axes = np.array(axes).flatten()

    meshes = []
    for i, (start_h, end_h) in enumerate(periods):
        ax = axes[i]
        setup_map(ax, bounds)

        mesh = ax.pcolormesh(plot_lons, plot_lats, period_means[i],
                             cmap=_cmap, vmin=vmin_precip, vmax=vmax,
                             transform=ccrs.PlateCarree(), zorder=2)
        meshes.append(mesh)

        if df_track is not None:
            period_track = df_track[
                (df_track['lead_time'] >= start_h) & (df_track['lead_time'] <= end_h)
            ]
            for member in sorted(period_track['ensemble_member'].unique()):
                mdf = period_track[period_track['ensemble_member'] == member].sort_values('lead_time')
                ax.plot(mdf['longitude'], mdf['latitude'],
                        color='orange', linewidth=0.8, alpha=0.35,
                        transform=ccrs.PlateCarree())
            hres_p = period_track[period_track['ensemble_member'] == 51].sort_values('lead_time')
            if not hres_p.empty:
                ax.plot(hres_p['longitude'], hres_p['latitude'],
                        color='red', linewidth=2.5, alpha=0.95,
                        transform=ccrs.PlateCarree(), zorder=5)

        ax.set_title(f"{start_h}–{end_h}h period  (max {period_means[i].max():.1f}mm)",
                     fontsize=11, fontweight='bold')
        print(f"  {start_h:>3d}–{end_h:>3d}h: max {period_means[i].max():.1f}mm")

    for i in range(n, len(axes)):
        axes[i].set_visible(False)

    fig.subplots_adjust(right=0.87)
    cbar_ax = fig.add_axes([0.90, 0.15, 0.02, 0.7])
    fig.colorbar(meshes[0], cax=cbar_ax, label='Period accumulation (mm)')

    title = 'Ensemble Mean Period Precipitation'
    if storm_name:
        title = f"{storm_name} -- {title}"
    title += f"\nForecast: {p['forecast_date']} {p['run_time']:02d}Z | {len(p['member_numbers'])} members"
    fig.suptitle(title, fontsize=14, fontweight='bold', y=1.01)

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        tag = f"{storm_name}_" if storm_name else ''
        filepath = Path(output_dir) / f"{tag}precip_period_panels.png"
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_precip_exceedance(zarr_path, threshold_mm=50, step_h=72, track_csv=None,
                                output_dir=DEFAULT_OUTPUT_DIR,
                                show_plot=True, save_plot=False):
    """
    Probability map: fraction of ensemble members that accumulate > threshold_mm by step_h.

    Args:
        zarr_path:      Path to tp_YYYYMMDD_HH.zarr.zip
        threshold_mm:   Precipitation threshold in mm
        step_h:         Forecast hour for the accumulated total (0–144)
        track_csv:      Optional TC track CSV for spatial extent + overlay
        output_dir:     Directory for saved PNG
        show_plot:      Display interactively
        save_plot:      Save PNG to output_dir
    """
    print("=" * 60)
    print(f"PRECIPITATION -- EXCEEDANCE PROBABILITY (>{threshold_mm}mm by {step_h}h)")
    print("=" * 60)

    p = _load_precip_zarr(zarr_path)
    sidx = _step_index(p['steps'], step_h)

    arr = np.asarray(p['data'][:, sidx, :, :]).astype(np.float32)    # (51, lat, lon)
    exceed_pct = (arr > threshold_mm).mean(axis=0) * 100              # (lat, lon) 0–100%

    if track_csv:
        bounds = _precip_bounds_from_track(track_csv)
        df_track = pd.read_csv(track_csv)
        storm_name = df_track['track_id'].iloc[0] if 'track_id' in df_track.columns else ''
    else:
        bounds = (float(p['lons'][0]), float(p['lons'][-1]),
                  float(p['lats'][-1]), float(p['lats'][0]))  # lats is N→S, so [-1]=S [0]=N
        df_track = None
        storm_name = ''

    lon_min, lon_max, lat_min, lat_max = bounds
    lat_idx = np.where((p['lats'] >= lat_min) & (p['lats'] <= lat_max))[0]
    lon_idx = np.where((p['lons'] >= lon_min) & (p['lons'] <= lon_max))[0]
    plot_lons = p['lons'][lon_idx]
    plot_lats = p['lats'][lat_idx]
    plot_data = exceed_pct[np.ix_(lat_idx, lon_idx)]

    print(f"  Max exceedance probability : {plot_data.max():.1f}%")
    print(f"  Grid cells with P > 50%   : {(plot_data > 50).sum()}")

    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    ax = plt.axes(projection=ccrs.PlateCarree())
    setup_map(ax, bounds)

    _cmap_ex = plt.cm.YlOrRd.copy()
    _cmap_ex.set_under('none')
    mesh = ax.pcolormesh(plot_lons, plot_lats, plot_data,
                         cmap=_cmap_ex, vmin=1, vmax=100,
                         transform=ccrs.PlateCarree(), zorder=2)
    plt.colorbar(mesh, ax=ax, orientation='vertical', pad=0.02,
                 label=f'P(accumulated tp > {threshold_mm}mm by {step_h}h)  [%]',
                 shrink=0.8)

    if df_track is not None:
        for member in sorted(df_track['ensemble_member'].unique()):
            mdf = df_track[df_track['ensemble_member'] == member].sort_values('lead_time')
            ax.plot(mdf['longitude'], mdf['latitude'],
                    color='black', linewidth=0.6, alpha=0.25,
                    transform=ccrs.PlateCarree())
        hres = df_track[df_track['ensemble_member'] == 51].sort_values('lead_time')
        if not hres.empty:
            ax.plot(hres['longitude'], hres['latitude'],
                    color='blue', linewidth=2.5, alpha=0.95,
                    transform=ccrs.PlateCarree(), label='HRES (member 51)', zorder=5)
        ax.legend(loc='upper right', fontsize=9)

    title = f"Exceedance Probability: >{threshold_mm}mm by {step_h}h"
    if storm_name:
        title = f"{storm_name} -- {title}"
    title += f"\nForecast: {p['forecast_date']} {p['run_time']:02d}Z | {len(p['member_numbers'])} members"
    plt.title(title, fontsize=14, fontweight='bold', pad=15)

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        tag = f"{storm_name}_" if storm_name else ''
        filepath = Path(output_dir) / f"{tag}precip_exceedance_{threshold_mm}mm_{step_h}h.png"
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def show_precip_ensemble_mean(zarr_path, step_h=72, track_csv=None):
    """Quick function to show ensemble mean accumulated precipitation map."""
    return visualize_precip_ensemble_mean(zarr_path, step_h=step_h, track_csv=track_csv,
                                          show_plot=True, save_plot=False)


def show_precip_period_panels(zarr_path, track_csv=None, periods=None):
    """Quick function to show ensemble mean period accumulation panels."""
    return visualize_precip_period_panels(zarr_path, track_csv=track_csv, periods=periods,
                                          show_plot=True, save_plot=False)


def show_precip_exceedance(zarr_path, threshold_mm=50, step_h=72, track_csv=None):
    """Quick function to show exceedance probability map."""
    return visualize_precip_exceedance(zarr_path, threshold_mm=threshold_mm,
                                       step_h=step_h, track_csv=track_csv,
                                       show_plot=True, save_plot=False)


# ─── GloFAS Riverine Discharge Visualizations ────────────────────────────────

GLOFAS_DEFAULT_BOUNDS = (-180.0, 180.0, -60.0, 60.0)  # matches the pipeline's own clip


def _setup_glofas_map(ax, bounds, regional=False, show_labels=True):
    """
    Map setup for GloFAS plots. Deliberately does NOT use setup_map()'s web-tile
    basemap (Carto Positron): that tile source is a) a live network fetch that
    silently falls back to a near-blank map if it fails, and b) intentionally
    very pale even when it succeeds (designed for busy data overlays, not for
    orienting a viewer who has no other context). Instead uses solid, always
    available Natural Earth land/ocean coloring + coastlines + borders, so the
    map is legible even at a glance and never depends on network access.

    regional=True (for zoomed views) also adds state/province boundaries and
    lake outlines, needed for orientation once you're zoomed past country scale.
    """
    lon_min, lon_max, lat_min, lat_max = bounds
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())

    ax.add_feature(cfeature.OCEAN, facecolor='#cfe3ee', zorder=0)
    ax.add_feature(cfeature.LAND, facecolor='#f2efe9', zorder=0)
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, color='#333333', zorder=3)
    ax.add_feature(cfeature.BORDERS, linewidth=0.7, color='#555555', zorder=3)
    ax.add_feature(cfeature.LAKES, facecolor='#cfe3ee', edgecolor='#333333', linewidth=0.4, zorder=1)

    if regional:
        states = cfeature.NaturalEarthFeature(
            category='cultural', name='admin_1_states_provinces_lines',
            scale='50m', facecolor='none')
        ax.add_feature(states, edgecolor='#888888', linewidth=0.5, linestyle='--', zorder=3)

    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=show_labels,
                      linewidth=0.4, color='gray', alpha=0.4, linestyle='--')
    if show_labels:
        gl.top_labels = False
        gl.right_labels = False
        if hasattr(gl, 'geo_labels'):
            gl.geo_labels = False


def _load_glofas_zarr(zarr_path):
    """Load GloFAS Zarr ZipStore, return sparse data array + cell coordinates."""
    import zarr as _zarr
    store = _zarr.storage.ZipStore(str(zarr_path), mode='r')
    try:
        root = _zarr.open_group(store=store, mode='r')
        z = root['data']
        attrs = dict(root.attrs)
        data = np.asarray(z)               # (member, step, n_cells)
        cell_lat = np.asarray(root['cell_lat'])
        cell_lon = np.asarray(root['cell_lon'])
    finally:
        store.close()
    return {
        'data': data,
        'cell_lat': cell_lat,
        'cell_lon': cell_lon,
        'leadtime_hours': attrs['leadtime_hours'],       # [24, 48, ..., 168]
        'member_numbers': attrs.get('member_numbers', list(range(1, 52))),
        'forecast_date': attrs.get('forecast_date', ''),
        'n_cells_kept': attrs.get('n_cells_kept', data.shape[2]),
        'n_cells_total': attrs.get('n_cells_total', None),
        'filter_threshold': attrs.get('filter_threshold', ''),
    }


def _glofas_step_index(leadtime_hours, hour):
    """Return Zarr step-axis index for a given forecast lead hour."""
    arr = np.array(leadtime_hours)
    hits = np.where(arr == hour)[0]
    if not len(hits):
        raise ValueError(f"Lead hour {hour} not in available steps {leadtime_hours}")
    return int(hits[0])


def _glofas_sparse_to_dense(cell_lat, cell_lon, values, res=0.05,
                            lat_min=-60.0, lat_max=60.0, lon_min=-180.0, lon_max=180.0):
    """
    Reconstruct a dense (lat, lon) raster from GloFAS's sparse cell arrays, so it
    can be rendered with pcolormesh/imshow instead of overlapping scatter dots.
    Cells never kept by the sparse filter are NaN (transparent when plotted).

    Native 0.05deg grid: row 0 = lat_max (grid is stored N->S, matching the
    forecast's own latitude order), col 0 = lon_min (grid is W->E).

    Returns (dense, lats_1d, lons_1d); dense.shape == (len(lats_1d), len(lons_1d)).
    """
    n_lat = int(round((lat_max - lat_min) / res)) + 1
    n_lon = int(round((lon_max - lon_min) / res)) + 1
    dense = np.full((n_lat, n_lon), np.nan, dtype=np.float32)

    row = np.round((lat_max - cell_lat) / res).astype(int)
    col = np.round((cell_lon - lon_min) / res).astype(int)
    in_bounds = (row >= 0) & (row < n_lat) & (col >= 0) & (col < n_lon)
    dense[row[in_bounds], col[in_bounds]] = values[in_bounds]

    lats_1d = lat_max - np.arange(n_lat) * res
    lons_1d = lon_min + np.arange(n_lon) * res
    return dense, lats_1d, lons_1d


def _glofas_crop(dense, lats_1d, lons_1d, bounds):
    """Crop a dense raster (+ its coordinate axes) to (lon_min, lon_max, lat_min, lat_max)."""
    lon_min, lon_max, lat_min, lat_max = bounds
    lat_idx = np.where((lats_1d >= lat_min) & (lats_1d <= lat_max))[0]
    lon_idx = np.where((lons_1d >= lon_min) & (lons_1d <= lon_max))[0]
    return dense[np.ix_(lat_idx, lon_idx)], lats_1d[lat_idx], lons_1d[lon_idx]


def _lookup_threshold_at_cells(threshold_path, rp, cell_lat, cell_lon):
    """
    Fast nearest-neighbor lookup of an official RP threshold at many cell
    coordinates.
    """
    import xarray as xr
    official = xr.open_dataset(threshold_path)
    try:
        thr_lat = official['lat'].values   # descending, full global domain
        thr_lon = official['lon'].values   # ascending
        thr_arr = official[f"rl_{rp}"].values  # (lat, lon), eager load: one bulk read
    finally:
        official.close()

    res = abs(thr_lat[0] - thr_lat[1])
    row = np.round((thr_lat[0] - cell_lat) / res).astype(int)
    col = np.round((cell_lon - thr_lon[0]) / res).astype(int)
    row = np.clip(row, 0, thr_arr.shape[0] - 1)
    col = np.clip(col, 0, thr_arr.shape[1] - 1)
    return thr_arr[row, col]


def find_glofas_hotspot(zarr_path, threshold_local_dir='glofas_data/thresholds_cache', rp='2.0',
                        step_h=72, pad_deg=12.0):
    """
    Find a real bounding box around the strongest RP-exceedance cluster, for use
    as the `bounds` argument to a zoomed-in visualize_glofas_* call. Returns
    (lon_min, lon_max, lat_min, lat_max) padded by pad_deg, or None if no
    threshold data / exceedance is found.
    """
    threshold_path = Path(threshold_local_dir) / f"rl_{rp}.nc"
    if not threshold_path.exists():
        print(f"  RP{rp} threshold file not found at {threshold_path} -- cannot find a hotspot.")
        return None

    g = _load_glofas_zarr(zarr_path)
    sidx = _glofas_step_index(g['leadtime_hours'], step_h)
    disch = g['data'][:, sidx, :]

    thr_at_cells = _lookup_threshold_at_cells(threshold_path, rp, g['cell_lat'], g['cell_lon'])

    valid = thr_at_cells > 0
    exceed_pct = np.zeros(len(g['cell_lat']))
    exceed_pct[valid] = (disch[:, valid] > thr_at_cells[valid]).mean(axis=0) * 100

    if not (exceed_pct > 0).any():
        print("  No exceedance found anywhere -- cannot pick a hotspot.")
        return None

    # Densest cluster of high-exceedance cells: bin into 2deg cells, find the bin
    # with the most >50%-exceedance cells, then bound the actual cells within it.
    hot = exceed_pct > 50
    if not hot.any():
        hot = exceed_pct > 0  # fall back to any exceedance at all
    hot_lat, hot_lon = g['cell_lat'][hot], g['cell_lon'][hot]
    bin_lat = np.round(hot_lat / 2.0).astype(int)
    bin_lon = np.round(hot_lon / 2.0).astype(int)
    bins, counts = np.unique(np.stack([bin_lat, bin_lon], axis=1), axis=0, return_counts=True)
    best_bin = bins[np.argmax(counts)]
    in_bin = (bin_lat == best_bin[0]) & (bin_lon == best_bin[1])

    lat_c, lon_c = hot_lat[in_bin], hot_lon[in_bin]
    bounds = (float(lon_c.min()) - pad_deg, float(lon_c.max()) + pad_deg,
              float(lat_c.min()) - pad_deg, float(lat_c.max()) + pad_deg)
    print(f"  Hotspot: {int(in_bin.sum())} cells near lat={lat_c.mean():.1f}, lon={lon_c.mean():.1f}"
          f"  -> bounds={tuple(round(b, 1) for b in bounds)}")
    return bounds


def find_glofas_active_cell(zarr_path):
    """
    Find a single "gauge" cell with a genuinely developing signal: the cell
    whose ensemble-max discharge rises the most from the first to the last lead
    time (a clear, visually dramatic rising hydrograph, not just a big river that
    happens to be uniformly high all week). Returns (cell_lat, cell_lon).
    """
    g = _load_glofas_zarr(zarr_path)
    max_per_step = g['data'].max(axis=0)   # (step, n_cells)
    rise = max_per_step[-1] - max_per_step[0]
    idx = int(np.argmax(rise))
    lat, lon = float(g['cell_lat'][idx]), float(g['cell_lon'][idx])
    print(f"  Most active cell: lat={lat:.2f}, lon={lon:.2f}  "
          f"(max discharge rises {max_per_step[0][idx]:.0f} -> {max_per_step[-1][idx]:.0f} m3/s "
          f"over the horizon)")
    return lat, lon


def _glofas_thresholds_at_point(cell_lat, cell_lon, threshold_local_dir, rps):
    """Look up official RP threshold values at one point, for each rp in rps."""
    import xarray as xr
    values = {}
    for rp in rps:
        threshold_path = Path(threshold_local_dir) / f"rl_{rp}.nc"
        if not threshold_path.exists():
            continue
        official = xr.open_dataset(threshold_path)
        try:
            val = official[f"rl_{rp}"].sel(
                lat=cell_lat, lon=cell_lon, method='nearest',
            ).item()
            if val > 0:
                values[rp] = float(val)
        finally:
            official.close()
    return values


def visualize_glofas_gauge_hydrograph(zarr_path, cell_lat=None, cell_lon=None,
                                      threshold_local_dir='glofas_data/thresholds_cache',
                                      rps=('2.0', '5.0', '20.0'),
                                      output_dir=DEFAULT_OUTPUT_DIR,
                                      show_plot=True, save_plot=False):
    """
    Point hydrograph for a single cell: all 51 ensemble member traces across the
    7-day lead-time horizon, an ensemble median, a min-max spread band, and
    official RP threshold lines, matching the style of Google Flood Hub /
    GloFAS's own single-gauge forecast view.

    Args:
        zarr_path:            Path to river_YYYYMMDD.zarr.zip
        cell_lat, cell_lon:   Which cell to plot; if either is None, uses
                               find_glofas_active_cell() to pick one automatically
        threshold_local_dir:  Local dir containing rl_{rp}.nc files
        rps:                  Which RP tiers to draw as threshold lines
        output_dir:           Directory for saved PNG
        show_plot:            Display interactively
        save_plot:            Save PNG to output_dir
    """
    print("=" * 60)
    print("GLOFAS -- GAUGE HYDROGRAPH")
    print("=" * 60)

    g = _load_glofas_zarr(zarr_path)

    if cell_lat is None or cell_lon is None:
        cell_lat, cell_lon = find_glofas_active_cell(zarr_path)

    # Nearest stored cell to the requested point (sparse array, no direct index)
    dist2 = (g['cell_lat'] - cell_lat) ** 2 + (g['cell_lon'] - cell_lon) ** 2
    idx = int(np.argmin(dist2))
    actual_lat, actual_lon = float(g['cell_lat'][idx]), float(g['cell_lon'][idx])

    series = g['data'][:, :, idx]              # (member, step)
    lead_hours = g['leadtime_hours']
    x = np.array(lead_hours)

    threshold_vals = _glofas_thresholds_at_point(actual_lat, actual_lon, threshold_local_dir, rps)

    print(f"  Cell: lat={actual_lat:.2f}, lon={actual_lon:.2f}")
    print(f"  Peak across all members/days: {series.max():.0f} m3/s")
    print(f"  Thresholds found: {', '.join(f'RP{rp}yr={v:.0f}' for rp, v in threshold_vals.items()) or 'none'}")

    fig = plt.figure(figsize=(11, 6.5), dpi=DPI)
    ax = fig.add_axes([0.08, 0.1, 0.88, 0.8])

    # Locator inset: regional context map with a marker at the gauge, so the
    # chart doesn't require already knowing what lat/lon 22.18, 113.43 means.
    pad = 12.0
    inset_bounds = (max(actual_lon - pad, -180), min(actual_lon + pad, 180),
                    max(actual_lat - pad, -60), min(actual_lat + pad, 60))
    inset_ax = fig.add_axes([0.70, 0.68, 0.27, 0.27], projection=ccrs.PlateCarree())
    _setup_glofas_map(inset_ax, inset_bounds, regional=True, show_labels=False)
    inset_ax.plot(actual_lon, actual_lat, marker='*', markersize=14, color='red',
                  markeredgecolor='black', markeredgewidth=0.6,
                  transform=ccrs.PlateCarree(), zorder=5)
    inset_ax.set_title(f"lat={actual_lat:.2f}, lon={actual_lon:.2f}", fontsize=8, pad=3)
    for spine in inset_ax.spines.values():
        spine.set_edgecolor('#666666')
        spine.set_linewidth(0.8)

    for m in range(series.shape[0]):
        ax.plot(x, series[m], color='#4C72B0', alpha=0.18, linewidth=1, zorder=2)

    median = np.median(series, axis=0)
    lo, hi = series.min(axis=0), series.max(axis=0)
    ax.fill_between(x, lo, hi, color='#4C72B0', alpha=0.12, zorder=1, label='Member min-max range')
    ax.plot(x, median, color='#1B3B6F', linewidth=2.5, zorder=4, label='Ensemble median')

    # Threshold lines: Warning/Danger/Extreme-style palette, lightest RP first
    threshold_colors = ['#F5A623', '#E85D4F', '#8B1A1A', '#4B0082', '#000000']
    threshold_labels_map = {'2.0': 'Warning', '5.0': 'Danger', '20.0': 'Extreme'}
    for i, rp in enumerate(rps):
        if rp not in threshold_vals:
            continue
        color = threshold_colors[i % len(threshold_colors)]
        label = threshold_labels_map.get(rp, f'RP{rp}yr')
        ax.axhline(threshold_vals[rp], color=color, linewidth=2, zorder=3,
                  label=f'{label} (RP{rp}yr, {threshold_vals[rp]:.0f} m3/s)')

    ax.set_xlabel('Lead time (hours)', fontsize=11)
    ax.set_ylabel('Discharge (m3/s)', fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels([f'+{h}h' for h in x])
    ax.set_ylim(bottom=0)
    ax.grid(alpha=0.25)
    ax.legend(loc='upper left', fontsize=9, framealpha=0.9)

    title = f"GloFAS Gauge Hydrograph -- lat={actual_lat:.2f}, lon={actual_lon:.2f}"
    title += f"\nIssued: {g['forecast_date']} | {series.shape[0]} members"
    ax.set_title(title, fontsize=13, fontweight='bold', pad=12)

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filepath = Path(output_dir) / f"glofas_gauge_{actual_lat:.2f}_{actual_lon:.2f}.png"
        plt.savefig(filepath, dpi=DPI, facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def show_glofas_gauge_hydrograph(zarr_path, cell_lat=None, cell_lon=None,
                                 threshold_local_dir='glofas_data/thresholds_cache', rps=('2.0', '5.0', '20.0')):
    """Quick function to show a single-gauge ensemble hydrograph with threshold lines."""
    return visualize_glofas_gauge_hydrograph(zarr_path, cell_lat=cell_lat, cell_lon=cell_lon,
                                             threshold_local_dir=threshold_local_dir, rps=rps,
                                             show_plot=True, save_plot=False)


def visualize_glofas_ensemble_mean(zarr_path, step_h=72, bounds=None,
                                   output_dir=DEFAULT_OUTPUT_DIR,
                                   show_plot=True, save_plot=False):
    """
    Global scatter map of ensemble mean river discharge at a given lead hour.

    Args:
        zarr_path:  Path to river_YYYYMMDD.zarr.zip
        step_h:     Lead hour shown (24, 48, ..., 168)
        bounds:     Optional (lon_min, lon_max, lat_min, lat_max); defaults global
        output_dir: Directory for saved PNG
        show_plot:  Display interactively
        save_plot:  Save PNG to output_dir
    """
    print("=" * 60)
    print("GLOFAS -- ENSEMBLE MEAN DISCHARGE")
    print("=" * 60)

    g = _load_glofas_zarr(zarr_path)
    sidx = _glofas_step_index(g['leadtime_hours'], step_h)

    mean_val = g['data'][:, sidx, :].mean(axis=0)  # (n_cells,) across 51 members
    dense, lats_1d, lons_1d = _glofas_sparse_to_dense(g['cell_lat'], g['cell_lon'], mean_val)

    bounds = bounds or GLOFAS_DEFAULT_BOUNDS
    dense, lats_1d, lons_1d = _glofas_crop(dense, lats_1d, lons_1d, bounds)
    n_total = g['n_cells_total']
    cells_note = f"{g['n_cells_kept']:,}" + (f" of {n_total:,} total" if n_total else "")
    print(f"  Lead: +{step_h}h  |  Cells: {cells_note}  |  Max mean discharge: {mean_val.max():.0f} m3/s")

    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    ax = plt.axes(projection=ccrs.PlateCarree())
    _setup_glofas_map(ax, bounds, regional=(bounds != GLOFAS_DEFAULT_BOUNDS))

    # Log color scale: discharge spans <1 to >200,000 m3/s
    from matplotlib.colors import LogNorm
    vmax = max(float(np.nanpercentile(dense, 99)), 1.0)
    mesh = ax.pcolormesh(lons_1d, lats_1d, np.clip(dense, 0.1, None),
                        cmap='YlGnBu', norm=LogNorm(vmin=1, vmax=vmax),
                        transform=ccrs.PlateCarree(), zorder=2, shading='auto')
    plt.colorbar(mesh, ax=ax, orientation='vertical', pad=0.02,
                 label='Ensemble mean discharge (m3/s, log scale)', shrink=0.7)

    title = f"GloFAS Ensemble Mean River Discharge -- +{step_h}h"
    title += (f"\nIssued: {g['forecast_date']} | {len(g['member_numbers'])} members"
              f" | filtered to {g['filter_threshold']}")
    plt.title(title, fontsize=14, fontweight='bold', pad=15)

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filepath = Path(output_dir) / f"glofas_mean_{step_h}h.png"
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_glofas_period_panels(zarr_path, lead_hours=None,
                                   output_dir=DEFAULT_OUTPUT_DIR,
                                   show_plot=True, save_plot=False):
    """
    Small-multiples panel: ensemble MAX discharge across several lead times,
    showing how the forecast evolves day by day.

    Args:
        zarr_path:   Path to river_YYYYMMDD.zarr.zip
        lead_hours:  List of lead hours to show (default: first, ~1/3, ~2/3, last
                     of the available steps, typically [24, 72, 120, 168])
        output_dir:  Directory for saved PNG
        show_plot:   Display interactively
        save_plot:   Save PNG to output_dir
    """
    print("=" * 60)
    print("GLOFAS -- MULTI-DAY ENSEMBLE MAX PANELS")
    print("=" * 60)

    g = _load_glofas_zarr(zarr_path)
    steps = g['leadtime_hours']
    if lead_hours is None:
        n = len(steps)
        idxs = sorted(set([0, n // 3, 2 * n // 3, n - 1]))
        lead_hours = [steps[i] for i in idxs]

    from matplotlib.colors import LogNorm
    all_max = g['data'].max(axis=0)  # (step, n_cells), for a shared color scale
    vmax = max(float(np.percentile(all_max, 99)), 1.0)

    n_panels = len(lead_hours)
    ncols = min(n_panels, 2)
    nrows = (n_panels + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(FIGSIZE[0], FIGSIZE[1] * nrows / 2),
                             dpi=DPI, subplot_kw={'projection': ccrs.PlateCarree()})
    axes = np.atleast_1d(axes).flatten()

    for i, h in enumerate(lead_hours):
        ax = axes[i]
        sidx = _glofas_step_index(steps, h)
        max_val = g['data'][:, sidx, :].max(axis=0)
        dense, lats_1d, lons_1d = _glofas_sparse_to_dense(g['cell_lat'], g['cell_lon'], max_val)
        _setup_glofas_map(ax, GLOFAS_DEFAULT_BOUNDS)
        mesh = ax.pcolormesh(lons_1d, lats_1d, np.clip(dense, 0.1, None),
                            cmap='YlOrRd', norm=LogNorm(vmin=1, vmax=vmax),
                            transform=ccrs.PlateCarree(), zorder=2, shading='auto')
        ax.set_title(f"+{h}h  (max discharge: {max_val.max():.0f} m3/s)", fontsize=11)

    for j in range(n_panels, len(axes)):
        axes[j].axis('off')

    fig.colorbar(mesh, ax=axes[:n_panels].tolist(), orientation='horizontal',
                pad=0.05, label='Ensemble max discharge (m3/s, log scale)', shrink=0.6)
    fig.suptitle(f"GloFAS Ensemble Max Discharge by Lead Time -- issued {g['forecast_date']}",
                fontsize=14, fontweight='bold')

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filepath = Path(output_dir) / "glofas_period_panels.png"
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_glofas_exceedance(zarr_path, threshold_local_dir='glofas_data/thresholds_cache',
                                rp='2.0', step_h=72, bounds=None,
                                output_dir=DEFAULT_OUTPUT_DIR,
                                show_plot=True, save_plot=False):
    """
    Probability map: fraction of ensemble members exceeding the official RPx
    threshold at a given lead hour. Requires the threshold file to already be
    cached locally (setup_glofas_thresholds.py --local-only); this is the same
    file the pipeline itself depends on for its sparse cell filter.

    Args:
        zarr_path:            Path to river_YYYYMMDD.zarr.zip
        threshold_local_dir:  Local dir containing rl_{rp}.nc
        rp:                   Return period tier, e.g. '2.0', '5.0', '20.0'
        step_h:               Lead hour shown
        bounds:               Optional (lon_min, lon_max, lat_min, lat_max) to zoom
                               in; see find_glofas_hotspot() to locate one automatically
        output_dir:           Directory for saved PNG
        show_plot:            Display interactively
        save_plot:            Save PNG to output_dir
    """
    zoomed = bounds is not None
    print("=" * 60)
    print(f"GLOFAS -- RP{rp}yr EXCEEDANCE PROBABILITY (+{step_h}h)" + ("  [ZOOMED]" if zoomed else ""))
    print("=" * 60)

    threshold_path = Path(threshold_local_dir) / f"rl_{rp}.nc"
    if not threshold_path.exists():
        print(f"  RP{rp} threshold file not found at {threshold_path} -- skipping.")
        print(f"  Run: python3 setup_glofas_thresholds.py --local-only {threshold_local_dir}")
        return None

    g = _load_glofas_zarr(zarr_path)
    sidx = _glofas_step_index(g['leadtime_hours'], step_h)
    disch = g['data'][:, sidx, :]  # (member, n_cells)

    thr_at_cells = _lookup_threshold_at_cells(threshold_path, rp, g['cell_lat'], g['cell_lon'])

    valid = thr_at_cells > 0
    exceed_pct = np.zeros(len(g['cell_lat']))
    exceed_pct[valid] = (disch[:, valid] > thr_at_cells[valid]).mean(axis=0) * 100

    dense, lats_1d, lons_1d = _glofas_sparse_to_dense(g['cell_lat'], g['cell_lon'], exceed_pct)
    plot_bounds = bounds or GLOFAS_DEFAULT_BOUNDS
    dense, lats_1d, lons_1d = _glofas_crop(dense, lats_1d, lons_1d, plot_bounds)

    print(f"  Cells with any threshold data : {valid.sum():,} of {len(g['cell_lat']):,}")
    print(f"  Cells with >50% members exceeding RP{rp}yr : {(exceed_pct > 50).sum():,}")

    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    ax = plt.axes(projection=ccrs.PlateCarree())
    _setup_glofas_map(ax, plot_bounds, regional=zoomed)

    mesh = ax.pcolormesh(lons_1d, lats_1d, dense,
                        cmap='YlOrRd', vmin=0, vmax=100,
                        transform=ccrs.PlateCarree(), zorder=2, shading='auto')
    plt.colorbar(mesh, ax=ax, orientation='vertical', pad=0.02,
                label=f'P(discharge > RP{rp}yr) at +{step_h}h  [%]', shrink=0.7)

    title = f"GloFAS RP{rp}yr Exceedance Probability -- +{step_h}h" + (" (zoomed to hotspot)" if zoomed else "")
    title += f"\nIssued: {g['forecast_date']} | {len(g['member_numbers'])} members"
    plt.title(title, fontsize=14, fontweight='bold', pad=15)

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        tag = "_zoomed" if zoomed else ""
        filepath = Path(output_dir) / f"glofas_exceedance_rp{rp}_{step_h}h{tag}.png"
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def show_glofas_ensemble_mean(zarr_path, step_h=72, bounds=None):
    """Quick function to show ensemble mean discharge map."""
    return visualize_glofas_ensemble_mean(zarr_path, step_h=step_h, bounds=bounds,
                                          show_plot=True, save_plot=False)


def show_glofas_period_panels(zarr_path, lead_hours=None):
    """Quick function to show multi-day ensemble max panels."""
    return visualize_glofas_period_panels(zarr_path, lead_hours=lead_hours,
                                          show_plot=True, save_plot=False)


def show_glofas_exceedance(zarr_path, threshold_local_dir='glofas_data/thresholds_cache', rp='2.0', step_h=72, bounds=None):
    """Quick function to show RP exceedance probability map."""
    return visualize_glofas_exceedance(zarr_path, threshold_local_dir=threshold_local_dir,
                                       rp=rp, step_h=step_h, bounds=bounds,
                                       show_plot=True, save_plot=False)


def visualize_glofas_extent_mask(zarr_path, jrc_local_dir='glofas_data/jrc_extent_cache', rp='100.0',
                                 step_h=168, threshold_local_dir='glofas_data/thresholds_cache',
                                 bounds=None, compare_naive=True,
                                 output_dir=DEFAULT_OUTPUT_DIR, show_plot=True, save_plot=False):
    """
    GloFAS x JRC extent-masking demo: calls the REAL production functions from
    glofas_extent_masking.py directly (compute_tier_probabilities, resolve_jrc_cache,
    combine_tier), not a reimplementation, so this shows the actual pipeline step.

    Naively multiplying a whole GloFAS 0.05deg (~5km) cell's population by its raw
    exceedance probability overstates exposure. A GloFAS cell is a channel/discharge
    property, not a locally-uniform hazard the way a rain cell is. combine_naive=True
    shows why, side by side: the naive whole-cell probability vs. the same probability
    masked down to JRC's actual flood-extent footprint within each cell.

    RP2/RP5 have no native JRC map (JRC only covers RP10 and up), they use RP10's
    own extent as a labeled upper-bound stand-in, noted in the title when applicable.

    Requires: the RP threshold file for `rp` cached locally (setup_glofas_thresholds.py)
    AND the JRC cache for this tier + permanent-water populated in jrc_local_dir
    (setup_jrc_extents.py --local-only, or GLOFAS_JRC_LOCAL_DIR default location).
    """
    from glofas_downloader import EXTENT_RP_LEVELS
    from glofas_extent_masking import (
        compute_tier_probabilities, resolve_jrc_cache, combine_tier, EXTENT_SOURCE_TIER, IS_STANDIN,
    )
    import rasterio as _rasterio

    standin_note = " [RP10 stand-in -- no native JRC map at this tier]" if IS_STANDIN.get(rp) else ""
    print("=" * 60)
    print(f"GLOFAS x JRC -- EXTENT-MASKED RP{rp}yr (+{step_h}h){standin_note}")
    print("=" * 60)

    if rp not in EXTENT_RP_LEVELS:
        print(f"  rp must be one of {EXTENT_RP_LEVELS}")
        return None

    jrc_paths = resolve_jrc_cache('local', jrc_local_dir)
    source_tier = EXTENT_SOURCE_TIER[rp]
    if source_tier not in jrc_paths or 'water' not in jrc_paths:
        print(f"  JRC cache for RP{source_tier} depth and/or permanent-water not found in {jrc_local_dir}.")
        print(f"  Run: python3 setup_jrc_extents.py --local-only {jrc_local_dir} "
              f"(or point jrc_local_dir at an existing cache)")
        return None

    g = _load_glofas_zarr(zarr_path)
    sidx = _glofas_step_index(g['leadtime_hours'], step_h)

    # Pre-filter to cells within the JRC cache's own raster bounds (+0.05deg pad)
    # BEFORE computing probabilities
    with _rasterio.open(jrc_paths[source_tier]) as _src:
        jb = _src.bounds
    pad = 0.05  # one GloFAS cell width
                # right at the raster edge to still have part of their own
                # footprint inside it; a looser pad only adds cells that
                # combine_tier will (correctly) skip anyway, noisily
    in_jrc = ((g['cell_lat'] >= jb.bottom - pad) & (g['cell_lat'] <= jb.top + pad) &
              (g['cell_lon'] >= jb.left - pad) & (g['cell_lon'] <= jb.right + pad))
    cell_lat = g['cell_lat'][in_jrc]
    cell_lon = g['cell_lon'][in_jrc]
    data = g['data'][:, :, in_jrc]
    print(f"  {int(in_jrc.sum())} of {len(g['cell_lat']):,} candidate cells fall within the "
          f"cached JRC coverage ({jrc_local_dir}) -- restricting to those before combining")

    prob_by_tier = compute_tier_probabilities(
        cell_lat, cell_lon, data, len(g['member_numbers']),
        threshold_source='local', threshold_local_dir=threshold_local_dir,
    )
    prob = prob_by_tier[rp]  # (n_steps, n_cells), fraction of members exceeding RP{rp}
    keep_mask = (prob > 0).any(axis=0)
    if not keep_mask.any():
        print(f"  No cells exceed RP{rp}yr anywhere in this forecast -- try a lower tier or a different date.")
        return None

    out_path = Path(output_dir) / f"_demo_extent_rp{rp}.tif"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    info = combine_tier(rp, cell_lat, cell_lon, prob, keep_mask, jrc_paths,
                        g['leadtime_hours'], out_path)
    print(f"  Extent-masked: {info['n_cells']} GloFAS cells, {info['n_pixels']:,} combined pixels "
          f"(using JRC RP{source_tier} extent + permanent-water mask)")

    with _rasterio.open(out_path) as src:
        band = src.read(sidx + 1)
        transform = src.transform
    h, w = band.shape
    lons_1d = transform.c + transform.a * (np.arange(w) + 0.5)
    lats_1d = transform.f + transform.e * (np.arange(h) + 0.5)
    plot_bounds = bounds or (float(lons_1d.min()), float(lons_1d.max()),
                             float(lats_1d.min()), float(lats_1d.max()))

    lat_idx = np.where((lats_1d >= plot_bounds[2]) & (lats_1d <= plot_bounds[3]))[0]
    lon_idx = np.where((lons_1d >= plot_bounds[0]) & (lons_1d <= plot_bounds[1]))[0]
    masked_pct = np.where(band > 0, band * 100, np.nan)[np.ix_(lat_idx, lon_idx)]
    lats_crop, lons_crop = lats_1d[lat_idx], lons_1d[lon_idx]

    if compare_naive:
        exceed_pct = np.zeros(len(cell_lat))
        exceed_pct[keep_mask] = prob[sidx][keep_mask] * 100
        naive_dense, naive_lats, naive_lons = _glofas_sparse_to_dense(cell_lat, cell_lon, exceed_pct)
        naive_dense, naive_lats, naive_lons = _glofas_crop(naive_dense, naive_lats, naive_lons, plot_bounds)

        naive_pixels = int(np.nansum(~np.isnan(naive_dense) & (naive_dense > 0)))
        masked_pixels = int(np.nansum(~np.isnan(masked_pct) & (masked_pct > 0)))
        print(f"  Naive whole-cell footprint: {naive_pixels:,} pixels at 0.05deg native resolution "
              f"(not directly comparable pixel-for-pixel to the {masked_pixels:,} JRC-resolution "
              f"pixels above -- different grids -- but illustrates the same 'whole cell vs. real "
              f"extent' gap the README's 84x/2x overstatement numbers quantify precisely)")

    fig = plt.figure(figsize=(FIGSIZE[0] * (1.8 if compare_naive else 1), FIGSIZE[1]), dpi=DPI)

    if compare_naive:
        ax1 = fig.add_subplot(1, 2, 1, projection=ccrs.PlateCarree())
        _setup_glofas_map(ax1, plot_bounds, regional=True)
        mesh1 = ax1.pcolormesh(naive_lons, naive_lats, naive_dense, cmap='YlOrRd', vmin=0, vmax=100,
                               transform=ccrs.PlateCarree(), zorder=2, shading='auto')
        plt.colorbar(mesh1, ax=ax1, orientation='horizontal', pad=0.05, shrink=0.8,
                    label=f'Naive whole-cell P(exceed RP{rp}yr)  [%]')
        ax1.set_title("Naive: whole 5km GloFAS cell\n(overstates exposure)", fontsize=11)
        ax2 = fig.add_subplot(1, 2, 2, projection=ccrs.PlateCarree())
    else:
        ax2 = plt.axes(projection=ccrs.PlateCarree())

    _setup_glofas_map(ax2, plot_bounds, regional=True)
    mesh2 = ax2.pcolormesh(lons_crop, lats_crop, masked_pct, cmap='YlOrRd', vmin=0, vmax=100,
                           transform=ccrs.PlateCarree(), zorder=2, shading='auto')
    plt.colorbar(mesh2, ax=ax2, orientation='horizontal', pad=0.05, shrink=0.8,
                label=f'Extent-masked P(exceed RP{rp}yr)  [%]')
    ax2.set_title(f"GloFAS x JRC extent-masked (RP{source_tier} extent)\nreal flood-prone area only", fontsize=11)

    suptitle = f"GloFAS x JRC Flood-Extent Masking -- RP{rp}yr -- +{step_h}h{standin_note}"
    plt.suptitle(suptitle, fontsize=14, fontweight='bold')

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filepath = Path(output_dir) / f"glofas_extent_mask_rp{rp}_{step_h}h.png"
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def show_glofas_extent_mask(zarr_path, jrc_local_dir='glofas_data/jrc_extent_cache', rp='100.0', step_h=168,
                            threshold_local_dir='glofas_data/thresholds_cache', bounds=None, compare_naive=True):
    """Quick function to show the GloFAS x JRC extent-masked result."""
    return visualize_glofas_extent_mask(zarr_path, jrc_local_dir=jrc_local_dir, rp=rp, step_h=step_h,
                                        threshold_local_dir=threshold_local_dir, bounds=bounds,
                                        compare_naive=compare_naive, show_plot=True, save_plot=False)


def visualize_glofas_member_extent(zarr_path, jrc_local_dir='glofas_data/jrc_extent_cache', rp='100.0',
                                    step_h=168, members=(1, 2), threshold_local_dir='glofas_data/thresholds_cache',
                                    bounds=None, output_dir=DEFAULT_OUTPUT_DIR, show_plot=True, save_plot=False):
    """
    GloFAS x JRC PER-MEMBER flood-extent demo

    Shows 2+ specific ensemble members' own flood-extent footprints side by
    side

    Requires: the RP threshold file for `rp` cached locally
    (setup_glofas_thresholds.py) AND the JRC cache for this tier +
    permanent-water populated in jrc_local_dir (setup_jrc_extents.py
    --local-only, or GLOFAS_JRC_LOCAL_DIR default location).
    """
    from glofas_downloader import EXTENT_RP_LEVELS
    from glofas_extent_masking import (
        compute_tier_member_exceedance, resolve_jrc_cache, combine_tier_per_member_parquet,
        EXTENT_SOURCE_TIER, IS_STANDIN,
    )
    import rasterio as _rasterio

    standin_note = " [RP10 stand-in -- no native JRC map at this tier]" if IS_STANDIN.get(rp) else ""
    print("=" * 60)
    print(f"GLOFAS x JRC -- PER-MEMBER FLOOD EXTENT RP{rp}yr (+{step_h}h){standin_note}")
    print("=" * 60)

    if rp not in EXTENT_RP_LEVELS:
        print(f"  rp must be one of {EXTENT_RP_LEVELS}")
        return None

    jrc_paths = resolve_jrc_cache('local', jrc_local_dir)
    source_tier = EXTENT_SOURCE_TIER[rp]
    if source_tier not in jrc_paths or 'water' not in jrc_paths:
        print(f"  JRC cache for RP{source_tier} depth and/or permanent-water not found in {jrc_local_dir}.")
        print(f"  Run: python3 setup_jrc_extents.py --local-only {jrc_local_dir} "
              f"(or point jrc_local_dir at an existing cache)")
        return None

    g = _load_glofas_zarr(zarr_path)

    # Same pre-filter as visualize_glofas_extent_mask() and for the same reason
    # restrict to cells within the cached JRC raster's own bounds before
    # computing anything, since cells outside it can never combine anyway.
    with _rasterio.open(jrc_paths[source_tier]) as _src:
        jb = _src.bounds
    pad = 0.05  # one GloFAS cell width
    in_jrc = ((g['cell_lat'] >= jb.bottom - pad) & (g['cell_lat'] <= jb.top + pad) &
              (g['cell_lon'] >= jb.left - pad) & (g['cell_lon'] <= jb.right + pad))
    cell_lat = g['cell_lat'][in_jrc]
    cell_lon = g['cell_lon'][in_jrc]
    data = g['data'][:, :, in_jrc]
    print(f"  {int(in_jrc.sum())} of {len(g['cell_lat']):,} candidate cells fall within the "
          f"cached JRC coverage ({jrc_local_dir}) -- restricting to those before combining")

    exceed_by_tier = compute_tier_member_exceedance(
        cell_lat, cell_lon, data, threshold_source='local', threshold_local_dir=threshold_local_dir,
    )
    exceed = exceed_by_tier[rp]  # (n_members, n_steps, n_cells)
    keep_mask = exceed.any(axis=(0, 1))
    if not keep_mask.any():
        print(f"  No cells exceed RP{rp}yr anywhere in this forecast -- try a lower tier or a different date.")
        return None

    member_numbers = np.asarray(g['member_numbers'])
    out_path = Path(output_dir) / f"_demo_extent_rp{rp}_bymember.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    info = combine_tier_per_member_parquet(rp, cell_lat, cell_lon, exceed_by_tier, keep_mask,
                                            jrc_paths, g['leadtime_hours'], member_numbers, out_path)
    print(f"  Per-member extent: {info['n_cells']} GloFAS cells, {info['n_rows']:,} flooded "
          f"pixel/member/step rows (using JRC RP{source_tier} extent + permanent-water mask)")

    table = pd.read_parquet(out_path)
    step_rows = table[table['step_h'] == step_h]

    plot_bounds = bounds or (float(cell_lon.min()), float(cell_lon.max()),
                             float(cell_lat.min()), float(cell_lat.max()))

    n_members = len(members)
    fig = plt.figure(figsize=(FIGSIZE[0] * n_members * 0.9, FIGSIZE[1]), dpi=DPI)
    for i, member_no in enumerate(members):
        ax = fig.add_subplot(1, n_members, i + 1, projection=ccrs.PlateCarree())
        _setup_glofas_map(ax, plot_bounds, regional=True)
        member_rows = step_rows[step_rows['member'] == member_no]
        n_px = len(member_rows)
        if n_px:
            ax.scatter(member_rows['pixel_lon'], member_rows['pixel_lat'], s=1.5, c='#c0392b',
                       marker='s', transform=ccrs.PlateCarree(), zorder=2, alpha=0.8)
        ax.set_title(f"Member {member_no}\n{n_px:,} flooded pixels", fontsize=11)

    suptitle = f"GloFAS x JRC Per-Member Flood Extent -- RP{rp}yr -- +{step_h}h{standin_note}"
    plt.suptitle(suptitle, fontsize=14, fontweight='bold')

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filepath = Path(output_dir) / f"glofas_member_extent_rp{rp}_{step_h}h.png"
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def show_glofas_member_extent(zarr_path, jrc_local_dir='glofas_data/jrc_extent_cache', rp='100.0', step_h=168,
                               members=(1, 2), threshold_local_dir='glofas_data/thresholds_cache', bounds=None):
    """Quick function to show per-member GloFAS x JRC flood extent, side by side."""
    return visualize_glofas_member_extent(zarr_path, jrc_local_dir=jrc_local_dir, rp=rp, step_h=step_h,
                                          members=members, threshold_local_dir=threshold_local_dir,
                                          bounds=bounds, show_plot=True, save_plot=False)


# ─── Gust Envelope Visualizations ────────────────────────────────────────────

# Gust threshold colors (oranges/reds, distinct from wind blues/greens)
GUST_THRESHOLD_COLORS = {
    17: '#FED8B1',   # Light orange: Gale force          (~34 kt equivalent)
    21: '#FFA500',   # Orange: Storm force         (~40 kt equivalent)
    26: '#FF4500',   # OrangeRed: Violent storm       (~50 kt equivalent)
    33: '#8B0000',   # Dark red: Hurricane force     (~64 kt equivalent)
    43: '#6A0DAD',   # Purple: Cat-2 gust          (~83 kt equivalent)
    49: '#4B0082',   # Indigo: Cat-3 gust          (~96 kt equivalent)
    58: '#00008B',   # Dark blue: Cat-4 gust          (~113 kt equivalent)
    70: '#000000',   # Black: Cat-5 gust          (~137 kt equivalent)
}

GUST_THRESHOLD_LABELS = {
    17: 'Gale Force Gust (17 m/s)',
    21: 'Storm Force Gust (21 m/s)',
    26: 'Violent Storm Gust (26 m/s)',
    33: 'Hurricane Force Gust (33 m/s)',
    43: 'Cat-2 Gust (43 m/s)',
    49: 'Cat-3 Gust (49 m/s)',
    58: 'Cat-4 Gust (58 m/s)',
    70: 'Cat-5 Gust (70 m/s)',
}


def visualize_individual_gust_envelopes(csv_file, output_dir=DEFAULT_OUTPUT_DIR,
                                        show_plot=True, save_plot=False, member=1):
    """
    Visualize individual gust envelopes per forecast step for one ensemble member.

    Args:
        csv_file (str): Path to individual gust envelopes CSV (gust_threshold column in m/s)
        output_dir (str): Output directory
        show_plot (bool): Whether to display the plot
        save_plot (bool): Whether to save the plot
        member (int): Ensemble member to display

    Returns:
        str: Path to saved file (if saved)
    """
    print("=" * 60)
    print("INDIVIDUAL GUST ENVELOPES")
    print("=" * 60)

    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} records from {Path(csv_file).name}")

    df_polygons = df[df['envelope_region'].notna() & (df['envelope_region'] != '')].copy()
    print(f"Found {len(df_polygons)} records with polygons")

    if df_polygons.empty:
        print("No polygons found to visualize!")
        return None

    storm_name = df_polygons['track_id'].iloc[0]
    forecast_time = df_polygons['forecast_time'].iloc[0]

    # Fall back to first available member if requested member has no data
    available_members = sorted(df_polygons['ensemble_member'].unique())
    if member not in available_members:
        print(f"Member {member} not in data (available: {available_members[:5]}{'...' if len(available_members) > 5 else ''}); using member {available_members[0]}")
        member = available_members[0]
    member_df = df_polygons[df_polygons['ensemble_member'] == member]

    if 'lead_time' in member_df.columns:
        step_col = 'lead_time'
    elif 'forecast_step' in member_df.columns:
        step_col = 'forecast_step'
    else:
        step_col = None

    forecast_steps = (sorted(member_df[step_col].unique()) if step_col
                      else list(range(len(member_df))))

    print(f"Storm: {storm_name}")
    print(f"Forecast: {forecast_time}")
    print(f"Member: {member}")
    print(f"Forecast steps: {len(forecast_steps)}")

    n_steps = len(forecast_steps)
    n_cols = min(4, n_steps)
    n_rows = (n_steps + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 4, n_rows * 3),
                             subplot_kw={'projection': ccrs.PlateCarree()})

    if n_steps == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = axes if n_steps > 1 else [axes]
    else:
        axes = axes.flatten()

    _polys = []
    for s in member_df['envelope_region'].dropna():
        try:
            p = wkt.loads(s)
            if p and not p.is_empty:
                _polys.append(p)
        except Exception:
            pass
    bounds = get_bounds_from_data(_polys) if _polys else get_bounds_from_data(member_df)

    for i, step in enumerate(forecast_steps):
        ax = axes[i]
        setup_map(ax, bounds)

        step_df = (member_df[member_df[step_col] == step] if step_col
                   else member_df.iloc[[i]] if i < len(member_df) else member_df.iloc[0:0])

        thresholds_present = []
        for _, row in step_df.iterrows():
            try:
                polygon = wkt.loads(row['envelope_region'])
                threshold = row['gust_threshold']
                if polygon and not polygon.is_empty:
                    color = GUST_THRESHOLD_COLORS.get(threshold, '#FFA500')
                    plot_polygon_on_map(ax, polygon, color, alpha=0.6)
                    thresholds_present.append(threshold)
            except Exception:
                continue

        ax.set_title(f"Step {step}h\n{len(thresholds_present)} thresholds",
                     fontsize=10, fontweight='bold')

    for i in range(n_steps, len(axes)):
        axes[i].set_visible(False)

    fig.suptitle(f"Storm: {storm_name} -- Member {member} Gust Envelopes\nForecast: {forecast_time}",
                 fontsize=14, fontweight='bold')

    if n_steps > 0:
        legend_patches = [plt.Rectangle((0, 0), 1, 1,
                                        facecolor=GUST_THRESHOLD_COLORS[t],
                                        label=GUST_THRESHOLD_LABELS[t])
                          for t in sorted(GUST_THRESHOLD_COLORS.keys())]
        legend = axes[0].legend(handles=legend_patches, loc='center left', fontsize=7,
                                framealpha=0.9, title='Gust Thresholds', bbox_to_anchor=(1.05, 0.5))
        legend.set_bbox_to_anchor((1.05, 0.5))

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{storm_name}_gust_individual_envelopes.png"
        filepath = Path(output_dir) / filename
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_combined_gust_envelopes(csv_file, output_dir=DEFAULT_OUTPUT_DIR,
                                      show_plot=True, save_plot=False, max_members=3):
    """
    Visualize combined gust envelopes (unioned across all forecast steps).

    Args:
        csv_file (str): Path to combined gust envelopes CSV (gust_threshold column in m/s)
        output_dir (str): Output directory
        show_plot (bool): Whether to display the plot
        save_plot (bool): Whether to save the plot
        max_members (int): Maximum number of members to show

    Returns:
        str: Path to saved file (if saved)
    """
    print("=" * 60)
    print("COMBINED GUST ENVELOPES")
    print("=" * 60)

    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} records from {Path(csv_file).name}")

    df_polygons = df[df['envelope_region'].notna() & (df['envelope_region'] != '')].copy()
    print(f"Found {len(df_polygons)} records with polygons")

    if df_polygons.empty:
        print("No polygons found to visualize!")
        return None

    storm_name = df_polygons['track_id'].iloc[0]
    forecast_time = df_polygons['forecast_time'].iloc[0]
    members = sorted(df_polygons['ensemble_member'].unique())[:max_members]

    print(f"Storm: {storm_name}")
    print(f"Forecast: {forecast_time}")
    print(f"Showing {len(members)} members")

    n_members = len(members)
    n_cols = min(3, n_members)
    n_rows = (n_members + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 5, n_rows * 4),
                             subplot_kw={'projection': ccrs.PlateCarree()})

    if n_members == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = axes if n_members > 1 else [axes]
    else:
        axes = axes.flatten()

    all_polygons = []
    for _, row in df_polygons.iterrows():
        try:
            polygon = wkt.loads(row['envelope_region'])
            if polygon and not polygon.is_empty:
                all_polygons.append(polygon)
        except Exception:
            continue
    bounds = get_bounds_from_data(all_polygons)

    for i, member in enumerate(members):
        ax = axes[i]
        setup_map(ax, bounds)

        member_df = df_polygons[df_polygons['ensemble_member'] == member]

        thresholds_present = []
        for _, row in member_df.iterrows():
            try:
                polygon = wkt.loads(row['envelope_region'])
                threshold = row['gust_threshold']
                if polygon and not polygon.is_empty:
                    color = GUST_THRESHOLD_COLORS.get(threshold, '#FFA500')
                    plot_polygon_on_map(ax, polygon, color, alpha=0.6)
                    thresholds_present.append(threshold)
            except Exception:
                continue

        ax.set_title(f"Member {member}\n{len(thresholds_present)} thresholds",
                     fontsize=12, fontweight='bold')

        if thresholds_present:
            ax.text(0.02, 0.98,
                    f"{min(thresholds_present)}–{max(thresholds_present)} m/s",
                    transform=ax.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    for i in range(n_members, len(axes)):
        axes[i].set_visible(False)

    fig.suptitle(f"Storm: {storm_name} -- Combined Gust Envelopes\nForecast: {forecast_time}",
                 fontsize=16, fontweight='bold')

    if n_members > 0:
        legend_patches = [plt.Rectangle((0, 0), 1, 1,
                                        facecolor=GUST_THRESHOLD_COLORS[t],
                                        label=GUST_THRESHOLD_LABELS[t])
                          for t in sorted(GUST_THRESHOLD_COLORS.keys())]
        legend = axes[0].legend(handles=legend_patches, loc='center left', fontsize=8,
                                framealpha=0.9, title='Gust Thresholds', bbox_to_anchor=(1.05, 0.5))
        legend.set_bbox_to_anchor((1.05, 0.5))

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{storm_name}_gust_combined_envelopes.png"
        filepath = Path(output_dir) / filename
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def visualize_wind_vs_gust_comparison(wind_individual_csv, gust_individual_csv,
                                      output_dir=DEFAULT_OUTPUT_DIR,
                                      show_plot=True, save_plot=False,
                                      wind_kt=34, gust_ms=17, member=1, max_steps=8,
                                      steps=None):
    """
    Overlay sustained-wind and gust envelopes at the same physical threshold (~17 m/s /
    34 kt) for a single ensemble member to illustrate that gust envelopes extend further.

    Args:
        wind_individual_csv (str): Path to wind individual envelopes CSV (wind_threshold in kt)
        gust_individual_csv (str): Path to gust individual envelopes CSV (gust_threshold in m/s)
        output_dir (str): Output directory
        show_plot (bool): Whether to display the plot
        save_plot (bool): Whether to save the plot
        wind_kt (int): Wind threshold in knots to compare (default 34 kt ≈ 17 m/s)
        gust_ms (int): Gust threshold in m/s to compare (default 17 m/s)
        member (int): Ensemble member to display
        max_steps (int): Maximum number of lead-time panels to show

    Returns:
        str: Path to saved file (if saved)
    """
    print("=" * 60)
    print(f"WIND vs GUST COMPARISON  ({wind_kt} kt sustained  vs  {gust_ms} m/s gust)")
    print("=" * 60)

    df_wind_all = pd.read_csv(wind_individual_csv)
    df_gust_all = pd.read_csv(gust_individual_csv)

    # Find a member with data in both datasets at the requested thresholds;
    # fall back to first member that has at least gust data
    wind_members = set(
        df_wind_all.loc[df_wind_all['wind_threshold'] == wind_kt, 'ensemble_member'].unique()
    )
    gust_members = set(
        df_gust_all.loc[df_gust_all['gust_threshold'] == gust_ms, 'ensemble_member'].unique()
    )
    both_members = sorted(wind_members & gust_members)
    if member not in (wind_members | gust_members):
        fallback = both_members[0] if both_members else sorted(gust_members or wind_members)[0]
        print(f"Member {member} has no data at these thresholds; using member {fallback}")
        member = fallback
    elif member not in both_members:
        if both_members:
            print(f"Member {member} missing from one dataset; using member {both_members[0]} (has both)")
            member = both_members[0]

    # Filter to selected member and threshold
    df_wind = df_wind_all[
        (df_wind_all['ensemble_member'] == member) &
        (df_wind_all['wind_threshold'] == wind_kt) &
        df_wind_all['envelope_region'].notna() &
        (df_wind_all['envelope_region'] != '')
    ].copy()

    df_gust = df_gust_all[
        (df_gust_all['ensemble_member'] == member) &
        (df_gust_all['gust_threshold'] == gust_ms) &
        df_gust_all['envelope_region'].notna() &
        (df_gust_all['envelope_region'] != '')
    ].copy()

    print(f"Wind ({wind_kt} kt): {len(df_wind)} steps with polygons")
    print(f"Gust ({gust_ms} m/s): {len(df_gust)} steps with polygons")

    if df_wind.empty and df_gust.empty:
        print("No polygons found for either dataset!")
        return None

    # Storm metadata
    storm_name = ''
    if not df_wind.empty and 'track_id' in df_wind.columns:
        storm_name = df_wind['track_id'].iloc[0]
    elif not df_gust.empty and 'track_id' in df_gust.columns:
        storm_name = df_gust['track_id'].iloc[0]

    forecast_time = ''
    if not df_wind.empty and 'forecast_time' in df_wind.columns:
        forecast_time = df_wind['forecast_time'].iloc[0]
    elif not df_gust.empty and 'forecast_time' in df_gust.columns:
        forecast_time = df_gust['forecast_time'].iloc[0]

    step_col_w = 'lead_time' if 'lead_time' in df_wind.columns else 'forecast_step'
    step_col_g = 'lead_time' if 'lead_time' in df_gust.columns else 'forecast_step'

    wind_steps = set(df_wind[step_col_w].unique()) if not df_wind.empty else set()
    gust_steps = set(df_gust[step_col_g].unique()) if not df_gust.empty else set()
    if steps is not None:
        all_steps = sorted(s for s in steps if s in (wind_steps | gust_steps))
    else:
        all_steps = sorted(wind_steps | gust_steps)[:max_steps]

    print(f"Showing {len(all_steps)} lead-time panels")

    # Overall bounding box
    all_polys = []
    for df_src in [df_wind, df_gust]:
        for s in df_src['envelope_region'].dropna():
            try:
                p = wkt.loads(s)
                if p and not p.is_empty:
                    all_polys.append(p)
            except Exception:
                pass
    bounds = get_bounds_from_data(all_polys) if all_polys else (-70, -50, 10, 30)

    n_steps = len(all_steps)
    if n_steps == 0:
        print("No lead-time steps found!")
        return None

    n_cols = min(4, n_steps)
    n_rows = (n_steps + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 4, n_rows * 3),
                             subplot_kw={'projection': ccrs.PlateCarree()})

    if n_steps == 1:
        axes = [axes]
    elif n_rows == 1:
        axes = axes if n_steps > 1 else [axes]
    else:
        axes = axes.flatten()

    WIND_COLOR = '#4169E1'    # Royal blue: sustained wind
    GUST_COLOR = '#FF4500'    # OrangeRed: gust

    for i, step in enumerate(all_steps):
        ax = axes[i]
        setup_map(ax, bounds)

        # Gust envelope drawn first (background) so wind core sits on top as a visible inset
        for _, row in df_gust[df_gust[step_col_g] == step].iterrows():
            try:
                polygon = wkt.loads(row['envelope_region'])
                if polygon and not polygon.is_empty:
                    plot_polygon_on_map(ax, polygon, GUST_COLOR, alpha=0.35, linewidth=1.2)
            except Exception:
                pass

        # Wind envelope (blue, drawn on top so the sustained-wind core is clearly visible)
        for _, row in df_wind[df_wind[step_col_w] == step].iterrows():
            try:
                polygon = wkt.loads(row['envelope_region'])
                if polygon and not polygon.is_empty:
                    plot_polygon_on_map(ax, polygon, WIND_COLOR, alpha=0.55, linewidth=1.2)
            except Exception:
                pass

        ax.set_title(f"Step {step}h", fontsize=10, fontweight='bold')

    for i in range(n_steps, len(axes)):
        axes[i].set_visible(False)

    fig.suptitle(
        f"Storm: {storm_name} -- Sustained Wind ({wind_kt} kt) vs Gust ({gust_ms} m/s) -- Member {member}\n"
        f"Forecast: {forecast_time}  |  Gust envelopes extend beyond sustained-wind envelopes",
        fontsize=12, fontweight='bold'
    )

    # Legend on first subplot
    legend_patches = [
        plt.Rectangle((0, 0), 1, 1, facecolor=WIND_COLOR,
                       label=f'Sustained wind  ≥ {wind_kt} kt  (~{gust_ms} m/s)'),
        plt.Rectangle((0, 0), 1, 1, facecolor=GUST_COLOR,
                       label=f'Wind gust  ≥ {gust_ms} m/s'),
    ]
    axes[0].legend(handles=legend_patches, loc='center left', fontsize=8,
                   framealpha=0.9, title='Layer', bbox_to_anchor=(1.05, 0.5))

    filepath = None
    if save_plot:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{storm_name}_wind_vs_gust_comparison.png"
        filepath = Path(output_dir) / filename
        plt.tight_layout()
        plt.savefig(filepath, dpi=DPI, bbox_inches='tight', facecolor='white')
        print(f"✓ Saved: {filepath}")

    if show_plot:
        plt.tight_layout()
        plt.show()
    else:
        plt.close()

    return str(filepath) if filepath else None


def show_gust_individual(csv_file, output_dir=DEFAULT_OUTPUT_DIR, member=1):
    """Quick function to show individual gust envelopes per forecast step."""
    return visualize_individual_gust_envelopes(csv_file, output_dir,
                                               show_plot=True, save_plot=False, member=member)


def show_gust_combined(csv_file, output_dir=DEFAULT_OUTPUT_DIR):
    """Quick function to show combined gust envelopes."""
    return visualize_combined_gust_envelopes(csv_file, output_dir,
                                             show_plot=True, save_plot=False)


def show_wind_vs_gust_comparison(wind_individual_csv, gust_individual_csv,
                                 output_dir=DEFAULT_OUTPUT_DIR, member=1,
                                 steps=None, max_steps=8):
    """Quick function to overlay sustained-wind and gust envelopes for comparison."""
    return visualize_wind_vs_gust_comparison(wind_individual_csv, gust_individual_csv,
                                             output_dir, show_plot=True, save_plot=False,
                                             member=member, steps=steps, max_steps=max_steps)
