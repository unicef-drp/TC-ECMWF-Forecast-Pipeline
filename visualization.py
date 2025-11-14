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


def setup_map(ax, bounds):
    """Setup map with standard features."""
    lon_min, lon_max, lat_min, lat_max = bounds

    # Set map extent
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())

    # Add map features
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, alpha=0.8)
    ax.add_feature(cfeature.BORDERS, linewidth=0.5, alpha=0.6)
    ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.2)
    ax.add_feature(cfeature.LAND, color='lightgray', alpha=0.2)

    # Add gridlines
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False


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

    # Add track legend
    ax.legend(loc='upper right', fontsize=9, framealpha=0.9)

    # Add threshold legend - only show available thresholds (outside the map)
    if available_thresholds:
        legend_patches = []
        for threshold in sorted(available_thresholds):
            legend_patches.append(plt.Rectangle((0, 0), 1, 1,
                                                facecolor=THRESHOLD_COLORS[threshold],
                                                label=f'{threshold} kt'))

        # Create a separate legend outside the map
        legend = ax.legend(handles=legend_patches, loc='center left', fontsize=8,
                           framealpha=0.9, title='Wind Thresholds', bbox_to_anchor=(1.05, 0.5))
        legend.set_bbox_to_anchor((1.05, 0.5))

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

    # Select member
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

    # Set fixed bounding box: lon 129-138°E, lat 9-14°N
    bounds = (129, 138, 9, 14)  # (lon_min, lon_max, lat_min, lat_max)

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
