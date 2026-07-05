#!/usr/bin/env python3
"""
Visualize combined wind envelopes per ensemble member for a given storm / forecast run.

Fetches TC_ENVELOPES_COMBINED and TC_TRACKS from Snowflake and produces a grid of
subplots — one per ensemble member — showing all available wind threshold envelopes
overlaid with the track.

Usage:
    python visualize_ensemble_envelopes.py --storm MAILA
    python visualize_ensemble_envelopes.py --storm MAILA --date "2026-04-06" --run 00
    python visualize_ensemble_envelopes.py --storm MAILA --date "2026-04-06" --run 00 --out /tmp/out.png
"""

import os
import sys
import argparse
import math
from collections import defaultdict
from pathlib import Path

# Load .env from repo root if present (allows running without sourcing .env)
_env_path = Path(__file__).parent.parent / '.env'
if _env_path.is_file():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith('#') and '=' in _line:
            _line = _line.removeprefix('export').strip()
            _k, _v = _line.split('=', 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import snowflake.connector
from shapely import wkt as shapely_wkt

# Wind threshold → fill color (yellow → dark red, low → high wind speed)
THRESHOLD_COLORS = {
    34:  '#FFFF00',
    40:  '#FFD700',
    50:  '#FFA500',
    64:  '#FF4500',
    83:  '#CC0000',
    96:  '#990000',
    113: '#660000',
}
THRESHOLD_ALPHA = 0.5


def get_connection():
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )
    return conn


def fetch_data(storm: str, forecast_time_filter: str):
    """Fetch envelopes and tracks from Snowflake."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT ENSEMBLE_MEMBER, WIND_THRESHOLD, ST_ASTEXT(ENVELOPE_REGION)
        FROM TC_ENVELOPES_COMBINED
        WHERE TRACK_ID = '{storm}'
          AND FORECAST_TIME LIKE '{forecast_time_filter}'
        ORDER BY WIND_THRESHOLD ASC, ENSEMBLE_MEMBER
    """)
    envelopes = cursor.fetchall()

    cursor.execute(f"""
        SELECT ENSEMBLE_MEMBER, LEAD_TIME, LATITUDE, LONGITUDE, WIND_SPEED_KNOTS
        FROM TC_TRACKS
        WHERE TRACK_ID = '{storm}'
          AND FORECAST_TIME LIKE '{forecast_time_filter}'
        ORDER BY ENSEMBLE_MEMBER, LEAD_TIME
    """)
    tracks = cursor.fetchall()

    cursor.close()
    conn.close()
    return envelopes, tracks


def draw_envelope(ax, region: str, threshold: int):
    """Parse WKT envelope and draw filled polygon(s) on ax."""
    if not region:
        return
    color = THRESHOLD_COLORS.get(threshold, '#888888')
    try:
        geom = shapely_wkt.loads(region)
        polys = list(geom.geoms) if hasattr(geom, 'geoms') else [geom]
        for poly in polys:
            x, y = poly.exterior.xy
            ax.fill(x, y, color=color, alpha=THRESHOLD_ALPHA, zorder=2)
            ax.plot(x, y, color=color, linewidth=0.4, alpha=0.8, zorder=3)
    except Exception:
        pass


def plot_ensemble(storm: str, forecast_time_filter: str, out_path: str):
    envelopes, tracks = fetch_data(storm, forecast_time_filter)

    if not envelopes:
        print(f'No envelope data found for storm={storm} filter={forecast_time_filter}')
        sys.exit(1)

    # Group data by member
    env_by_member = defaultdict(list)
    for member, threshold, region in envelopes:
        env_by_member[member].append((threshold, region))

    track_by_member = defaultdict(list)
    for member, lead, lat, lon, wind in tracks:
        track_by_member[member].append((lat, lon, lead, wind))

    members = sorted(env_by_member.keys())
    thresholds_present = sorted(set(t for _, t, _ in envelopes))

    # Compute shared bounding box from all envelope geometries
    all_lons, all_lats = [], []
    for _, threshold, region in envelopes:
        if not region:
            continue
        try:
            geom = shapely_wkt.loads(region)
            b = geom.bounds  # (minx, miny, maxx, maxy)
            all_lons += [b[0], b[2]]
            all_lats += [b[1], b[3]]
        except Exception:
            pass
    pad = 2
    xlim = (min(all_lons) - pad, max(all_lons) + pad)
    ylim = (min(all_lats) - pad, max(all_lats) + pad)

    ncols = 6
    nrows = math.ceil(len(members) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 4, nrows * 3.5))
    axes = axes.flatten()

    for idx, member in enumerate(members):
        ax = axes[idx]

        # Draw envelopes threshold ascending (low on bottom, high on top)
        for threshold, region in sorted(env_by_member[member], key=lambda x: x[0]):
            draw_envelope(ax, region, threshold)

        # Draw track
        pts = track_by_member[member]
        if pts:
            lats, lons, leads, winds = zip(*pts)
            ax.plot(lons, lats, 'k-', linewidth=1, alpha=0.7, zorder=4)
            ax.plot(lons[0], lats[0], 'ko', markersize=3, zorder=5)
            for lat, lon, lead, wind in zip(lats, lons, leads, winds):
                if lead % 48 == 0:
                    ax.annotate(f'{lead}h', (lon, lat), textcoords='offset points',
                                xytext=(4, 4), fontsize=6, zorder=6)

        ax.set_title(f'M{member}', fontsize=9, pad=3)
        ax.set_xlim(xlim)
        ax.set_ylim(ylim)
        ax.tick_params(labelsize=6)
        ax.grid(True, alpha=0.2)

    for idx in range(len(members), len(axes)):
        axes[idx].set_visible(False)

    legend_handles = [
        mpatches.Patch(color=THRESHOLD_COLORS[t], label=f'{t}kt')
        for t in thresholds_present if t in THRESHOLD_COLORS
    ]
    fig.legend(handles=legend_handles, loc='lower right', ncol=len(legend_handles),
               fontsize=9, title='Wind threshold', bbox_to_anchor=(0.98, 0.01))

    fig.suptitle(
        f'{storm} — Combined wind envelopes per member\n{forecast_time_filter.replace("%", "*")}',
        fontsize=14, y=1.01
    )
    plt.tight_layout()
    plt.savefig(out_path, dpi=110, bbox_inches='tight')
    print(f'Saved: {out_path}')
    plt.show()


def main():
    parser = argparse.ArgumentParser(description='Visualize ensemble wind envelopes from Snowflake')
    parser.add_argument('--storm', required=True, help='Storm name e.g. MAILA')
    parser.add_argument('--date', default=None, help='Forecast date YYYY-MM-DD (default: latest)')
    parser.add_argument('--run', default=None, help='Run time 00/06/12/18 (default: any)')
    parser.add_argument('--out', default=None, help='Output PNG path (default: /tmp/<storm>_ensemble_envelopes.png)')
    args = parser.parse_args()

    if args.date and args.run:
        forecast_filter = f'{args.date} {args.run}%'
    elif args.date:
        forecast_filter = f'{args.date}%'
    else:
        # Use the latest available forecast for this storm
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT MAX(FORECAST_TIME) FROM TC_ENVELOPES_COMBINED
            WHERE TRACK_ID = '{args.storm}'
        """)
        latest = cursor.fetchone()[0]
        cursor.close(); conn.close()
        if not latest:
            print(f'No data found for storm {args.storm}')
            sys.exit(1)
        forecast_filter = str(latest)[:13] + '%'
        print(f'Using latest forecast: {forecast_filter}')

    out_path = args.out or f'/tmp/{args.storm.lower()}_ensemble_envelopes.png'
    plot_ensemble(args.storm, forecast_filter, out_path)


if __name__ == '__main__':
    main()
