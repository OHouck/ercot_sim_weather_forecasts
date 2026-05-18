# functions shared across files
import socket
import os

import numpy as np

# T90 -> turbine >90 MW, LE90 -> <= 90
THERMAL_RESOURCE_TYPES = {
    "CCGT90", "CCLE90", # combined-cycle gas turbines > or <= 90 MW
    "SCGT90", "SCLE90", # simple-cycle gas turbines > or <= 90 MW
    "GSREH", "GSNONR", "GSSUP", # gas steam resources (REH: reheatable, NONR: non-reheatable, SUP: supercritical)
    "CLLIG" # lignite coal 
}


def setup_directories():
    """Set up directory structure based on environment."""
    nodename = socket.gethostname()
    if nodename == "oMac.local":
        root = os.path.expanduser(f"/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts")

    else:
        raise Exception(f"Unknown environment, Please specify the root directory. "
                        f"Nodename found: {nodename}")

    dirs = {
        'root': root,
        'raw': os.path.join(root, 'raw_data'),
        'processed': os.path.join(root, 'processed_data'),
        'figures': os.path.join(root, 'figures'),
        'tables': os.path.join(root, 'tables'),
    }

    for path in dirs.values():
        os.makedirs(path, exist_ok=True)
    return dirs


# ── Weather-zone shapefile path ──────────────────────────────────────────────

DEFAULT_WEATHER_ZONE_SHP = (
    '/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/'
    'ercot_sim_weather_forecasts/Texas_GIS_Data/Weather Zone/Weather_Zone.shp'
)

_ZONE_NAME_MAP = {
    'coast': 'coast',
    'east': 'east',
    'farwest': 'far_west',
    'far_west': 'far_west',
    'north': 'north',
    'northc': 'north_central',
    'northcentral': 'north_central',
    'north_central': 'north_central',
    'south': 'south',
    'southern': 'south',
    'southc': 'south_central',
    'southcentral': 'south_central',
    'south_central': 'south_central',
    'west': 'west',
}


def normalize_weather_zone_name(name):
    """Normalize weather-zone naming across shapefile and ERCOT datasets."""
    base = str(name).strip().lower().replace('-', '_').replace(' ', '_')
    return _ZONE_NAME_MAP.get(base)


def map_pixels_to_weather_zones(lats, lons, weather_zone_shp=None):
    """Map arrays of lat/lon coordinates to ERCOT weather zones.

    Uses a spatial join of point geometries against weather-zone polygons.

    Parameters
    ----------
    lats, lons : array-like
        Latitude and longitude arrays (same length).
    weather_zone_shp : str, optional
        Path to weather-zone shapefile. Defaults to DEFAULT_WEATHER_ZONE_SHP.

    Returns
    -------
    numpy array of str
        Normalized weather zone names (same length as inputs).
        NaN for points outside all zones.
    """
    import geopandas as gpd

    if weather_zone_shp is None:
        weather_zone_shp = DEFAULT_WEATHER_ZONE_SHP

    zones = gpd.read_file(weather_zone_shp)
    zone_col = None
    for candidate in ['Zone_name', 'zone_name', 'ZONE_NAME']:
        if candidate in zones.columns:
            zone_col = candidate
            break
    if zone_col is None:
        raise ValueError("Could not find zone name column in weather-zone shapefile.")

    zones = zones[[zone_col, 'geometry']].copy()
    zones['weather_zone'] = zones[zone_col].apply(normalize_weather_zone_name)
    zones = zones.dropna(subset=['weather_zone']).to_crs('EPSG:4326')

    points = gpd.GeoDataFrame(
        {'lat': lats, 'lon': lons},
        geometry=gpd.points_from_xy(lons, lats),
        crs='EPSG:4326',
    )

    joined = gpd.sjoin(points, zones[['weather_zone', 'geometry']], how='left', predicate='intersects')

    # Deduplicate first (point on polygon boundary can match multiple zones)
    joined = joined[~joined.index.duplicated(keep='first')]

    # Fallback: nearest zone for points that didn't intersect any polygon
    missing_idx = joined.index[joined['weather_zone'].isna()]
    if len(missing_idx) > 0:
        missing_pts = points.loc[missing_idx].copy()
        nearest = gpd.sjoin_nearest(
            missing_pts.to_crs('EPSG:3857'),
            zones[['weather_zone', 'geometry']].to_crs('EPSG:3857'),
            how='left',
        )
        # Deduplicate nearest results too
        nearest = nearest[~nearest.index.duplicated(keep='first')]
        joined.loc[missing_idx, 'weather_zone'] = nearest['weather_zone'].values

    return joined['weather_zone'].values