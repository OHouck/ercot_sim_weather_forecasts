"""
Build the pixel masks used by the two manual basis-selection methods.

The manual methods in `analysis/main_analysis.py` aggregate weather fields over
fixed sets of pixels ("masks") instead of estimating data-driven basis functions.
This module constructs those masks for an arbitrary pixel grid, given only the
flattened pixel latitude/longitude arrays that come out of
`analysis.pls_analysis_v2.build_block_matrix` — so the same functions work for
both the HRRR/ERA5 0.1-degree grid and the coarser GFS 0.25-degree grid.

Two mask families are produced:

1. Infrastructure masks (`build_infrastructure_pixel_masks`)
   - wind_generation    : pixels containing EIA Form 860 onshore wind capacity
   - thermal_generation : pixels containing large dispatchable thermal capacity
                          (combined-cycle gas, combustion-turbine gas, gas steam,
                          coal, nuclear)
   - load_center        : pixels within 40 km of the centers of the four largest
                          ERCOT metros (Dallas-Fort Worth, Houston, San Antonio,
                          Austin)

2. Weather-zone masks (`build_weather_zone_pixel_masks`)
   - one boolean mask per ERCOT weather zone (8 zones), assigned by
     point-in-polygon against the ERCOT weather-zone shapefile via
     `helper_funcs.map_pixels_to_weather_zones`.

Running the module as a script builds both mask families on both forecast grids
and saves a diagnostic map figure plus per-mask pixel counts, so the masks can
be visually validated before running the main analysis:

    uv run python -m process_data.build_basis_masks
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories, map_pixels_to_weather_zones


# ── Configuration ──────────────────────────────────────────────────────────────

# EIA Form 860 technology names that count as utility-scale wind generation.
WIND_TECHNOLOGIES = ["Onshore Wind Turbine"]

# EIA Form 860 technology names that count as large dispatchable thermal
# generation. Small distributed units (natural-gas internal-combustion engines,
# petroleum liquids) are deliberately excluded: they are numerous, tiny, and
# sited near load, which would blur the thermal mask into the load-center mask.
THERMAL_TECHNOLOGIES = [
    "Natural Gas Fired Combined Cycle",
    "Natural Gas Fired Combustion Turbine",
    "Natural Gas Steam Turbine",
    "Conventional Steam Coal",
    "Nuclear",
]

# A pixel joins the wind (thermal) mask only if the total binned wind (thermal)
# nameplate capacity in that pixel meets this threshold, so isolated tiny units
# do not create single-pixel masks.
MINIMUM_PIXEL_CAPACITY_MW = 10.0

# Centers of the four largest ERCOT metropolitan areas (the DFW metroplex gets
# two anchor points so the 40 km radius covers both Dallas and Fort Worth).
MAJOR_METRO_CENTERS = {
    "Dallas":      (32.7767, -96.7970),
    "Fort Worth":  (32.7555, -97.3308),
    "Houston":     (29.7604, -95.3698),
    "San Antonio": (29.4241, -98.4936),
    "Austin":      (30.2672, -97.7431),
}
LOAD_CENTER_RADIUS_KM = 40.0

EARTH_RADIUS_KM = 6371.0


def build_infrastructure_pixel_masks(pixel_latitudes, pixel_longitudes,
                                     minimum_capacity_mw=MINIMUM_PIXEL_CAPACITY_MW):
    """Boolean infrastructure masks (wind / thermal / load-center) for a pixel grid.

    Wind and thermal masks come from binning EIA Form 860 generator locations to
    the nearest pixel center and thresholding the summed nameplate capacity.
    Load-center pixels are all pixels within LOAD_CENTER_RADIUS_KM of any major
    metro center (great-circle distance).

    Parameters
    ----------
    pixel_latitudes     : ndarray (n_pixels,) — pixel-center latitudes
    pixel_longitudes    : ndarray (n_pixels,) — pixel-center longitudes
    minimum_capacity_mw : float — capacity threshold for wind/thermal membership

    Returns
    -------
    dict {mask_name: ndarray bool (n_pixels,)} with keys
    'wind_generation', 'thermal_generation', 'load_center'.
    """
    from scipy.spatial import cKDTree

    pixel_latitudes  = np.asarray(pixel_latitudes,  dtype=float)
    pixel_longitudes = np.asarray(pixel_longitudes, dtype=float)
    n_pixels = len(pixel_latitudes)

    # Step 1: load the EIA Form 860 generator list (plant lat/lon + technology).
    project_directories = setup_directories()
    eia_generators_path = Path(project_directories["raw"]) / "eia860" / "texas_generators.csv"
    eia_generators = pd.read_csv(eia_generators_path)
    eia_generators = eia_generators.dropna(subset=["lat", "lon", "nameplate_capacity_mw"])

    # Step 2: infer the pixel-grid spacing so a generator is only assigned to a
    # pixel if it actually falls inside that pixel's footprint (nearest-neighbor
    # match capped at ~one grid step). Longitude degrees are compressed by
    # cos(latitude) so the KD-tree distance is approximately isotropic in km.
    grid_step_degrees = float(np.median(np.diff(np.unique(pixel_longitudes))))
    mean_latitude_radians = np.deg2rad(pixel_latitudes.mean())
    longitude_scale = np.cos(mean_latitude_radians)
    pixel_tree = cKDTree(np.column_stack([
        pixel_latitudes, pixel_longitudes * longitude_scale]))

    generator_coordinates = np.column_stack([
        eia_generators["lat"].values,
        eia_generators["lon"].values * longitude_scale,
    ])
    nearest_distance, nearest_pixel_index = pixel_tree.query(generator_coordinates)
    # A generator within ~0.75 grid steps of a pixel center is inside (or on the
    # border of) that pixel; farther generators fall on masked-out (non-ERCOT)
    # cells and are dropped.
    generator_is_on_grid = nearest_distance <= grid_step_degrees * 0.75

    # Step 3: sum nameplate capacity per pixel separately for wind and thermal
    # technologies, then threshold to get boolean masks.
    wind_capacity_per_pixel    = np.zeros(n_pixels)
    thermal_capacity_per_pixel = np.zeros(n_pixels)
    generator_is_wind    = eia_generators["technology"].isin(WIND_TECHNOLOGIES).values
    generator_is_thermal = eia_generators["technology"].isin(THERMAL_TECHNOLOGIES).values
    generator_capacity   = eia_generators["nameplate_capacity_mw"].values
    np.add.at(wind_capacity_per_pixel,
              nearest_pixel_index[generator_is_on_grid & generator_is_wind],
              generator_capacity[generator_is_on_grid & generator_is_wind])
    np.add.at(thermal_capacity_per_pixel,
              nearest_pixel_index[generator_is_on_grid & generator_is_thermal],
              generator_capacity[generator_is_on_grid & generator_is_thermal])

    # Step 4: load-center mask — great-circle (haversine) distance from each
    # pixel to each metro center, keep pixels within the radius of any metro.
    pixel_within_any_metro = np.zeros(n_pixels, dtype=bool)
    pixel_lat_radians = np.deg2rad(pixel_latitudes)
    pixel_lon_radians = np.deg2rad(pixel_longitudes)
    for metro_name, (metro_lat, metro_lon) in MAJOR_METRO_CENTERS.items():
        metro_lat_radians = np.deg2rad(metro_lat)
        metro_lon_radians = np.deg2rad(metro_lon)
        haversine_term = (
            np.sin((pixel_lat_radians - metro_lat_radians) / 2) ** 2
            + np.cos(pixel_lat_radians) * np.cos(metro_lat_radians)
            * np.sin((pixel_lon_radians - metro_lon_radians) / 2) ** 2
        )
        distance_km = 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(haversine_term))
        pixel_within_any_metro |= distance_km <= LOAD_CENTER_RADIUS_KM

    return {
        "wind_generation":    wind_capacity_per_pixel    >= minimum_capacity_mw,
        "thermal_generation": thermal_capacity_per_pixel >= minimum_capacity_mw,
        "load_center":        pixel_within_any_metro,
    }


def build_weather_zone_pixel_masks(pixel_latitudes, pixel_longitudes):
    """Boolean masks assigning each pixel to an ERCOT weather zone.

    Uses `helper_funcs.map_pixels_to_weather_zones` (point-in-polygon against
    the ERCOT weather-zone shapefile, with nearest-zone fallback for pixels on
    polygon boundaries) and converts the zone labels to one boolean mask per
    zone.

    Parameters
    ----------
    pixel_latitudes  : ndarray (n_pixels,) — pixel-center latitudes
    pixel_longitudes : ndarray (n_pixels,) — pixel-center longitudes

    Returns
    -------
    dict {zone_name: ndarray bool (n_pixels,)} — one mask per weather zone,
    ordered by zone name.
    """
    zone_label_per_pixel = map_pixels_to_weather_zones(pixel_latitudes, pixel_longitudes)
    zone_names = sorted(pd.unique(pd.Series(zone_label_per_pixel).dropna()))
    return {zone_name: zone_label_per_pixel == zone_name for zone_name in zone_names}


def main():
    """Build masks on both forecast grids and save a diagnostic map + counts.

    Loads one month of channel fields (enough to define both the HRRR/ERA5 grid
    and the GFS grid), builds every mask on each grid, prints per-mask pixel
    counts, and saves a diagnostic scatter-map figure per grid to
    figures/basis_masks/.
    """
    import matplotlib.pyplot as plt

    # Importing from analysis/ is intentional: the masks must align exactly with
    # the flattened pixel arrays the analysis pipeline builds, so we reuse the
    # same loader instead of re-deriving the grids from raw NetCDF files.
    from analysis.pca_decomposition import load_channel_fields, _draw_texas, _get_cartopy_crs
    from analysis.pls_analysis_v2 import build_block_matrix

    project_directories = setup_directories()
    figure_output_dir = Path(project_directories["figures"]) / "basis_masks"
    figure_output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading one month of channel fields to define the two grids ...")
    channel_bundle = load_channel_fields([(2025, 7)], project_directories)

    # One representative single-channel block per grid (wind channel only — both
    # channels of a block share the same grid up to small land-mask differences).
    grids_to_check = {
        "hrrr_era5_grid": ["wspd100_error_1h"],
        "gfs_grid":       ["wspd100_error_0h"],
    }

    for grid_name, block_fields in grids_to_check.items():
        _, _, pixel_lats, pixel_lons = build_block_matrix(channel_bundle, block_fields)
        print(f"\n=== {grid_name}: {len(pixel_lats)} pixels ===")

        infrastructure_masks = build_infrastructure_pixel_masks(pixel_lats, pixel_lons)
        weather_zone_masks   = build_weather_zone_pixel_masks(pixel_lats, pixel_lons)
        for mask_name, mask in {**infrastructure_masks, **weather_zone_masks}.items():
            print(f"  {mask_name:22s}: {int(mask.sum()):5d} pixels")

        # Diagnostic figure: left panel = infrastructure masks (colored by type,
        # overlaps drawn in mask order), right panel = weather zones.
        cartopy_crs    = _get_cartopy_crs()
        map_projection = cartopy_crs.PlateCarree() if cartopy_crs is not None else None
        subplot_kwargs = {"projection": map_projection} if map_projection is not None else {}
        fig = plt.figure(figsize=(11, 4.5))

        infrastructure_axis = fig.add_subplot(1, 2, 1, **subplot_kwargs)
        infrastructure_axis.scatter(pixel_lons, pixel_lats, s=2, c="#dddddd", rasterized=True)
        for mask_name, mask_color in [("wind_generation", "#2980b9"),
                                      ("thermal_generation", "#c0392b"),
                                      ("load_center", "#27ae60")]:
            mask = infrastructure_masks[mask_name]
            infrastructure_axis.scatter(pixel_lons[mask], pixel_lats[mask], s=4,
                                        c=mask_color, label=mask_name, rasterized=True)
        _draw_texas(infrastructure_axis)
        infrastructure_axis.legend(fontsize=7, loc="lower left")
        infrastructure_axis.set_title(f"Infrastructure masks — {grid_name}", fontsize=9)

        zone_axis = fig.add_subplot(1, 2, 2, **subplot_kwargs)
        zone_colormap = plt.get_cmap("tab10")
        for zone_index, (zone_name, mask) in enumerate(weather_zone_masks.items()):
            zone_axis.scatter(pixel_lons[mask], pixel_lats[mask], s=4,
                              color=zone_colormap(zone_index % 10), label=zone_name,
                              rasterized=True)
        _draw_texas(zone_axis)
        zone_axis.legend(fontsize=6, loc="lower left", ncol=2)
        zone_axis.set_title(f"Weather-zone masks — {grid_name}", fontsize=9)

        figure_path = figure_output_dir / f"basis_masks_{grid_name}.png"
        fig.savefig(figure_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Figure: {figure_path}")


if __name__ == "__main__":
    main()
