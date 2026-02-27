# How `build_node_coordinates` Works

## The Problem

ERCOT publishes electricity prices at named **settlement points** (e.g., `AJAXWIND_RN`, `STP_STP_G1`). To do anything spatial with these prices — overlay them on weather data, plot them on a map, or build a geographically-informed forecast model — we need latitude/longitude coordinates for each settlement point.

ERCOT does not publish a coordinate table. None of ERCOT's 77 public API endpoints return geographic data. The API is purely for time-series market data.

This pipeline solves the problem by combining three independent coordinate sources, each covering a different subset of nodes. The result is a single CSV mapping 544 of the 937 known resource nodes (58%) to lat/lon coordinates.

## Input Data

The pipeline requires three pre-downloaded datasets:

| Dataset | What it provides | How to get it |
|---------|-----------------|---------------|
| **NP4-160** (`raw_data/ercot/np4_160/`) | The master list of 937 resource nodes and their substation names | `uv run python -m download_data.pull_np4160` |
| **ERCOT contour map HTML** (`data/*_html_source.txt`) | Pixel coordinates for ~295 nodes on ERCOT's price maps | Save page source from ERCOT website (see below) |
| **ERCOT KML snapshot** (`data/rtmLmpPoints.kml`) | Lat/lon for 254 nodes from a 2019 ERCOT snapshot | Already included in repo |
| **EIA Form 860** (`raw_data/eia860/texas_plants.csv`) | Lat/lon for 1,369 Texas power plants | `uv run python -m download_data.pull_eia860` |

## The Three Sources (in priority order)

### Source 1: HTML Contour Maps (227 nodes)

ERCOT publishes four live contour map pages showing real-time and day-ahead prices on a map of Texas. Each page renders a 600x600 pixel PNG with an HTML `<area>` image map overlay. The `<area>` tags encode each node's name and pixel position:

```html
<area shape="circle" coords="460,328,6" title="SWEETWN2_2:   $7.05" alt=""/>
```

Here `460,328` are pixel coordinates on the image. The four pages are:

| Page | URL | Description |
|------|-----|-------------|
| `rtmLmp` | `/content/cdr/contours/rtmLmp.html` | Real-time LMP (bus-level) |
| `rtmSpp` | `/content/cdr/contours/rtmSpp.html` | Real-time SPP (settlement point) |
| `damSpp2` | `/content/cdr/contours/damSpp2.html` | Day-ahead SPP (2-hour) |
| `damSpp7` | `/content/cdr/contours/damSpp7.html` | Day-ahead SPP (7-day) |

Each page shows ~253 nodes, but they are overlapping subsets — some nodes appear on only one page. Combined, they yield **295 unique nodes**.

**Converting pixels to lat/lon.** We use the 2019 KML file (Source 2) as ground truth. Of the 295 HTML nodes, 212 also appear in the KML with known lat/lon. These 212 common nodes serve as ground control points for a least-squares affine transformation:

```
lat = 36.797 + 0.000005 * x  -  0.01876 * y
lon = -107.01 + 0.02311 * x  -  0.000004 * y
```

The near-zero cross terms confirm that the image is a simple axis-aligned map projection (x maps almost purely to longitude, y maps almost purely to latitude). Accuracy on the control points: **mean error 0.9 km, max 1.4 km**.

Of the 295 converted nodes, 227 match a current NP4-160 resource node name.

### Source 2: KML Snapshot (18 additional nodes)

A 2019 snapshot of ERCOT's contour map KML file provides authoritative lat/lon for 254 settlement points. This file was formerly served live at `ercot.com/content/cdr/contours/rtmLmpPoints.kml`, but all KML endpoints now return 404. A cached copy was retrieved from [GitHub](https://github.com/arnavgautam/ERCOT-Data-Forecasting/blob/master/rtmLmpPoints.kml).

After filtering out nodes already covered by the HTML source, the KML provides **18 additional matches** for nodes that existed in 2019 but no longer appear on the current contour maps.

The KML also serves as the calibration reference for the HTML pixel-to-lat/lon transformation described above.

### Source 3: EIA Form 860 Name Matching (299 additional nodes)

For the ~700 resource nodes not covered by either map source, we attempt to match ERCOT substation names (from NP4-160) to EIA Form 860 power plant names. EIA 860 provides lat/lon for every registered U.S. power plant.

ERCOT substation names are abbreviated (e.g., `SWEETWN2`), while EIA plant names are full text (e.g., `Sweetwater 2 Wind Farm`). Both names are normalized by uppercasing and stripping non-alphanumeric characters, then matched using three strategies applied in order:

1. **Prefix match** (~179 nodes): The normalized ERCOT name is a prefix of the normalized EIA name. Example: `SWEETWN` matches `SWEETWATER2WINDFARM`.

2. **Substring containment** (~52 nodes): The ERCOT name appears anywhere within the EIA name. Catches cases where the plant name has a longer prefix. Example: `BOSQUE` matches `LAKEBOSQUEPEAKINGSTATION`.

3. **Fuzzy match** (~68 nodes): Uses Python's `difflib.get_close_matches()` with a similarity cutoff of 0.7 (on a 0-1 scale). Handles abbreviation differences and minor spelling variations. Example: `ANSON1` matches `ANSON`.

Before matching, common ERCOT suffixes (`ESS`, `BESS`, `SLR`, `SOLAR`, `WND`, `WIND`) are stripped from substation names to improve match rates — these suffixes indicate technology type rather than location.

**Why 0.7 cutoff?** Testing showed that a 0.6 cutoff produces false positives (e.g., `ANSON1` matching `Hanson` instead of `Anson`). At 0.7, matches are reliable enough to use without manual review.

## Output

Running the pipeline produces three files in `processed_data/`:

| File | Contents |
|------|----------|
| `node_coordinates.csv` | 544 matched resource nodes with lat, lon, plant_name, match_method |
| `unmatched_ercot_settlement_points.csv` | 393 resource nodes with no coordinate match |
| `unmatched_eia860_plants.csv` | 1,144 EIA plants with no ERCOT match |

The `match_method` column indicates which source provided each coordinate:

| Method | Count | Description |
|--------|-------|-------------|
| `html_contour` | 227 | Current ERCOT contour map HTML (pixel-to-lat/lon) |
| `kml` | 18 | 2019 KML snapshot |
| `prefix` | 179 | EIA 860 prefix name match |
| `fuzzy` | 68 | EIA 860 fuzzy name match |
| `contains` | 52 | EIA 860 substring name match |

## Usage

```python
from process_ercot import build_node_coordinates

# First run builds from scratch (~5 seconds)
coords = build_node_coordinates(force_rebuild=True)

# Subsequent runs load from cache
coords = build_node_coordinates()

# coords is a DataFrame:
#   settlement_point     lat       lon   plant_name   match_method
#   AJAXWIND_RN       32.8214  -100.1234  Ajax Wind    html_contour
#   STP_STP_G1        28.7954   -96.0489  South Texas  kml
#   ...
```

## Refreshing the HTML Source Files

The four HTML files in `data/` are snapshots of live ERCOT pages. To update them:

1. Open each URL in a browser:
   - `https://www.ercot.com/content/cdr/contours/rtmLmp.html`
   - `https://www.ercot.com/content/cdr/contours/rtmSpp.html`
   - `https://www.ercot.com/content/cdr/contours/damSpp2.html`
   - `https://www.ercot.com/content/cdr/contours/damSpp7.html`
2. View Page Source and save as `data/{rtmLmp,rtmSpp,damSpp2,damSpp7}_html_source.txt`
3. Re-run `build_node_coordinates(force_rebuild=True)`

Or use curl:
```bash
for page in rtmLmp rtmSpp damSpp2 damSpp7; do
    curl -s "https://www.ercot.com/content/cdr/contours/${page}.html" \
         -o "data/${page}_html_source.txt"
done
```

New nodes added to ERCOT's maps will be automatically picked up on the next rebuild.

## What the Unmatched 42% Are

The 393 unmatched resource nodes are not random failures. They fall into a few categories:

- **No map presence**: Many small or new generators don't appear on the ~295-node contour maps, and their abbreviated substation names are too short or ambiguous for EIA matching.
- **Non-generation nodes**: Some resource nodes represent demand response, energy storage, or other non-plant resources that have no EIA 860 entry.
- **Name divergence**: ERCOT substation names can differ substantially from EIA plant names, especially for facilities that have been renamed or merged.

The `unmatched_ercot_settlement_points.csv` file lists all unmatched nodes with their substation names for manual review.

## Possible Improvements

- **CRR Network Model KML**: Available through ERCOT's MIS portal after free [IMRE registration](https://www.ercot.com/services/rq/imre). Contains geographic polygons for every bus in the ERCOT network. A parser exists at [pvvm_pvtos](https://github.com/patrickbrown4/pvvm_pvtos). This would cover close to 100% of nodes.
- **EIA 860 generator-level matching**: Using the `3_1_Generator*.xlsx` sheet instead of plant-level data would allow matching by individual unit names rather than plant names.
- **Manual review**: The unmatched CSVs are designed for human review. Even matching 20-30 nodes manually would meaningfully improve coverage.
