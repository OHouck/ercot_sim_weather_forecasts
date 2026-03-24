"""
Assemble all analysis outputs into a unified Typst PDF report.

Reads figures (PNG) and tables (CSV) produced by the other analysis scripts
and compiles them into output/analysis_report.pdf via the typst CLI.

Usage:
    uv run python -m analysis.create_analysis_report

Or call from main.py:
    from analysis.create_analysis_report import create_analysis_report
    create_analysis_report()
"""

import os
import sys
import subprocess
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Expected output manifest
# ---------------------------------------------------------------------------

def _get_expected_files(dirs: dict) -> dict:
    """Return dict mapping key → absolute path for all expected analysis outputs."""
    fig_root = dirs["figures"]
    tab_root = str(ROOT / "tables")
    return {
        # Stage 2: correlation heatmaps
        "corr_heatmap": os.path.join(fig_root, "correlation_heatmaps", "corr_heatmap_2x2.png"),
        # Stage 3: pixel-level regression maps
        "pixel_reg_map": os.path.join(fig_root, "pixel_regressions", "pixel_regression_2x2.png"),
        "pixel_table": os.path.join(tab_root, "pixel_regression_summary.csv"),
        # Stage 4: infrastructure regressions
        "infra_coef": os.path.join(fig_root, "infrastructure_regressions", "coef_plot_main.png"),
        "infra_seasonal": os.path.join(fig_root, "infrastructure_regressions", "coef_plot_seasonal.png"),
        "infra_table": os.path.join(tab_root, "infrastructure_regression_main.csv"),
        # Stage 1: cluster heterogeneity
        "cluster_map": os.path.join(fig_root, "cluster_heterogeneity", "cluster_map.png"),
        "coef_plot": os.path.join(fig_root, "cluster_heterogeneity", "coef_plot_combined.png"),
        "hist_1h": os.path.join(fig_root, "cluster_heterogeneity", "hist_grid_1h.png"),
        "hist_dah": os.path.join(fig_root, "cluster_heterogeneity", "hist_grid_dah.png"),
        "cluster_table": os.path.join(tab_root, "cluster_regression_results.csv"),
    }


# ---------------------------------------------------------------------------
# CSV → Typst table helper
# ---------------------------------------------------------------------------

def _fmt_num(val, decimals=3):
    """Format a numeric value for Typst display."""
    try:
        f = float(val)
        if abs(f) >= 1000:
            return f"{f:,.0f}"
        return f"{f:.{decimals}f}"
    except (TypeError, ValueError):
        return str(val)


def csv_to_typst_table(csv_path: str, caption: str, sig_bold: bool = True,
                       max_rows: int = 50) -> str:
    """
    Read a regression results CSV and return a Typst figure(table(...)) block.

    Expects columns: variable, coef (or coefficient), std_error, p_value, stars
    (plus optional others). Rows where p_value < 0.05 are bolded if sig_bold.
    """
    df = pd.read_csv(csv_path)

    # Normalise column names
    col_map = {
        "coefficient": "coef",
        "Estimate": "coef",
        "Std. Error": "std_error",
        "Pr(>|t|)": "p_value",
        "t value": "t_stat",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Select display columns (keep whatever is present from this priority list)
    display_cols = ["variable"]
    for c in ["season", "cluster", "coef", "std_error", "t_stat", "p_value", "stars", "n_obs"]:
        if c in df.columns:
            display_cols.append(c)
    df = df[display_cols].head(max_rows)

    # Derive stars if missing
    if "stars" not in df.columns and "p_value" in df.columns:
        df["stars"] = df["p_value"].apply(
            lambda p: "***" if p < 0.01 else ("**" if p < 0.05 else ("*" if p < 0.10 else ""))
        )

    headers = [c.replace("_", " ").title() for c in df.columns]
    col_count = len(headers)
    header_cells = ", ".join(f'[*{h}*]' for h in headers)

    rows_typ = []
    for _, row in df.iterrows():
        try:
            is_sig = float(row.get("p_value", 1.0)) < 0.05
        except (TypeError, ValueError):
            is_sig = False

        cells = []
        for c in df.columns:
            val = row[c]
            if c in ("coef", "std_error", "t_stat", "p_value"):
                formatted = _fmt_num(val)
            else:
                formatted = str(val) if pd.notna(val) else ""
            if sig_bold and is_sig and c == "coef":
                cells.append(f"[*{formatted}*]")
            else:
                cells.append(f"[{formatted}]")
        rows_typ.append(", ".join(cells))

    rows_str = ",\n    ".join(rows_typ)
    typ = f"""
#figure(
  table(
    columns: {col_count},
    stroke: 0.5pt,
    align: (left,) + (right,) * {col_count - 1},
    {header_cells},
    {rows_str},
  ),
  caption: [{caption}],
)
"""
    return typ


# ---------------------------------------------------------------------------
# Typst document builder
# ---------------------------------------------------------------------------

def build_typst_source(files: dict, output_dir: str) -> str:
    """
    Build the full Typst source for the analysis report.

    Parameters
    ----------
    files : dict
        Map of key → absolute path; only keys whose files exist are included.
    output_dir : str
        Directory where the .typ file will be written (for relative path computation).
    """

    def rel(path: str) -> str:
        """Return path relative to output_dir, using forward slashes."""
        return os.path.relpath(path, output_dir).replace("\\", "/")

    def img(key: str, width: str = "95%", caption: str = "") -> str:
        if key not in files or not os.path.exists(files[key]):
            return f"// {key} not available\n"
        cap = f", caption: [{caption}]" if caption else ""
        return f'#figure(image("{rel(files[key])}", width: {width}){cap})\n\n'

    def tbl(key: str, caption: str) -> str:
        if key not in files or not os.path.exists(files[key]):
            return f"// {key} not available\n"
        return csv_to_typst_table(files[key], caption) + "\n"

    src = r"""
#set page(paper: "us-letter", margin: (top: 0.75in, bottom: 0.75in, left: 0.75in, right: 0.75in))
#set text(font: "New Computer Modern", size: 10pt)
#set par(justify: true, leading: 0.65em)
#set heading(numbering: "1.1")
#show heading.where(level: 1): it => { pagebreak(weak: true); it }

#align(center)[
  #text(size: 18pt, weight: "bold")[
    Forecast Errors and Electricity Price Dispersion in ERCOT
  ]

  #v(0.15in)
  #text(size: 11pt, fill: luma(80))[
    How do joint errors in short-range (HRRR 1h) and day-ahead (GFS)
    wind and temperature forecasts impact locational marginal prices in ERCOT?
  ]
  #v(0.1in)
  #text(size: 9pt, fill: luma(120))[Full year 2025 | Combined HRRR 1h + GFS day-ahead pipeline]
]

#v(0.3in)

= Introduction

This report examines how forecast errors in two weather models --- the High-Resolution
Rapid Refresh (HRRR, 1-hour ahead) and the Global Forecast System (GFS, day-ahead) ---
affect system-level electricity price dispersion in ERCOT. The analysis uses ERA5-Land
reanalysis as ground truth to compute forecast errors at each ~0.1° grid cell across Texas,
then links those errors to real-time locational marginal prices (LMP) at ERCOT resource nodes.

The key outcome variable throughout is #text(weight: "bold")[system LMP standard deviation]
(system\_lmp\_std), which captures price dispersion across nodes --- a measure of
transmission congestion and spatial market imbalances. Forecast errors are measured for
both temperature (affecting demand) and wind speed (affecting renewable generation).

All regressions absorb hour-of-day and month fixed effects unless otherwise noted.
Standard errors are clustered by date except where noted.

"""

    # ── Section 2: Raw Correlation Heatmaps ──
    src += """= Raw Correlation: Forecast Error vs. LMP Spread

Before controlling for weather conditions and time patterns, we examine the raw
Pearson correlation between each pixel's forecast error and the system-wide LMP
standard deviation. Each panel covers all 12 months of 2025 using all ERA5 pixels
inside Texas.

"""
    src += img(
        "corr_heatmap",
        width="100%",
        caption=(
            "Per-pixel Pearson correlation between forecast error and system LMP "
            "standard deviation (system_lmp_std). Top row: HRRR 1h errors; "
            "bottom row: GFS day-ahead errors. Left column: temperature; "
            "right column: wind speed. Markers show wind (squares), "
            "solar (triangles), and gas (diamonds) generation sites."
        ),
    )

    # ── Section 3: Pixel Regression Maps ──
    src += """= Pixel-Level Controlled Regression Coefficients

Each panel shows the estimated coefficient on a forecast error variable from a
regression run independently for each ERA5 pixel. The regression controls for
observed temperature and wind speed, a weekend dummy, and absorbs hour-of-day
and month fixed effects. Only pixels with statistically significant coefficients
(p < 0.05) are filled; insignificant pixels appear blank.

The four panels correspond to the same 2×2 layout as the correlation maps:
HRRR 1h vs. GFS day-ahead × temperature vs. wind speed error.

"""
    src += img(
        "pixel_reg_map",
        width="100%",
        caption=(
            "Pixel-level OLS coefficient estimates for each forecast error variable "
            "on system LMP standard deviation. Controls: observed ERA5 temperature "
            "and wind speed, weekend indicator. FE: hour-of-day, month. "
            "Only significant pixels (p < 0.05) are colored; colorbar shows "
            "$/MWh per unit forecast error. Blank pixels are not significant."
        ),
    )

    # ── Section 4: Infrastructure Regressions ──
    src += """= Infrastructure-Level Forecast Error Effects

To understand which types of generation infrastructure mediate the relationship
between forecast errors and price dispersion, we aggregate forecast errors to
the infrastructure-category level. For each hour, we compute capacity-weighted
mean forecast errors separately for wind, solar, gas, battery, and coal
generation pixels, and unweighted means for transmission corridor and load-center
pixels. This produces an hour-level dataset where each observation captures
spatially-disaggregated error signals.

The regression includes cross-category interaction terms to capture amplification
effects --- e.g., whether temperature errors at load centers matter more when wind
speed errors at wind farms are simultaneously large.

== Main Regression (Full Year)

"""
    src += img(
        "infra_coef",
        width="95%",
        caption=(
            "Coefficient estimates from the main infrastructure-level regression. "
            "HRRR 1h coefficients shown in blue; GFS day-ahead in orange. "
            "FE: hour-of-day, month. Clustered SE by date. "
            "Outcome: system LMP standard deviation ($/MWh)."
        ),
    )
    src += tbl(
        "infra_table",
        "Infrastructure-level regression results. Dependent variable: system LMP standard deviation. "
        "Bold coefficients are significant at the 5% level.",
    )

    src += "== Seasonal Heterogeneity\n\n"
    src += img(
        "infra_seasonal",
        width="100%",
        caption=(
            "Seasonal subsample regressions. Left: Summer (Jun–Aug); "
            "center: Winter (Dec–Feb); right: Shoulder (Mar–May, Sep–Nov). "
            "FE: hour-of-day only. Clustered SE by date."
        ),
    )

    # ── Section 5: Cluster Heterogeneity ──
    src += """= Cluster Heterogeneity

To capture geographic variation in how forecast errors propagate to prices, we
cluster ERCOT resource nodes using agglomerative clustering with a geographic
contiguity constraint. Nodes are clustered on standardized latitude/longitude
(with geographic weight) plus LMP summary statistics. For each cluster, we run
a joint regression including both HRRR 1h and GFS day-ahead error terms.

== Cluster Map

"""
    src += img(
        "cluster_map",
        width="80%",
        caption=(
            "Geographic distribution of ERCOT resource node clusters. "
            "Formed via agglomerative clustering with geographic connectivity."
        ),
    )

    src += "== Coefficient Estimates by Cluster\n\n"
    src += img(
        "coef_plot",
        width="100%",
        caption=(
            "Per-cluster OLS estimates from a joint regression including both "
            "HRRR 1h (top row) and GFS day-ahead (bottom row) forecast errors. "
            "Error bars: 95% confidence intervals. "
            "FE: hour-of-day, month. Controls: weekday, actual load."
        ),
    )

    src += "== Distribution of Marginal Effects — HRRR 1h\n\n"
    src += img(
        "hist_1h",
        width="100%",
        caption=(
            "Marginal effect distributions for HRRR 1h errors by cluster. "
            "Each histogram shows the distribution of (variable × estimated coefficient), "
            "i.e., the contribution to LMP std dev. "
            "Solid: temperature error; dashed: wind speed error; dotted: load error."
        ),
    )

    src += "== Distribution of Marginal Effects — GFS Day-Ahead\n\n"
    src += img(
        "hist_dah",
        width="100%",
        caption=(
            "Same layout as previous figure but for GFS day-ahead forecast errors."
        ),
    )

    # ── Section 6: GNN placeholder ──
    src += """= Graph Neural Network Results

_Results pending._ A graph attention network (GAT) trained on the ERCOT
transmission topology will be incorporated here once training and evaluation
are complete. The GNN learns to propagate nodal weather features across the
transmission graph to predict locational LMP, capturing non-linear and
network-mediated effects that the linear regressions above cannot recover.

"""

    # ── Appendix ──
    src += """= Appendix

== Cluster Regression Summary Table

"""
    src += tbl(
        "cluster_table",
        "Per-cluster regression coefficients. Joint HRRR 1h + GFS day-ahead specification. "
        "Bold: significant at 5%.",
    )

    return src


# ---------------------------------------------------------------------------
# Compile
# ---------------------------------------------------------------------------

def compile_report(typst_source: str, output_pdf: str) -> bool:
    """Write Typst source and compile to PDF. Returns True on success."""
    typ_path = output_pdf.replace(".pdf", ".typ")
    with open(typ_path, "w", encoding="utf-8") as f:
        f.write(typst_source)

    result = subprocess.run(
        ["typst", "compile", typ_path, output_pdf],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  typst compile FAILED:\n{result.stderr}")
        # Keep .typ for debugging
        return False
    else:
        print(f"  Compiled → {output_pdf}")
        os.remove(typ_path)
        return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def create_analysis_report(output_dir: str = None) -> str:
    """
    Assemble all analysis outputs into a unified Typst PDF report.

    Parameters
    ----------
    output_dir : str, optional
        Directory for the PDF output. Defaults to {ROOT}/output/.

    Returns
    -------
    str : path to the compiled PDF (or .typ if compilation failed).
    """
    dirs = setup_directories()

    if output_dir is None:
        output_dir = str(ROOT / "output")
    os.makedirs(output_dir, exist_ok=True)

    files = _get_expected_files(dirs)

    # Report which files are present / missing
    missing = [k for k, p in files.items() if not os.path.exists(p)]
    present = [k for k in files if k not in missing]
    print(f"\nAnalysis outputs found: {len(present)}/{len(files)}")
    if missing:
        print(f"  Missing (sections will be skipped): {', '.join(missing)}")

    # Build Typst source
    print("\nBuilding Typst source ...")
    typ_src = build_typst_source(files, output_dir)

    # Compile
    pdf_path = os.path.join(output_dir, "analysis_report.pdf")
    print("Compiling report ...")
    success = compile_report(typ_src, pdf_path)

    if success:
        print(f"\nReport ready: {pdf_path}")
        return pdf_path
    else:
        typ_fallback = pdf_path.replace(".pdf", ".typ")
        print(f"\nCompilation failed. Typst source saved at: {typ_fallback}")
        return typ_fallback


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    create_analysis_report()
