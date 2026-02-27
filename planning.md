# Next steps

Simple linear regression analysis in analysis/local_county_lr.py

Create hourly, county level data. Columns are county name, and then mean, max, and standard deviation of LMP, temperature, wind speed, temperature forecast error, and wind speed forecast error. 

The script should have one function called prepare_county_level_data that aggregates the forecast_error point files from process_data/calculate_forecast_errors with the matched node price data from process_ercot.py. This will require loading in both points geodataframes, downloading and loading in a shapefile for texas counties, and creating collapses of the weather and price variables at the county level. The final output of this function should be a county level polygon geodataframe. 

Then following the qmd example below, I would like the script to allow me to test running multiple different regressions using these varibales. To start I would like to predict mean lmp price using temperature error, wind speed error, temperature level, and windspeed level and county fixed effects with standard errors clustered at the county level. Use the pyfixest package for regressions.


# Example linear regression qmd notebook:
---
title: "Linear Regression Analysis: Dry Spells and Rice Yields in India"
format:
  html:
    code-fold: false
    code-tools: true
jupyter: python3
---

## Overview

Linear Regression analysis of how dry spells of different lengths affect rice yields in India.

**Models:**

1. **Base**: Effect of dry spell measures on ln_yield, controlling for unit FE + time trends
2. **Weather controls**: Same as Model 1 but additionally controlling for KDD
3. **Interactions**: Model 2 + treatment x district-characteristic interactions (heterogeneity)

Uses pyfixest (feols) with unit fixed effects and cluster-robust SEs at the district level.

## Setup

```{python}
#| label: setup
#| echo: true
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyfixest as pf
import os

from helper_funcs import setup_directories

```

```{python}
def load_merged_data(dirs):
    """
    Load the merged rice-dryspell dataset and perform basic cleaning.
    Drops rows missing ln_yield or all dry spell season features.
    """
    path = os.path.join(dirs["processed"], "merged_rice_dryspell.csv")
    df = pd.read_csv(path)

    n_raw = len(df)

    # Drop rows missing outcome
    df = df.dropna(subset=["ln_yield"])
    n_after_yield = len(df)

    # Drop rows missing all season-level dry spell features
    season_cols = [c for c in df.columns if c.endswith("_season")]
    df = df.dropna(subset=season_cols, how="all")
    n_after_dryspell = len(df)

    print(f"Loaded {n_raw} rows, {n_after_yield} with ln_yield, "
          f"{n_after_dryspell} with dry spell data")

    # Fill remaining NaN in dry spell features with 0
    dryspell_cols = [c for c in df.columns if "spells" in c or "spell_length" in c or "dry_days" in c]
    df[dryspell_cols] = df[dryspell_cols].fillna(0)

    return df


def prepare_data(df, depvar, treatments, controls, fe):
    """Drop rows with NaN in any analysis column and return clean DataFrame."""
    all_cols = [depvar] + treatments + controls + [fe]
    df_clean = df.dropna(subset=all_cols).copy()
    print(f"Analysis sample: {len(df_clean)} observations "
          f"(dropped {len(df) - len(df_clean)} with missing values)")
    return df_clean


def build_formula(depvar, treatments, controls, fe="uid", interactions=None):
    """
    Build a pyfixest formula string.

    Args:
        depvar: dependent variable name
        treatments: list of treatment variable names
        controls: list of control variable names
        fe: fixed effect variable (or None)
        interactions: list of (treatment, x_var) tuples for interaction terms
    """
    rhs_parts = treatments + controls
    if interactions:
        for tname, xname in interactions:
            rhs_parts.append(f"{tname}:{xname}")
    rhs = " + ".join(rhs_parts)
    fml = f"{depvar} ~ {rhs}"
    if fe:
        fml += f" | {fe}"
    return fml

STAGES = [
    "nursing", "transplant", "active_tillering",
    "panicle_init", "flowering", "maturity",
]

STAGE_LABELS = {
    "nursing": "Nursing (0–30d)",
    "transplant": "Transplant (31–55d)",
    "active_tillering": "Active Tillering (56–82d)",
    "panicle_init": "Panicle Init. (83–111d)",
    "flowering": "Flowering (112–142d)",
    "maturity": "Maturity (143d+)",
}


def plot_treatment_coefs(model, treatments, title="Treatment Coefficients",
                         max_per_row=8):
    """
    Plot treatment coefficients with 95% CIs as a horizontal point-range plot.

    For models with many treatments, splits into grouped subplots — one row
    per growth stage (if stage-level variables) or per month (if month-level).

    Args:
        model: fitted pyfixest model
        treatments: list of treatment variable names
        title: plot title
        max_per_row: if more treatments than this, group into subplots
    """
    tidy = model.tidy(alpha=0.05)
    tidy = tidy.loc[tidy.index.isin(treatments)]

    if len(tidy) == 0:
        print("No treatment coefficients found in model output")
        return

    def _strip_suffix(name, suffix):
        """Remove a known suffix and clean up the base variable name."""
        base = name[: -len(suffix)].rstrip("_")
        return base.replace("n_spells_", "spells ").replace("_", " ")

    def _group_key(name):
        """Return (group_key, display_label) for a variable name."""
        # Growth-stage suffix takes priority
        for stage in STAGES:
            if name.endswith(f"_{stage}"):
                return stage, STAGE_LABELS[stage]
        # Fall back to calendar month
        for suffix, abbr in {"_m6": "June", "_m7": "July",
                              "_m8": "August", "_m9": "September"}.items():
            if name.endswith(suffix):
                return suffix.lstrip("_"), abbr
        return "other", "Other"

    if len(tidy) <= max_per_row:
        # Simple single plot — label each variable fully
        fig, ax = plt.subplots(figsize=(9, max(3, len(tidy) * 0.55)))
        y_pos = range(len(tidy))

        full_labels = []
        for name in tidy.index:
            group_key, group_label = _group_key(name)
            suffix = f"_{group_key}" if group_key != "other" else ""
            base = _strip_suffix(name, suffix) if suffix else name.replace("_", " ")
            full_labels.append(f"{base} | {group_label}" if group_key != "other" else base)

        coefs = tidy["Estimate"].values
        ci_lo = tidy["2.5%"].values
        ci_hi = tidy["97.5%"].values

        ax.hlines(y_pos, ci_lo, ci_hi, color="steelblue", linewidth=2)
        ax.scatter(coefs, y_pos, color="steelblue", s=50, zorder=5)
        ax.axvline(x=0, color="black", linestyle="--", alpha=0.5)
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(full_labels)
        ax.set_xlabel("Effect on ln(yield)")
        ax.set_title(title)
        ax.invert_yaxis()
        plt.tight_layout()
        plt.show()
    else:
        # Group into subplots by stage / month
        # Preserve the canonical ordering of stages, then months, then other
        ordered_keys = STAGES + ["m6", "m7", "m8", "m9", "other"]
        groups = {}
        for name in tidy.index:
            key, _ = _group_key(name)
            groups.setdefault(key, []).append(name)

        panel_keys = [k for k in ordered_keys if k in groups]

        n_panels = len(panel_keys)
        fig, axes = plt.subplots(n_panels, 1,
                                 figsize=(9, n_panels * 2.2),
                                 sharex=True)
        if n_panels == 1:
            axes = [axes]

        month_labels = {"m6": "June", "m7": "July", "m8": "August", "m9": "September"}

        for ax, key in zip(axes, panel_keys):
            names = groups[key]
            subset = tidy.loc[names]
            y_pos = range(len(subset))

            # Short labels: strip the group suffix since it's in the panel title
            suffix = f"_{key}" if key != "other" else ""
            short_labels = [
                _strip_suffix(n, suffix) if suffix else n.replace("_", " ")
                for n in subset.index
            ]

            panel_title = STAGE_LABELS.get(key, month_labels.get(key, key.title()))

            coefs = subset["Estimate"].values
            ci_lo = subset["2.5%"].values
            ci_hi = subset["97.5%"].values

            ax.hlines(y_pos, ci_lo, ci_hi, color="steelblue", linewidth=2)
            ax.scatter(coefs, y_pos, color="steelblue", s=50, zorder=5)
            ax.axvline(x=0, color="black", linestyle="--", alpha=0.5)
            ax.set_yticks(list(y_pos))
            ax.set_yticklabels(short_labels)
            ax.set_title(panel_title, fontsize=10, fontweight="bold", loc="left")
            ax.invert_yaxis()

        axes[-1].set_xlabel("Effect on ln(yield)")
        fig.suptitle(title, fontsize=12, fontweight="bold")
        plt.tight_layout()
        plt.show()


dirs = setup_directories()

# Load data
df = load_merged_data(dirs)

```

| label: config

── Regression configuration ──
Toggle covariates by adding/removing from these lists.

Available treatments:
  Season-level: max_spell_length_season, n_spells_ge7_season,
    n_spells_ge14_season, n_spells_ge21_season
  Month-level (m6–m9): max_spell_length_m{6-9}, n_spells_ge7_m{6-9}, etc.

Available controls: irrigated_share, total_dry_days_season, kdd, time,
  time_sqr, lr_tmax_crop, lr_prcp_crop, gdd, prcp_poly_1..4, ln_gdppc


Facts learned from basic EDA
- dry spells are constnat over time,
- average of 7 >=3, 4 >= 7 , 2 >= 14, 1 >= 21 per season
- dry spell features are moderately correlated with each other. all positive


Total number of dry days has negative effect on yields, controlling for the total number of dry days, the effect of the single longest spell length is insignificant.  
```{python}
DEPVAR = "ln_yield"
FE = "adm2"
CLUSTER = "adm2"

TREATMENTS = [
    "max_spell_length_season",
]

CONTROLS = [
    "total_dry_days_season",
    "kdd",
    "time",
]
# Prepare clean analysis sample
df_clean = prepare_data(df, DEPVAR, TREATMENTS, CONTROLS, FE)

# ── Model 1: Base specification (unit FE + time trends) ──
print("\n" + "=" * 60)
print("MODEL 1: Dry spells -> Yield (base controls)")
print("=" * 60)
fml1 = build_formula(DEPVAR, TREATMENTS, CONTROLS, fe=FE)
print(f"  Formula: {fml1}")
model1 = pf.feols(fml=fml1, data=df_clean, vcov={"CRV1": CLUSTER})
print(model1.summary())
plot_treatment_coefs(model1, TREATMENTS, title="Model 1: Season-Level Treatments")
```

Now test with max spell length in each growth stage:


Coefficient interpretation: Holding dry days throughout a season fixed, does a concentration of those days in a given growth stage have a stronger effect than if they were spread out.

```{python}
# Model 2: Max spell length by growth stage
DEPVAR = "ln_yield"
FE = "adm2"
CLUSTER = "adm2"

TREATMENTS = [f"max_spell_length_{s}" for s in STAGES]

CONTROLS = [
    "total_dry_days_season",
    "kdd",
    "time",
]

df_clean2 = prepare_data(df, DEPVAR, TREATMENTS, CONTROLS, FE)
fml2 = build_formula(DEPVAR, TREATMENTS, CONTROLS, fe=FE)
print("\n" + "=" * 60)
print("MODEL 2: Dry spells -> Yield (growth-stage treatments)")
print("=" * 60)
print(f"  Formula: {fml2}")
model2 = pf.feols(fml=fml2, data=df_clean2, vcov={"CRV1": CLUSTER})
print(model2.summary())
plot_treatment_coefs(model2, TREATMENTS, title="Model 2: Max Spell Length by Growth Stage")
```


## Max spell length by growth stage w/ stage-level total dry day controls

Controlling for total dry days within each growth stage isolates whether spell
concentration (a single long spell) is more damaging than the same days spread out.

Coefficient interpretation: Holding dry days within each growth stage fixed, is it better for them to be concentrated or spread out?

Take Away: max spell length during transplant is significantly negative

```{python}
# Model 3: Max spell length by stage, controlling for total dry days per stage
DEPVAR = "ln_yield"
FE = "adm2"
CLUSTER = "adm2"

TREATMENTS = [f"max_spell_length_{s}" for s in STAGES]

CONTROLS = ["kdd", "time"] + [f"total_dry_days_{s}" for s in STAGES]

df_clean3 = prepare_data(df, DEPVAR, TREATMENTS, CONTROLS, FE)
fml3 = build_formula(DEPVAR, TREATMENTS, CONTROLS, fe=FE)
print("\n" + "=" * 60)
print("MODEL 3: Max spell length by stage (stage-level dry day controls)")
print("=" * 60)
print(f"  Formula: {fml3}")
model3 = pf.feols(fml=fml3, data=df_clean3, vcov={"CRV1": CLUSTER})
print(model3.summary())
plot_treatment_coefs(model3, TREATMENTS, title="Model 3: Max Spell Length by Stage (controlling for total dry days)")
```


## Spell length bins by growth stage

Which duration thresholds (not just max spell length) matter most, and does this vary by growth stage? 

Results: See some negative coeffs especially for long spells, but data is too messy. 

```{python}
# Model 4: Spell length bins by growth stage
DEPVAR = "ln_yield"
FE = "adm2"
CLUSTER = "adm2"

TREATMENTS = []
for s in STAGES:
    TREATMENTS.extend([
        f"n_spells_ge3_{s}",
        f"n_spells_ge7_{s}",
        f"n_spells_ge14_{s}",
    ])

CONTROLS = ["kdd", "time"] + [f"total_dry_days_{s}" for s in STAGES]

df_clean4 = prepare_data(df, DEPVAR, TREATMENTS, CONTROLS, FE)
fml4 = build_formula(DEPVAR, TREATMENTS, CONTROLS, fe=FE)
print("\n" + "=" * 60)
print("MODEL 4: Spell length bins (≥3, ≥7, ≥14) by growth stage")
print("=" * 60)
print(f"  Formula: {fml4}")
model4 = pf.feols(fml=fml4, data=df_clean4, vcov={"CRV1": CLUSTER})
print(model4.summary())
plot_treatment_coefs(model4, TREATMENTS, title="Model 4: Spell Length Bins by Growth Stage")
```
