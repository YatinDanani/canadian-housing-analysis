"""
src/transform.py
~~~~~~~~~~~~~~~~
Feature engineering for the CMHC rental dataset.

Consumes data/processed/cmhc_rental.parquet (output of ingest.py) and
produces data/processed/cmhc_features.parquet — one row per
(city, zone, bedroom_type) with YoY changes, growth rates, and a
composite market-tightness score.

Two granularities are produced:
  - CMA-level  (is_cma_total == True)  → city-level analytics
  - Zone-level (is_cma_total == False) → sub-city heatmaps
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
INPUT_FILE = PROCESSED_DIR / "cmhc_rental.parquet"
OUTPUT_FILE = PROCESSED_DIR / "cmhc_features.parquet"

# Survey dates present in the data.
DATE_OCT24 = pd.Timestamp("2024-10-01")
DATE_OCT25 = pd.Timestamp("2025-10-01")


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def load_processed(path: Path = INPUT_FILE) -> pd.DataFrame:
    """Load the cleaned CMHC rental DataFrame from Parquet.

    Args:
        path: Path to the input Parquet file.

    Returns:
        DataFrame as produced by ``ingest.py``.

    Raises:
        FileNotFoundError: If *path* does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"Processed file not found: {path}")
    return pd.read_parquet(path)


def save_features(df: pd.DataFrame, path: Path = OUTPUT_FILE) -> Path:
    """Persist the feature DataFrame to Parquet.

    Args:
        df: Feature DataFrame to save.
        path: Destination path (must end in ``.parquet``).

    Returns:
        Absolute Path to the written file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"saved {len(df):,} rows → {path}")
    return path


# ---------------------------------------------------------------------------
# Core transformations
# ---------------------------------------------------------------------------


def pivot_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot the long-format rental data into one row per (city, zone, bedroom_type).

    Each metric (vacancy_rate, avg_rent, rental_universe) gets two columns —
    one per survey date — so year-over-year calculations can be done row-wise.

    Args:
        df: Long-format DataFrame from ``load_processed``.

    Returns:
        Wide DataFrame with columns:
        ``city``, ``zone``, ``is_cma_total``, ``bedroom_type``,
        ``vacancy_rate_oct24``, ``vacancy_rate_oct25``,
        ``avg_rent_oct24``,      ``avg_rent_oct25``,
        ``rental_universe_oct24``, ``rental_universe_oct25``.
    """
    metrics = ["vacancy_rate", "avg_rent", "rental_universe"]
    key_cols = ["city", "zone", "is_cma_total", "bedroom_type"]

    # Keep only the columns we need for pivoting.
    sub = df[key_cols + ["survey_date"] + metrics].copy()

    # Pivot on survey_date → one column per (metric, date).
    wide = sub.pivot_table(
        index=key_cols,
        columns="survey_date",
        values=metrics,
        aggfunc="first",
    )

    # Flatten the MultiIndex columns → e.g. ("vacancy_rate", Timestamp) → "vacancy_rate_oct24"
    date_suffix = {DATE_OCT24: "oct24", DATE_OCT25: "oct25"}
    wide.columns = [
        f"{metric}_{date_suffix[date]}"
        for metric, date in wide.columns
    ]
    wide = wide.reset_index()
    return wide


def compute_yoy_changes(wide: pd.DataFrame) -> pd.DataFrame:
    """Add year-over-year absolute and percentage change columns.

    For each of vacancy_rate, avg_rent, and rental_universe the function
    appends ``<metric>_yoy_change`` (Oct-25 minus Oct-24) and
    ``<metric>_yoy_pct`` (percentage change, ``NaN`` when base is zero/null).

    Args:
        wide: Wide-format DataFrame from ``pivot_to_wide``.

    Returns:
        Same DataFrame with six additional columns.
    """
    for metric in ("vacancy_rate", "avg_rent", "rental_universe"):
        col24 = f"{metric}_oct24"
        col25 = f"{metric}_oct25"
        wide[f"{metric}_yoy_change"] = wide[col25] - wide[col24]
        wide[f"{metric}_yoy_pct"] = np.where(
            wide[col24].notna() & (wide[col24] != 0),
            (wide[col25] - wide[col24]) / wide[col24] * 100,
            np.nan,
        )
    return wide


def compute_market_tightness(wide: pd.DataFrame) -> pd.DataFrame:
    """Add a composite market-tightness score for the most-recent survey date.

    The score combines two signals at the CMA / zone level:
      1. **Low vacancy** — a tight market has a low vacancy rate.
      2. **High rent growth** — rapid appreciation signals excess demand.

    Each signal is ranked (1 = tightest) across all (city, bedroom_type) pairs
    with valid data.  The tightness score is the average rank, normalised to
    [0, 1] where **1 = tightest**.

    The column is set to ``NaN`` for rows where either input is missing.

    Args:
        wide: Wide-format DataFrame (after ``compute_yoy_changes``).

    Returns:
        Same DataFrame with a ``market_tightness`` column appended.
    """
    mask = wide["vacancy_rate_oct25"].notna() & wide["avg_rent_yoy_pct"].notna()
    scored = wide[mask].copy()

    # Rank vacancy ascending (lower vacancy → higher rank value after inversion).
    vac_rank = scored["vacancy_rate_oct25"].rank(ascending=True, method="average")
    rent_rank = scored["avg_rent_yoy_pct"].rank(ascending=False, method="average")

    n = len(scored)
    # Invert vacancy rank so that rank 1 (lowest vacancy) → score near 1.
    tightness_raw = ((n + 1 - vac_rank) + rent_rank) / (2 * n)

    wide["market_tightness"] = np.nan
    wide.loc[scored.index, "market_tightness"] = tightness_raw.values
    return wide


def add_city_ranks(wide: pd.DataFrame) -> pd.DataFrame:
    """Add per-metric city rankings within the CMA-total, Total-bedroom slice.

    Rankings are computed only over CMA-total rows with ``bedroom_type == 'Total'``
    and are broadcast back to all rows for the same city so that the column
    is available when filtering to any subset.

    Columns added:
      - ``rent_growth_rank``    — 1 = highest rent growth city
      - ``vacancy_rank``        — 1 = lowest (tightest) vacancy city
      - ``universe_growth_rank``— 1 = fastest-growing rental stock

    Args:
        wide: Wide-format DataFrame (after ``compute_market_tightness``).

    Returns:
        Same DataFrame with three additional rank columns.
    """
    cma_total = wide[wide["is_cma_total"] & (wide["bedroom_type"] == "Total")].copy()

    cma_total["rent_growth_rank"] = cma_total["avg_rent_yoy_pct"].rank(
        ascending=False, method="min", na_option="bottom"
    ).astype("Int64")
    cma_total["vacancy_rank"] = cma_total["vacancy_rate_oct25"].rank(
        ascending=True, method="min", na_option="bottom"
    ).astype("Int64")
    cma_total["universe_growth_rank"] = cma_total["rental_universe_yoy_pct"].rank(
        ascending=False, method="min", na_option="bottom"
    ).astype("Int64")

    rank_cols = ["city", "rent_growth_rank", "vacancy_rank", "universe_growth_rank"]
    wide = wide.merge(cma_total[rank_cols], on="city", how="left")
    return wide


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Run the full feature-engineering pipeline on the raw rental DataFrame.

    Steps:
      1. Pivot long → wide (one row per city/zone/bedroom_type).
      2. Compute YoY absolute and percentage changes.
      3. Compute composite market-tightness score.
      4. Add city-level rankings (CMA Total slice).

    Args:
        df: Long-format DataFrame as returned by ``ingest.load_processed``
            or ``ingest.ingest_all``.

    Returns:
        Feature DataFrame ready for loading into PostgreSQL or further
        analysis in the EDA notebook.
    """
    wide = pivot_to_wide(df)
    wide = compute_yoy_changes(wide)
    wide = compute_market_tightness(wide)
    wide = add_city_ranks(wide)
    # Stable sort: CMA totals first, then zones; cities alphabetically.
    wide = wide.sort_values(
        ["is_cma_total", "city", "bedroom_type", "zone"],
        ascending=[False, True, True, True],
    ).reset_index(drop=True)
    return wide


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print("loading processed data …")
    raw = load_processed()

    print("building features …")
    features = build_features(raw)

    print(f"\nshape         : {features.shape}")
    print(f"columns       : {features.columns.tolist()}")

    cma = features[features["is_cma_total"] & (features["bedroom_type"] == "Total")]
    print("\nCMA-level Total — rent growth ranking:")
    print(
        cma[["city", "avg_rent_oct24", "avg_rent_oct25",
             "avg_rent_yoy_pct", "vacancy_rate_oct25",
             "market_tightness", "rent_growth_rank"]]
        .sort_values("rent_growth_rank")
        .to_string(index=False)
    )

    save_features(features)
