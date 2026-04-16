"""
tests/test_transform.py
~~~~~~~~~~~~~~~~~~~~~~~
Tests for src/transform.py — exercises the feature-engineering pipeline
against the real processed parquet file.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.transform import (
    build_features,
    compute_market_tightness,
    compute_yoy_changes,
    load_processed,
    pivot_to_wide,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def raw_df():
    """Load the processed parquet once for the whole module."""
    return load_processed()


@pytest.fixture(scope="module")
def wide_df(raw_df):
    """Pivot the raw DataFrame to wide format once."""
    return pivot_to_wide(raw_df)


@pytest.fixture(scope="module")
def features_df(raw_df):
    """Run the full feature pipeline once."""
    return build_features(raw_df)


# ---------------------------------------------------------------------------
# load_processed
# ---------------------------------------------------------------------------


class TestLoadProcessed:
    def test_returns_dataframe(self, raw_df):
        assert isinstance(raw_df, pd.DataFrame)

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_processed(Path("/nonexistent/path.parquet"))

    def test_expected_columns_present(self, raw_df):
        expected = {"city", "zone", "is_cma_total", "survey_date",
                    "bedroom_type", "vacancy_rate", "avg_rent", "rental_universe"}
        assert expected.issubset(set(raw_df.columns))


# ---------------------------------------------------------------------------
# pivot_to_wide
# ---------------------------------------------------------------------------


class TestPivotToWide:
    def test_shape_halved(self, raw_df, wide_df):
        """Wide format has half the rows of long format (2 dates → 1 row)."""
        assert len(wide_df) == len(raw_df) // 2

    def test_expected_columns(self, wide_df):
        expected = {
            "city", "zone", "is_cma_total", "bedroom_type",
            "vacancy_rate_oct24", "vacancy_rate_oct25",
            "avg_rent_oct24", "avg_rent_oct25",
            "rental_universe_oct24", "rental_universe_oct25",
        }
        assert expected.issubset(set(wide_df.columns))

    def test_no_survey_date_column(self, wide_df):
        assert "survey_date" not in wide_df.columns

    def test_all_18_cities(self, wide_df):
        assert wide_df["city"].nunique() == 18


# ---------------------------------------------------------------------------
# compute_yoy_changes
# ---------------------------------------------------------------------------


class TestComputeYoyChanges:
    def test_adds_six_columns(self, wide_df):
        result = compute_yoy_changes(wide_df.copy())
        new_cols = {
            "vacancy_rate_yoy_change", "vacancy_rate_yoy_pct",
            "avg_rent_yoy_change", "avg_rent_yoy_pct",
            "rental_universe_yoy_change", "rental_universe_yoy_pct",
        }
        assert new_cols.issubset(set(result.columns))

    def test_rent_yoy_change_correct(self, wide_df):
        result = compute_yoy_changes(wide_df.copy())
        diff = result["avg_rent_oct25"] - result["avg_rent_oct24"]
        pd.testing.assert_series_equal(
            result["avg_rent_yoy_change"].dropna(),
            diff.dropna(),
            check_names=False,
        )

    def test_rent_yoy_pct_correct(self, wide_df):
        result = compute_yoy_changes(wide_df.copy())
        mask = result["avg_rent_oct24"].notna() & (result["avg_rent_oct24"] != 0)
        expected_pct = (
            (result.loc[mask, "avg_rent_oct25"] - result.loc[mask, "avg_rent_oct24"])
            / result.loc[mask, "avg_rent_oct24"]
            * 100
        )
        pd.testing.assert_series_equal(
            result.loc[mask, "avg_rent_yoy_pct"],
            expected_pct,
            check_names=False,
        )

    def test_zero_base_yields_nan(self):
        """Percentage change is NaN when the base value is 0."""
        df = pd.DataFrame({
            "vacancy_rate_oct24": [0.0],
            "vacancy_rate_oct25": [1.0],
            "avg_rent_oct24": [0.0],
            "avg_rent_oct25": [100.0],
            "rental_universe_oct24": [0],
            "rental_universe_oct25": [10],
        })
        result = compute_yoy_changes(df)
        assert np.isnan(result["vacancy_rate_yoy_pct"].iloc[0])
        assert np.isnan(result["avg_rent_yoy_pct"].iloc[0])


# ---------------------------------------------------------------------------
# compute_market_tightness
# ---------------------------------------------------------------------------


class TestComputeMarketTightness:
    def test_column_added(self, features_df):
        assert "market_tightness" in features_df.columns

    def test_range_zero_to_one(self, features_df):
        valid = features_df["market_tightness"].dropna()
        assert (valid >= 0).all()
        assert (valid <= 1).all()

    def test_nan_when_inputs_missing(self):
        """market_tightness is NaN when vacancy_rate_oct25 or avg_rent_yoy_pct is NaN."""
        df = pd.DataFrame({
            "vacancy_rate_oct25": [np.nan, 2.0],
            "avg_rent_yoy_pct": [5.0, np.nan],
        })
        result = compute_market_tightness(df)
        assert result["market_tightness"].isna().all()


# ---------------------------------------------------------------------------
# add_city_ranks
# ---------------------------------------------------------------------------


class TestAddCityRanks:
    def test_rank_columns_added(self, features_df):
        for col in ("rent_growth_rank", "vacancy_rank", "universe_growth_rank"):
            assert col in features_df.columns

    def test_ranks_cover_all_18_cities(self, features_df):
        cma = features_df[features_df["is_cma_total"] & (features_df["bedroom_type"] == "Total")]
        assert cma["rent_growth_rank"].notna().sum() == 18
        assert cma["vacancy_rank"].notna().sum() == 18

    def test_rent_growth_rank_1_is_highest_growth(self, features_df):
        cma = features_df[features_df["is_cma_total"] & (features_df["bedroom_type"] == "Total")]
        rank1_city = cma.loc[cma["rent_growth_rank"] == 1, "avg_rent_yoy_pct"].iloc[0]
        assert rank1_city == cma["avg_rent_yoy_pct"].max()

    def test_vacancy_rank_1_is_lowest_vacancy(self, features_df):
        cma = features_df[features_df["is_cma_total"] & (features_df["bedroom_type"] == "Total")]
        rank1_vacancy = cma.loc[cma["vacancy_rank"] == 1, "vacancy_rate_oct25"].iloc[0]
        assert rank1_vacancy == cma["vacancy_rate_oct25"].min()


# ---------------------------------------------------------------------------
# build_features (integration)
# ---------------------------------------------------------------------------


class TestBuildFeatures:
    def test_returns_dataframe(self, features_df):
        assert isinstance(features_df, pd.DataFrame)

    def test_row_count(self, raw_df, features_df):
        assert len(features_df) == len(raw_df) // 2

    def test_18_cities(self, features_df):
        assert features_df["city"].nunique() == 18

    def test_no_duplicate_cma_totals(self, features_df):
        cma = features_df[features_df["is_cma_total"]]
        dupes = cma.duplicated(subset=["city", "bedroom_type"])
        assert not dupes.any(), f"Duplicate CMA rows:\n{cma[dupes]}"

    def test_cma_total_columns_complete(self, features_df):
        """CMA-total Total rows must have rent, vacancy, and rank data."""
        cma = features_df[
            features_df["is_cma_total"] & (features_df["bedroom_type"] == "Total")
        ]
        for col in ("avg_rent_oct24", "avg_rent_oct25", "avg_rent_yoy_pct",
                    "vacancy_rate_oct25", "rent_growth_rank", "vacancy_rank"):
            null_cities = cma.loc[cma[col].isna(), "city"].tolist()
            assert not null_cities, f"Column '{col}' is null for: {null_cities}"

    def test_montreal_highest_rent_growth(self, features_df):
        """Montreal had the highest YoY rent growth in Oct-24 → Oct-25."""
        cma = features_df[
            features_df["is_cma_total"] & (features_df["bedroom_type"] == "Total")
        ]
        top_city = cma.loc[cma["rent_growth_rank"] == 1, "city"].iloc[0]
        assert top_city == "Montreal"

    def test_output_sorted(self, features_df):
        """CMA rows must come before zone rows (is_cma_total descending)."""
        first_cma = features_df[features_df["is_cma_total"]].index[0]
        last_cma = features_df[features_df["is_cma_total"]].index[-1]
        first_zone = features_df[~features_df["is_cma_total"]].index[0]
        assert first_cma < first_zone
        assert last_cma < first_zone
