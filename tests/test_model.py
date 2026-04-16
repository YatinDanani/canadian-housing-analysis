"""
tests/test_model.py
~~~~~~~~~~~~~~~~~~~~
Tests for src/model.py — exercises the XGBoost forecasting pipeline
against the real processed parquet files in data/processed/.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from xgboost import XGBRegressor

from src.model import (
    BEDROOM_TYPES,
    FORECAST_DATE,
    _build_model_df,
    _feature_cols,
    _make_xgb,
    forecast_next_period,
    leave_one_city_out_cv,
    print_cv_summary,
)

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def features() -> pd.DataFrame:
    """Load the features parquet once for the whole module."""
    return pd.read_parquet(PROCESSED_DIR / "cmhc_features.parquet")


@pytest.fixture(scope="module")
def model_df(features) -> pd.DataFrame:
    """Build the model-ready DataFrame once."""
    return _build_model_df(features)


@pytest.fixture(scope="module")
def cv_results(model_df) -> pd.DataFrame:
    """Run leave-one-city-out CV once."""
    return leave_one_city_out_cv(model_df)


@pytest.fixture(scope="module")
def forecasts_df(model_df, features) -> pd.DataFrame:
    """Run the full Oct-26 forecast once."""
    return forecast_next_period(model_df, features)


# ---------------------------------------------------------------------------
# Unit tests — no file I/O
# ---------------------------------------------------------------------------


class TestFeatureCols:
    def test_returns_four_features(self):
        assert len(_feature_cols()) == 4

    def test_expected_names(self):
        cols = _feature_cols()
        assert "bedroom_type_enc" in cols
        assert "vacancy_rate_oct24" in cols
        assert "log_universe_oct24" in cols
        assert "avg_rent_oct24" in cols


class TestMakeXgb:
    def test_returns_xgb_regressor(self):
        assert isinstance(_make_xgb(), XGBRegressor)

    def test_fixed_random_state(self):
        assert _make_xgb().random_state == 42

    def test_consistent_hyperparameters(self):
        """Two calls must return models with the same hyperparameters."""
        m1, m2 = _make_xgb(), _make_xgb()
        assert m1.n_estimators == m2.n_estimators
        assert m1.max_depth == m2.max_depth
        assert m1.learning_rate == m2.learning_rate


# ---------------------------------------------------------------------------
# Tests on real data
# ---------------------------------------------------------------------------


class TestBuildModelDf:
    def test_returns_dataframe(self, model_df):
        assert isinstance(model_df, pd.DataFrame)

    def test_expected_columns(self, model_df):
        expected = {
            "city", "bedroom_type", "bedroom_type_enc",
            "vacancy_rate_oct24", "log_universe_oct24",
            "avg_rent_oct24", "avg_rent_oct25",
        }
        assert expected.issubset(set(model_df.columns))

    def test_excludes_total_bedroom_type(self, model_df):
        """'Total' bedroom type must not appear — it is a derived aggregate."""
        assert "Total" not in model_df["bedroom_type"].values

    def test_only_known_bedroom_types(self, model_df):
        assert set(model_df["bedroom_type"].unique()).issubset(set(BEDROOM_TYPES))

    def test_no_nulls_in_feature_columns(self, model_df):
        for col in _feature_cols():
            null_count = model_df[col].isna().sum()
            assert null_count == 0, f"NaN in feature column '{col}': {null_count} rows"

    def test_log_universe_non_negative(self, model_df):
        assert (model_df["log_universe_oct24"] >= 0).all()

    def test_18_cities_present(self, model_df):
        assert model_df["city"].nunique() == 18

    def test_four_bedroom_types(self, model_df):
        assert model_df["bedroom_type"].nunique() == 4

    def test_bedroom_type_enc_is_integer(self, model_df):
        assert model_df["bedroom_type_enc"].dtype.kind in ("i", "u")


class TestLeaveOneCityOutCV:
    def test_returns_dataframe(self, cv_results):
        assert isinstance(cv_results, pd.DataFrame)

    def test_expected_columns(self, cv_results):
        assert {"city", "bedroom_type", "actual", "predicted", "residual"}.issubset(
            set(cv_results.columns)
        )

    def test_all_18_cities_evaluated(self, cv_results):
        """Every city must appear in the CV output (held-out once)."""
        assert cv_results["city"].nunique() == 18

    def test_residual_definition(self, cv_results):
        """residual must equal actual minus predicted."""
        expected = cv_results["actual"] - cv_results["predicted"]
        pd.testing.assert_series_equal(
            cv_results["residual"].round(6),
            expected.round(6),
            check_names=False,
        )

    def test_mae_below_threshold(self, cv_results):
        """LOO-CV MAE should be below $500 for a basic cross-sectional model."""
        mae = cv_results["residual"].abs().mean()
        assert mae < 500, f"MAE=${mae:.0f} is unexpectedly high"

    def test_no_null_predictions(self, cv_results):
        assert cv_results["predicted"].notna().all()


class TestForecastNextPeriod:
    def test_returns_dataframe(self, forecasts_df):
        assert isinstance(forecasts_df, pd.DataFrame)

    def test_expected_columns(self, forecasts_df):
        assert {"city", "bedroom_type", "forecast_date",
                "predicted_rent", "lower_ci", "upper_ci"}.issubset(
            set(forecasts_df.columns)
        )

    def test_forecast_date_is_oct_2026(self, forecasts_df):
        assert (forecasts_df["forecast_date"] == FORECAST_DATE).all()

    def test_18_cities_forecasted(self, forecasts_df):
        assert forecasts_df["city"].nunique() == 18

    def test_four_bedroom_types_forecasted(self, forecasts_df):
        assert forecasts_df["bedroom_type"].nunique() == 4

    def test_point_estimate_within_ci(self, forecasts_df):
        assert (forecasts_df["lower_ci"] <= forecasts_df["predicted_rent"]).all()
        assert (forecasts_df["predicted_rent"] <= forecasts_df["upper_ci"]).all()

    def test_predicted_rents_positive(self, forecasts_df):
        assert (forecasts_df["predicted_rent"] > 0).all()

    def test_ci_width_positive(self, forecasts_df):
        width = forecasts_df["upper_ci"] - forecasts_df["lower_ci"]
        assert (width > 0).all()

    def test_no_null_values(self, forecasts_df):
        for col in ("predicted_rent", "lower_ci", "upper_ci"):
            assert forecasts_df[col].notna().all(), f"NaN in column '{col}'"


class TestPrintCvSummary:
    def test_runs_without_error(self, cv_results, capsys):
        """print_cv_summary must not raise and must print MAE and RMSE."""
        print_cv_summary(cv_results)
        captured = capsys.readouterr()
        assert "MAE" in captured.out
        assert "RMSE" in captured.out
