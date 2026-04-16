"""
src/model.py
~~~~~~~~~~~~
Cross-sectional XGBoost model that predicts next-period average rent
for each CMA using vacancy rate and rental universe as features.

With only two survey dates (Oct-24, Oct-25) true time-series forecasting
is not possible.  Instead we:
  1. Train on Oct-24 features → Oct-25 rent (18 cities × 4 bedroom types).
  2. Evaluate with leave-one-city-out cross-validation.
  3. Re-train on all data and predict Oct-26 rent from Oct-25 features.
  4. Build bootstrap prediction intervals from LOO residuals.
  5. Save predictions to data/processed/forecasts.parquet.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
FEATURES_PATH = PROCESSED_DIR / "cmhc_features.parquet"
FORECASTS_PATH = PROCESSED_DIR / "forecasts.parquet"

BEDROOM_TYPES = ["Studio", "1 Bedroom", "2 Bedroom", "3 Bedroom +"]
FORECAST_DATE = pd.Timestamp("2026-10-01")
N_BOOTSTRAP = 200
CI_LEVEL = 0.90


def _build_model_df(features: pd.DataFrame) -> pd.DataFrame:
    """Extract CMA-level rows and encode bedroom type for modelling.

    Args:
        features: Wide-format DataFrame from ``transform.build_features``.

    Returns:
        DataFrame with columns: ``city``, ``bedroom_type_enc``,
        ``vacancy_rate_oct24``, ``log_universe_oct24``,
        ``avg_rent_oct24``, ``avg_rent_oct25``.
    """
    cma = features[
        features["is_cma_total"] &
        features["bedroom_type"].isin(BEDROOM_TYPES)
    ].copy()

    le = LabelEncoder()
    cma["bedroom_type_enc"] = le.fit_transform(cma["bedroom_type"])
    cma["log_universe_oct24"] = np.log1p(cma["rental_universe_oct24"])

    keep = [
        "city", "bedroom_type", "bedroom_type_enc",
        "vacancy_rate_oct24", "log_universe_oct24",
        "avg_rent_oct24", "avg_rent_oct25",
    ]
    return cma[keep].dropna().reset_index(drop=True)


def _feature_cols() -> list[str]:
    """Return the ordered list of feature column names.

    Returns:
        List of feature column name strings.
    """
    return ["bedroom_type_enc", "vacancy_rate_oct24",
            "log_universe_oct24", "avg_rent_oct24"]


def _make_xgb() -> XGBRegressor:
    """Construct the XGBoost regressor with fixed hyperparameters.

    Returns:
        Untrained XGBRegressor.
    """
    return XGBRegressor(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=0,
    )


def leave_one_city_out_cv(model_df: pd.DataFrame) -> pd.DataFrame:
    """Evaluate via leave-one-city-out cross-validation.

    Trains on 17 cities, predicts the held-out city, repeats for all 18.

    Args:
        model_df: DataFrame from ``_build_model_df``.

    Returns:
        DataFrame with columns ``city``, ``bedroom_type``, ``actual``,
        ``predicted``, ``residual``.
    """
    feat = _feature_cols()
    results = []
    cities = model_df["city"].unique()

    for city in cities:
        train = model_df[model_df["city"] != city]
        test = model_df[model_df["city"] == city]

        model = _make_xgb()
        model.fit(train[feat], train["avg_rent_oct25"])
        preds = model.predict(test[feat])

        for (_, row), pred in zip(test.iterrows(), preds):
            results.append({
                "city": row["city"],
                "bedroom_type": row["bedroom_type"],
                "actual": row["avg_rent_oct25"],
                "predicted": float(pred),
                "residual": row["avg_rent_oct25"] - float(pred),
            })

    return pd.DataFrame(results)


def forecast_next_period(
    model_df: pd.DataFrame,
    features: pd.DataFrame,
) -> pd.DataFrame:
    """Train on all data and forecast Oct-26 rent with bootstrap CIs.

    Uses Oct-25 vacancy/universe/rent as the feature vector for prediction.
    Bootstrap CIs are constructed by resampling LOO residuals.

    Args:
        model_df: DataFrame from ``_build_model_df``.
        features: Full wide features DataFrame (for Oct-25 columns).

    Returns:
        DataFrame with columns: ``city``, ``bedroom_type``,
        ``forecast_date``, ``predicted_rent``, ``lower_ci``, ``upper_ci``.
    """
    feat = _feature_cols()

    # Full model trained on Oct-24 → Oct-25.
    full_model = _make_xgb()
    full_model.fit(model_df[feat], model_df["avg_rent_oct25"])

    # LOO residuals for bootstrap CI width.
    loo = leave_one_city_out_cv(model_df)
    residuals = loo["residual"].values

    # Build Oct-25 feature rows for predicting Oct-26.
    le = LabelEncoder()
    le.fit(model_df["bedroom_type"])

    cma = features[
        features["is_cma_total"] &
        features["bedroom_type"].isin(BEDROOM_TYPES)
    ].copy()
    cma = cma.dropna(subset=["vacancy_rate_oct25", "rental_universe_oct25", "avg_rent_oct25"])
    cma["bedroom_type_enc"] = le.transform(cma["bedroom_type"])
    cma["log_universe_oct24"] = np.log1p(cma["rental_universe_oct25"])
    # Rename so feature names match training columns.
    X_next = cma[["bedroom_type_enc", "vacancy_rate_oct25",
                  "log_universe_oct24", "avg_rent_oct25"]].copy()
    X_next.columns = feat

    point_preds = full_model.predict(X_next)

    rng = np.random.default_rng(42)
    boot_preds = np.array([
        point_preds + rng.choice(residuals, size=len(point_preds), replace=True)
        for _ in range(N_BOOTSTRAP)
    ])
    alpha = (1 - CI_LEVEL) / 2
    lower = np.quantile(boot_preds, alpha, axis=0)
    upper = np.quantile(boot_preds, 1 - alpha, axis=0)

    records = []
    for i, (_, row) in enumerate(cma.iterrows()):
        records.append({
            "city": row["city"],
            "bedroom_type": row["bedroom_type"],
            "forecast_date": FORECAST_DATE,
            "predicted_rent": float(point_preds[i]),
            "lower_ci": float(lower[i]),
            "upper_ci": float(upper[i]),
        })
    return pd.DataFrame(records)


def print_cv_summary(cv_df: pd.DataFrame) -> None:
    """Print leave-one-city-out CV metrics to stdout.

    Args:
        cv_df: DataFrame from ``leave_one_city_out_cv``.
    """
    mae = cv_df["residual"].abs().mean()
    rmse = (cv_df["residual"] ** 2).mean() ** 0.5
    print(f"\nLOO-CV  MAE=${mae:.0f}  RMSE=${rmse:.0f}")

    per_city = (
        cv_df.groupby("city")["residual"]
        .apply(lambda x: x.abs().mean())
        .sort_values(ascending=False)
        .rename("mae")
        .reset_index()
    )
    print("\nMAE by city (most uncertain first):")
    print(per_city.to_string(index=False))


if __name__ == "__main__":
    print("loading features …")
    features = pd.read_parquet(FEATURES_PATH)
    model_df = _build_model_df(features)
    print(f"model dataset: {len(model_df)} rows ({model_df['city'].nunique()} cities)")

    print("\nrunning leave-one-city-out CV …")
    cv_results = leave_one_city_out_cv(model_df)
    print_cv_summary(cv_results)

    print("\nforecasting Oct-26 …")
    forecasts = forecast_next_period(model_df, features)
    forecasts.to_parquet(FORECASTS_PATH, index=False)
    print(f"\nsaved {len(forecasts)} forecasts → {FORECASTS_PATH}")
    print(forecasts[["city", "bedroom_type", "predicted_rent",
                      "lower_ci", "upper_ci"]].to_string(index=False))
