"""
src/api.py
~~~~~~~~~~
FastAPI REST API serving CMHC housing data as JSON.

Reads pre-built parquet files from data/processed/ at startup.
All data is cached in memory — no database required.

Run locally:
    uvicorn src.api:app --reload --port 8000
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

# Module-level cache populated on startup.
_features: pd.DataFrame = pd.DataFrame()
_forecasts: pd.DataFrame = pd.DataFrame()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load parquet files into memory when the server starts.

    Args:
        app: FastAPI application instance.

    Yields:
        None — yields control back to FastAPI after loading.
    """
    global _features, _forecasts
    _features = pd.read_parquet(PROCESSED_DIR / "cmhc_features.parquet")
    _forecasts = pd.read_parquet(PROCESSED_DIR / "forecasts.parquet")
    yield


app = FastAPI(
    title="Canadian Housing Affordability API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://canadian-housing-analysis.vercel.app",
        "https://*.vercel.app",  # covers preview deployments
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _to_json(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame to a JSON-serializable list of dicts.

    Uses pandas to_json internally so all numpy / pandas nullable types
    (Int64, float64, Timestamp, NA) are converted to Python natives.

    Args:
        df: DataFrame to serialize.

    Returns:
        List of dicts safe to return from a FastAPI endpoint.
    """
    return json.loads(df.to_json(orient="records", date_format="iso"))


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict:
    """Health check — confirm the API is up and data is loaded.

    Returns:
        Dict with status and row counts.
    """
    return {
        "status": "ok",
        "features_rows": len(_features),
        "forecast_rows": len(_forecasts),
    }


@app.get("/api/cities")
def get_cities() -> list[str]:
    """Return a sorted list of all 18 CMA city names.

    Returns:
        Alphabetically sorted list of city name strings.
    """
    return sorted(_features["city"].unique().tolist())


@app.get("/api/bedroom-types")
def get_bedroom_types() -> list[str]:
    """Return the ordered list of available bedroom type options.

    Returns:
        List of bedroom type strings in display order.
    """
    return ["Total", "Studio", "1 Bedroom", "2 Bedroom", "3 Bedroom +"]


@app.get("/api/features")
def get_features(
    bedroom: str = Query("Total", description="Bedroom type filter"),
) -> list[dict]:
    """Return CMA-level feature data for the requested bedroom type.

    Args:
        bedroom: Bedroom type string (e.g. ``'Total'``, ``'1 Bedroom'``).

    Returns:
        List of feature record dicts — one per city — with vacancy rates,
        average rents, YoY changes, market tightness, and city rankings.
    """
    cma = _features[
        _features["is_cma_total"] & (_features["bedroom_type"] == bedroom)
    ].copy()
    return _to_json(cma)


@app.get("/api/forecasts")
def get_forecasts(
    bedroom: str = Query("1 Bedroom", description="Bedroom type filter"),
) -> list[dict]:
    """Return Oct-26 rent forecasts for the requested bedroom type.

    Args:
        bedroom: Bedroom type string.

    Returns:
        List of forecast dicts with ``city``, ``predicted_rent``,
        ``lower_ci``, ``upper_ci``, and ``forecast_date``.
    """
    f = _forecasts[_forecasts["bedroom_type"] == bedroom].copy()
    return _to_json(f)
