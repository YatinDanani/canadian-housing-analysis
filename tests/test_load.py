"""
tests/test_load.py
~~~~~~~~~~~~~~~~~~
Tests for src/load.py.

Schema-definition and province-map tests run without a database.
Integration tests (load_cities, load_rental_data) require a live
PostgreSQL instance and are skipped when DATABASE_URL is not set.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import MetaData, text

from src.load import (
    _PROVINCE_MAP,
    define_schema,
    get_engine,
    load_cities,
    load_rental_data,
)

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

# Skip DB-touching tests when no DATABASE_URL is configured (e.g. local dev
# without docker-compose running).  CI always sets DATABASE_URL.
_db_available = bool(os.environ.get("DATABASE_URL"))
needs_db = pytest.mark.skipif(not _db_available, reason="DATABASE_URL not set")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rental_df() -> pd.DataFrame:
    """Load the processed rental parquet once for the whole module."""
    return pd.read_parquet(PROCESSED_DIR / "cmhc_rental.parquet")


@pytest.fixture(scope="module")
def test_engine():
    """Return an engine pointed at the test database."""
    return get_engine()


@pytest.fixture
def fresh_schema(test_engine):
    """Create all tables before a test and drop them after.

    Yields:
        Tuple of (tables dict, engine) ready to use.
    """
    metadata = MetaData()
    tables = define_schema(metadata)
    metadata.create_all(test_engine)
    yield tables, test_engine
    metadata.drop_all(test_engine)


# ---------------------------------------------------------------------------
# Unit tests — no database required
# ---------------------------------------------------------------------------


class TestGetEngine:
    def test_returns_engine(self):
        """get_engine must return a SQLAlchemy engine object."""
        engine = get_engine()
        assert engine is not None

    def test_engine_url_contains_postgresql(self):
        """Connection URL must use the PostgreSQL dialect."""
        engine = get_engine()
        assert "postgresql" in str(engine.url)


class TestDefineSchema:
    def test_returns_four_tables(self):
        """define_schema must expose exactly four table objects."""
        metadata = MetaData()
        tables = define_schema(metadata)
        assert set(tables.keys()) == {"cities", "vacancy_rates", "avg_rents", "forecasts"}

    def test_cities_columns(self):
        metadata = MetaData()
        tables = define_schema(metadata)
        col_names = {c.name for c in tables["cities"].columns}
        assert {"id", "name", "province"}.issubset(col_names)

    def test_vacancy_rates_columns(self):
        metadata = MetaData()
        tables = define_schema(metadata)
        col_names = {c.name for c in tables["vacancy_rates"].columns}
        assert {"city_id", "survey_date", "bedroom_type", "vacancy_rate"}.issubset(col_names)

    def test_avg_rents_columns(self):
        metadata = MetaData()
        tables = define_schema(metadata)
        col_names = {c.name for c in tables["avg_rents"].columns}
        assert {"city_id", "survey_date", "bedroom_type", "avg_rent"}.issubset(col_names)

    def test_forecasts_columns(self):
        metadata = MetaData()
        tables = define_schema(metadata)
        col_names = {c.name for c in tables["forecasts"].columns}
        assert {"city_id", "forecast_date", "predicted_rent",
                "lower_ci", "upper_ci"}.issubset(col_names)


class TestProvinceMap:
    def test_all_18_cities_mapped(self):
        """Province map must cover all 18 CMAs in the dataset."""
        assert len(_PROVINCE_MAP) == 18

    def test_bc_cities(self):
        assert _PROVINCE_MAP["Vancouver"] == "BC"
        assert _PROVINCE_MAP["Victoria"] == "BC"

    def test_on_cities(self):
        assert _PROVINCE_MAP["Toronto"] == "ON"
        assert _PROVINCE_MAP["Ottawa"] == "ON"
        assert _PROVINCE_MAP["Hamilton"] == "ON"

    def test_qc_cities(self):
        assert _PROVINCE_MAP["Montreal"] == "QC"
        assert _PROVINCE_MAP["Quebec"] == "QC"
        assert _PROVINCE_MAP["Gatineau"] == "QC"

    def test_prairie_cities(self):
        assert _PROVINCE_MAP["Calgary"] == "AB"
        assert _PROVINCE_MAP["Edmonton"] == "AB"
        assert _PROVINCE_MAP["Regina"] == "SK"
        assert _PROVINCE_MAP["Saskatoon"] == "SK"
        assert _PROVINCE_MAP["Winnipeg"] == "MB"


# ---------------------------------------------------------------------------
# Integration tests — require DATABASE_URL
# ---------------------------------------------------------------------------


@needs_db
class TestLoadCities:
    def test_returns_city_id_mapping(self, fresh_schema, rental_df):
        """load_cities must return a dict of 18 city name → int id pairs."""
        tables, engine = fresh_schema
        with engine.begin() as conn:
            city_ids = load_cities(conn, tables, rental_df)
        assert isinstance(city_ids, dict)
        assert len(city_ids) == 18

    def test_all_cities_have_integer_ids(self, fresh_schema, rental_df):
        tables, engine = fresh_schema
        with engine.begin() as conn:
            city_ids = load_cities(conn, tables, rental_df)
        assert all(isinstance(v, int) for v in city_ids.values())

    def test_all_source_cities_present(self, fresh_schema, rental_df):
        """Every city in the source data must appear in the returned mapping."""
        tables, engine = fresh_schema
        with engine.begin() as conn:
            city_ids = load_cities(conn, tables, rental_df)
        assert set(rental_df["city"].unique()).issubset(set(city_ids.keys()))

    def test_idempotent_upsert(self, fresh_schema, rental_df):
        """Calling load_cities twice must not raise or duplicate rows."""
        tables, engine = fresh_schema
        with engine.begin() as conn:
            load_cities(conn, tables, rental_df)
        with engine.begin() as conn:
            load_cities(conn, tables, rental_df)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM cities")).scalar()
        assert count == 18


@needs_db
class TestLoadRentalData:
    def test_inserts_vacancy_and_rent_rows(self, fresh_schema, rental_df):
        """Both vacancy_rates and avg_rents tables must have rows after loading."""
        tables, engine = fresh_schema
        with engine.begin() as conn:
            city_ids = load_cities(conn, tables, rental_df)
            load_rental_data(conn, tables, rental_df, city_ids)
        with engine.connect() as conn:
            vac_count = conn.execute(text("SELECT COUNT(*) FROM vacancy_rates")).scalar()
            rent_count = conn.execute(text("SELECT COUNT(*) FROM avg_rents")).scalar()
        assert vac_count > 0
        assert rent_count > 0

    def test_vacancy_row_count_matches_source(self, fresh_schema, rental_df):
        """vacancy_rates must have one row per row in the source DataFrame."""
        tables, engine = fresh_schema
        with engine.begin() as conn:
            city_ids = load_cities(conn, tables, rental_df)
            load_rental_data(conn, tables, rental_df, city_ids)
        with engine.connect() as conn:
            db_count = conn.execute(text("SELECT COUNT(*) FROM vacancy_rates")).scalar()
        assert db_count == len(rental_df)

    def test_idempotent_reload(self, fresh_schema, rental_df):
        """Loading twice truncates and reloads — row count must not double."""
        tables, engine = fresh_schema
        with engine.begin() as conn:
            city_ids = load_cities(conn, tables, rental_df)
            load_rental_data(conn, tables, rental_df, city_ids)
        with engine.begin() as conn:
            load_rental_data(conn, tables, rental_df, city_ids)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM vacancy_rates")).scalar()
        assert count == len(rental_df)

    def test_all_18_cities_loaded(self, fresh_schema, rental_df):
        """After loading, cities table must contain all 18 CMAs."""
        tables, engine = fresh_schema
        with engine.begin() as conn:
            city_ids = load_cities(conn, tables, rental_df)
            load_rental_data(conn, tables, rental_df, city_ids)
        with engine.connect() as conn:
            city_count = conn.execute(text("SELECT COUNT(*) FROM cities")).scalar()
        assert city_count == 18
