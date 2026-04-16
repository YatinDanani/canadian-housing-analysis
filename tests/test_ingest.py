"""
tests/test_ingest.py
~~~~~~~~~~~~~~~~~~~~
Tests for src/ingest.py — exercises real xlsx files in data/raw/.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.ingest import (
    RAW_DIR,
    _clean_numeric,
    extract_city_name,
    ingest_all,
    ingest_city,
    list_raw_files,
    parse_rent_table,
    parse_universe_table,
    parse_vacancy_table,
)



# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def vancouver_path() -> Path:
    """Return the path to the Vancouver RMR xlsx file."""
    return RAW_DIR / "rmr-vancouver-2025-en.xlsx"


@pytest.fixture(scope="module")
def all_df():
    """Ingest all cities once for the whole test module."""
    return ingest_all()


# ---------------------------------------------------------------------------
# Unit tests — helpers
# ---------------------------------------------------------------------------


class TestCleanNumeric:
    def test_none_returns_none(self):
        assert _clean_numeric(None) is None

    def test_suppressed_returns_none(self):
        assert _clean_numeric("**") is None

    def test_integer(self):
        assert _clean_numeric(1) == 1.0

    def test_float(self):
        assert _clean_numeric(2.5) == 2.5

    def test_string_float(self):
        assert _clean_numeric("1.3") == 1.3

    def test_thousands_separator(self):
        assert _clean_numeric("1,667") == 1667.0

    def test_non_numeric_string_returns_none(self):
        assert _clean_numeric("↑") is None


class TestExtractCityName:
    def test_simple_city(self):
        assert extract_city_name(Path("rmr-vancouver-2025-en.xlsx")) == "Vancouver"

    def test_multi_word_city(self):
        result = extract_city_name(Path("rmr-st-catharines-niagara-2025-en.xlsx"))
        assert result == "St Catharines Niagara"

    def test_cma_suffix_dropped(self):
        result = extract_city_name(Path("rmr-quebec-cma-2025-en.xlsx"))
        assert result == "Quebec"
        assert "Cma" not in result


# ---------------------------------------------------------------------------
# Unit tests — per-table parsers (Vancouver)
# ---------------------------------------------------------------------------


class TestParseVacancyTable:
    def test_returns_records(self, vancouver_path):
        records = parse_vacancy_table(vancouver_path)
        assert len(records) > 0

    def test_record_keys(self, vancouver_path):
        records = parse_vacancy_table(vancouver_path)
        expected = {"zone", "is_cma_total", "survey_date", "bedroom_type",
                    "vacancy_rate", "vacancy_quality"}
        assert set(records[0].keys()) == expected

    def test_cma_row_detected(self, vancouver_path):
        records = parse_vacancy_table(vancouver_path)
        cma_records = [r for r in records if r["is_cma_total"]]
        assert len(cma_records) > 0
        assert all(r["zone"] == "Vancouver CMA" for r in cma_records)

    def test_two_survey_dates(self, vancouver_path):
        records = parse_vacancy_table(vancouver_path)
        dates = {r["survey_date"] for r in records}
        assert datetime(2024, 10, 1) in dates
        assert datetime(2025, 10, 1) in dates

    def test_vacancy_rate_is_float_or_none(self, vancouver_path):
        records = parse_vacancy_table(vancouver_path)
        for r in records:
            assert r["vacancy_rate"] is None or isinstance(r["vacancy_rate"], float)

    def test_bedroom_types_present(self, vancouver_path):
        records = parse_vacancy_table(vancouver_path)
        found_types = {r["bedroom_type"] for r in records}
        assert {"Studio", "1 Bedroom", "2 Bedroom", "Total"}.issubset(found_types)

    def test_no_remainder_cma_zones_flagged(self, vancouver_path):
        """Zones like 'Remainder of X CMA' must not be flagged as CMA total."""
        records = parse_vacancy_table(vancouver_path)
        false_positives = [
            r for r in records
            if r["is_cma_total"] and "Remainder" in r["zone"]
        ]
        assert false_positives == []


class TestParseRentTable:
    def test_returns_records(self, vancouver_path):
        records = parse_rent_table(vancouver_path)
        assert len(records) > 0

    def test_avg_rent_positive_for_cma(self, vancouver_path):
        records = parse_rent_table(vancouver_path)
        cma_total = [
            r for r in records
            if r["is_cma_total"] and r["bedroom_type"] == "Total"
        ]
        assert len(cma_total) == 2  # one per survey date
        for r in cma_total:
            assert r["avg_rent"] is not None
            assert r["avg_rent"] > 0


class TestParseUniverseTable:
    def test_returns_records(self, vancouver_path):
        records = parse_universe_table(vancouver_path)
        assert len(records) > 0

    def test_universe_is_int_or_none(self, vancouver_path):
        records = parse_universe_table(vancouver_path)
        for r in records:
            assert r["rental_universe"] is None or isinstance(r["rental_universe"], int)


# ---------------------------------------------------------------------------
# Integration tests — ingest_city and ingest_all
# ---------------------------------------------------------------------------


class TestIngestCity:
    def test_returns_dataframe(self):
        path = RAW_DIR / "rmr-vancouver-2025-en.xlsx"
        df = ingest_city(path)
        assert isinstance(df, pd.DataFrame)

    def test_city_column_set(self):
        path = RAW_DIR / "rmr-vancouver-2025-en.xlsx"
        df = ingest_city(path)
        assert (df["city"] == "Vancouver").all()

    def test_expected_columns(self):
        path = RAW_DIR / "rmr-vancouver-2025-en.xlsx"
        df = ingest_city(path)
        expected = {
            "city", "zone", "is_cma_total", "survey_date", "bedroom_type",
            "vacancy_rate", "vacancy_quality", "avg_rent", "rent_quality",
            "rental_universe",
        }
        assert expected.issubset(set(df.columns))

    def test_no_rows_with_all_null_metrics(self):
        path = RAW_DIR / "rmr-vancouver-2025-en.xlsx"
        df = ingest_city(path)
        metric_cols = ["vacancy_rate", "avg_rent", "rental_universe"]
        fully_null = df[metric_cols].isna().all(axis=1)
        assert not fully_null.all(), "Every row has null metrics — parsing likely broken"


class TestIngestAll:
    def test_row_count(self, all_df):
        assert len(all_df) > 0

    def test_eighteen_cities(self, all_df):
        assert all_df["city"].nunique() == 18

    def test_two_survey_dates(self, all_df):
        assert all_df["survey_date"].nunique() == 2

    def test_no_duplicate_cma_totals(self, all_df):
        """Each city should have exactly one CMA-total row per date per bedroom type."""
        cma = all_df[all_df["is_cma_total"]]
        dupes = cma.duplicated(subset=["city", "survey_date", "bedroom_type"])
        assert not dupes.any(), f"Duplicate CMA rows found:\n{cma[dupes]}"

    def test_cma_vacancy_rates_plausible(self, all_df):
        """Vacancy rates at CMA level should be between 0 and 25 %."""
        cma_rates = all_df[all_df["is_cma_total"]]["vacancy_rate"].dropna()
        assert (cma_rates >= 0).all()
        assert (cma_rates <= 25).all()

    def test_cma_rents_plausible(self, all_df):
        """Average rents at CMA level should be between $500 and $5,000."""
        cma_rents = all_df[all_df["is_cma_total"]]["avg_rent"].dropna()
        assert (cma_rents >= 500).all()
        assert (cma_rents <= 5000).all()

    def test_list_raw_files_finds_all(self):
        files = list_raw_files()
        assert len(files) == 18
