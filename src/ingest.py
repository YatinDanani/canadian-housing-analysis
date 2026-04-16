"""
src/ingest.py
~~~~~~~~~~~~~
Read, clean, and normalize CMHC Rental Market Report data from
per-city xlsx files in data/raw/.

Each file contains Rental Market Survey tables for one CMA.  We
extract three sheets:
  - Table 1.1.1  Private Apartment Vacancy Rates (%)
  - Table 1.1.2  Private Apartment Average Rents ($)
  - Table 1.1.3  Number of Private Apartment Units in the Universe

Output: data/processed/cmhc_rental.parquet
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

# Known survey dates encoded in xlsx column headers.
_DATE_MAP: dict[str, datetime] = {
    "Oct-24": datetime(2024, 10, 1),
    "Oct-25": datetime(2025, 10, 1),
}

# Bedroom type labels exactly as they appear in the xlsx header rows.
_BEDROOM_TYPES: set[str] = {"Studio", "1 Bedroom", "2 Bedroom", "3 Bedroom +", "Total"}

# Prefixes that identify footnote / metadata rows (not data rows).
_FOOTNOTE_PREFIXES: tuple[str, ...] = (
    "§", "Quality", "Other", "a —", "** —", "↑", "↓", "–", "Source", "©",
)

# Suffixes to strip when normalizing CMA zone names into clean city names.
_CMA_SUFFIX_RE = re.compile(
    r"\s+(CMA|CA|Census Metropolitan Area|Census Agglomeration)\s*$",
    re.IGNORECASE,
)
# Parenthesized province codes, e.g. "(ON)", "(BC)".
_PROVINCE_CODE_RE = re.compile(r"\s*\([A-Z]{2}\)\s*$")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def list_raw_files(data_dir: Path = RAW_DIR) -> list[Path]:
    """Return sorted list of CMHC RMR xlsx files found in *data_dir*.

    Args:
        data_dir: Directory to search for ``rmr-*.xlsx`` files.

    Returns:
        Sorted list of matching Path objects.
    """
    return sorted(data_dir.glob("rmr-*.xlsx"))


def extract_city_name(filepath: Path) -> str:
    """Derive a human-readable city name from an RMR xlsx filename.

    Strips the leading ``rmr-`` prefix, the trailing year and ``-en``
    suffix, and title-cases the remaining parts.  Also drops the
    literal token ``cma`` when it appears as part of the filename
    (e.g. ``rmr-quebec-cma-2025-en.xlsx`` → ``'Quebec'``).

    Args:
        filepath: Path such as ``rmr-st-catharines-niagara-2025-en.xlsx``.

    Returns:
        Title-cased city name, e.g. ``'St Catharines Niagara'``.
    """
    stem = filepath.stem  # e.g. 'rmr-vancouver-2025-en'
    parts = stem.split("-")
    city_parts = [
        p for p in parts[1:]
        if not p.isdigit() and p not in ("en", "cma")
    ]
    return " ".join(p.title() for p in city_parts)


def normalize_city_name(raw_name: str) -> str:
    """Strip CMA/CA suffixes and province codes from a zone label.

    Used to convert zone labels like ``'Vancouver CMA'`` or
    ``'Kelowna CA (BC)'`` into clean city names.

    Args:
        raw_name: Raw zone label as it appears in the xlsx data.

    Returns:
        Clean city name with suffixes and province codes removed.
    """
    name = _CMA_SUFFIX_RE.sub("", raw_name).strip()
    name = _PROVINCE_CODE_RE.sub("", name).strip()
    return name


# ---------------------------------------------------------------------------
# Internal parsing helpers
# ---------------------------------------------------------------------------


def _get_sheet_rows(filepath: Path, sheet_name: str) -> list[tuple]:
    """Read one worksheet via ``pd.read_excel`` and return non-empty rows.

    Uses ``header=None`` so the raw cell layout is preserved.  Pandas
    NaN values (empty cells) are replaced with ``None`` for consistent
    downstream handling.

    Args:
        filepath: Path to the xlsx file.
        sheet_name: Name of the worksheet to read.

    Returns:
        List of row tuples with NaN replaced by ``None``, all-empty
        rows dropped.
    """
    df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
    df = df.dropna(how="all")

    rows: list[tuple] = []
    for row in df.itertuples(index=False, name=None):
        cleaned = tuple(None if pd.isna(v) else v for v in row)
        if any(v is not None for v in cleaned):
            rows.append(cleaned)
    return rows


def _find_header_rows(rows: list[tuple]) -> tuple[int, int]:
    """Locate the bedroom-type and date header rows within *rows*.

    The date header row has ``'Zone'`` as its first cell; the bedroom
    header row is immediately above it.

    Args:
        rows: All non-empty rows from the worksheet.

    Returns:
        Tuple ``(bedroom_row_idx, date_row_idx)`` into *rows*.

    Raises:
        ValueError: If the ``'Zone'`` sentinel cannot be found.
    """
    for i, row in enumerate(rows):
        if row[0] == "Zone":
            return i - 1, i
    raise ValueError("Could not locate 'Zone' header row in worksheet")


def _bedroom_offsets(bedroom_row: tuple) -> dict[str, int]:
    """Map each bedroom type label to its starting column index.

    Args:
        bedroom_row: The header row containing bedroom type labels.

    Returns:
        Dict mapping bedroom type (e.g. ``'Studio'``) to the 0-based
        column index where that bedroom's data begins.
    """
    return {
        str(cell): col_idx
        for col_idx, cell in enumerate(bedroom_row)
        if cell in _BEDROOM_TYPES
    }


def _clean_numeric(value: object) -> float | None:
    """Convert a raw cell value to float.

    Handles suppressed values (``**``), thousands-separated strings
    (``'1,667'``), bare numbers, and ``None``.

    Args:
        value: Raw cell value from the worksheet.

    Returns:
        Float value, or ``None`` for suppressed / missing data.
    """
    if value is None or value == "**":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _is_data_row(row: tuple) -> bool:
    """Return ``True`` if *row* represents a zone or CMA data row.

    Filters out title rows, footnotes, and quality-indicator legends.

    Args:
        row: A worksheet row tuple.

    Returns:
        ``True`` for data rows; ``False`` for metadata / footnotes.
    """
    zone = row[0]
    if not isinstance(zone, str) or not zone.strip():
        return False
    return not any(zone.startswith(p) for p in _FOOTNOTE_PREFIXES)


def _is_cma_total(zone: str) -> bool:
    """Return ``True`` if *zone* is the aggregate CMA total row.

    Matches labels like ``'Vancouver CMA'`` and multi-part names like
    ``'Kitchener - Cambridge - Waterloo CMA'``, but excludes sub-zone labels
    like ``'Remainder of Halifax CMA'`` (which start with a descriptive word
    before a dash separator).

    Args:
        zone: Zone label string.

    Returns:
        ``True`` for CMA aggregate rows only.
    """
    upper = zone.upper()
    if not upper.endswith(" CMA"):
        return False
    # Sub-zone "Remainder" rows appear either as "Remainder of X CMA" or
    # "Zone N - Remainder of X CMA".  Multi-city CMAs use " - " between city
    # names (e.g. "Kitchener - Cambridge - Waterloo CMA") and must NOT be
    # excluded.  We exclude any zone that contains the word "REMAINDER".
    return "REMAINDER" not in upper


# ---------------------------------------------------------------------------
# Per-table parsers
# ---------------------------------------------------------------------------


def parse_vacancy_table(filepath: Path) -> list[dict]:
    """Extract vacancy rates from Table 1.1.1.

    Column layout per bedroom type (5 columns):
    ``value_oct24, quality, value_oct25, quality, trend``

    Args:
        filepath: Path to a city's RMR xlsx file.

    Returns:
        List of record dicts with keys: ``zone``, ``is_cma_total``,
        ``survey_date``, ``bedroom_type``, ``vacancy_rate``, ``vacancy_quality``.
    """
    rows = _get_sheet_rows(filepath, "Table 1.1.1")
    bdr_idx, date_idx = _find_header_rows(rows)
    offsets = _bedroom_offsets(rows[bdr_idx])
    date_row = rows[date_idx]

    records: list[dict] = []
    for row in rows[date_idx + 1 :]:
        if not _is_data_row(row):
            continue
        zone = str(row[0]).strip()
        for btype, start_col in offsets.items():
            for year_offset in (0, 2):  # Oct-24 at +0, Oct-25 at +2
                val_col = start_col + year_offset
                qual_col = val_col + 1
                if val_col >= len(row):
                    continue
                date_str = date_row[val_col]
                if date_str not in _DATE_MAP:
                    continue
                records.append(
                    {
                        "zone": zone,
                        "is_cma_total": _is_cma_total(zone),
                        "survey_date": _DATE_MAP[date_str],
                        "bedroom_type": btype,
                        "vacancy_rate": _clean_numeric(row[val_col]),
                        "vacancy_quality": row[qual_col] if qual_col < len(row) else None,
                    }
                )
    return records


def parse_rent_table(filepath: Path) -> list[dict]:
    """Extract average rents from Table 1.1.2.

    Column layout per bedroom type (4 columns):
    ``value_oct24, quality, value_oct25, quality``

    Args:
        filepath: Path to a city's RMR xlsx file.

    Returns:
        List of record dicts with keys: ``zone``, ``is_cma_total``,
        ``survey_date``, ``bedroom_type``, ``avg_rent``, ``rent_quality``.
    """
    rows = _get_sheet_rows(filepath, "Table 1.1.2")
    bdr_idx, date_idx = _find_header_rows(rows)
    offsets = _bedroom_offsets(rows[bdr_idx])
    date_row = rows[date_idx]

    records: list[dict] = []
    for row in rows[date_idx + 1 :]:
        if not _is_data_row(row):
            continue
        zone = str(row[0]).strip()
        for btype, start_col in offsets.items():
            for year_offset in (0, 2):  # Oct-24 at +0, Oct-25 at +2
                val_col = start_col + year_offset
                qual_col = val_col + 1
                if val_col >= len(row):
                    continue
                date_str = date_row[val_col]
                if date_str not in _DATE_MAP:
                    continue
                records.append(
                    {
                        "zone": zone,
                        "is_cma_total": _is_cma_total(zone),
                        "survey_date": _DATE_MAP[date_str],
                        "bedroom_type": btype,
                        "avg_rent": _clean_numeric(row[val_col]),
                        "rent_quality": row[qual_col] if qual_col < len(row) else None,
                    }
                )
    return records


def parse_universe_table(filepath: Path) -> list[dict]:
    """Extract rental universe counts from Table 1.1.3.

    Column layout per bedroom type (2 columns, no quality flags):
    ``value_oct24, value_oct25``

    Args:
        filepath: Path to a city's RMR xlsx file.

    Returns:
        List of record dicts with keys: ``zone``, ``is_cma_total``,
        ``survey_date``, ``bedroom_type``, ``rental_universe``.
    """
    rows = _get_sheet_rows(filepath, "Table 1.1.3")
    bdr_idx, date_idx = _find_header_rows(rows)
    offsets = _bedroom_offsets(rows[bdr_idx])
    date_row = rows[date_idx]

    records: list[dict] = []
    for row in rows[date_idx + 1 :]:
        if not _is_data_row(row):
            continue
        zone = str(row[0]).strip()
        for btype, start_col in offsets.items():
            for year_offset in (0, 1):  # Oct-24 at +0, Oct-25 at +1
                val_col = start_col + year_offset
                if val_col >= len(row):
                    continue
                date_str = date_row[val_col]
                if date_str not in _DATE_MAP:
                    continue
                raw_val = _clean_numeric(row[val_col])
                records.append(
                    {
                        "zone": zone,
                        "is_cma_total": _is_cma_total(zone),
                        "survey_date": _DATE_MAP[date_str],
                        "bedroom_type": btype,
                        "rental_universe": int(raw_val) if raw_val is not None else None,
                    }
                )
    return records


# ---------------------------------------------------------------------------
# City and batch ingestion
# ---------------------------------------------------------------------------


def ingest_city(filepath: Path) -> pd.DataFrame:
    """Ingest one CMHC RMR xlsx file into a tidy DataFrame.

    Reads Tables 1.1.1, 1.1.2, and 1.1.3 via ``pd.read_excel`` and
    merges them on ``(zone, is_cma_total, survey_date, bedroom_type)``.

    Args:
        filepath: Path to a single city's RMR xlsx file.

    Returns:
        DataFrame with columns: ``city``, ``zone``, ``is_cma_total``,
        ``survey_date``, ``bedroom_type``, ``vacancy_rate``,
        ``vacancy_quality``, ``avg_rent``, ``rent_quality``,
        ``rental_universe``.
    """
    city = extract_city_name(filepath)
    merge_keys = ["zone", "is_cma_total", "survey_date", "bedroom_type"]

    vacancy_df = pd.DataFrame(parse_vacancy_table(filepath))
    rent_df = pd.DataFrame(parse_rent_table(filepath))
    universe_df = pd.DataFrame(parse_universe_table(filepath))

    df = (
        vacancy_df
        .merge(rent_df, on=merge_keys, how="outer")
        .merge(universe_df, on=merge_keys, how="outer")
    )
    df.insert(0, "city", city)
    return df


def ingest_all(data_dir: Path = RAW_DIR) -> pd.DataFrame:
    """Ingest all CMHC RMR xlsx files in *data_dir* into one DataFrame.

    Args:
        data_dir: Directory containing ``rmr-*.xlsx`` files.

    Returns:
        Combined DataFrame for all cities, sorted by
        ``city``, ``survey_date``, ``bedroom_type``.

    Raises:
        FileNotFoundError: If no xlsx files are found in *data_dir*.
    """
    files = list_raw_files(data_dir)
    if not files:
        raise FileNotFoundError(f"No rmr-*.xlsx files found in {data_dir}")

    frames: list[pd.DataFrame] = []
    for f in files:
        print(f"  ingesting {f.name} …")
        frames.append(ingest_city(f))

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(
        ["city", "survey_date", "bedroom_type"]
    ).reset_index(drop=True)
    return combined


def save_processed(df: pd.DataFrame, filename: str = "cmhc_rental.parquet") -> Path:
    """Persist the processed DataFrame to *data/processed/* as Parquet.

    Args:
        df: Cleaned, combined DataFrame to save.
        filename: Output filename (must end in ``.parquet``).

    Returns:
        Absolute Path to the written file.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / filename
    df.to_parquet(out_path, index=False)
    print(f"saved {len(df):,} rows → {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print("ingesting CMHC Rental Market Report data …\n")
    df = ingest_all()

    print(f"\ntotal rows : {len(df):,}")
    print(f"cities     : {sorted(df['city'].unique())}")
    print(f"dates      : {sorted(df['survey_date'].dt.date.unique())}")

    cma_total = df[df["is_cma_total"] & (df["bedroom_type"] == "Total")].copy()
    cma_total = cma_total.sort_values(["city", "survey_date"])
    print("\nCMA-level totals (all bedroom types combined):")
    print(
        cma_total[
            ["city", "survey_date", "vacancy_rate", "avg_rent", "rental_universe"]
        ].to_string(index=False)
    )

    save_processed(df)  # → data/processed/cmhc_rental.parquet
