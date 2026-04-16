"""
src/load.py
~~~~~~~~~~~
Load processed CMHC data into PostgreSQL using SQLAlchemy.

Tables created/populated:
  cities        — one row per CMA
  vacancy_rates — vacancy rate + rental universe by city/date/bedroom
  avg_rents     — average rent by city/date/bedroom
  forecasts     — model predictions (populated by model.py)
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Boolean, Column, Date, Float, Integer, MetaData,
    String, Table, create_engine, text,
)
from sqlalchemy.dialects.postgresql import insert

load_dotenv()

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

_PROVINCE_MAP: dict[str, str] = {
    "Calgary": "AB", "Edmonton": "AB",
    "Gatineau": "QC", "Montreal": "QC", "Quebec": "QC",
    "Halifax": "NS",
    "Hamilton": "ON", "Kitchener Cambridge Waterloo": "ON",
    "London": "ON", "Ottawa": "ON", "St Catharines Niagara": "ON",
    "Toronto": "ON", "Windsor": "ON",
    "Regina": "SK", "Saskatoon": "SK",
    "Vancouver": "BC", "Victoria": "BC",
    "Winnipeg": "MB",
}


def get_engine():
    """Create a SQLAlchemy engine from environment variables.

    Returns:
        SQLAlchemy Engine connected to the configured PostgreSQL database.
    """
    url = os.environ.get("DATABASE_URL") or (
        "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            db=os.environ.get("POSTGRES_DB", "housing"),
        )
    )
    return create_engine(url)


def define_schema(metadata: MetaData) -> dict[str, Table]:
    """Define all table objects against *metadata*.

    Args:
        metadata: SQLAlchemy MetaData instance.

    Returns:
        Dict mapping table name to Table object.
    """
    cities = Table(
        "cities", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(120), nullable=False, unique=True),
        Column("province", String(2)),
    )
    vacancy_rates = Table(
        "vacancy_rates", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("city_id", Integer, nullable=False),
        Column("zone", String(200)),
        Column("is_cma_total", Boolean),
        Column("survey_date", Date, nullable=False),
        Column("bedroom_type", String(20)),
        Column("vacancy_rate", Float),
        Column("rental_universe", Integer),
    )
    avg_rents = Table(
        "avg_rents", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("city_id", Integer, nullable=False),
        Column("zone", String(200)),
        Column("is_cma_total", Boolean),
        Column("survey_date", Date, nullable=False),
        Column("bedroom_type", String(20)),
        Column("avg_rent", Float),
    )
    forecasts = Table(
        "forecasts", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("city_id", Integer, nullable=False),
        Column("bedroom_type", String(20)),
        Column("forecast_date", Date, nullable=False),
        Column("predicted_rent", Float),
        Column("lower_ci", Float),
        Column("upper_ci", Float),
    )
    return {"cities": cities, "vacancy_rates": vacancy_rates,
            "avg_rents": avg_rents, "forecasts": forecasts}


def load_cities(conn, tables: dict[str, Table], df: pd.DataFrame) -> dict[str, int]:
    """Upsert city rows and return a name→id mapping.

    Args:
        conn: Active SQLAlchemy connection.
        tables: Table dict from ``define_schema``.
        df: Raw rental DataFrame with a ``city`` column.

    Returns:
        Dict mapping city name to integer primary key.
    """
    cities = df["city"].unique()
    for name in cities:
        stmt = insert(tables["cities"]).values(
            name=name, province=_PROVINCE_MAP.get(name)
        ).on_conflict_do_nothing(index_elements=["name"])
        conn.execute(stmt)

    rows = conn.execute(
        text("SELECT id, name FROM cities WHERE name = ANY(:names)"),
        {"names": list(cities)},
    ).fetchall()
    return {name: id_ for id_, name in rows}


def load_rental_data(
    conn, tables: dict[str, Table], df: pd.DataFrame, city_ids: dict[str, int]
) -> None:
    """Insert vacancy and rent rows (truncates existing data first).

    Args:
        conn: Active SQLAlchemy connection.
        tables: Table dict from ``define_schema``.
        df: Long-format rental DataFrame.
        city_ids: City name → id mapping.
    """
    conn.execute(tables["vacancy_rates"].delete())
    conn.execute(tables["avg_rents"].delete())

    vac_rows, rent_rows = [], []
    for row in df.itertuples(index=False):
        city_id = city_ids[row.city]
        base = dict(
            city_id=city_id,
            zone=row.zone,
            is_cma_total=bool(row.is_cma_total),
            survey_date=row.survey_date.date(),
            bedroom_type=row.bedroom_type,
        )
        vac_rows.append({**base, "vacancy_rate": row.vacancy_rate,
                         "rental_universe": row.rental_universe})
        if not pd.isna(row.avg_rent):
            rent_rows.append({**base, "avg_rent": row.avg_rent})

    if vac_rows:
        conn.execute(tables["vacancy_rates"].insert(), vac_rows)
    if rent_rows:
        conn.execute(tables["avg_rents"].insert(), rent_rows)
    print(f"  vacancy_rates: {len(vac_rows):,} rows")
    print(f"  avg_rents:     {len(rent_rows):,} rows")


def run_load(rental_path: Path = PROCESSED_DIR / "cmhc_rental.parquet") -> None:
    """Full load: create schema, upsert cities, insert rental data.

    Args:
        rental_path: Path to the processed rental Parquet file.
    """
    engine = get_engine()
    metadata = MetaData()
    tables = define_schema(metadata)

    print("creating tables …")
    metadata.create_all(engine)

    df = pd.read_parquet(rental_path)
    print(f"loaded {len(df):,} rows from {rental_path.name}")

    with engine.begin() as conn:
        print("upserting cities …")
        city_ids = load_cities(conn, tables, df)
        print(f"  {len(city_ids)} cities")
        print("loading rental data …")
        load_rental_data(conn, tables, df, city_ids)

    print("done.")


if __name__ == "__main__":
    run_load()
