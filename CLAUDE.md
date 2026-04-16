# Canadian Housing Affordability Analyzer

## Project overview
End-to-end data pipeline analyzing Canadian housing affordability using real
government datasets (CMHC + Statistics Canada). Built to demonstrate ETL,
SQL analytics, ML forecasting, and a deployed interactive dashboard.

## Developer
Yatin Danani — third-year B.Sc. CS + Math, University of Victoria.

---

## Architecture
```
CMHC xlsx + StatCan API
        │
        ▼
   src/ingest.py        ← fetch, clean, normalize
        │
        ▼
   src/transform.py     ← feature engineering, indicators
        │
        ▼
   src/load.py          ← PostgreSQL via SQLAlchemy
        │
        ▼
   notebooks/eda.ipynb  ← EDA + SQL window queries
        │
        ▼
   src/model.py         ← XGBoost forecasting, walk-forward validation
        │
        ▼
   src/dashboard.py     ← Streamlit app (deployed on Railway)
```

---

## Stack
- **Language:** Python 3.11
- **Data:** Pandas, NumPy, SciPy
- **Database:** PostgreSQL 15, SQLAlchemy, psycopg2
- **ML:** XGBoost, scikit-learn, Prophet (optional)
- **Viz:** Streamlit, Plotly
- **Infra:** Docker, docker-compose, GitHub Actions, Railway
- **Testing:** pytest, ruff (linter)

---

## Data sources
| Dataset | Source | Format | Location |
|---|---|---|---|
| Rental Market Report (per CMA) | CMHC Housing Market Data | xlsx download | data/raw/rmr-{city}-2025-en.xlsx |
| Housing Price Index | StatCan Table 18-10-0205-01 | xlsx download | data/raw/statcan_hpi.xlsx |
| Median household income | StatCan Table 11-10-0190-01 | xlsx download | data/raw/statcan_income.xlsx |

One xlsx per city, named `rmr-{city}-2025-en.xlsx` (e.g. `rmr-vancouver-2025-en.xlsx`).
ingest.py reads Tables 1.1.1 (vacancy rates), 1.1.2 (avg rents), 1.1.3 (rental universe)
from each file and outputs `data/processed/cmhc_rental.parquet`.

All raw files go in data/raw/ and are gitignored — only processed outputs in
data/processed/ are committed.

---

## Key analytical questions to answer
Data covers two survey points: Oct-2024 and Oct-2025 (18 CMAs).

1. Which Canadian cities saw the fastest rent growth Oct-24 → Oct-25?
2. Do lower-vacancy cities show higher rent growth — and how strong is that relationship?
3. Which cities are most/least affordable relative to each other right now?
4. Which CMAs have the tightest rental markets (low vacancy + high growth)?
5. Which CMAs is the forecasting model most uncertain about — and why?

---

## Conventions
- All source files in src/ with type hints and docstrings on every function
- Tests in tests/ — no mocking core logic, test real functionality
- SQL queries as .sql files in src/queries/ AND embedded in notebooks
- Environment variables via .env (never hardcoded) — see .env.example
- Use snake_case for everything
- Every function must have a docstring with Args and Returns

---

## Database schema (target)
```sql
cities          (id, name, province, cma_code)
vacancy_rates   (id, city_id, date, vacancy_rate, rental_universe)
price_index     (id, city_id, date, hpi_value, property_type)
affordability   (id, city_id, date, median_income, price_to_income_ratio)
forecasts       (id, city_id, forecast_date, predicted_vacancy, lower_ci, upper_ci)
```

---

## CI/CD pipeline
- **All branches:** ruff lint + pytest
- **main only:** Docker build → push to GHCR → Railway webhook deploy
- Secrets needed: GHCR_TOKEN, RAILWAY_WEBHOOK_URL (set in GitHub repo settings)

---

## Running locally
```bash
cp .env.example .env        # fill in your DB credentials
docker-compose up -d        # starts app + postgres
python3 src/ingest.py       # fetch and clean data
python3 src/load.py         # load into postgres
streamlit run src/dashboard.py
```

---

## Claude Code instructions
- Always run ruff src/ after editing any Python file
- Always run pytest tests/ after adding new functionality
- When writing SQL, prefer CTEs over subqueries for readability
- Keep Streamlit components in separate functions — one function per chart
- When in doubt about time-series validation, use walk-forward (not random split)
- Ask me before installing any new dependency not in requirements.txt