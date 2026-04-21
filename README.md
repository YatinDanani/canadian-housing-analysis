# 🏠 Canadian Rental Housing Dashboard

> An end-to-end data pipeline and interactive dashboard analyzing Canadian rental housing affordability across 18 CMAs using real government data.

**Live:** [cadhouseanalyze.vercel.app](https://cadhouseanalyze.vercel.app)

---

## Overview

This project ingests CMHC Rental Market Report data and Statistics Canada datasets, runs ETL and feature engineering pipelines, trains an XGBoost forecasting model, and serves everything through a REST API and interactive Next.js dashboard. It covers two survey snapshots — **October 2024** and **October 2025** — across 18 Canadian Metropolitan Areas (CMAs).

### Key questions answered

1. Which Canadian cities saw the fastest rent growth Oct 2024 → Oct 2025?
2. Do lower-vacancy cities show higher rent growth — and how strong is that relationship?
3. Which cities are most and least affordable relative to each other?
4. Which CMAs have the tightest rental markets (low vacancy + high growth)?
5. Which CMAs is the forecasting model most uncertain about — and why?

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
   src/api.py           ← FastAPI REST API (serves parquet, no DB needed)
        │
        ▼
   frontend/            ← Next.js 14 + Recharts dashboard (deployed on Vercel)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Data | Pandas, NumPy, SciPy |
| Database | PostgreSQL 15, SQLAlchemy, psycopg2 |
| ML | XGBoost, scikit-learn, Prophet (optional) |
| API | FastAPI, uvicorn |
| Frontend | Next.js 14, TypeScript, Tailwind CSS, Recharts |
| Infra | Docker, docker-compose, GitHub Actions, Railway, Vercel |
| Testing | pytest, ruff |

---

## Data Sources

| Dataset | Source | Format | Path |
|---|---|---|---|
| Rental Market Report (per CMA) | CMHC Housing Market Data | `.xlsx` | `data/raw/rmr-{city}-2025-en.xlsx` |
| Housing Price Index | StatCan Table 18-10-0205-01 | `.xlsx` | `data/raw/statcan_hpi.xlsx` |
| Median Household Income | StatCan Table 11-10-0190-01 | `.xlsx` | `data/raw/statcan_income.xlsx` |

`ingest.py` reads Tables 1.1.1 (vacancy rates), 1.1.2 (avg rents), and 1.1.3 (rental universe) from each city file and outputs `data/processed/cmhc_rental.parquet`. All raw files are gitignored — only processed outputs are committed.

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker & docker-compose
- Node.js 18+ (for frontend development)

### Backend

```bash
# 1. Clone the repo
git clone https://github.com/your-username/canadian-housing-analysis.git
cd canadian-housing-analysis

# 2. Set up environment
cp .env.example .env   # fill in your DB credentials

# 3. Start Postgres
docker-compose up -d

# 4. Run the pipeline
python3 src/ingest.py     # fetch and clean data
python3 src/transform.py  # feature engineering
python3 src/load.py       # load into postgres
python3 src/model.py      # train forecasting model

# 5. Start the API
uvicorn src.api:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
cp .env.local.example .env.local   # set NEXT_PUBLIC_API_URL
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Health check — confirms API is up and data is loaded |
| `GET` | `/api/cities` | Sorted list of all 18 CMA city names |
| `GET` | `/api/bedroom-types` | Available bedroom type options |
| `GET` | `/api/features` | Full feature dataset (supports query filters) |
| `GET` | `/api/forecasts` | Oct 2026 predicted rents with confidence intervals |

---

## Database Schema

```sql
cities          (id, name, province, cma_code)
vacancy_rates   (id, city_id, date, vacancy_rate, rental_universe)
price_index     (id, city_id, date, hpi_value, property_type)
affordability   (id, city_id, date, median_income, price_to_income_ratio)
forecasts       (id, city_id, forecast_date, predicted_vacancy, lower_ci, upper_ci)
```

---

## CI/CD

- **All branches:** `ruff` lint + `pytest`
- **`main` only:** Docker build → push to GHCR → Railway webhook deploy → Vercel frontend deploy

Secrets required in GitHub repo settings: `GHCR_TOKEN`, `RAILWAY_WEBHOOK_URL`.

---

## Project Structure

```
.
├── data/
│   ├── raw/               # gitignored CMHC + StatCan xlsx files
│   └── processed/         # committed parquet outputs
├── frontend/              # Next.js 14 dashboard
│   └── src/
│       ├── app/
│       └── components/
├── notebooks/
│   └── eda.ipynb          # EDA + SQL window queries
├── src/
│   ├── ingest.py
│   ├── transform.py
│   ├── load.py
│   ├── model.py
│   ├── api.py
│   ├── dashboard.py       # legacy Streamlit dashboard
│   └── queries/           # SQL files
├── tests/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## Developer

**Yatin Danani** — Third-year B.Sc. CS + Math, University of Victoria

---

## License

MIT
