export type BedroomType = "Total" | "Studio" | "1 Bedroom" | "2 Bedroom" | "3 Bedroom +";

export interface FeatureRecord {
  city: string;
  zone: string;
  is_cma_total: boolean;
  bedroom_type: string;
  vacancy_rate_oct24: number | null;
  vacancy_rate_oct25: number | null;
  avg_rent_oct24: number | null;
  avg_rent_oct25: number | null;
  rental_universe_oct24: number | null;
  rental_universe_oct25: number | null;
  vacancy_rate_yoy_change: number | null;
  vacancy_rate_yoy_pct: number | null;
  avg_rent_yoy_change: number | null;
  avg_rent_yoy_pct: number | null;
  rental_universe_yoy_change: number | null;
  rental_universe_yoy_pct: number | null;
  market_tightness: number | null;
  rent_growth_rank: number | null;
  vacancy_rank: number | null;
  universe_growth_rank: number | null;
}

export interface ForecastRecord {
  city: string;
  bedroom_type: string;
  forecast_date: string;
  predicted_rent: number;
  lower_ci: number;
  upper_ci: number;
}
