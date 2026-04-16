import { FeatureRecord, ForecastRecord } from "@/types";

const API_URL =
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`);
  return res.json();
}

export function fetchCities(): Promise<string[]> {
  return apiFetch<string[]>("/api/cities");
}

export function fetchFeatures(bedroom: string): Promise<FeatureRecord[]> {
  return apiFetch<FeatureRecord[]>(
    `/api/features?bedroom=${encodeURIComponent(bedroom)}`
  );
}

export function fetchForecasts(bedroom: string): Promise<ForecastRecord[]> {
  return apiFetch<ForecastRecord[]>(
    `/api/forecasts?bedroom=${encodeURIComponent(bedroom)}`
  );
}
