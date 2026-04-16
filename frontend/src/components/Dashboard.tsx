"use client";

import { useState, useEffect, ReactNode } from "react";
import { BedroomType, FeatureRecord, ForecastRecord } from "@/types";
import { fetchCities, fetchFeatures, fetchForecasts } from "@/lib/api";
import Sidebar from "./Sidebar";
import MetricCard from "./MetricCard";
import RentComparisonChart from "./charts/RentComparisonChart";
import RentGrowthChart from "./charts/RentGrowthChart";
import VacancyScatterChart from "./charts/VacancyScatterChart";
import MarketTightnessChart from "./charts/MarketTightnessChart";
import ForecastsChart from "./charts/ForecastsChart";

function ChartCard({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="bg-white rounded-2xl shadow-sm border border-slate-100 p-6">
      <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-5">
        {title}
      </h2>
      {children}
    </div>
  );
}

function Skeleton({ className }: { className?: string }) {
  return (
    <div className={`animate-pulse bg-slate-100 rounded-2xl ${className}`} />
  );
}

export default function Dashboard() {
  const [bedroom, setBedroom] = useState<BedroomType>("Total");
  const [cities, setCities] = useState<string[]>([]);
  const [selectedCities, setSelectedCities] = useState<string[]>([]);
  const [features, setFeatures] = useState<FeatureRecord[]>([]);
  const [forecasts, setForecasts] = useState<ForecastRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Load city list once on mount
  useEffect(() => {
    fetchCities()
      .then((c) => {
        setCities(c);
        setSelectedCities(c);
      })
      .catch((e: Error) => setError(e.message));
  }, []);

  // Reload features + forecasts when bedroom changes
  useEffect(() => {
    setLoading(true);
    const forecastBedroom = bedroom === "Total" ? "1 Bedroom" : bedroom;
    Promise.all([fetchFeatures(bedroom), fetchForecasts(forecastBedroom)])
      .then(([f, fc]) => {
        setFeatures(f);
        setForecasts(fc);
        setLoading(false);
      })
      .catch((e: Error) => {
        setError(e.message);
        setLoading(false);
      });
  }, [bedroom]);

  // Client-side city filter
  const filtered = features.filter((f) => selectedCities.includes(f.city));
  const filteredForecasts = forecasts.filter((f) =>
    selectedCities.includes(f.city)
  );

  // Summary metrics
  const byGrowth = [...filtered]
    .filter((d) => d.avg_rent_yoy_pct != null)
    .sort((a, b) => (b.avg_rent_yoy_pct ?? 0) - (a.avg_rent_yoy_pct ?? 0));
  const top = byGrowth[0];
  const bottom = byGrowth[byGrowth.length - 1];
  const tightest = [...filtered]
    .filter((d) => d.market_tightness != null)
    .sort((a, b) => (b.market_tightness ?? 0) - (a.market_tightness ?? 0))[0];

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen bg-slate-50">
        <div className="text-center max-w-sm px-6">
          <div className="text-4xl mb-4">⚠️</div>
          <h2 className="text-lg font-semibold text-slate-800">
            Could not reach the API
          </h2>
          <p className="text-sm text-slate-500 mt-2">{error}</p>
          <p className="text-xs text-slate-400 mt-4">
            Make sure the FastAPI service is running and{" "}
            <code className="bg-slate-100 px-1 rounded">
              NEXT_PUBLIC_API_URL
            </code>{" "}
            is set correctly.
          </p>
        </div>
      </div>
    );
  }

  const forecastLabel =
    bedroom === "Total" ? "1 Bedroom" : bedroom;

  return (
    <div className="flex h-screen bg-slate-50 overflow-hidden">
      <Sidebar
        bedroom={bedroom}
        setBedroom={setBedroom}
        cities={cities}
        selectedCities={selectedCities}
        setSelectedCities={setSelectedCities}
      />

      <main className="flex-1 overflow-y-auto">
        {/* Header */}
        <header className="bg-white border-b border-slate-100 px-8 py-5 sticky top-0 z-10">
          <h1 className="text-xl font-bold text-slate-800">
            Canadian Rental Housing Dashboard
          </h1>
          <p className="text-xs text-slate-400 mt-0.5">
            CMHC Rental Market Reports · 18 CMAs · Oct 2024 &amp; Oct 2025
          </p>
        </header>

        <div className="p-8 space-y-6">
          {loading ? (
            <div className="space-y-6">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {[...Array(4)].map((_, i) => (
                  <Skeleton key={i} className="h-24" />
                ))}
              </div>
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                {[...Array(4)].map((_, i) => (
                  <Skeleton key={i} className="h-80" />
                ))}
              </div>
              <Skeleton className="h-80" />
            </div>
          ) : (
            <>
              {/* KPI cards */}
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                <MetricCard title="Cities shown" value={String(filtered.length)} />
                {top && (
                  <MetricCard
                    title="Highest rent growth"
                    value={top.city}
                    badge={`+${top.avg_rent_yoy_pct?.toFixed(1)}%`}
                    badgeColor="red"
                  />
                )}
                {bottom && (
                  <MetricCard
                    title="Lowest rent growth"
                    value={bottom.city}
                    badge={`${bottom.avg_rent_yoy_pct?.toFixed(1)}%`}
                    badgeColor="green"
                  />
                )}
                {tightest && (
                  <MetricCard
                    title="Tightest market"
                    value={tightest.city}
                    badge={`score ${tightest.market_tightness?.toFixed(2)}`}
                    badgeColor="blue"
                  />
                )}
              </div>

              {/* Row 1 */}
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                <ChartCard title="Average Rent by City">
                  <RentComparisonChart data={filtered} />
                </ChartCard>
                <ChartCard title="YoY Rent Growth — Oct-24 → Oct-25">
                  <RentGrowthChart data={filtered} />
                </ChartCard>
              </div>

              {/* Row 2 */}
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                <ChartCard title="Vacancy Rate vs Rent Growth">
                  <VacancyScatterChart data={filtered} />
                </ChartCard>
                <ChartCard title="Market Tightness Score  (1 = tightest)">
                  <MarketTightnessChart data={filtered} />
                </ChartCard>
              </div>

              {/* Forecasts */}
              <ChartCard
                title={`Oct-26 Predicted Rent — ${forecastLabel} (90% CI)`}
              >
                <ForecastsChart data={filteredForecasts} />
              </ChartCard>
            </>
          )}
        </div>
      </main>
    </div>
  );
}
