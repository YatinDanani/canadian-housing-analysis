"use client";

import {
  ScatterChart, Scatter, XAxis, YAxis, ZAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine,
} from "recharts";
import { FeatureRecord } from "@/types";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CustomTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload as FeatureRecord;
  return (
    <div className="bg-white border border-slate-200 rounded-xl p-3 shadow-lg text-xs space-y-1">
      <p className="font-semibold text-slate-800">{d.city}</p>
      <p className="text-slate-500">Vacancy Oct-25: <span className="text-slate-700 font-medium">{d.vacancy_rate_oct25?.toFixed(1)}%</span></p>
      <p className="text-slate-500">Rent growth YoY: <span className="text-slate-700 font-medium">{d.avg_rent_yoy_pct?.toFixed(1)}%</span></p>
      <p className="text-slate-500">Rental universe: <span className="text-slate-700 font-medium">{d.rental_universe_oct25?.toLocaleString()}</span></p>
      <p className="text-slate-500">Tightness score: <span className="text-slate-700 font-medium">{d.market_tightness?.toFixed(2)}</span></p>
    </div>
  );
}

export default function VacancyScatterChart({ data }: { data: FeatureRecord[] }) {
  const valid = data.filter(
    (d) => d.vacancy_rate_oct25 != null && d.avg_rent_yoy_pct != null
  );

  return (
    <ResponsiveContainer width="100%" height={340}>
      <ScatterChart margin={{ top: 8, right: 24, left: 8, bottom: 8 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="vacancy_rate_oct25"
          name="Vacancy Rate"
          type="number"
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(v) => `${v}%`}
          label={{ value: "Vacancy Rate Oct-25 (%)", position: "insideBottom", offset: -4, fontSize: 11, fill: "#94a3b8" }}
        />
        <YAxis
          dataKey="avg_rent_yoy_pct"
          name="Rent Growth"
          type="number"
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(v) => `${v}%`}
          label={{ value: "YoY Rent Growth (%)", angle: -90, position: "insideLeft", offset: 8, fontSize: 11, fill: "#94a3b8" }}
        />
        <ZAxis
          dataKey="rental_universe_oct25"
          range={[40, 400]}
          name="Rental Universe"
        />
        <Tooltip content={<CustomTooltip />} />
        <ReferenceLine y={0} stroke="#cbd5e1" />
        <Scatter data={valid} fill="#3b82f6" fillOpacity={0.7} />
      </ScatterChart>
    </ResponsiveContainer>
  );
}
