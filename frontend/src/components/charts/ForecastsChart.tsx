"use client";

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ErrorBar,
} from "recharts";
import { ForecastRecord } from "@/types";

export default function ForecastsChart({ data }: { data: ForecastRecord[] }) {
  const sorted = [...data].sort((a, b) => b.predicted_rent - a.predicted_rent);

  const chartData = sorted.map((d) => ({
    ...d,
    error_plus: d.upper_ci - d.predicted_rent,
    error_minus: d.predicted_rent - d.lower_ci,
  }));

  return (
    <ResponsiveContainer width="100%" height={340}>
      <BarChart data={chartData} margin={{ top: 16, right: 8, left: 8, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="city"
          tick={{ fontSize: 11, fill: "#64748b" }}
          angle={-40}
          textAnchor="end"
          interval={0}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(v) => `$${(v / 1000).toFixed(1)}k`}
        />
        <Tooltip
          formatter={(v: number) => [`$${Math.round(v).toLocaleString()}`, "Predicted rent"]}
          contentStyle={{ borderRadius: 8, border: "1px solid #e2e8f0", fontSize: 12 }}
        />
        <Bar dataKey="predicted_rent" name="Oct-26 Forecast" fill="#7c3aed" radius={[3, 3, 0, 0]}>
          <ErrorBar
            dataKey="error_plus"
            width={4}
            strokeWidth={2}
            stroke="#4c1d95"
            direction="y"
          />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
