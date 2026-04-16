"use client";

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { FeatureRecord } from "@/types";

export default function RentComparisonChart({ data }: { data: FeatureRecord[] }) {
  const sorted = [...data].sort((a, b) =>
    (b.avg_rent_oct25 ?? 0) - (a.avg_rent_oct25 ?? 0)
  );

  return (
    <ResponsiveContainer width="100%" height={340}>
      <BarChart data={sorted} margin={{ top: 4, right: 8, left: 8, bottom: 60 }}>
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
          formatter={(v: number) => [`$${v.toLocaleString()}`, ""]}
          contentStyle={{ borderRadius: 8, border: "1px solid #e2e8f0", fontSize: 12 }}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Bar dataKey="avg_rent_oct24" name="Oct 2024" fill="#93c5fd" radius={[3, 3, 0, 0]} />
        <Bar dataKey="avg_rent_oct25" name="Oct 2025" fill="#1d4ed8" radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
