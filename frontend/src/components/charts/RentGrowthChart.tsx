"use client";

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell, ReferenceLine,
} from "recharts";
import { FeatureRecord } from "@/types";

export default function RentGrowthChart({ data }: { data: FeatureRecord[] }) {
  const sorted = [...data]
    .filter((d) => d.avg_rent_yoy_pct != null)
    .sort((a, b) => (a.avg_rent_yoy_pct ?? 0) - (b.avg_rent_yoy_pct ?? 0));

  return (
    <ResponsiveContainer width="100%" height={340}>
      <BarChart
        data={sorted}
        layout="vertical"
        margin={{ top: 4, right: 48, left: 8, bottom: 4 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
        <XAxis
          type="number"
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(v) => `${v}%`}
        />
        <YAxis
          type="category"
          dataKey="city"
          tick={{ fontSize: 11, fill: "#64748b" }}
          width={130}
        />
        <Tooltip
          formatter={(v: number) => [`${v.toFixed(1)}%`, "YoY growth"]}
          contentStyle={{ borderRadius: 8, border: "1px solid #e2e8f0", fontSize: 12 }}
        />
        <ReferenceLine x={0} stroke="#cbd5e1" />
        <Bar dataKey="avg_rent_yoy_pct" radius={[0, 3, 3, 0]}>
          {sorted.map((entry) => (
            <Cell
              key={entry.city}
              fill={(entry.avg_rent_yoy_pct ?? 0) >= 0 ? "#ef4444" : "#22c55e"}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
