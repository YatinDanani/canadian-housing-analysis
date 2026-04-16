"use client";

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell,
} from "recharts";
import { FeatureRecord } from "@/types";

function tightnessColor(score: number): string {
  // Purple-to-red gradient: high score (tightest) = deeper red
  const t = Math.min(Math.max(score, 0), 1);
  const r = Math.round(139 + (220 - 139) * t);
  const g = Math.round(92 - 92 * t);
  const b = Math.round(246 - 246 * t);
  return `rgb(${r},${g},${b})`;
}

export default function MarketTightnessChart({ data }: { data: FeatureRecord[] }) {
  const sorted = [...data]
    .filter((d) => d.market_tightness != null)
    .sort((a, b) => (a.market_tightness ?? 0) - (b.market_tightness ?? 0));

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
          domain={[0, 1]}
          tick={{ fontSize: 11, fill: "#64748b" }}
          tickFormatter={(v) => v.toFixed(2)}
        />
        <YAxis
          type="category"
          dataKey="city"
          tick={{ fontSize: 11, fill: "#64748b" }}
          width={130}
        />
        <Tooltip
          formatter={(v: number) => [v.toFixed(3), "Tightness score"]}
          contentStyle={{ borderRadius: 8, border: "1px solid #e2e8f0", fontSize: 12 }}
        />
        <Bar dataKey="market_tightness" radius={[0, 3, 3, 0]}>
          {sorted.map((entry) => (
            <Cell
              key={entry.city}
              fill={tightnessColor(entry.market_tightness ?? 0)}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
