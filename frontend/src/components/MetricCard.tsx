interface MetricCardProps {
  title: string;
  value: string;
  badge?: string;
  badgeColor?: "red" | "green" | "blue" | "slate";
}

const badgeClasses: Record<string, string> = {
  red: "bg-red-50 text-red-600",
  green: "bg-green-50 text-green-600",
  blue: "bg-blue-50 text-blue-600",
  slate: "bg-slate-100 text-slate-600",
};

export default function MetricCard({
  title,
  value,
  badge,
  badgeColor = "slate",
}: MetricCardProps) {
  return (
    <div className="bg-white rounded-2xl shadow-sm border border-slate-100 p-5">
      <p className="text-xs font-semibold text-slate-400 uppercase tracking-wide">
        {title}
      </p>
      <p className="mt-1 text-xl font-bold text-slate-800 truncate">{value}</p>
      {badge && (
        <span
          className={`mt-2 inline-block text-xs font-semibold px-2 py-0.5 rounded-full ${badgeClasses[badgeColor]}`}
        >
          {badge}
        </span>
      )}
    </div>
  );
}
