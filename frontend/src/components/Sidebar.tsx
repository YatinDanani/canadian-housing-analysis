"use client";

import { BedroomType } from "@/types";

const BEDROOM_TYPES: BedroomType[] = [
  "Total",
  "Studio",
  "1 Bedroom",
  "2 Bedroom",
  "3 Bedroom +",
];

interface SidebarProps {
  bedroom: BedroomType;
  setBedroom: (b: BedroomType) => void;
  cities: string[];
  selectedCities: string[];
  setSelectedCities: (c: string[]) => void;
}

export default function Sidebar({
  bedroom,
  setBedroom,
  cities,
  selectedCities,
  setSelectedCities,
}: SidebarProps) {
  const allSelected = selectedCities.length === cities.length;

  function toggleCity(city: string) {
    setSelectedCities(
      selectedCities.includes(city)
        ? selectedCities.filter((c) => c !== city)
        : [...selectedCities, city]
    );
  }

  return (
    <aside className="w-56 flex-shrink-0 bg-slate-900 flex flex-col h-screen sticky top-0">
      {/* Logo / title */}
      <div className="px-5 py-6 border-b border-slate-700">
        <p className="text-xs font-bold text-blue-400 uppercase tracking-widest">
          Housing Analysis
        </p>
        <p className="text-xs text-slate-500 mt-1">18 CMAs · CMHC 2024–25</p>
      </div>

      <div className="flex-1 overflow-y-auto sidebar-scroll px-5 py-5 space-y-6">
        {/* Bedroom type */}
        <div>
          <p className="text-xs font-semibold text-slate-400 uppercase tracking-wide mb-3">
            Bedroom Type
          </p>
          <div className="space-y-1">
            {BEDROOM_TYPES.map((b) => (
              <button
                key={b}
                onClick={() => setBedroom(b)}
                className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors ${
                  bedroom === b
                    ? "bg-blue-600 text-white font-medium"
                    : "text-slate-300 hover:bg-slate-800"
                }`}
              >
                {b}
              </button>
            ))}
          </div>
        </div>

        {/* Cities */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <p className="text-xs font-semibold text-slate-400 uppercase tracking-wide">
              Cities
            </p>
            <button
              onClick={() =>
                setSelectedCities(allSelected ? [] : [...cities])
              }
              className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
            >
              {allSelected ? "Clear" : "All"}
            </button>
          </div>
          <div className="space-y-1">
            {cities.map((city) => (
              <label
                key={city}
                className="flex items-center gap-2.5 px-1 py-1 cursor-pointer group"
              >
                <input
                  type="checkbox"
                  checked={selectedCities.includes(city)}
                  onChange={() => toggleCity(city)}
                  className="w-3.5 h-3.5 rounded accent-blue-500 cursor-pointer"
                />
                <span className="text-sm text-slate-300 group-hover:text-white transition-colors truncate">
                  {city}
                </span>
              </label>
            ))}
          </div>
        </div>
      </div>

      <div className="px-5 py-4 border-t border-slate-700">
        <p className="text-xs text-slate-500">
          {selectedCities.length} of {cities.length} cities
        </p>
      </div>
    </aside>
  );
}
