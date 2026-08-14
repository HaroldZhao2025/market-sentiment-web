type Props = {
  ticker: string;
  name?: string | null;
  sector?: string | null;
  size?: "sm" | "md" | "lg";
  className?: string;
};

const sectorTheme: Record<string, { from: string; to: string; accent: string }> = {
  "Information Technology": { from: "#0f766e", to: "#172554", accent: "#5eead4" },
  Technology: { from: "#0f766e", to: "#172554", accent: "#5eead4" },
  Financials: { from: "#1e3a8a", to: "#312e81", accent: "#93c5fd" },
  "Health Care": { from: "#115e59", to: "#064e3b", accent: "#6ee7b7" },
  Healthcare: { from: "#115e59", to: "#064e3b", accent: "#6ee7b7" },
  Industrials: { from: "#374151", to: "#1f2937", accent: "#d1d5db" },
  "Consumer Discretionary": { from: "#7c2d12", to: "#4c1d95", accent: "#fdba74" },
  "Consumer Staples": { from: "#365314", to: "#14532d", accent: "#bef264" },
  Energy: { from: "#78350f", to: "#422006", accent: "#fbbf24" },
  Materials: { from: "#3f3f46", to: "#713f12", accent: "#facc15" },
  Utilities: { from: "#164e63", to: "#1e3a8a", accent: "#67e8f9" },
  "Real Estate": { from: "#581c87", to: "#312e81", accent: "#d8b4fe" },
  "Communication Services": { from: "#7e22ce", to: "#172554", accent: "#c4b5fd" },
};

function hashTicker(ticker: string) {
  return Array.from(ticker).reduce((sum, char, index) => sum + char.charCodeAt(0) * (index + 3), 17);
}

function initials(ticker: string) {
  const clean = ticker.replace(/[^A-Z0-9]/gi, "").toUpperCase();
  return clean.slice(0, 4) || "CO";
}

export default function CompanyVisual({ ticker, name, sector, size = "md", className = "" }: Props) {
  const theme = sectorTheme[String(sector || "")] ?? { from: "#27272a", to: "#0f172a", accent: "#a3a3a3" };
  const hash = hashTicker(ticker);
  const id = `company-visual-${ticker.replace(/[^a-z0-9]/gi, "")}-${hash}`;
  const dimensions = size === "sm" ? "h-11 w-11" : size === "lg" ? "h-24 w-24" : "h-14 w-14";
  const bars = [0, 1, 2, 3].map((index) => 9 + ((hash >> (index * 2)) % 19));

  return (
    <div
      className={`relative shrink-0 overflow-hidden rounded-2xl border border-white/10 shadow-lg shadow-black/20 ${dimensions} ${className}`}
      title={`${name || ticker}${sector ? ` · ${sector}` : ""}`}
      aria-label={`${name || ticker} company illustration`}
    >
      <svg viewBox="0 0 96 96" className="h-full w-full" role="img" aria-hidden="true">
        <defs>
          <linearGradient id={`${id}-bg`} x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor={theme.from} />
            <stop offset="100%" stopColor={theme.to} />
          </linearGradient>
          <radialGradient id={`${id}-glow`} cx="78%" cy="18%" r="75%">
            <stop offset="0%" stopColor={theme.accent} stopOpacity="0.34" />
            <stop offset="100%" stopColor={theme.accent} stopOpacity="0" />
          </radialGradient>
        </defs>
        <rect width="96" height="96" rx="20" fill={`url(#${id}-bg)`} />
        <rect width="96" height="96" rx="20" fill={`url(#${id}-glow)`} />
        <path d={`M8 ${76 - (hash % 13)} C28 ${51 + (hash % 11)}, 52 ${83 - (hash % 17)}, 88 ${38 + (hash % 23)}`} fill="none" stroke={theme.accent} strokeOpacity="0.24" strokeWidth="2" />
        {bars.map((height, index) => (
          <rect key={index} x={10 + index * 9} y={81 - height} width="5" height={height} rx="2.5" fill={theme.accent} fillOpacity={0.12 + index * 0.035} />
        ))}
        <circle cx={76 - (hash % 8)} cy={20 + (hash % 9)} r={10 + (hash % 5)} fill={theme.accent} fillOpacity="0.08" />
        <text x="48" y="54" textAnchor="middle" fill="white" fontSize={ticker.length > 3 ? "22" : "26"} fontWeight="750" letterSpacing="-1.2">
          {initials(ticker)}
        </text>
        <text x="48" y="70" textAnchor="middle" fill={theme.accent} fillOpacity="0.78" fontSize="6.5" fontWeight="700" letterSpacing="1.2">
          {String(sector || "COMPANY").toUpperCase().slice(0, 14)}
        </text>
      </svg>
    </div>
  );
}
