export default function CompanyMetrics({ total, sp500, midcap }: { total: number; sp500: number; midcap: number }) {
  const ready = total > 0;
  const cards = [
    { label: "Extended universe", value: ready ? total.toLocaleString() : "Pending", note: "S&P 500 + S&P MidCap 400, deduplicated" },
    { label: "S&P 500 core", value: ready ? sp500.toLocaleString() : "Pending", note: "Only this core enters SPX weighting and attribution" },
    { label: "MidCap extension", value: ready ? midcap.toLocaleString() : "Pending", note: "Additional company intelligence coverage" },
  ];
  return (
    <section className="grid gap-3 md:grid-cols-3">
      {cards.map((card) => (
        <div key={card.label} className="rounded-2xl border border-white/10 bg-white/[0.025] p-4 md:p-5">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-neutral-600">{card.label}</div>
          <div className="mt-2 text-3xl font-semibold tracking-tight text-white">{card.value}</div>
          <div className="mt-1 text-xs leading-5 text-neutral-600">{card.note}</div>
        </div>
      ))}
    </section>
  );
}
