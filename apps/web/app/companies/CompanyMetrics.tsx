export default function CompanyMetrics({ total, sp500, midcap, smallcap }: { total: number; sp500: number; midcap: number; smallcap: number }) {
  const ready = total > 0;
  const cards = [
    { label: "Composite coverage", value: ready ? total.toLocaleString() : "Pending", note: "Large, mid and small-cap companies" },
    { label: "S&P 500", value: ready ? sp500.toLocaleString() : "Pending", note: "Core index universe" },
    { label: "MidCap 400", value: ready ? midcap.toLocaleString() : "Pending", note: "Mid-cap extension" },
    { label: "SmallCap 600", value: ready ? smallcap.toLocaleString() : "Pending", note: "New Phase 7 coverage" },
  ];
  return (
    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
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
