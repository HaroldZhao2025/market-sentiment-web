'use client';
import Link from 'next/link';
export default function TickerCard({ t, s, pred }:{ t:string, s:number, pred:number }){
  const tone = s>0 ? 'text-emerald-600' : s<0 ? 'text-rose-600' : 'text-gray-600';
  return (
    <Link href={`/ticker/${t}`} className="card hover:shadow-md transition">
      <div className="text-sm text-gray-500">{t}</div>
      <div className={`kpi ${tone}`}>{(pred*100).toFixed(2)}%</div>
      <div className="kpi-sub">Predicted Return</div>
    </Link>
  );
}
