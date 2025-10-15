'use client';
import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import TickerCard from '../components/TickerCard';

export default function Home(){
  const [data,setData] = useState<any[]>([]);
  const [q,setQ] = useState('');
  useEffect(()=>{ fetch('/data/index.json').then(r=>r.json()).then(setData).catch(()=>setData([])); },[]);
  const filtered = useMemo(()=> data.filter(d => d.ticker.includes(q.toUpperCase())), [data,q]);
  const top = filtered.slice(0,30).sort((a,b)=>b.predicted_return-a.predicted_return).slice(0,6);
  const bottom = filtered.slice(0,30).sort((a,b)=>a.predicted_return-b.predicted_return).slice(0,6);
  return (
    <div className="space-y-6">
      <div className="card flex items-center gap-3">
        <input className="border rounded-xl px-3 py-2 w-64" placeholder="Search ticker (e.g. MSFT)" value={q} onChange={e=>setQ(e.target.value)} />
        <Link href={`/ticker/${(q||'MSFT').toUpperCase()}`} className="btn-primary">Go</Link>
        <Link href="/portfolio" className="btn">Portfolio</Link>
      </div>

      <div className="grid md:grid-cols-2 gap-6">
        <div className="card">
          <div className="font-semibold mb-2">Top Predicted</div>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">{top.map((r,i)=>(<TickerCard key={i} t={r.ticker} s={r.S} pred={r.predicted_return}/>))}</div>
        </div>
        <div className="card">
          <div className="font-semibold mb-2">Bottom Predicted</div>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">{bottom.map((r,i)=>(<TickerCard key={i} t={r.ticker} s={r.S} pred={r.predicted_return}/>))}</div>
        </div>
      </div>
    </div>
  );
}
