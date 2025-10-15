'use client';
import { useEffect, useState } from 'react';
import { Line } from 'react-chartjs-2';
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend } from 'chart.js';
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend);

export default function Portfolio(){
  const [p,setP] = useState<any|null>(null);
  useEffect(()=>{ fetch('/data/portfolio.json').then(r=>r.json()).then(setP).catch(()=>setP(null)); },[]);
  if(!p) return <div className="card">Portfolio will appear after first full run.</div>;
  const data={ labels:p.date, datasets:[{ label:'Equity (Net)', data:p.equity, yAxisID:'y' }] };
  const options:any={ responsive:true, scales:{ y:{ type:'linear', position:'left'}}};
  return (<div className="space-y-6"><div className="card"><h2 className="text-xl font-semibold">Sentiment Long/Short Portfolio</h2></div><div className="card"><Line data={data} options={options}/></div></div>);
}
